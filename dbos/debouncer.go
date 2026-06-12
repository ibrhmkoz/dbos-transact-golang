package dbos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	_DEBOUNCER_TOPIC       = "_dbos_debouncer_topic"
	_DEBOUNCER_NAME_SUFFIX = ".debouncer"
)

type debouncerInput[P any] struct {
	InitialInput     P
	TargetWorkflowId string
	Delay            time.Duration
	Timeout          time.Duration
	WorkflowOptions  workflowOptions
}

type DebounceMessage[P any] struct {
	Input P
	Delay time.Duration
	Id    string
}

// debounceWindow returns the workflow function implementing one debounce window for target:
// it absorbs new inputs until the window closes, then calls target as a child workflow.
// target is captured by the closure; only the window's registered name and its input are durable.
func debounceWindow[P any, R any](target Workflow[P, R]) WorkflowFn[debouncerInput[P], R] {
	return func(ctx Context, input debouncerInput[P]) (R, error) {
		var zero R

		startTime, err := Run(ctx, func(ctx context.Context) (time.Time, error) {
			return time.Now(), nil
		}, WithStepName("DBOS.debounce.startTime"))
		if err != nil {
			return zero, err
		}
		currentInput := input.InitialInput
		delay := input.Delay
		timeout := input.Timeout
		maxStartTime := startTime.Add(timeout)

		targetStartTime := startTime.Add(delay)

		// If timeout is set, ensure target start time doesn't exceed startTime + timeout
		if timeout > 0 {
			if targetStartTime.After(maxStartTime) {
				targetStartTime = maxStartTime
			}
		}

		for {
			var now time.Time
			now, err = Run(ctx, func(ctx context.Context) (time.Time, error) {
				return time.Now(), nil
			}, WithStepName("DBOS.debounce.loopTime"))
			if err != nil {
				return zero, err
			}
			remainingTime := targetStartTime.Sub(now)

			if remainingTime <= 0 {
				break
			}

			msg, err := Recv[DebounceMessage[P]](ctx, _DEBOUNCER_TOPIC, remainingTime)
			if err != nil {

				break
			}

			currentInput = msg.Input

			newTargetStartTime := now.Add(msg.Delay)

			if timeout > 0 {
				if newTargetStartTime.After(maxStartTime) {
					newTargetStartTime = maxStartTime
				}
			}

			targetStartTime = newTargetStartTime

			if msg.Id != "" {
				err = SetEvent(ctx, msg.Id, true)
				if err != nil {
					ctx.(*dbosContext).logger.Error("failed to ACK debounce message", "error", err)
				}
			}
		}

		var workflowOpts []WorkflowOption
		if input.WorkflowOptions.WorkflowId != "" {
			workflowOpts = append(workflowOpts, withWorkflowId(input.WorkflowOptions.WorkflowId))
		}
		if input.WorkflowOptions.ApplicationVersion != "" {
			workflowOpts = append(workflowOpts, WithApplicationVersion(input.WorkflowOptions.ApplicationVersion))
		}
		if input.WorkflowOptions.DeduplicationId != "" {
			workflowOpts = append(workflowOpts, WithDeduplicationId(input.WorkflowOptions.DeduplicationId))
		}
		if input.WorkflowOptions.Priority > 0 {
			workflowOpts = append(workflowOpts, WithPriority(input.WorkflowOptions.Priority))
		}
		if input.WorkflowOptions.AuthenticatedUser != "" {
			workflowOpts = append(workflowOpts, WithAuthenticatedUser(input.WorkflowOptions.AuthenticatedUser))
		}
		if input.WorkflowOptions.AssumedRole != "" {
			workflowOpts = append(workflowOpts, WithAssumedRole(input.WorkflowOptions.AssumedRole))
		}
		if len(input.WorkflowOptions.AuthenticatedRoles) > 0 {
			workflowOpts = append(workflowOpts, WithAuthenticatedRoles(input.WorkflowOptions.AuthenticatedRoles))
		}
		if input.WorkflowOptions.QueuePartitionKey != "" {
			workflowOpts = append(workflowOpts, WithQueuePartitionKey(input.WorkflowOptions.QueuePartitionKey))
		}

		_, err = target(ctx, currentInput, workflowOpts...)
		if err != nil {
			return zero, fmt.Errorf("failed to run target workflow: %w", err)
		}

		return zero, nil
	}
}

// startDebounced is the caller-side rendezvous with the debounce window keyed by the
// deduplication id: start the window, or if one is already running for the key, push the
// new input to it. Returns a handle to the target workflow, whose ID is fixed up front.
func startDebounced[P any, R any](ctx Context, window Workflow[debouncerInput[P], R], input P, options workflowOptions) (*WorkflowHandle[R], error) {
	delay := options.debounceDelay
	timeout := options.debounceTimeout
	key := options.debounceKey

	targetWorkflowId, err := Uuid(ctx)
	if err != nil {
		return nil, err
	}
	options.WorkflowId = targetWorkflowId

	messageId, err := Uuid(ctx)
	if err != nil {
		return nil, err
	}

	dInput := debouncerInput[P]{
		InitialInput:     input,
		TargetWorkflowId: targetWorkflowId,
		Delay:            delay,
		Timeout:          timeout,
		WorkflowOptions:  options,
	}

	for {
		handle, err := window(ctx, dInput, WithDeduplicationId(key))
		if err != nil {
			return nil, err
		}

		windowStatus, err := ListWorkflows(ctx, WithWorkflowIds([]string{handle.GetWorkflowId()}), WithLoadInput(true))
		if err != nil {
			return nil, err
		}
		if len(windowStatus) == 0 {
			continue
		}
		windowWorkflowId := handle.GetWorkflowId()

		encodedInput, ok := windowStatus[0].Input.(string)
		if !ok {
			return nil, fmt.Errorf("debounce window workflow input is not encoded")
		}
		var decodedInput debouncerInput[P]
		if err := json.Unmarshal([]byte(encodedInput), &decodedInput); err != nil {
			return nil, fmt.Errorf("failed to unmarshal debounce window workflow input: %w", err)
		}

		if decodedInput.TargetWorkflowId == dInput.TargetWorkflowId {
			return newWorkflowHandle[R](ctx, dInput.TargetWorkflowId), nil
		}

		switch windowStatus[0].Status {
		case WorkflowStatusSuccess, WorkflowStatusError, WorkflowStatusCancelled, WorkflowStatusMaxRecoveryAttemptsExceeded:
			return newWorkflowHandle[R](ctx, decodedInput.TargetWorkflowId), nil
		}

		err = Send(ctx, windowWorkflowId, DebounceMessage[P]{
			Input: input,
			Delay: delay,
			Id:    messageId,
		}, _DEBOUNCER_TOPIC)
		if err != nil {
			return nil, err
		}

		_, err = GetEvent[bool](ctx, windowWorkflowId, messageId, 2*time.Second)
		if errors.Is(err, &DbosError{Code: TimeoutError}) {
			continue
		} else if err != nil {
			return nil, err
		}

		return newWorkflowHandle[R](ctx, decodedInput.TargetWorkflowId), nil
	}
}
