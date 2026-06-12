package dbos

import (
	"context"
	"fmt"
	"time"
)

const _DEBOUNCER_TOPIC = "_dbos_debouncer_topic"

type debouncerInput[P any] struct {
	InitialInput                  P
	TargetWorkflowFQNOrCustomName string
	TargetWorkflowId              string
	Delay                         time.Duration
	Timeout                       time.Duration
	WorkflowOptions               workflowOptions
}

type DebounceMessage[P any] struct {
	Input P
	Delay time.Duration
	Id    string
}

func internalDebouncerWF[P any, R any](ctx DbosContext, input debouncerInput[P]) (R, error) {
	var zero R

	dbosCtx, ok := ctx.(*dbosContext)
	if !ok {
		return zero, nil
	}

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

	targetWorkflowName := input.TargetWorkflowFQNOrCustomName
	if name, ok := dbosCtx.workflowRegistry.ResolveName(targetWorkflowName); ok {
		targetWorkflowName = name
	}

	registeredWorkflow, exists := dbosCtx.workflowRegistry.Load(targetWorkflowName)
	if !exists {
		return zero, fmt.Errorf("target workflow %s not found in registry", input.TargetWorkflowFQNOrCustomName)
	}

	workflowOpts := []WorkflowOption{}
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

	// Which doesn't do any pre-encoding of the input, and calls a type-erased function that expects an encoded input
	// So we need to serialize the input here
	workflowOpts = append(workflowOpts, withAlreadyEncodedInput())
	ser := resolveEncoder(ctx)
	encodedInput, err := ser.Encode(currentInput)
	if err != nil {
		return zero, fmt.Errorf("failed to serialize input: %w", err)
	}

	_, err = registeredWorkflow.wrappedFunction(ctx, encodedInput, ser.Name(), workflowOpts...)
	if err != nil {
		return zero, fmt.Errorf("failed to run target workflow: %w", err)
	}

	return zero, nil
}
