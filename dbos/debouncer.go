package dbos

import (
	"context"
	"fmt"
	"time"
)

const _DEBOUNCER_TOPIC = "_dbos_debouncer_topic"

// debouncerInput is the input to the internal debouncer workflow
type debouncerInput[P any] struct {
	InitialInput                  P
	TargetWorkflowFQNOrCustomName string
	TargetWorkflowID              string
	Delay                         time.Duration   // Time by which to delay workflow execution
	Timeout                       time.Duration   // Maximum time before starting the workflow
	WorkflowOptions               workflowOptions // Options to pass to target workflow (serializable)
}

// DebounceMessage is sent to the debouncer workflow to update inputs
type DebounceMessage[P any] struct {
	Input P
	Delay time.Duration
	ID    string // Used for ACK protocol
}

// internalDebouncerWF is the internal workflow that implements debouncing logic.
// It collects inputs, delays execution, and runs the target workflow with the latest input.
func internalDebouncerWF[P any, R any](ctx DBOSContext, input debouncerInput[P]) (R, error) {
	var zero R

	dbosCtx, ok := ctx.(*dbosContext)
	if !ok { // do nothing if the context is not a dbosContext
		return zero, nil
	}

	// Track the first creation time and current input
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

	// Calculate initial target start time: startTime + delay
	targetStartTime := startTime.Add(delay)

	// If timeout is set, ensure target start time doesn't exceed startTime + timeout
	if timeout > 0 {
		if targetStartTime.After(maxStartTime) {
			targetStartTime = maxStartTime
		}
	}

	// Loop until we reach the target start time
	for {
		var now time.Time
		now, err = Run(ctx, func(ctx context.Context) (time.Time, error) {
			return time.Now(), nil
		}, WithStepName("DBOS.debounce.loopTime"))
		if err != nil {
			return zero, err
		}
		remainingTime := targetStartTime.Sub(now)
		// If we've reached or passed the target start time, break and execute
		if remainingTime <= 0 {
			break
		}

		// Try to receive a new input message with the remaining time as timeout
		msg, err := Recv[DebounceMessage[P]](ctx, _DEBOUNCER_TOPIC, remainingTime)
		if err != nil {
			// Timeout or error - break and execute with current input
			break
		}

		// Update the current input with the new message
		currentInput = msg.Input

		// Calculate new target start time: now + delay
		newTargetStartTime := now.Add(msg.Delay)

		// If timeout is set, cap the new target start time
		if timeout > 0 {
			if newTargetStartTime.After(maxStartTime) {
				newTargetStartTime = maxStartTime
			}
		}

		targetStartTime = newTargetStartTime

		// ACK the message by setting an event with the message ID
		if msg.ID != "" {
			err = SetEvent(ctx, msg.ID, true)
			if err != nil {
				ctx.(*dbosContext).logger.Error("failed to ACK debounce message", "error", err)
			}
		}
	}

	// Now execute the target workflow with the latest input
	targetWorkflowName := input.TargetWorkflowFQNOrCustomName
	if name, ok := dbosCtx.workflowRegistry.ResolveName(targetWorkflowName); ok {
		targetWorkflowName = name
	}

	registeredWorkflow, exists := dbosCtx.workflowRegistry.Load(targetWorkflowName)
	if !exists {
		return zero, fmt.Errorf("target workflow %s not found in registry", input.TargetWorkflowFQNOrCustomName)
	}

	// Reconstruct WorkflowOptions from serializable format
	workflowOpts := []WorkflowOption{}
	if input.WorkflowOptions.WorkflowID != "" {
		workflowOpts = append(workflowOpts, withWorkflowID(input.WorkflowOptions.WorkflowID))
	}
	if input.WorkflowOptions.ApplicationVersion != "" {
		workflowOpts = append(workflowOpts, WithApplicationVersion(input.WorkflowOptions.ApplicationVersion))
	}
	if input.WorkflowOptions.DeduplicationID != "" {
		workflowOpts = append(workflowOpts, WithDeduplicationID(input.WorkflowOptions.DeduplicationID))
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

	// We use the wrapped, type-erased workflow wrapper from the workflow registry that calls ctx.RunWorkflow
	// Which doesn't do any pre-encoding of the input, and calls a type-erased function that expects an encoded input
	// So we need to serialize the input here
	workflowOpts = append(workflowOpts, withAlreadyEncodedInput())
	ser := resolveEncoder(ctx)
	encodedInput, err := ser.Encode(currentInput)
	if err != nil {
		return zero, fmt.Errorf("failed to serialize input: %w", err)
	}

	// Call the target workflow using its wrapped function
	_, err = registeredWorkflow.wrappedFunction(ctx, encodedInput, ser.Name(), workflowOpts...)
	if err != nil {
		return zero, fmt.Errorf("failed to run target workflow: %w", err)
	}

	return zero, nil
}
