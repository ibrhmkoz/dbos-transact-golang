package dbos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
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

// debounceWorkflow is the debounce path of the callable returned by NewWorkflow. It delays
// execution, collapsing rapid repeated calls under the same key into a single run. Each call
// pushes the start time back by delay, capped at timeout from the first call (0 = no cap).
func debounceWorkflow[P any, R any](ctx DBOSContext, targetWorkflowName, internalDebouncerFQN string, timeout, delay time.Duration, key string, input P, opts ...WorkflowOption) (*WorkflowHandle[R], error) {
	workflowState, ok := ctx.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil

	// Resolve workflow ID.
	options := workflowOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	if options.WorkflowID == "" {
		if isWithinWorkflow {
			workflowID, err := Run(ctx, func(ctx context.Context) (string, error) {
				return uuid.New().String(), nil
			}, WithStepName("DBOS.debounce.assignWorkflowID"))
			if err != nil {
				return nil, err
			}
			options.WorkflowID = workflowID
		} else {
			options.WorkflowID = uuid.New().String()
		}
		opts = append(opts, WithWorkflowID(options.WorkflowID))
	}

	// Generate a message ID if communicating with an existing internal debouncing workflow.
	var messageID string
	if isWithinWorkflow {
		msgID, err := Run(ctx, func(ctx context.Context) (string, error) {
			return uuid.New().String(), nil
		}, WithStepName("DBOS.debounce.assignMessageID"))
		if err != nil {
			return nil, err
		}
		messageID = msgID
	} else {
		messageID = uuid.New().String()
	}

	dInput := debouncerInput[P]{
		InitialInput:                  input,
		TargetWorkflowFQNOrCustomName: targetWorkflowName,
		TargetWorkflowID:              options.WorkflowID,
		Delay:                         delay,
		Timeout:                       timeout,
		WorkflowOptions:               options,
	}

	// Type-erased wrapper so we can start the internal debouncer via the engine without pre-encoding.
	internalWF := WorkflowFunc(func(ctx DBOSContext, in any) (any, error) {
		return internalDebouncerWF[P, R](ctx, in.(debouncerInput[P]))
	})

	var internalWorkflowID string
	if isWithinWorkflow {
		var err error
		internalWorkflowID, err = Run(ctx, func(ctx context.Context) (string, error) {
			return uuid.New().String(), nil
		}, WithStepName("DBOS.debounce.assignInternalWorkflowID"))
		if err != nil {
			return nil, err
		}
	} else {
		internalWorkflowID = uuid.New().String()
	}

	for {
		handle, err := ctx.RunWorkflow(internalWF, dInput, WithWorkflowID(internalWorkflowID), WithDeduplicationID(key), withWorkflowName(internalDebouncerFQN))
		if err != nil {
			return nil, err
		}
		if handle.GetWorkflowID() == internalWorkflowID {
			return newWorkflowHandle[R](ctx, dInput.TargetWorkflowID), nil
		}

		debouncerWorkflowStatus, err := ListWorkflows(ctx, WithWorkflowIDs([]string{handle.GetWorkflowID()}), WithLoadInput(true))
		if err != nil {
			return nil, err
		}
		if len(debouncerWorkflowStatus) == 0 {
			continue
		}
		debouncerWorkflowID := handle.GetWorkflowID()

		encodedInput, ok := debouncerWorkflowStatus[0].Input.(string)
		if !ok {
			return nil, fmt.Errorf("internal debouncer workflow input is not encoded")
		}
		var decodedInput debouncerInput[P]
		if err := json.Unmarshal([]byte(encodedInput), &decodedInput); err != nil {
			return nil, fmt.Errorf("failed to unmarshal debouncer workflow input: %w", err)
		}

		switch debouncerWorkflowStatus[0].Status {
		case WorkflowStatusSuccess, WorkflowStatusError, WorkflowStatusCancelled, WorkflowStatusMaxRecoveryAttemptsExceeded:
			return newWorkflowHandle[R](ctx, decodedInput.TargetWorkflowID), nil
		}

		err = Send(ctx, debouncerWorkflowID, DebounceMessage[P]{
			Input: input,
			Delay: delay,
			ID:    messageID,
		}, _DEBOUNCER_TOPIC)
		if err != nil {
			return nil, err
		}

		_, err = GetEvent[bool](ctx, debouncerWorkflowID, messageID, 2*time.Second) // XXX unclear what's a good timeout here.
		if errors.Is(err, &DBOSError{Code: TimeoutError}) {
			continue
		} else if err != nil {
			return nil, err
		}

		return newWorkflowHandle[R](ctx, decodedInput.TargetWorkflowID), nil
	}
}

// DebouncerOption configures a DebouncerClient.
type DebouncerOption func(*time.Duration)

// WithDebouncerTimeout sets the maximum time before starting the workflow.
// If timeout is zero (the default), there is no maximum time limit.
func WithDebouncerTimeout(timeout time.Duration) DebouncerOption {
	return func(t *time.Duration) {
		*t = timeout
	}
}

// DebouncerAdmin is the subset of control-plane operations required by DebouncerClient.
type DebouncerAdmin interface {
	Enqueue(queueName, workflowName string, input any, opts ...EnqueueOption) (*WorkflowHandle[any], error)
	ListWorkflows(opts ...ListWorkflowsOption) ([]WorkflowStatus, error)
	Send(destinationID string, message any, topic string, opts ...SendOption) error
	GetEvent(targetWorkflowID, key string, timeout time.Duration) (any, error)
	RetrieveWorkflow(workflowID string) (*WorkflowHandle[any], error)
}

// DebouncerClient provides workflow debouncing functionality using the narrow
// subset of DBOSAdmin operations required by debouncing.
type DebouncerClient[P any, R any] struct {
	WorkflowName         string // Name of the target workflow
	admin                DebouncerAdmin
	Timeout              time.Duration // Maximum time before starting the workflow (0 = no timeout)
	internalDebouncerFQN string        // Fully qualified name of the internal debouncer workflow
}

// NewDebouncerClient creates a new debouncer client for the specified workflow.
//
// Parameters:
//   - workflowName: The name of the workflow to debounce
//   - dbosAdmin: The DBOS admin to use for operations
//   - opts: Optional functional options for configuring the debouncer:
//   - WithDebouncerTimeout: Maximum time before starting the workflow (0 = no timeout) [optional]
//
// Returns a pointer to a DebouncerClient instance that can be used to call Debounce.
func NewDebouncerClient[P any, R any](
	workflowName string,
	admin DebouncerAdmin,
	opts ...DebouncerOption,
) *DebouncerClient[P, R] {
	timeout := time.Duration(0) // Default: no timeout
	for _, opt := range opts {
		opt(&timeout)
	}

	return &DebouncerClient[P, R]{
		WorkflowName: workflowName,
		admin:        admin,
		Timeout:      timeout,
		// Use the any,any internal debouncer workflow FQN because that's all the server knows
		internalDebouncerFQN: resolveWorkflowFunctionName(internalDebouncerWF[any, any]),
	}
}

// Debounce delays workflow execution by a configurable delay amount, with each
// subsequent call pushing back the start time by the delay (up to an optional maximum timeout).
//
// Unlike Debouncer.Debounce, this method never checks if we're within a workflow
// and never attempts to run operations as steps. It uses the DBOSAdmin's Enqueue,
// Send, ListWorkflows, and GetEvent methods.
//
// Parameters:
//   - key: A unique key to group debounce calls (calls with the same key are debounced together)
//   - delay: Time by which to delay workflow execution
//   - input: Input parameters to pass to the workflow
//   - opts: Optional workflow options (e.g., WithWorkflowID, WithQueue, etc.)
//
// Returns a WorkflowHandle that can be used to check status and retrieve results.
func (dc *DebouncerClient[P, R]) Debounce(key string, delay time.Duration, input P, opts ...WorkflowOption) (*WorkflowHandle[R], error) {
	// Resolve workflow options
	options := workflowOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	// Generate workflow ID if not provided
	if options.WorkflowID == "" {
		options.WorkflowID = uuid.New().String()
	}

	// Generate message ID for ACK protocol
	messageID := uuid.New().String()

	// Create debouncer input
	dInput := debouncerInput[P]{
		InitialInput:                  input,
		TargetWorkflowFQNOrCustomName: dc.WorkflowName,
		TargetWorkflowID:              options.WorkflowID,
		Delay:                         delay,
		Timeout:                       dc.Timeout,
		WorkflowOptions:               options,
	}

	internalWorkflowID := uuid.New().String()
	for {
		handle, err := dc.admin.Enqueue(_DBOS_INTERNAL_QUEUE_NAME, dc.internalDebouncerFQN, dInput,
			WithEnqueueWorkflowID(internalWorkflowID), WithEnqueueDeduplicationID(key))
		if err != nil {
			return nil, err
		}
		if handle.GetWorkflowID() == internalWorkflowID {
			return retrieveTypedWorkflow[R](dc.admin, dInput.TargetWorkflowID)
		}

		debouncerWorkflowStatus, err := dc.admin.ListWorkflows(WithWorkflowIDs([]string{handle.GetWorkflowID()}), WithLoadInput(true))
		if err != nil {
			return nil, err
		}
		if len(debouncerWorkflowStatus) == 0 {
			continue
		}
		debouncerWorkflowID := handle.GetWorkflowID()

		err = dc.admin.Send(debouncerWorkflowID, DebounceMessage[P]{
			Input: input,
			Delay: delay,
			ID:    messageID,
		}, _DEBOUNCER_TOPIC)
		if err != nil {
			return nil, err
		}

		_, err = dc.admin.GetEvent(debouncerWorkflowID, messageID, 2*time.Second)
		if errors.Is(err, &DBOSError{Code: TimeoutError}) {
			continue
		} else if err != nil {
			return nil, err
		}

		encodedInputStr, ok := debouncerWorkflowStatus[0].Input.(string)
		if !ok {
			return nil, fmt.Errorf("internal debouncer workflow input is not encoded")
		}
		var decodedInput debouncerInput[P]
		if err := json.Unmarshal([]byte(encodedInputStr), &decodedInput); err != nil {
			return nil, fmt.Errorf("failed to unmarshal debouncer workflow input: %w", err)
		}
		return retrieveTypedWorkflow[R](dc.admin, decodedInput.TargetWorkflowID)
	}
}

func retrieveTypedWorkflow[R any](admin DebouncerAdmin, workflowID string) (*WorkflowHandle[R], error) {
	handle, err := admin.RetrieveWorkflow(workflowID)
	if err != nil {
		return nil, err
	}
	return &WorkflowHandle[R]{workflowHandle: handle.workflowHandle}, nil
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
		workflowOpts = append(workflowOpts, WithWorkflowID(input.WorkflowOptions.WorkflowID))
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
