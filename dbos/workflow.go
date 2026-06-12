package dbos

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

/*******************************/
/******* WORKFLOW STATUS *******/
/*******************************/

// WorkflowStatusType represents the current execution state of a workflow.
type WorkflowStatusType string

const (
	WorkflowStatusPending                     WorkflowStatusType = "PENDING"                        // WorkflowFn is running or ready to run
	WorkflowStatusEnqueued                    WorkflowStatusType = "ENQUEUED"                       // WorkflowFn is queued and waiting for execution
	WorkflowStatusDelayed                     WorkflowStatusType = "DELAYED"                        // WorkflowFn is delayed and will transition to ENQUEUED after the delay expires
	WorkflowStatusSuccess                     WorkflowStatusType = "SUCCESS"                        // WorkflowFn completed successfully
	WorkflowStatusError                       WorkflowStatusType = "ERROR"                          // WorkflowFn completed with an error
	WorkflowStatusCancelled                   WorkflowStatusType = "CANCELLED"                      // WorkflowFn was cancelled (manually or due to timeout)
	WorkflowStatusMaxRecoveryAttemptsExceeded WorkflowStatusType = "MAX_RECOVERY_ATTEMPTS_EXCEEDED" // WorkflowFn exceeded maximum retry attempts
)

// WorkflowStatus contains comprehensive information about a workflow's current state and execution history.
type WorkflowStatus struct {
	ID                 string             `json:"workflow_uuid"`                 // Unique identifier for the workflow
	Status             WorkflowStatusType `json:"status"`                        // Current execution status
	Name               string             `json:"name"`                          // Function name of the workflow
	AuthenticatedUser  string             `json:"authenticated_user,omitempty"`  // User who initiated the workflow (if applicable)
	AssumedRole        string             `json:"assumed_role,omitempty"`        // Role assumed during execution (if applicable)
	AuthenticatedRoles []string           `json:"authenticated_roles,omitempty"` // Roles available to the user (if applicable)
	Output             any                `json:"output,omitempty"`              // WorkflowFn output (available after completion)
	Error              error              `json:"error,omitempty"`               // Error information (if status is ERROR)
	ExecutorID         string             `json:"executor_id"`                   // ID of the executor running this workflow
	CreatedAt          time.Time          `json:"created_at"`                    // When the workflow was created
	UpdatedAt          time.Time          `json:"updated_at"`                    // When the workflow status was last updated
	ApplicationVersion string             `json:"application_version"`           // Version of the application that created this workflow
	ApplicationID      string             `json:"application_id,omitempty"`      // Application identifier
	Attempts           int                `json:"attempts"`                      // Number of execution attempts
	QueueName          string             `json:"queue_name,omitempty"`          // Queue name (if workflow was enqueued)
	Timeout            time.Duration      `json:"timeout,omitempty"`             // WorkflowFn timeout duration
	Deadline           time.Time          `json:"deadline"`                      // Absolute deadline for workflow completion
	StartedAt          time.Time          `json:"started_at"`                    // When the workflow execution actually started
	DeduplicationID    string             `json:"deduplication_id,omitempty"`    // Queue deduplication identifier
	Input              any                `json:"input,omitempty"`               // Input parameters passed to the workflow
	Priority           int                `json:"priority,omitempty"`            // Queue execution priority (lower numbers have higher priority)
	QueuePartitionKey  string             `json:"queue_partition_key,omitempty"` // Queue partition key for partitioned queues
	ForkedFrom         string             `json:"forked_from,omitempty"`         // ID of the original workflow if this is a fork
	WasForkedFrom      bool               `json:"was_forked_from,omitempty"`     // Whether this workflow has been forked from
	ParentWorkflowID   string             `json:"parent_workflow_id,omitempty"`  // ID of the parent workflow if this is a child
	CompletedAt        time.Time          `json:"completed_at,omitempty"`        // When the workflow reached a terminal state (SUCCESS, ERROR, or CANCELLED)
	ClassName          string             `json:"class_name,omitempty"`          // Class/namespace name for cross-language dispatch
	ConfigName         *string            `json:"config_name,omitempty"`         // Instance/config name for cross-language dispatch (nil = unset, pointer to "" = explicit empty)
	Serialization      string             `json:"serialization,omitempty"`       // Serialization format used for inputs/outputs (e.g., "DBOS_JSON", "portable_json")
	DelayUntil         time.Time          `json:"delay_until,omitempty"`         // The time before which the workflow should not be dequeued
}

// workflowState holds the runtime state for a workflow execution
type workflowState struct {
	workflowID         string
	stepID             int
	isWithinStep       bool
	isPortableWorkflow bool
	// auth identity carried so child workflows can inherit it automatically
	authenticatedUser  string
	assumedRole        string
	authenticatedRoles []string
}

// nextStepID returns the next step ID and increments the counter
func (ws *workflowState) nextStepID() int {
	ws.stepID++
	return ws.stepID
}

/********************************/
/******* WORKFLOW HANDLES ********/
/********************************/

type stepCheckpointedOutcome struct {
	value         any    // The encoded value (should be a *string)
	serialization string // DB-stored serialization format
}

type workflowHandle struct {
	workflowID  string
	dbosContext DBOSContext
}

// GetResultOption is a functional option for configuring GetResult behavior.
type GetResultOption func(*getResultOptions)

// getResultOptions holds the configuration for GetResult execution.
type getResultOptions struct {
	timeout      time.Duration
	pollInterval time.Duration
}

func defaultGetResultOptions() *getResultOptions {
	return &getResultOptions{pollInterval: _DB_RETRY_INTERVAL}
}

// WithHandleTimeout sets a timeout for the GetResult operation.
// If the timeout is reached before the workflow completes, GetResult will return a timeout error.
func WithHandleTimeout(timeout time.Duration) GetResultOption {
	return func(opts *getResultOptions) {
		opts.timeout = timeout
	}
}

// WithHandlePollingInterval sets the polling interval for awaiting workflow completion in GetResult.
// If a non-positive interval is provided, the default interval is used.
func WithHandlePollingInterval(interval time.Duration) GetResultOption {
	return func(opts *getResultOptions) {
		if interval > 0 {
			opts.pollInterval = interval
		}
	}
}

// GetStatus returns the current status of the workflow from the database
// If the DBOSContext is running in client mode, do not load input and outputs
func (h *workflowHandle) GetStatus() (WorkflowStatus, error) {
	loadInput := false
	loadOutput := false
	if h.dbosContext.(*dbosContext).launched.Load() {
		loadInput = false
		loadOutput = false
	}
	c := h.dbosContext.(*dbosContext)
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var workflowStatuses []WorkflowStatus
	var err error
	if isWithinWorkflow {
		workflowStatuses, err = Run(c, func(ctx context.Context) ([]WorkflowStatus, error) {
			return retryWithResult(ctx, func() ([]WorkflowStatus, error) {
				return c.kernel.listWorkflows(ctx, listWorkflowsDBInput{
					workflowIDs: []string{h.workflowID},
					loadInput:   loadInput,
					loadOutput:  loadOutput,
				})
			}, withRetrierLogger(c.logger))
		}, WithStepName("DBOS.getStatus"))
	} else {
		workflowStatuses, err = retryWithResult(c, func() ([]WorkflowStatus, error) {
			return c.kernel.listWorkflows(c, listWorkflowsDBInput{
				workflowIDs: []string{h.workflowID},
				loadInput:   loadInput,
				loadOutput:  loadOutput,
			})
		})
	}
	if err != nil {
		return WorkflowStatus{}, fmt.Errorf("failed to get workflow status: %w", err)
	}
	if len(workflowStatuses) == 0 {
		return WorkflowStatus{}, newNonExistentWorkflowError(h.workflowID)
	}
	return workflowStatuses[0], nil
}

func (h *workflowHandle) GetWorkflowID() string {
	return h.workflowID
}

func newWorkflowHandle[R any](ctx DBOSContext, workflowID string) *WorkflowHandle[R] {
	return &WorkflowHandle[R]{
		workflowHandle: workflowHandle{
			workflowID:  workflowID,
			dbosContext: ctx,
		},
	}
}

type WorkflowHandle[R any] struct {
	workflowHandle
}

func (h *WorkflowHandle[R]) GetResult(opts ...GetResultOption) (R, error) {
	options := defaultGetResultOptions()
	for _, opt := range opts {
		opt(options)
	}

	// Use timeout if specified, otherwise use DBOS context directly
	ctx := h.dbosContext
	var cancel context.CancelFunc
	if options.timeout > 0 {
		ctx, cancel = WithTimeout(h.dbosContext, options.timeout)
		defer cancel()
	}

	awaitResult, awaitErr := retryWithResult(ctx, func() (*awaitWorkflowResultOutput, error) {
		return h.dbosContext.(*dbosContext).kernel.awaitWorkflowResult(ctx, h.workflowID, options.pollInterval)
	}, withRetrierLogger(h.dbosContext.(*dbosContext).logger))

	// awaitErr is a real DB/network/cancellation error; the workflow's recorded error is in awaitResult.errStr
	if awaitErr != nil && options.timeout > 0 && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return *new(R), fmt.Errorf("workflow result timeout after %v: %w", options.timeout, context.DeadlineExceeded)
	}
	err := awaitErr
	if awaitResult != nil && awaitResult.errStr != nil {
		if awaitErr == nil {
			err = deserializeWorkflowError(awaitResult.errStr, awaitResult.errEncoded, awaitResult.serialization)
		} else if dbosErr, ok := awaitErr.(*DBOSError); ok && dbosErr.Code == AwaitedWorkflowCancelled {
			// Cancelled workflows deterministically yield AwaitedWorkflowCancelled.
			// When the workflow already recorded its outcome, attach the precise
			// cause (e.g. context.Canceled) as the wrapped error.
			dbosErr.wrappedErr = deserializeWorkflowError(awaitResult.errStr, awaitResult.errEncoded, awaitResult.serialization)
		}
	}

	// Deserialize the result directly into the target type
	var typedResult R
	var encodedStr *string
	var storedSerialization string
	if awaitResult != nil {
		encodedStr = awaitResult.output
		storedSerialization = awaitResult.serialization
	}
	if encodedStr != nil {
		var deserErr error
		decoder, deserErr := resolveDecoder[R](storedSerialization, h.dbosContext.(*dbosContext).serializer)
		if deserErr != nil {
			return *new(R), fmt.Errorf("failed to resolve decoder: %w", deserErr)
		}
		typedResult, deserErr = decoder.Decode(encodedStr)
		if deserErr != nil {
			return *new(R), fmt.Errorf("failed to deserialize workflow result: %w", deserErr)
		}

		return typedResult, err
	}
	return *new(R), err
}

/**********************************/
/******* WORKFLOW REGISTRY *******/
/**********************************/
func storeWorkflowRegistryEntry(ctx DBOSContext, workflowFQN string, fn wrappedWorkflowFunc, maxRetries int, customName string) {
	// Skip if we don't have a concrete dbosContext
	c, ok := ctx.(*dbosContext)
	if !ok {
		return
	}

	if c.launched.Load() {
		panic("Cannot register workflow after DBOS has launched")
	}

	// Check if workflow already exists and store atomically using LoadOrStore
	entry := WorkflowRegistryEntry{
		wrappedFunction: fn,
		FQN:             workflowFQN,
		MaxRetries:      maxRetries,
		Name:            customName,
		CronSchedule:    "",
		Retention:       _DEFAULT_WORKFLOW_RETENTION,
	}

	workflowName := workflowFQN
	if customName != "" {
		workflowName = customName
	}
	if _, exists := c.workflowRegistry.LoadOrStore(workflowName, entry); exists {
		c.logger.Error("workflow function already registered", "workflow_name", workflowName, "fqn", workflowFQN)
		panic(newConflictingRegistrationError(workflowName))
	}
}

func registerScheduledWorkflow(ctx DBOSContext, workflowFQN, customName string, fn WorkflowFunc, cronSchedule string) {
	// Skip if we don't have a concrete dbosContext
	c, ok := ctx.(*dbosContext)
	if !ok {
		return
	}

	if c.launched.Load() {
		panic("Cannot register scheduled workflow after DBOS has launched")
	}

	// Update the existing workflow entry with the cron schedule
	workflowName := workflowFQN
	if customName != "" {
		workflowName = customName
	}
	if !c.workflowRegistry.SetCronSchedule(workflowName, cronSchedule) {
		panic(fmt.Sprintf("workflow %s must be registered before scheduling", workflowFQN))
	}

	name := workflowName
	scheduled := ScheduledWorkflowFunc(func(ctx DBOSContext, input ScheduledWorkflowInput) (any, error) {
		scheduledTime := input.ScheduledTime
		wfID := fmt.Sprintf("sched-%s-%s", name, scheduledTime)
		// fn is the type-erased wrapper, which expects an encoded input.
		ser := resolveEncoder(ctx)
		encodedInput, err := ser.Encode(scheduledTime)
		if err != nil {
			return nil, fmt.Errorf("failed to encode scheduled workflow input: %w", err)
		}
		opts := []WorkflowOption{
			withWorkflowID(wfID),
			withWorkflowName(workflowFQN),
			withAlreadyEncodedInput(),
		}
		return ctx.RunWorkflow(fn, encodedInput, opts...)
	})

	if _, err := c.addScheduleCronEntry(name, cronSchedule, scheduled, nil); err != nil {
		panic(fmt.Sprintf("failed to register scheduled workflow: %v", err))
	}
	c.logger.Info("Registered scheduled workflow", "fqn", workflowFQN, "customName", customName, "cron_schedule", cronSchedule)
}

const (
	_DEFAULT_MAX_RECOVERY_ATTEMPTS = 100
	_DEFAULT_WORKFLOW_RETENTION    = 24 * time.Hour

	// Step retry defaults
	_DEFAULT_STEP_BASE_INTERVAL  = 100 * time.Millisecond
	_DEFAULT_STEP_MAX_INTERVAL   = 5 * time.Second
	_DEFAULT_STEP_BACKOFF_FACTOR = 2.0
)

// WithMaxRetries sets the maximum number of retry attempts for workflow recovery.
// If a workflow fails or is interrupted, it will be retried up to this many times.
// After exceeding max retries, the workflow status becomes MAX_RECOVERY_ATTEMPTS_EXCEEDED.
// Registration-time option.
func WithMaxRetries(maxRetries int) WorkflowOption {
	return func(p *workflowOptions) {
		p.MaxRetries = maxRetries
	}
}

// WithSchedule registers the workflow as a scheduled workflow using cron syntax.
// The schedule string follows standard cron format with second precision.
// Scheduled workflows automatically receive a time.Time input parameter.
// Registration-time option.
func WithSchedule(schedule string) WorkflowOption {
	return func(p *workflowOptions) {
		p.CronSchedule = schedule
	}
}

// WithWorkflowName sets a custom name for the workflow.
// Registration-time option.
func WithWorkflowName(name string) WorkflowOption {
	return func(p *workflowOptions) {
		p.WorkflowName = name
	}
}

// WithGlobalConcurrency caps the number of concurrently executing instances of the
// workflow across all workers.
// Registration-time option for NewWorkflow.
func WithGlobalConcurrency(concurrency int) WorkflowOption {
	return func(p *workflowOptions) {
		p.GlobalConcurrency = &concurrency
	}
}

// WithRateLimit limits how many instances of the workflow may start within the given period.
// Registration-time option for NewWorkflow.
func WithRateLimit(limit int, period time.Duration) WorkflowOption {
	return func(p *workflowOptions) {
		p.RateLimit = &rateLimiter{limit: limit, period: period}
	}
}

// WithWorkflowRetention sets how long completed workflow executions and their results are retained.
// Registration-time option for NewWorkflow.
func WithWorkflowRetention(retention time.Duration) WorkflowOption {
	return func(p *workflowOptions) {
		p.Retention = &retention
	}
}

// Workflow is a callable, self-contained, durable workflow.
type Workflow[P any, R any] func(ctx DBOSContext, input P, opts ...WorkflowOption) (*WorkflowHandle[R], error)

// NewWorkflow creates and registers a workflow.
// Calling the returned function starts a workflow execution.
// Execution behavior is configured with options such as WithGlobalConcurrency and WithRateLimit.
func NewWorkflow[P any, R any](ctx DBOSContext, fn WorkflowFn[P, R], opts ...WorkflowOption) Workflow[P, R] {
	c, ok := ctx.(*dbosContext)
	if !ok {
		panic("ctx must be a DBOS context")
	}

	params := workflowOptions{}
	for _, opt := range opts {
		opt(&params)
	}
	retention := _DEFAULT_WORKFLOW_RETENTION
	if params.Retention != nil {
		retention = *params.Retention
	}
	if retention <= 0 {
		panic("workflow retention must be greater than 0")
	}

	registerWorkflow(ctx, fn, opts...)

	name := resolveWorkflowFunctionName(fn)
	if params.WorkflowName != "" {
		name = params.WorkflowName
	} else if resolved, exists := c.workflowRegistry.ResolveName(name); exists {
		name = resolved
	}
	if !c.workflowRegistry.SetExecutionPolicies(name, params.GlobalConcurrency, params.RateLimit, retention) {
		panic(fmt.Sprintf("workflow %s must be registered before assigning execution policies", name))
	}

	// Register the internal debouncer workflow for this P,R so the returned callable can debounce.
	internalDebouncerFQN := resolveWorkflowFunctionName(internalDebouncerWF[P, R])
	if _, exists := c.workflowRegistry.ResolveName(internalDebouncerFQN); !exists {
		registerWorkflow(ctx, internalDebouncerWF[P, R])
	}

	return func(ctx DBOSContext, input P, workflowOpts ...WorkflowOption) (*WorkflowHandle[R], error) {
		if ctx == nil {
			return nil, fmt.Errorf("ctx cannot be nil")
		}

		callParams := workflowOptions{}
		for _, opt := range workflowOpts {
			opt(&callParams)
		}

		// Debounce path: delay execution, collapsing rapid repeated calls under the same key.
		// Each call pushes the start time back by delay, capped at timeout from the first call (0 = no cap).
		if callParams.debounce {
			timeout := callParams.debounceTimeout
			delay := callParams.debounceDelay
			key := callParams.debounceKey

			// The target workflow ID is always internally generated (callers group calls
			// via the deduplication key, not WorkflowID). UUID is durable: recorded as a
			// step within a workflow, returned identically on replay.
			options := workflowOptions{}
			for _, opt := range workflowOpts {
				opt(&options)
			}
			targetWorkflowID, err := UUID(ctx)
			if err != nil {
				return nil, err
			}
			options.WorkflowID = targetWorkflowID

			// Message ID for communicating with an existing internal debouncing workflow.
			messageID, err := UUID(ctx)
			if err != nil {
				return nil, err
			}

			dInput := debouncerInput[P]{
				InitialInput:                  input,
				TargetWorkflowFQNOrCustomName: name,
				TargetWorkflowID:              options.WorkflowID,
				Delay:                         delay,
				Timeout:                       timeout,
				WorkflowOptions:               options,
			}

			// Type-erased wrapper so we can start the internal debouncer via the engine without pre-encoding.
			internalWF := WorkflowFunc(func(ctx DBOSContext, in any) (any, error) {
				return internalDebouncerWF[P, R](ctx, in.(debouncerInput[P]))
			})

			internalWorkflowID, err := UUID(ctx)
			if err != nil {
				return nil, err
			}

			for {
				handle, err := ctx.RunWorkflow(internalWF, dInput, withWorkflowID(internalWorkflowID), WithDeduplicationID(key), withWorkflowName(internalDebouncerFQN))
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

		workflowOpts = append(workflowOpts, withWorkflowName(name))

		typedErasedWorkflow := WorkflowFunc(func(ctx DBOSContext, input any) (any, error) {
			return fn(ctx, input.(P))
		})

		handle, err := ctx.RunWorkflow(typedErasedWorkflow, input, workflowOpts...)
		if err != nil {
			return nil, err
		}

		// Every dispatched workflow yields a polling handle; return its typed version.
		return newWorkflowHandle[R](handle.dbosContext, handle.workflowID), nil
	}
}

// resolveWorkflowFunctionName resolves the function name for a workflow function,
// handling generic workflows by appending the actual type parameters.
func resolveWorkflowFunctionName[P any, R any](fn WorkflowFn[P, R]) string {
	ptr := reflect.ValueOf(fn).Pointer()
	fqn := runtime.FuncForPC(ptr).Name()

	// If this is a generic workflow, append the actual types to the FQN
	if strings.Contains(fqn, "[") {
		fqn = strings.Split(fqn, "[")[0]
		fqn = fmt.Sprintf("%s[%s,%s]",
			fqn,
			reflect.TypeFor[P]().String(),
			reflect.TypeFor[R]().String(),
		)
	}

	return fqn
}

// registerWorkflow registers a function as a durable workflow that can be executed and recovered.
// The function is registered with type safety - P represents the input type and R the return type.
// Scheduled workflows (WithSchedule) receive a time.Time as input representing the scheduled execution time.
// This is internal: NewWorkflow is the only way to make a workflow available to the runtime.
func registerWorkflow[P any, R any](ctx DBOSContext, fn WorkflowFn[P, R], opts ...WorkflowOption) {
	if ctx == nil {
		panic("ctx cannot be nil")
	}

	if fn == nil {
		panic("workflow function cannot be nil")
	}

	var p P

	registrationParams := workflowOptions{
		MaxRetries: _DEFAULT_MAX_RECOVERY_ATTEMPTS,
	}

	for _, opt := range opts {
		opt(&registrationParams)
	}

	fqn := resolveWorkflowFunctionName(fn)

	// Register a type-erased version of the durable workflow for recovery and queue runner
	// Input will always come, encoded, from the database, so we decode it into the target type (captured by this wrapped closure)
	// inputSerialization is the DB-stored serialization format for the encoded input.
	typedErasedWorkflow := func(ctx DBOSContext, input any, inputSerialization string) (any, error) {
		workflowID, err := GetWorkflowID(ctx)
		if err != nil {
			return *new(R), newWorkflowExecutionError("", fmt.Errorf("getting workflow ID: %w", err))
		}
		encodedInput, ok := input.(*string)
		if !ok {
			return *new(R), newWorkflowUnexpectedInputType(fqn, "*string (encoded)", fmt.Sprintf("%T", input))
		}
		var typedInput P
		if inputSerialization == PortableSerializerName {
			typedInput, err = decodePortableArgs[P](encodedInput)
		} else {
			inputDecoder, resolveErr := resolveDecoder[P](inputSerialization, getCustomSerializerFromCtx(ctx))
			if resolveErr != nil {
				return *new(R), newWorkflowExecutionError(workflowID, resolveErr)
			}
			typedInput, err = inputDecoder.Decode(encodedInput)
		}
		if err != nil {
			return *new(R), newWorkflowExecutionError(workflowID, err)
		}
		return fn(ctx, typedInput)
	}

	typeErasedWrapper := wrappedWorkflowFunc(func(ctx DBOSContext, input any, inputSerialization string, opts ...WorkflowOption) (*WorkflowHandle[any], error) {
		wfFunc := WorkflowFunc(func(ctx DBOSContext, input any) (any, error) {
			return typedErasedWorkflow(ctx, input, inputSerialization)
		})
		opts = append(opts, withWorkflowName(fqn), withAlreadyEncodedInput()) // Append the name so ctx.RunWorkflow can look it up from the registry to apply registration-time options
		if inputSerialization == PortableSerializerName {
			opts = append(opts, WithPortableWorkflow())
		}
		handle, err := ctx.RunWorkflow(wfFunc, input, opts...)
		if err != nil {
			return nil, err
		}
		return newWorkflowHandle[any](ctx, handle.GetWorkflowID()), nil // this is only used by recovery -- the queue runner dismisses it
	})
	storeWorkflowRegistryEntry(ctx, fqn, typeErasedWrapper, registrationParams.MaxRetries, registrationParams.WorkflowName)

	// If this is a scheduled workflow, register a cron job
	if registrationParams.CronSchedule != "" {
		if reflect.TypeOf(p) != reflect.TypeFor[time.Time]() {
			panic(fmt.Sprintf("scheduled workflow function must accept a time.Time as input, got %T", p))
		}
		scheduledWfFunc := WorkflowFunc(func(ctx DBOSContext, input any) (any, error) {
			return typedErasedWorkflow(ctx, input, resolveEncoder(ctx).Name())
		})
		registerScheduledWorkflow(ctx, fqn, registrationParams.WorkflowName, scheduledWfFunc, registrationParams.CronSchedule)
	}
}

// resolveWorkflowName returns either the FQN or the custom name of a function, if present in the workflow registry
func (c *dbosContext) resolveWorkflowName(workflowFn any) (string, error) {
	if workflowFn == nil {
		return "", errors.New("workflow function is required")
	}
	fqn := runtime.FuncForPC(reflect.ValueOf(workflowFn).Pointer()).Name()
	name, ok := c.workflowRegistry.ResolveName(fqn)
	if !ok {
		return "", fmt.Errorf("workflow function not registered: %s", fqn)
	}
	return name, nil
}

/**********************************/
/******* WORKFLOW FUNCTIONS *******/
/**********************************/

type dbosContextKey string

const workflowStateKey dbosContextKey = "workflowState"

// WorkflowFn represents a type-safe workflow function with specific input and output types.
// P is the input parameter type and R is the return type.
// All workflow functions must accept a DBOSContext as their first parameter.
type WorkflowFn[P any, R any] func(ctx DBOSContext, input P) (R, error)

// WorkflowFunc represents a type-erased workflow function used internally.
type WorkflowFunc func(ctx DBOSContext, input any) (any, error)

type activeWorkflowEntry struct{}

type workflowOptions struct {
	WorkflowName        string
	CronSchedule        string
	GlobalConcurrency   *int
	RateLimit           *rateLimiter
	Retention           *time.Duration
	WorkflowID          string
	QueueName           string
	ApplicationVersion  string
	MaxRetries          int
	DeduplicationID     string
	Priority            uint
	AuthenticatedUser   string
	AssumedRole         string
	AuthenticatedRoles  []string
	QueuePartitionKey   string
	DelayDuration       time.Duration
	debounce            bool
	debounceKey         string
	debounceDelay       time.Duration
	debounceTimeout     time.Duration
	alreadyEncodedInput bool
	isDequeue           bool
	isRecovery          bool
	isPortableWorkflow  bool
}

// WorkflowOption is a functional option for configuring a workflow, both at
// registration time (NewWorkflow) and at invocation time (RunWorkflow).
type WorkflowOption func(*workflowOptions)

// withWorkflowID sets a custom workflow ID instead of generating one
// automatically. Internal only: workflow IDs are runtime-assigned. To make a
// child workflow invocation idempotent, generate a durable key with UUID and
// pass it via WithDeduplicationID — the invocation then behaves like any other
// retryable step.
func withWorkflowID(id string) WorkflowOption {
	return func(p *workflowOptions) {
		p.WorkflowID = id
	}
}

// WithApplicationVersion overrides the DBOS Context application version for this workflow.
// This affects workflow recovery.
func WithApplicationVersion(version string) WorkflowOption {
	return func(p *workflowOptions) {
		p.ApplicationVersion = version
	}
}

// WithDeduplicationID sets a deduplication ID for a queue workflow.
func WithDeduplicationID(id string) WorkflowOption {
	return func(p *workflowOptions) {
		p.DeduplicationID = id
	}
}

// WithPriority sets the execution priority for a queue workflow.
func WithPriority(priority uint) WorkflowOption {
	return func(p *workflowOptions) {
		p.Priority = priority
	}
}

// WithQueuePartitionKey sets the queue partition key for partitioned queues.
// When a queue is partitioned, workflows with the same partition key are processed
// with separate concurrency limits per partition.
func WithQueuePartitionKey(partitionKey string) WorkflowOption {
	return func(p *workflowOptions) {
		p.QueuePartitionKey = partitionKey
	}
}

// WithDelay delays execution of a queued workflow by the specified duration.
// The workflow starts in the DELAYED status and transitions to ENQUEUED after the delay expires.
// Must be used together with WithQueue.
func WithDelay(delay time.Duration) WorkflowOption {
	return func(p *workflowOptions) {
		p.DelayDuration = delay
	}
}

// An internal option we use to map the reflection function name to the registration options.
func withWorkflowName(name string) WorkflowOption {
	return func(p *workflowOptions) {
		if p.WorkflowName == "" {
			p.WorkflowName = name
		}
	}
}

// An internal option we use to indicate that the input is already encoded, so we don't need to encode it again
func withAlreadyEncodedInput() WorkflowOption {
	return func(p *workflowOptions) {
		p.alreadyEncodedInput = true
	}
}

// Private option set when RunWorkflow is invoked from the queue runner (dbos/queue.go).
func withIsDequeue() WorkflowOption {
	return func(p *workflowOptions) {
		p.isDequeue = true
	}
}

// Private option set when RunWorkflow is invoked from the recovery path (dbos/recovery.go).
func withIsRecovery() WorkflowOption {
	return func(p *workflowOptions) {
		p.isRecovery = true
	}
}

// WithPortableWorkflow marks the workflow to use the cross-language portable JSON format
// for all serialized data (inputs, step outputs, events, messages, streams).
// This is set automatically during dequeue/recovery for workflows stored with portable serialization.
func WithPortableWorkflow() WorkflowOption {
	return func(p *workflowOptions) {
		p.isPortableWorkflow = true
	}
}

// Sets the authenticated user for the workflow
func WithAuthenticatedUser(user string) WorkflowOption {
	return func(p *workflowOptions) {
		p.AuthenticatedUser = user
	}
}

// Sets the assumed role for the workflow
func WithAssumedRole(role string) WorkflowOption {
	return func(p *workflowOptions) {
		p.AssumedRole = role
	}
}

// Sets the authenticated role for the workflow
func WithAuthenticatedRoles(roles []string) WorkflowOption {
	return func(p *workflowOptions) {
		p.AuthenticatedRoles = roles
	}
}

// WithDebounce delays this invocation, collapsing rapid repeated calls that share the same
// key into a single execution. Each call with the same key pushes the start time back by delay,
// up to the optional WithDebounceTimeout cap measured from the first call.
func WithDebounce(key string, delay time.Duration) WorkflowOption {
	return func(p *workflowOptions) {
		p.debounce = true
		p.debounceKey = key
		p.debounceDelay = delay
	}
}

// WithDebounceTimeout caps how long debouncing may push back execution from the first call.
// Zero (default) means no cap. Only meaningful together with WithDebounce.
func WithDebounceTimeout(timeout time.Duration) WorkflowOption {
	return func(p *workflowOptions) {
		p.debounceTimeout = timeout
	}
}

func (c *dbosContext) RunWorkflow(fn WorkflowFunc, input any, opts ...WorkflowOption) (*WorkflowHandle[any], error) {
	// Apply options to build params
	params := workflowOptions{
		ApplicationVersion: c.GetApplicationVersion(),
	}
	for _, opt := range opts {
		opt(&params)
	}

	// Lookup the registry for registration-time options
	if workflowName, ok := c.workflowRegistry.ResolveName(params.WorkflowName); ok {
		params.WorkflowName = workflowName
	}
	registeredWorkflow, exists := c.workflowRegistry.Load(params.WorkflowName)
	if !exists {
		c.logger.Error("workflow not found in registry", "workflow_name", params.WorkflowName)
		return nil, newNonExistentWorkflowError(params.WorkflowName)
	}
	if registeredWorkflow.MaxRetries > 0 {
		params.MaxRetries = registeredWorkflow.MaxRetries
	}
	if len(registeredWorkflow.Name) > 0 {
		params.WorkflowName = registeredWorkflow.Name
	}
	// A workflow is enqueued for a worker unless we are the worker dequeuing it or recovering it.
	enqueue := params.QueueName != "" && !params.isDequeue && !params.isRecovery

	// Validate delay is only provided when enqueuing
	if params.DelayDuration > 0 && !enqueue {
		return nil, newWorkflowExecutionError("", fmt.Errorf("delay can only be applied when enqueuing a workflow"))
	}

	// Preserve the initiating workflow relationship for observability only.
	parentWorkflowState, ok := c.Value(workflowStateKey).(*workflowState)
	hasParentWorkflow := ok && parentWorkflowState != nil

	// Generate an ID for the workflow if not provided
	var workflowID string
	if params.WorkflowID == "" {
		workflowID = uuid.New().String()
	} else {
		workflowID = params.WorkflowID
	}

	// Create an uncancellable context for the DBOS operations
	// This detaches it from any deadline or cancellation signal set by the user
	uncancellableCtx := WithoutCancel(c)

	var status WorkflowStatusType
	if enqueue {
		if params.DelayDuration > 0 {
			status = WorkflowStatusDelayed
		} else {
			status = WorkflowStatusEnqueued
		}
	} else {
		status = WorkflowStatusPending
	}

	var delayUntil time.Time
	if params.DelayDuration > 0 {
		delayUntil = time.Now().Add(params.DelayDuration)
	}

	// Compute the timeout based on the context deadline, if any
	deadline, ok := c.Deadline()
	if !ok {
		deadline = time.Time{} // No deadline set
	}
	var timeout time.Duration
	if !deadline.IsZero() {
		timeout = time.Until(deadline)
		// The timeout could be in the past, for small deadlines, to propagation delays. If so set it to a minimal value
		if timeout < 0 {
			timeout = 1 * time.Millisecond
		}
	}
	// When enqueuing or delaying, we do not set a deadline. It'll be computed with the timeout during dequeue.
	if status == WorkflowStatusEnqueued || status == WorkflowStatusDelayed {
		deadline = time.Time{}
	}

	if params.Priority > uint(math.MaxInt) {
		c.logger.Error("priority exceeds maximum allowed value", "workflow_name", params.WorkflowName, "priority", params.Priority, "max_allowed_value", math.MaxInt)
		return nil, fmt.Errorf("priority %d exceeds maximum allowed value %d", params.Priority, math.MaxInt)
	}

	// Serialize input before storing in workflow status
	var encodedInput any
	if params.alreadyEncodedInput { // Comes from the queue runner, or the recovery path
		encodedInput = input
	} else if params.isPortableWorkflow { // Direct call to a portable workflow
		var serErr error
		encodedInput, serErr = encodePortableArgs(input)
		if serErr != nil {
			c.logger.Error("failed to serialize portable workflow input", "error", serErr, "workflow_id", workflowID)
			return nil, newWorkflowExecutionError(workflowID, fmt.Errorf("failed to serialize portable workflow input: %w", serErr))
		}
	} else {
		var serErr error
		encodedInput, serErr = resolveEncoder(c).Encode(input)
		if serErr != nil {
			c.logger.Error("failed to serialize workflow input", "error", serErr, "workflow_id", workflowID)
			return nil, newWorkflowExecutionError(workflowID, fmt.Errorf("failed to serialize workflow input: %w", serErr))
		}
	}

	workflowStatus := WorkflowStatus{
		Name:               params.WorkflowName,
		ApplicationVersion: params.ApplicationVersion,
		ExecutorID:         c.GetExecutorID(),
		Status:             status,
		ID:                 workflowID,
		CreatedAt:          time.Now(),
		Deadline:           deadline,
		Timeout:            timeout,
		Input:              encodedInput,
		ApplicationID:      c.GetApplicationID(),
		DeduplicationID:    params.DeduplicationID,
		Priority:           int(params.Priority),
		AuthenticatedUser:  params.AuthenticatedUser,
		AssumedRole:        params.AssumedRole,
		AuthenticatedRoles: params.AuthenticatedRoles,
		QueuePartitionKey:  params.QueuePartitionKey,
		DelayUntil:         delayUntil,
		Serialization: func() string {
			if params.isPortableWorkflow {
				return PortableSerializerName
			}
			return resolveEncoder(c).Name()
		}(),
	}
	if hasParentWorkflow {
		workflowStatus.ParentWorkflowID = parentWorkflowState.workflowID
	}

	var earlyReturnPollingHandle *WorkflowHandle[any]
	var insertStatusResult *insertWorkflowResult
	// Initialize workflow status.
	insertWorkflowStatusTx := func() error {
		tx, err := c.kernel.pool.BeginTx(uncancellableCtx, TxOptions{})
		if err != nil {
			return newWorkflowExecutionError(workflowID, fmt.Errorf("failed to begin transaction: %w", err))
		}
		defer tx.Rollback(uncancellableCtx) // Rollback if not committed

		// Insert workflow status with transaction
		ownerXID := uuid.New().String()
		insertInput := insertWorkflowStatusDBInput{
			status:            workflowStatus,
			maxRetries:        params.MaxRetries,
			tx:                tx,
			ownerXID:          &ownerXID,
			incrementAttempts: params.isDequeue || params.isRecovery,
		}
		insertStatusResult, err = c.kernel.insertWorkflowStatus(uncancellableCtx, insertInput)
		if err != nil {
			if !errors.Is(err, errDeduplicationCollision) {
				c.logger.Error("failed to insert workflow status", "error", err, "workflow_id", workflowID)
			}
			return newWorkflowExecutionError(workflowID, fmt.Errorf("failed to insert workflow status: %w", err))
		}

		var loaded bool
		if c.activeWorkflowIDs != nil {
			_, loaded = c.activeWorkflowIDs.Load(workflowID)
		}

		shouldSkip :=
			enqueue || // We are enqueuing for a worker OR
				insertStatusResult.status == WorkflowStatusSuccess || // workflow is in a terminal state (success) OR
				insertStatusResult.status == WorkflowStatusError || // workflow is in a terminal state (error) OR
				(!params.isDequeue && !params.isRecovery && insertStatusResult.ownerXID != ownerXID) || // another executor, not us dequeueing or being instructed to recover, is already owning the workflow OR
				loaded // this executor is already running the workflow

		if shouldSkip {
			// Commit the transaction to update the number of attempts and/or enact the enqueue
			if err := tx.Commit(uncancellableCtx); err != nil {
				return newWorkflowExecutionError(workflowID, fmt.Errorf("failed to commit transaction: %w", err))
			}
			earlyReturnPollingHandle = newWorkflowHandle[any](uncancellableCtx, workflowStatus.ID)
			return nil
		}

		// Commit the transaction. This must happen before we start the goroutine to ensure the workflow is found by steps in the database
		if err := tx.Commit(uncancellableCtx); err != nil {
			return newWorkflowExecutionError(workflowID, fmt.Errorf("failed to commit transaction: %w", err))
		}

		return nil
	}

	for {
		err := retry(c, insertWorkflowStatusTx, withRetrierLogger(c.logger))
		if err == nil {
			// Common path
			break
		}
		// Now handle the case where the insert failed because the deduplication ID is already held by another workflow.
		if !errors.Is(err, errDeduplicationCollision) {
			return nil, err
		}
		existingID, lookupErr := retryWithResult(uncancellableCtx, func() (*string, error) {
			return c.kernel.getDeduplicatedWorkflow(uncancellableCtx, params.WorkflowName, params.DeduplicationID)
		}, withRetrierLogger(c.logger))
		if lookupErr != nil {
			return nil, newWorkflowExecutionError(workflowID, fmt.Errorf("looking up deduplicated workflow: %w", lookupErr))
		}
		if existingID == nil {
			continue // the slot was cleared between our insert and the lookup; try to claim it
		}
		c.logger.Info("returning handle to existing deduplicated workflow", "workflow_name", params.WorkflowName, "queue_name", params.QueueName, "deduplication_id", params.DeduplicationID, "existing_workflow_id", *existingID)
		return newWorkflowHandle[any](uncancellableCtx, *existingID), nil
	}
	if earlyReturnPollingHandle != nil {
		return earlyReturnPollingHandle, nil
	}

	// Create workflow state to track step execution
	wfState := &workflowState{
		workflowID:         workflowID,
		stepID:             -1, // Steps are O-indexed
		isPortableWorkflow: params.isPortableWorkflow,
		authenticatedUser:  params.AuthenticatedUser,
		assumedRole:        params.AssumedRole,
		authenticatedRoles: params.AuthenticatedRoles,
	}
	workflowCtx := WithValue(c, workflowStateKey, wfState)

	// If the workflow has a timeout but no deadline, compute the deadline from the timeout.
	// Else use the durable deadline.
	durableDeadline := time.Time{}
	if insertStatusResult.timeout > 0 && insertStatusResult.workflowDeadline.IsZero() {
		durableDeadline = time.Now().Add(insertStatusResult.timeout)
	} else if !insertStatusResult.workflowDeadline.IsZero() {
		durableDeadline = insertStatusResult.workflowDeadline
	}

	var stopFunc func() bool
	cancelFuncCompleted := make(chan struct{})
	if !durableDeadline.IsZero() {
		workflowCtx, _ = WithTimeout(workflowCtx, time.Until(durableDeadline))
		// Register a cancel function that cancels the workflow in the DB as soon as the context is cancelled
		workflowCancelFunction := func() {
			c.logger.Info("Cancelling workflow", "workflow_id", workflowID)
			err := retry(c, func() error {
				_, err := c.kernel.cancelWorkflows(uncancellableCtx, cancelWorkflowsDBInput{workflowIDs: []string{workflowID}})
				return err
			}, withRetrierLogger(c.logger))
			if err != nil {
				c.logger.Error("Failed to cancel workflow", "error", err)
			}
			close(cancelFuncCompleted)
		}
		stopFunc = context.AfterFunc(workflowCtx, workflowCancelFunction)
	}

	// Run the function in a goroutine. The result is persisted to the database; callers
	// retrieve it via the polling handle (awaitWorkflowResult).
	c.workflowsWg.Add(1)
	go func() {
		defer c.workflowsWg.Done()

		if c.activeWorkflowIDs != nil {
			_, loaded := c.activeWorkflowIDs.LoadOrStore(workflowID, activeWorkflowEntry{})
			if loaded { // This should never happen, but if it does, we need to log it
				c.logger.Error("UNREACHABLE: workflow already running on this context", "workflow_id", workflowID)
			}
			defer c.activeWorkflowIDs.Delete(workflowID)
		}

		var result any
		var err error

		result, err = fn(workflowCtx, input)

		// Handle DBOS ID conflict errors by waiting for the owning workflow to complete.
		// The owning executor records the outcome; nothing to persist here.
		if errors.Is(err, &DBOSError{Code: ConflictingIDError}) {
			c.logger.Warn("Workflow ID conflict detected. Waiting for existing workflow to complete", "workflow_id", workflowID)
			_, awaitErr := retryWithResult(c, func() (*awaitWorkflowResultOutput, error) {
				return c.kernel.awaitWorkflowResult(uncancellableCtx, workflowID, _DB_RETRY_INTERVAL)
			}, withRetrierLogger(c.logger))
			if awaitErr != nil {
				c.logger.Error("Error awaiting conflicting workflow", "workflow_id", workflowID, "error", awaitErr)
			}
			return
		}
		status := WorkflowStatusSuccess

		// If an error occurred, set the status to error
		if err != nil {
			status = WorkflowStatusError
		}

		// If the afterFunc has started, the workflow was cancelled and the status should be set to cancelled
		if stopFunc != nil && !stopFunc() {
			c.logger.Info("Workflow was cancelled. Waiting for cancel function to complete", "workflow_id", workflowID)
			<-cancelFuncCompleted // Wait for the cancel function to complete
			status = WorkflowStatusCancelled
		}

		// Serialize the output before recording
		encodedOutput, serErr := resolveEncoder(workflowCtx).Encode(result)
		if serErr != nil {
			c.logger.Error("Failed to serialize workflow output", "workflow_id", workflowID, "error", serErr)
			return
		}

		var serializedErr string
		var encodedErr *string
		if err != nil {
			serializedErr = serializeWorkflowError(err, resolveEncoder(workflowCtx).Name())
			encodedErr = encodeWorkflowError(err)
		}
		recordErr := retry(c, func() error {
			return c.kernel.updateWorkflowOutcome(uncancellableCtx, updateWorkflowOutcomeDBInput{
				workflowID: workflowID,
				status:     status,
				errStr:     serializedErr,
				errEncoded: encodedErr,
				output:     encodedOutput,
			})
		}, withRetrierLogger(c.logger))
		if recordErr != nil {
			c.logger.Error("Error recording workflow outcome", "workflow_id", workflowID, "error", recordErr)
		}
	}()

	return newWorkflowHandle[any](uncancellableCtx, workflowID), nil
}

/******************************/
/******* STEP FUNCTIONS *******/
/******************************/

// StepFunc represents a type-erased step function used internally.
type StepFunc func(ctx context.Context) (any, error)

// Step represents a type-safe step function with a specific output type R.
type Step[R any] func(ctx context.Context) (R, error)

// txnFunc represents a type-erased step function that receives a transaction.
// Used internally by runAsTxn when the step body and checkpoint share one transaction.
type txnFunc func(ctx context.Context, tx Tx) (any, error)

// txn represents a type-safe step function with output type R that receives a transaction.
type txn[R any] func(ctx context.Context, tx Tx) (R, error)

// stepOptions holds the configuration for step execution using functional options pattern.
type stepOptions struct {
	maxRetries         int           // Maximum number of retry attempts (0 = no retries)
	backoffFactor      float64       // Exponential backoff multiplier between retries (default: 2.0)
	baseInterval       time.Duration // Initial delay between retries (default: 100ms)
	maxInterval        time.Duration // Maximum delay between retries (default: 5s)
	stepName           string        // Custom name for the step (defaults to function name)
	preGeneratedStepID *int          // Pre generated stepID
	txIsoLevel         *IsoLevel     // Transaction isolation level for runAsTxn (nil = ReadCommitted)
}

// setDefaults applies default values to stepOptions
func (opts *stepOptions) setDefaults() {
	if opts.backoffFactor == 0 {
		opts.backoffFactor = _DEFAULT_STEP_BACKOFF_FACTOR
	}
	if opts.baseInterval == 0 {
		opts.baseInterval = _DEFAULT_STEP_BASE_INTERVAL
	}
	if opts.maxInterval == 0 {
		opts.maxInterval = _DEFAULT_STEP_MAX_INTERVAL
	}
}

// StepOption is a functional option for configuring step execution parameters.
type StepOption func(*stepOptions)

// WithStepName sets a custom name for the step. If the step name has already been set
// by a previous call to WithStepName, this option will be ignored
func WithStepName(name string) StepOption {
	return func(opts *stepOptions) {
		if opts.stepName == "" {
			opts.stepName = name
		}
	}
}

// WithStepMaxRetries sets the maximum number of retry attempts for the step.
// A value of 0 means no retries (default behavior).
func WithStepMaxRetries(maxRetries int) StepOption {
	return func(opts *stepOptions) {
		opts.maxRetries = maxRetries
	}
}

// WithBackoffFactor sets the exponential backoff multiplier between retries.
// The delay between retries is calculated as: BaseInterval * (BackoffFactor^(retry-1))
// Default value is 2.0.
func WithBackoffFactor(factor float64) StepOption {
	return func(opts *stepOptions) {
		opts.backoffFactor = factor
	}
}

// WithBaseInterval sets the initial delay between retries.
// Default value is 100ms.
func WithBaseInterval(interval time.Duration) StepOption {
	return func(opts *stepOptions) {
		opts.baseInterval = interval
	}
}

// WithMaxInterval sets the maximum delay between retries.
// Default value is 5s.
func WithMaxInterval(interval time.Duration) StepOption {
	return func(opts *stepOptions) {
		opts.maxInterval = interval
	}
}

func WithNextStepID(stepID int) StepOption {
	return func(opts *stepOptions) {
		opts.preGeneratedStepID = &stepID
	}
}

// StepOutcome holds the result and error from a step execution
// This struct is returned as part of a channel from the Go function when running the step inside a Go routine
type StepOutcome[R any] struct {
	Result R     `json:"result"`
	Err    error `json:"err"`
}

// StreamValue holds a value, error, and closed status from a stream read operation
// This struct is returned as part of a channel from ReadStreamAsync
type StreamValue[R any] struct {
	Value  R     // The stream value (zero value if error/closed)
	Err    error // Error if one occurred (nil otherwise)
	Closed bool  // Whether the stream is closed
}

// convertStepResult converts a generic step result to a typed result R.
// It handles both checkpointed outcomes (encoded values from database) and direct type conversions.
// Supports both real DBOS contexts and testing/mocking scenarios.
func convertStepResult[R any](ctx DBOSContext, result any) (R, error) {
	var typedResult R
	// Check if we're in a real DBOS context (not a mock)
	if _, ok := ctx.(*dbosContext); ok {
		// First check if this is a checkpointed outcome (encoded value from database)
		if checkpointed, ok := result.(stepCheckpointedOutcome); ok {
			// This came from the database and needs decoding
			encodedOutput, ok := checkpointed.value.(*string)
			if !ok {
				workflowID, _ := GetWorkflowID(ctx)
				return *new(R), newWorkflowExecutionError(workflowID, fmt.Errorf("checkpointed outcome value is not *string, got %T", checkpointed.value))
			}
			var decodeErr error
			stepDecoder, resolveErr := resolveDecoder[R](checkpointed.serialization, getCustomSerializerFromCtx(ctx))
			if resolveErr != nil {
				workflowID, err := GetWorkflowID(ctx)
				if err != nil {
					return *new(R), fmt.Errorf("getting workflow ID from context: %w; original error: %v", err, resolveErr)
				}
				return *new(R), newWorkflowExecutionError(workflowID, resolveErr)
			}
			typedResult, decodeErr = stepDecoder.Decode(encodedOutput)
			if decodeErr != nil {
				workflowID, _ := GetWorkflowID(ctx)
				return *new(R), newWorkflowExecutionError(workflowID, fmt.Errorf("decoding step result to expected type %T: %w", *new(R), decodeErr))
			}
		} else if typedRes, ok := result.(R); ok {
			// When the step is executed, the result is already decoded and should be directly convertible
			typedResult = typedRes
		} else {
			workflowID, _ := GetWorkflowID(ctx) // Must be within a workflow so we can ignore the error
			return *new(R), newWorkflowUnexpectedResultType(workflowID, fmt.Sprintf("%T", *new(R)), fmt.Sprintf("%T", result))
		}
	} else {
		// Fallback for testing/mocking scenarios
		if typedRes, ok := result.(R); ok {
			typedResult = typedRes
		} else {
			workflowID, _ := GetWorkflowID(ctx)
			return *new(R), newWorkflowUnexpectedResultType(workflowID, fmt.Sprintf("%T", *new(R)), fmt.Sprintf("%T", result))
		}
	}
	return typedResult, nil
}

type preparedStep struct {
	WorkflowID   string         // for error messages when StepState is nil
	StepOpts     *stepOptions   // always set
	StepState    *workflowState // nil when IsWithinStep
	IsWithinStep bool
}

// prepareStepExecution parses opts, loads workflow state, and optionally computes stepState.
// When wfState.isWithinStep, returns IsWithinStep=true and StepState=nil; caller should return fn(c) or fn(c,nil) and not continue.
func prepareStepExecution(c *dbosContext, opts []StepOption) (*preparedStep, error) {
	stepOpts := &stepOptions{}
	for _, opt := range opts {
		opt(stepOpts)
	}
	stepOpts.setDefaults()

	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, newStepExecutionError("", stepOpts.stepName, fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}

	if wfState.isWithinStep {
		return &preparedStep{WorkflowID: wfState.workflowID, StepOpts: stepOpts, StepState: nil, IsWithinStep: true}, nil
	}

	var stepID int
	if stepOpts.preGeneratedStepID != nil {
		stepID = *stepOpts.preGeneratedStepID
	} else {
		stepID = wfState.nextStepID()
	}
	stepState := workflowState{
		workflowID:   wfState.workflowID,
		stepID:       stepID,
		isWithinStep: true,
	}
	return &preparedStep{WorkflowID: wfState.workflowID, StepOpts: stepOpts, StepState: &stepState, IsWithinStep: false}, nil
}

// executeStepWithRetry runs runOnce (the step body) and retries with backoff on error when maxRetries > 0.
func executeStepWithRetry(c *dbosContext, workflowID string, stepOpts *stepOptions, runOnce func() (any, error)) (stepOutput any, stepError error) {
	stepOutput, stepError = runOnce()
	if stepError == nil || stepOpts.maxRetries <= 0 {
		return stepOutput, stepError
	}
	var joinedErrors error
	joinedErrors = errors.Join(joinedErrors, stepError)
	for retry := 1; retry <= stepOpts.maxRetries; retry++ {
		delay := stepOpts.baseInterval
		if retry > 1 {
			exponentialDelay := float64(stepOpts.baseInterval) * math.Pow(stepOpts.backoffFactor, float64(retry-1))
			delay = time.Duration(math.Min(exponentialDelay, float64(stepOpts.maxInterval)))
		}
		c.logger.Error("step failed, retrying", "step_name", stepOpts.stepName, "retry", retry, "max_retries", stepOpts.maxRetries, "delay", delay, "error", stepError)
		select {
		case <-c.Done():
			return nil, newStepExecutionError(workflowID, stepOpts.stepName, fmt.Errorf("context cancelled during retry: %w", c.Err()))
		case <-time.After(delay):
		}
		stepOutput, stepError = runOnce()
		if stepError == nil {
			return stepOutput, stepError
		}
		joinedErrors = errors.Join(joinedErrors, stepError)
		if retry == stepOpts.maxRetries {
			stepError = newMaxStepRetriesExceededError(workflowID, stepOpts.stepName, stepOpts.maxRetries, joinedErrors)
			break
		}
	}
	return stepOutput, stepError
}

// Run executes a function as a durable step within a workflow.
// Steps provide at-least-once execution guarantees and automatic retry capabilities.
// If a step has already been executed (e.g., during workflow recovery), its recorded
// result is returned instead of re-executing the function.
//
// Steps can be configured with functional options:
//
//	data, err := dbos.RunAsStep(ctx, func(ctx context.Context) ([]byte, error) {
//	    return MyStep(ctx, "https://api.example.com/data")
//	}, dbos.WithStepMaxRetries(3), dbos.WithBaseInterval(500*time.Millisecond))
//
// Available options:
//   - WithStepName: Custom name for the step (only sets if not already set)
//   - WithStepMaxRetries: Maximum retry attempts (default: 0)
//   - WithBackoffFactor: Exponential backoff multiplier (default: 2.0)
//   - WithBaseInterval: Initial delay between retries (default: 100ms)
//   - WithMaxInterval: Maximum delay between retries (default: 5s)
//
// Example:
//
//	func MyStep(ctx context.Context, url string) ([]byte, error) {
//	    resp, err := http.Get(url)
//	    if err != nil {
//	        return nil, err
//	    }
//	    defer resp.Body.Close()
//	    return io.ReadAll(resp.Body)
//	}
//
//	// Within a workflow:
//	data, err := dbos.RunAsStep(ctx, func(ctx context.Context) ([]byte, error) {
//	    return MyStep(ctx, "https://api.example.com/data")
//	}, dbos.WithStepName("FetchData"), dbos.WithStepMaxRetries(3))
//	if err != nil {
//	    return nil, err
//	}
//
// Note that the function passed to Run must accept a context.Context as its first parameter
// and this context *must* be the one specified in the function's signature (not the context passed to Run).
// Under the hood, DBOS uses the provided context to manage durable execution.
func Run[R any](ctx DBOSContext, fn Step[R], opts ...StepOption) (R, error) {
	if ctx == nil {
		return *new(R), newStepExecutionError("", "", fmt.Errorf("ctx cannot be nil"))
	}

	if fn == nil {
		return *new(R), newStepExecutionError("", "", fmt.Errorf("step function cannot be nil"))
	}

	// Append WithStepName option to ensure the step name is set. This will not erase a user-provided step name
	stepName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	opts = append(opts, WithStepName(stepName))

	// Type-erase the function
	typeErasedFn := StepFunc(func(ctx context.Context) (any, error) { return fn(ctx) })

	result, err := ctx.RunAsStep(typeErasedFn, opts...)
	// Step function could return a nil result
	if result == nil {
		return *new(R), err
	}
	typedResult, convertErr := convertStepResult[R](ctx, result)
	if convertErr != nil {
		return *new(R), convertErr
	}
	return typedResult, err
}

// UUID returns a durable UUIDv7. Within a workflow, the generated value is recorded as a step
// result and the same value is returned on replay. Outside a workflow, it returns a fresh UUID.
func UUID(ctx DBOSContext) (string, error) {
	newUUID := func(context.Context) (string, error) {
		id, err := uuid.NewV7()
		if err != nil {
			return "", err
		}
		return id.String(), nil
	}
	if workflowState, ok := ctx.Value(workflowStateKey).(*workflowState); !ok || workflowState == nil {
		return newUUID(ctx)
	}
	return Run(ctx, newUUID, WithStepName("DBOS.uuid"))
}

func (c *dbosContext) RunAsStep(fn StepFunc, opts ...StepOption) (any, error) {
	prep, err := prepareStepExecution(c, opts)
	if err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, newStepExecutionError(prep.WorkflowID, prep.StepOpts.stepName, fmt.Errorf("step function cannot be nil"))
	}
	if prep.IsWithinStep {
		return fn(c)
	}

	uncancellableCtx := WithoutCancel(c)
	stepState := prep.StepState
	stepOpts := prep.StepOpts

	// Check the step is cancelled, has already completed, or is called with a different name
	recordedOutput, err := retryWithResult(c, func() (*recordedResult, error) {
		return c.kernel.checkOperationExecution(uncancellableCtx, checkOperationExecutionDBInput{
			workflowID: stepState.workflowID,
			stepID:     stepState.stepID,
			stepName:   stepOpts.stepName,
		})
	}, withRetrierLogger(c.logger))
	if err != nil {
		return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("checking operation execution: %w", err))
	}
	if recordedOutput != nil {
		// Return the encoded output wrapped in stepCheckpointedOutcome
		// This allows RunAsStep[R] to distinguish encoded values from direct values
		return stepCheckpointedOutcome{value: recordedOutput.output, serialization: recordedOutput.serialization}, deserializeWorkflowError(recordedOutput.errStr, recordedOutput.errEncoded, recordedOutput.serialization)
	}

	stepCtx := WithValue(c, workflowStateKey, stepState)
	stepStartTime := time.Now()
	stepOutput, stepError := executeStepWithRetry(c, stepState.workflowID, stepOpts, func() (any, error) { return fn(stepCtx) })

	// Serialize step output before recording
	ser := resolveEncoder(c)
	encodedStepOutput, serErr := ser.Encode(stepOutput)
	if serErr != nil {
		return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to serialize step output: %w", serErr))
	}

	// Record the final result
	stepCompletedTime := time.Now()
	var serializedStepErr *string
	var encodedStepErr *string
	if stepError != nil {
		s := serializeWorkflowError(stepError, ser.Name())
		serializedStepErr = &s
		encodedStepErr = encodeWorkflowError(stepError)
	}
	dbInput := recordOperationResultDBInput{
		workflowID:    stepState.workflowID,
		stepName:      stepOpts.stepName,
		stepID:        stepState.stepID,
		errStr:        serializedStepErr,
		errEncoded:    encodedStepErr,
		startedAt:     stepStartTime,
		completedAt:   stepCompletedTime,
		output:        encodedStepOutput,
		serialization: ser.Name(),
	}
	recErr := retry(c, func() error {
		return c.kernel.recordOperationResult(uncancellableCtx, dbInput)
	}, withRetrierLogger(c.logger))
	if recErr != nil {
		return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, recErr)
	}

	return stepOutput, stepError
}

// runAsTxn executes a step function that receives a transaction when run on its own.
// The step body and checkpoint share one transaction, so system DB writes and recordOperationResult commit together.
// Like Run but uses txn[R] / txnFunc; transaction is begun and committed inside this function.
func runAsTxn[R any](ctx DBOSContext, fn txn[R], opts ...StepOption) (R, error) {
	if ctx == nil {
		return *new(R), newStepExecutionError("", "", fmt.Errorf("ctx cannot be nil"))
	}

	if fn == nil {
		return *new(R), newStepExecutionError("", "", fmt.Errorf("step function cannot be nil"))
	}

	c, ok := ctx.(*dbosContext)
	if !ok {
		return *new(R), newStepExecutionError("", "", fmt.Errorf("runAsTxn requires *dbosContext. Mock the caller of this function if you are testing."))
	}

	stepName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	opts = append(opts, WithStepName(stepName))

	typeErasedFn := txnFunc(func(ctx context.Context, tx Tx) (any, error) { return fn(ctx, tx) })

	result, err := c.runAsTxn(typeErasedFn, opts...)
	if result == nil {
		return *new(R), err
	}
	typedResult, convertErr := convertStepResult[R](ctx, result)
	if convertErr != nil {
		return *new(R), convertErr
	}
	return typedResult, err
}

func (c *dbosContext) runAsTxn(fn txnFunc, opts ...StepOption) (any, error) {
	prep, err := prepareStepExecution(c, opts)
	if err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, newStepExecutionError(prep.WorkflowID, prep.StepOpts.stepName, fmt.Errorf("step function cannot be nil"))
	}
	if prep.IsWithinStep {
		return fn(c, nil)
	}

	uncancellableCtx := WithoutCancel(c)
	stepState := prep.StepState
	stepOpts := prep.StepOpts
	pool := c.kernel.pool
	stepCtx := WithValue(c, workflowStateKey, stepState)
	stepStartTime := time.Now()

	txOpts := TxOptions{IsoLevel: IsoLevelReadCommitted}
	if stepOpts.txIsoLevel != nil {
		txOpts.IsoLevel = *stepOpts.txIsoLevel
	}
	return retryWithResult(c, func() (any, error) {
		tx, err := pool.BeginTx(uncancellableCtx, txOpts)
		if err != nil {
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to begin transaction: %w", err))
		}
		defer tx.Rollback(uncancellableCtx)

		recordedOutput, err := c.kernel.checkOperationExecution(uncancellableCtx, checkOperationExecutionDBInput{
			workflowID: stepState.workflowID,
			stepID:     stepState.stepID,
			stepName:   stepOpts.stepName,
			tx:         tx,
		})
		if err != nil {
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("checking operation execution: %w", err))
		}
		if recordedOutput != nil {
			return stepCheckpointedOutcome{value: recordedOutput.output, serialization: recordedOutput.serialization}, deserializeWorkflowError(recordedOutput.errStr, recordedOutput.errEncoded, recordedOutput.serialization)
		}

		stepOutput, stepError := executeStepWithRetry(c, stepState.workflowID, stepOpts, func() (any, error) { return fn(stepCtx, tx) })

		txnSer := resolveEncoder(c)
		encodedStepOutput, serErr := txnSer.Encode(stepOutput)
		if serErr != nil {
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to serialize step output: %w", serErr))
		}

		var serializedTxnErr *string
		var encodedTxnErr *string
		if stepError != nil {
			s := serializeWorkflowError(stepError, txnSer.Name())
			serializedTxnErr = &s
			encodedTxnErr = encodeWorkflowError(stepError)
		}
		dbInput := recordOperationResultDBInput{
			workflowID:    stepState.workflowID,
			stepName:      stepOpts.stepName,
			stepID:        stepState.stepID,
			errStr:        serializedTxnErr,
			errEncoded:    encodedTxnErr,
			startedAt:     stepStartTime,
			completedAt:   time.Now(),
			output:        encodedStepOutput,
			tx:            tx,
			serialization: txnSer.Name(),
		}
		recErr := c.kernel.recordOperationResult(uncancellableCtx, dbInput)
		if recErr != nil {
			if stepError != nil {
				recErr = errors.Join(recErr, stepError)
			}
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, recErr)
		}
		if err := tx.Commit(uncancellableCtx); err != nil {
			return nil, newStepExecutionError(stepState.workflowID, stepOpts.stepName, fmt.Errorf("failed to commit transaction: %w", err))
		}
		return stepOutput, stepError
	}, withRetrierLogger(c.logger))
}

// Go runs a step inside a Go routine and returns a channel to receive the result.
// Go generates a deterministic step ID for the step before running the step in a routine, since goroutines are not deterministic.
// Example:
//
// resultChan, err := dbos.Go(ctx, func(ctx context.Context) (string, error) {
//   return "Hello, World!", nil
// })
//
// resultChan := <-resultChan // wait for the channel to receive
// if resultChan.err != nil {
//   // Handle error
// }

func Go[R any](ctx DBOSContext, fn Step[R], opts ...StepOption) (chan StepOutcome[R], error) {
	if ctx == nil {
		return nil, newStepExecutionError("", "", errors.New("ctx cannot be nil"))
	}

	if fn == nil {
		return nil, newStepExecutionError("", "", errors.New("step function cannot be nil"))
	}

	// Append WithStepName option to ensure the step name is set. This will not erase a user-provided step name
	stepName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	opts = append(opts, WithStepName(stepName))

	// Type-erase the function
	typeErasedFn := StepFunc(func(ctx context.Context) (any, error) { return fn(ctx) })

	result, err := ctx.Go(typeErasedFn, opts...)
	if err != nil {
		return nil, err
	}

	// Create the typed channel to return immediately (non-blocking)
	outcomeChan := make(chan StepOutcome[R], 1)

	// Start a goroutine to handle decoding and type conversion asynchronously
	go func() {
		defer close(outcomeChan)

		outcome := <-result // Block here waiting for the step to complete

		// If the step function returns a nil result, send the error through the channel
		if outcome.Result == nil {
			outcomeChan <- StepOutcome[R]{
				Result: *new(R),
				Err:    outcome.Err,
			}
			return
		}

		typedResult, convertErr := convertStepResult[R](ctx, outcome.Result)
		if convertErr != nil {
			outcomeChan <- StepOutcome[R]{
				Result: *new(R),
				Err:    convertErr,
			}
			return
		}

		outcomeChan <- StepOutcome[R]{
			Result: typedResult,
			Err:    outcome.Err,
		}
	}()

	return outcomeChan, nil
}

func (c *dbosContext) Go(fn StepFunc, opts ...StepOption) (chan StepOutcome[any], error) {
	// Create a deterministic step ID
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, newStepExecutionError("", "", errors.New("workflow state not found in context: are you running this step within a workflow?"))
	}
	opts = append(opts, WithNextStepID(wfState.nextStepID()))

	// Run step inside a Go routine
	result := make(chan StepOutcome[any], 1)
	go func() {
		defer close(result)
		res, err := c.RunAsStep(fn, opts...)
		result <- StepOutcome[any]{
			Result: res,
			Err:    err,
		}
	}()

	return result, nil
}

// Select performs a durable select operation over a slice of channels obtained from Go.
// It checkpoints the selected channel index and value so that workflow replay produces deterministic results.
// Select can only be called from within a workflow and becomes part of the workflow's durable state.
//
// Example:
//
//	ch1, _ := dbos.Go(ctx, func(ctx context.Context) (string, error) { return "result1", nil })
//	ch2, _ := dbos.Go(ctx, func(ctx context.Context) (string, error) { return "result2", nil })
//	outcome, err := dbos.Select(ctx, []<-chan dbos.StepOutcome[string]{ch1, ch2})
//	if err != nil {
//	    // Handle error
//	    return err
//	}
//	log.Printf("Selected result: %v, error: %v", outcome.result, outcome.err)
func Select[R any](ctx DBOSContext, channels []<-chan StepOutcome[R]) (R, error) {
	if ctx == nil {
		var zero R
		return zero, errors.New("ctx cannot be nil")
	}

	// If channels slice is empty, log warning and return zero value
	if len(channels) == 0 {
		if c, ok := ctx.(*dbosContext); ok {
			c.logger.Warn("Select called with empty channels slice, returning zero value")
		}
		var zero R
		return zero, nil
	}

	// Convert typed channels to any channels for internal processing
	// Create a context that will be cancelled when Select completes to prevent goroutine leaks
	selectCtx, cancelSelect := context.WithCancel(ctx)
	defer cancelSelect()

	anyChannels := make([]<-chan StepOutcome[any], len(channels))
	for i := range channels {
		anyCh := make(chan StepOutcome[any], cap(channels[i]))
		srcCh := channels[i]
		go func() {
			defer close(anyCh)
			for {
				select {
				case <-selectCtx.Done():
					return
				case outcome, ok := <-srcCh:
					if !ok {
						// Source channel closed
						return
					}
					select {
					case anyCh <- StepOutcome[any]{
						Result: outcome.Result,
						Err:    outcome.Err,
					}:
					case <-selectCtx.Done():
						// Select completed while trying to send, discard value
						return
					}
				}
			}
		}()
		anyChannels[i] = anyCh
	}

	result, err := ctx.Select(anyChannels)
	// Step function could return a nil result
	if result == nil {
		return *new(R), err
	}
	typedResult, convertErr := convertStepResult[R](ctx, result)
	if convertErr != nil {
		return *new(R), convertErr
	}
	return typedResult, err
}

func (c *dbosContext) Select(channels []<-chan StepOutcome[any]) (any, error) {
	// If channels slice is empty, log warning and return zero value
	if len(channels) == 0 {
		c.logger.Warn("Select called with empty channels slice, returning zero value")
		return nil, nil
	}

	// Use RunAsStep to wrap the select operation
	result, err := c.RunAsStep(func(ctx context.Context) (any, error) {
		// Build select cases using reflect.Select
		cases := make([]reflect.SelectCase, 0, len(channels)+1)

		// Add context cancellation case first (highest priority)
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ctx.Done()),
		})

		// Add all channel cases
		for _, ch := range channels {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ch),
			})
		}

		// Perform the select
		chosen, value, ok := reflect.Select(cases)

		// Handle context cancellation (chosen == 0 means context.Done() was selected)
		if chosen == 0 {
			return nil, ctx.Err()
		}

		// Check if channel was closed
		if !ok {
			// Adjust index since context case is at index 0
			selectedIndex := chosen - 1
			// If context was cancelled, return cancellation error instead of channel closed error
			// This handles the race condition after a closed channel (due to cancellation) is selected
			// instead of context.Done() (both are eligible to be selected).
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("channel at index %d was closed", selectedIndex)
		}

		// Extract the StepOutcome[any] from the reflect.Value
		outcomeValue := value.Interface()
		outcome, ok := outcomeValue.(StepOutcome[any])
		if !ok {
			// Adjust index since context case is at index 0
			selectedIndex := chosen - 1
			return nil, fmt.Errorf("unexpected value type from channel at index %d: expected StepOutcome[any], got %T", selectedIndex, outcomeValue)
		}

		return outcome.Result, outcome.Err
	}, WithStepName("DBOS.select"))

	// Return both result and error, similar to RunAsStep
	// The step function can return both a result and an error
	return result, err
}

/****************************************/
/******* WORKFLOW COMMUNICATIONS ********/
/****************************************/

// sendOptions holds configuration for a Send call.
type sendOptions struct {
	usePortableSerializer bool
}

// SendOption is a functional option for configuring a Send call.
type SendOption func(*sendOptions)

// WithPortableSend configures Send to use the portable JSON serializer,
// enabling cross-language interoperability regardless of the workflow's serializer.
func WithPortableSend() SendOption {
	return func(opts *sendOptions) {
		opts.usePortableSerializer = true
	}
}

func (c *dbosContext) Send(destinationID string, message any, topic string, opts ...SendOption) error {
	// Send cannot be sent from within a step if used within a workflow
	isWithinWorkflow := false
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if ok && wfState != nil {
		isWithinWorkflow = true
		if wfState.isWithinStep {
			return newStepExecutionError(wfState.workflowID, "DBOS.send", fmt.Errorf("cannot call Send within a step"))
		}
	}

	options := &sendOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var sendSer Serializer[any]
	if options.usePortableSerializer {
		sendSer = newPortableSerializer[any]()
	} else {
		sendSer = resolveEncoder(c)
	}

	encodedMessage, err := sendSer.Encode(message)
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	input := WorkflowSendInput{
		DestinationID: destinationID,
		Message:       encodedMessage,
		Topic:         topic,
		serialization: sendSer.Name(),
	}

	if isWithinWorkflow {
		_, err = runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
			input.tx = tx
			return nil, ctx.(*dbosContext).kernel.send(ctx, input)
		}, WithStepName("DBOS.send"))
	} else {
		err = retry(c, func() error {
			return c.kernel.send(c, input)
		}, withRetrierLogger(c.logger))
	}
	return err
}

// Send sends a message to another workflow with type safety.
//
// Send can be called from within a workflow (as a durable step) or from outside workflows.
// When called within a workflow, the send operation becomes part of the workflow's durable state.
//
// Example:
//
//	err := dbos.Send(ctx, "target-workflow-id", "Hello from sender", "notifications")
func Send[P any](ctx DBOSContext, destinationID string, message P, topic string, opts ...SendOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.Send(destinationID, message, topic, opts...)
}

type recvInput struct {
	Topic         string        // Topic to listen for (empty string receives from default topic)
	Timeout       time.Duration // Maximum time to wait for a message
	serialization string        // fallback serialization format (receiver's) for recording when no message is found
}

// recvResult carries the received message along with its serialization format from the notifications table.
type recvResult struct {
	message       *string
	serialization string
}

func (c *dbosContext) Recv(topic string, timeout time.Duration) (any, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, newStepExecutionError("", "DBOS.recv", fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}
	if wfState.isWithinStep {
		return nil, newStepExecutionError(wfState.workflowID, "DBOS.recv", fmt.Errorf("cannot call Recv within a step"))
	}
	input := recvInput{
		Topic:         topic,
		Timeout:       timeout,
		serialization: resolveEncoder(c).Name(),
	}
	return retryWithResult(c, func() (*recvResult, error) {
		return c.kernel.recv(c, input)
	}, withRetrierLogger(c.logger))
}

// Recv receives a message sent to this workflow with type safety.
// This function blocks until a message is received or the timeout is reached.
// Messages are consumed in FIFO order and each message is delivered exactly once.
//
// Recv can only be called from within a workflow and becomes part of the workflow's durable state.
//
// Example:
//
//	message, err := dbos.Recv[string](ctx, "notifications", 30 * time.Second)
//	if err != nil {
//	    // Handle timeout or error
//	    return err
//	}
//	log.Printf("Received: %s", message)
func Recv[R any](ctx DBOSContext, topic string, timeout time.Duration) (R, error) {
	if ctx == nil {
		return *new(R), errors.New("ctx cannot be nil")
	}
	msg, err := ctx.Recv(topic, timeout)
	if err != nil {
		return *new(R), err
	}

	// Handle nil message
	if msg == nil {
		return *new(R), nil
	}

	result, ok := msg.(*recvResult)
	if !ok {
		workflowID, _ := GetWorkflowID(ctx) // Must be within a workflow so we can ignore the error
		return *new(R), newWorkflowUnexpectedResultType(workflowID, "*recvResult", fmt.Sprintf("%T", msg))
	}
	if result.message == nil {
		return *new(R), nil
	}
	msgDecoder, resolveErr := resolveDecoder[R](result.serialization, getCustomSerializerFromCtx(ctx))
	if resolveErr != nil {
		return *new(R), resolveErr
	}
	typedMessage, decodeErr := msgDecoder.Decode(result.message)
	if decodeErr != nil {
		return *new(R), fmt.Errorf("decoding received message to type %T: %w", *new(R), decodeErr)
	}
	return typedMessage, nil
}

// setEventOptions holds configuration for a SetEvent call.
type setEventOptions struct {
	usePortableSerializer bool
}

// SetEventOption is a functional option for configuring a SetEvent call.
type SetEventOption func(*setEventOptions)

// WithPortableSetEvent configures SetEvent to use the portable JSON serializer,
// enabling cross-language interoperability regardless of the workflow's serializer.
func WithPortableSetEvent() SetEventOption {
	return func(opts *setEventOptions) {
		opts.usePortableSerializer = true
	}
}

func (c *dbosContext) SetEvent(key string, message any, opts ...SetEventOption) error {
	options := &setEventOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var evtSer Serializer[any]
	if options.usePortableSerializer {
		evtSer = newPortableSerializer[any]()
	} else {
		evtSer = resolveEncoder(c)
	}

	encodedMessage, err := evtSer.Encode(message)
	if err != nil {
		return fmt.Errorf("failed to serialize event value: %w", err)
	}

	_, err = runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
		return nil, c.kernel.setEvent(ctx, WorkflowSetEventInput{
			Key:           key,
			Message:       encodedMessage,
			tx:            tx,
			serialization: evtSer.Name(),
		})
	}, WithStepName("DBOS.setEvent"))
	return err
}

// SetEvent sets a key-value event for the current workflow with type safety.
// Events are persistent and can be retrieved by other workflows using GetEvent.
//
// SetEvent can only be called from within a workflow and becomes part of the workflow's durable state.
// Setting an event with the same key will overwrite the previous value.
//
// Example:
//
//	err := dbos.SetEvent(ctx, "status", "processing-complete")
func SetEvent[P any](ctx DBOSContext, key string, message P, opts ...SetEventOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.SetEvent(key, message, opts...)
}

type getEventInput struct {
	TargetWorkflowID string        // WorkflowFn ID to get the event from
	Key              string        // Event key to retrieve
	Timeout          time.Duration // Maximum time to wait for the event to be set
	serialization    string        // fallback serialization format (caller's) for recording when no event is found
}

// getEventResult carries the event value along with its serialization format from the workflow_events table.
type getEventResult struct {
	value         *string
	serialization string
}

func (c *dbosContext) GetEvent(targetWorkflowID, key string, timeout time.Duration) (any, error) {
	input := getEventInput{
		TargetWorkflowID: targetWorkflowID,
		Key:              key,
		Timeout:          timeout,
		serialization:    resolveEncoder(c).Name(),
	}
	return retryWithResult(c, func() (*getEventResult, error) {
		return c.kernel.getEvent(c, input)
	}, withRetrierLogger(c.logger))
}

// GetEvent retrieves a key-value event from a target workflow with type safety.
// This function blocks until the event is set or the timeout is reached.
//
// When called within a workflow, the get operation becomes part of the workflow's durable state.
// The returned value is of type R and will be type-checked at runtime.
//
// Example:
//
//	status, err := dbos.GetEvent[string](ctx, "target-workflow-id", "status", 30 * time.Second)
//	if err != nil {
//	    // Handle timeout or error
//	    return err
//	}
//	log.Printf("Status: %s", status)
func GetEvent[R any](ctx DBOSContext, targetWorkflowID, key string, timeout time.Duration) (R, error) {
	if ctx == nil {
		return *new(R), errors.New("ctx cannot be nil")
	}
	value, err := ctx.GetEvent(targetWorkflowID, key, timeout)
	if err != nil {
		return *new(R), err
	}
	if value == nil {
		return *new(R), nil
	}

	var typedValue R
	// Check if we're in a real DBOS context (not a mock)
	if _, ok := ctx.(*dbosContext); ok {
		result, ok := value.(*getEventResult)
		if !ok {
			workflowID, _ := GetWorkflowID(ctx) // Must be within a workflow so we can ignore the error
			return *new(R), newWorkflowUnexpectedResultType(workflowID, "*getEventResult", fmt.Sprintf("%T", value))
		}
		if result.value == nil {
			return *new(R), nil
		}
		evtDecoder, resolveErr := resolveDecoder[R](result.serialization, getCustomSerializerFromCtx(ctx))
		if resolveErr != nil {
			return *new(R), resolveErr
		}
		var decodeErr error
		typedValue, decodeErr = evtDecoder.Decode(result.value)
		if decodeErr != nil {
			return *new(R), fmt.Errorf("decoding event value to type %T: %w", *new(R), decodeErr)
		}
		return typedValue, nil
	} else {
		var ok bool
		typedValue, ok = value.(R)
		if !ok {
			workflowID, _ := GetWorkflowID(ctx) // Must be within a workflow so we can ignore the error
			return *new(R), newWorkflowUnexpectedResultType(workflowID, fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", value))
		}
	}
	return typedValue, nil
}

// writeStreamOptions holds configuration for a WriteStream call.
type writeStreamOptions struct {
	usePortableSerializer bool
}

// WriteStreamOption is a functional option for configuring a WriteStream call.
type WriteStreamOption func(*writeStreamOptions)

// WithPortableWriteStream configures WriteStream to use the portable JSON serializer,
// enabling cross-language interoperability regardless of the workflow's serializer.
func WithPortableWriteStream() WriteStreamOption {
	return func(opts *writeStreamOptions) {
		opts.usePortableSerializer = true
	}
}

func (c *dbosContext) WriteStream(key string, value any, opts ...WriteStreamOption) error {
	options := &writeStreamOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var ser Serializer[any]
	if options.usePortableSerializer {
		ser = newPortableSerializer[any]()
	} else {
		ser = resolveEncoder(c)
	}

	encodedValue, err := ser.Encode(value)
	if err != nil {
		return fmt.Errorf("failed to serialize stream value: %w", err)
	}

	_, err = runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
		return "", c.kernel.writeStream(ctx, writeStreamDBInput{
			Key:           key,
			Value:         encodedValue,
			tx:            tx,
			serialization: ser.Name(),
		})
	}, WithStepName("DBOS.writeStream"))
	return err
}

// WriteStream writes a value to a durable stream with type safety.
// Streams are append-only and ordered by offset.
//
// WriteStream can only be called from within a workflow and becomes part of the workflow's durable state.
//
// Example:
//
//	err := dbos.WriteStream(ctx, "my-stream", "stream-value")
func WriteStream[P any](ctx DBOSContext, key string, value P, opts ...WriteStreamOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.WriteStream(key, value, opts...)
}

type ReadStreamOption func(*readStreamOptions)

type readStreamOptions struct {
	snapshot   bool
	fromOffset int
}

// WithReadStreamSnapshot makes a stream read return as soon as all currently-available
// values have been drained, instead of blocking until the stream is closed or
// the workflow becomes inactive. fromOffset sets the base offset to read from.
func WithReadStreamSnapshot(fromOffset int) ReadStreamOption {
	return func(o *readStreamOptions) {
		o.snapshot = true
		o.fromOffset = fromOffset
	}
}

// readStream runs the read stream polling logic in a goroutine
// and sends values through a channel as they're read
func (c *dbosContext) readStream(workflowID string, key string, snapshot bool, fromOffset int) <-chan StreamValue[any] {
	ch := make(chan StreamValue[any], 1) // Buffered to allow non-blocking sends

	go func() {
		defer close(ch)

		// send delivers v to ch, returning false if the context is cancelled first.
		// This prevents the goroutine from leaking when the consumer stops reading.
		send := func(v StreamValue[any]) bool {
			select {
			case ch <- v:
				return true
			case <-c.Done():
				return false
			}
		}

		currentOffset := fromOffset
		closed := false

		// Continue reading until workflow is inactive or stream is closed
		for {
			// Read stream entries from current offset
			input := readStreamDBInput{
				WorkflowID: workflowID,
				Key:        key,
				FromOffset: currentOffset,
			}

			var entries []streamEntry
			err := retry(c, func() error {
				var retryErr error
				entries, closed, retryErr = c.kernel.readStream(c, input)
				return retryErr
			}, withRetrierLogger(c.logger))

			if err != nil {
				send(StreamValue[any]{Err: err})
				return
			}

			// Send each entry value to the channel
			for _, entry := range entries {
				if !send(StreamValue[any]{Value: streamEntryWithSerialization{value: entry.Value, serialization: entry.Serialization}}) {
					return
				}
				currentOffset = entry.Offset + 1 // Next offset to read from
			}

			// If stream is closed (sentinel found), send final message and stop
			if closed {
				send(StreamValue[any]{Closed: true})
				return
			}

			// Snapshot mode: all currently-available values have been drained,
			// so stop here instead of polling for more.
			if snapshot {
				return
			}

			// Check if workflow is still active (PENDING or ENQUEUED)
			status, err := retryWithResult(c, func() (WorkflowStatusType, error) {
				workflows, err := c.kernel.listWorkflows(c, listWorkflowsDBInput{
					workflowIDs: []string{workflowID},
					loadInput:   false,
					loadOutput:  false,
				})
				if err != nil {
					return "", err
				}
				if len(workflows) == 0 {
					return "", newNonExistentWorkflowError(workflowID)
				}
				return workflows[0].Status, nil
			}, withRetrierLogger(c.logger))

			if err != nil {
				send(StreamValue[any]{Err: err})
				return
			}

			// If workflow is inactive, send final message with Closed: true (BUG FIX)
			if status != WorkflowStatusPending && status != WorkflowStatusEnqueued {
				send(StreamValue[any]{Closed: true})
				return
			}

			// If no new entries, wait a bit before polling again
			if len(entries) == 0 {
				select {
				case <-c.Done():
					send(StreamValue[any]{Err: c.Err()})
					return
				case <-time.After(_DB_RETRY_INTERVAL):
					// Continue loop to read again
				}
			}
		}
	}()

	return ch
}

// streamEntryWithSerialization wraps a stream value with its stored serialization format.
type streamEntryWithSerialization struct {
	value         string
	serialization string
}

func (c *dbosContext) ReadStream(workflowID string, key string, opts ...ReadStreamOption) ([]any, bool, error) {
	var o readStreamOptions
	for _, opt := range opts {
		opt(&o)
	}

	var allValues []any
	closed := false

	ch := c.readStream(workflowID, key, o.snapshot, o.fromOffset)

	for streamValue := range ch {
		if streamValue.Err != nil {
			return nil, false, streamValue.Err
		}

		if streamValue.Closed {
			closed = true
			break
		}

		// Collect the value
		allValues = append(allValues, streamValue.Value)
	}

	return allValues, closed, nil
}

// ReadStream reads values from a durable stream.
// This method blocks until the stream is closed or an error occurs.
// The stream is considered close when the sentinel value is found or the workflow becomes inactive (status is not PENDING or ENQUEUED)
//
// Returns the values, whether the stream is closed, and any error.
//
// Example:
//
//	values, closed, err := dbos.ReadStream[string](ctx, "workflow-id", "my-stream")
//	if err != nil {
//	    return err
//	}
//	for _, value := range values {
//	    log.Printf("Stream value: %s", value)
//	}
func ReadStream[R any](ctx DBOSContext, workflowID string, key string, opts ...ReadStreamOption) ([]R, bool, error) {
	if ctx == nil {
		return nil, false, errors.New("ctx cannot be nil")
	}
	values, closed, err := ctx.ReadStream(workflowID, key, opts...)
	if err != nil {
		return nil, false, err
	}

	// Decode each value using the serialization stored with that stream entry.
	typedValues := make([]R, len(values))
	if _, ok := ctx.(*dbosContext); ok {
		customSer := getCustomSerializerFromCtx(ctx)
		for i, val := range values {
			entry, ok := val.(streamEntryWithSerialization)
			if !ok {
				return nil, false, fmt.Errorf("stream value is not streamEntryWithSerialization, got %T", val)
			}
			decoder, resolveErr := resolveDecoder[R](entry.serialization, customSer)
			if resolveErr != nil {
				return nil, false, resolveErr
			}
			decodedValue, decodeErr := decoder.Decode(&entry.value)
			if decodeErr != nil {
				return nil, false, fmt.Errorf("decoding stream value to type %T: %w", *new(R), decodeErr)
			}
			typedValues[i] = decodedValue
		}
	} else {
		// Fallback for testing/mocking scenarios
		for i, val := range values {
			typedVal, ok := val.(R)
			if !ok {
				return nil, false, fmt.Errorf("stream value is not %T, got %T", *new(R), val)
			}
			typedValues[i] = typedVal
		}
	}

	return typedValues, closed, nil
}

// ReadStreamAsync reads values from a durable stream asynchronously.
// Returns a channel that will receive StreamValue items as they're read.
func (c *dbosContext) ReadStreamAsync(workflowID string, key string) (<-chan StreamValue[any], error) {
	return c.readStream(workflowID, key, false, 0), nil
}

// ReadStreamAsync reads values from a durable stream asynchronously.
// Returns a channel that will receive StreamValue items as they're read.
//
// This method returns immediately with a channel. Values will be sent to the channel
// as they're read from the stream. The channel will be closed when the stream is closed or an error occurs.
// The stream is considered close when the sentinel value is found or the workflow becomes inactive (status is not PENDING or ENQUEUED)
//
// Example:
//
//	ch, err := dbos.ReadStreamAsync[string](ctx, "workflow-id", "my-stream")
//	if err != nil {
//	    return err
//	}
//	for streamValue := range ch {
//	    if streamValue.Err != nil {
//	        log.Printf("Error: %v", streamValue.Err)
//	        break
//	    }
//	    if streamValue.Closed {
//	        log.Println("Stream closed")
//	        break
//	    }
//	    log.Printf("Received value: %s", streamValue.Value)
//	}
func ReadStreamAsync[R any](ctx DBOSContext, workflowID string, key string) (<-chan StreamValue[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	anyCh, err := ctx.ReadStreamAsync(workflowID, key)
	if err != nil {
		return nil, err
	}

	typedCh := make(chan StreamValue[R], 1)

	_, isReal := ctx.(*dbosContext)

	go func() {
		defer close(typedCh)

		send := func(v StreamValue[R]) bool {
			select {
			case typedCh <- v:
				return true
			case <-ctx.Done():
				return false
			}
		}

		customSer := getCustomSerializerFromCtx(ctx)

		for streamValue := range anyCh {
			if streamValue.Err != nil {
				send(StreamValue[R]{Err: streamValue.Err})
				return
			}

			if streamValue.Closed {
				send(StreamValue[R]{Closed: true})
				return
			}

			if isReal {
				entry, ok := streamValue.Value.(streamEntryWithSerialization)
				if !ok {
					send(StreamValue[R]{Err: fmt.Errorf("stream value is not streamEntryWithSerialization, got %T", streamValue.Value)})
					return
				}

				asyncDecoder, resolveErr := resolveDecoder[R](entry.serialization, customSer)
				if resolveErr != nil {
					send(StreamValue[R]{Err: resolveErr})
					return
				}

				decodedValue, decodeErr := asyncDecoder.Decode(&entry.value)
				if decodeErr != nil {
					send(StreamValue[R]{Err: fmt.Errorf("decoding stream value to type %T: %w", *new(R), decodeErr)})
					return
				}

				if !send(StreamValue[R]{Value: decodedValue}) {
					return
				}
			} else {
				// Fallback for testing/mocking scenarios
				typedVal, ok := streamValue.Value.(R)
				if !ok {
					send(StreamValue[R]{Err: fmt.Errorf("stream value is not %T, got %T", *new(R), streamValue.Value)})
					return
				}
				if !send(StreamValue[R]{Value: typedVal}) {
					return
				}
			}
		}
	}()

	return typedCh, nil
}

func (c *dbosContext) CloseStream(key string) error {
	_, err := runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
		sentinel := _DBOS_STREAM_CLOSED_SENTINEL
		return "", c.kernel.writeStream(ctx, writeStreamDBInput{
			Key:   key,
			Value: &sentinel,
			tx:    tx,
		})
	}, WithStepName("DBOS.closeStream"))
	return err
}

// CloseStream closes a durable stream by writing the sentinel value.
//
// CloseStream can only be called from within a workflow and becomes part of the workflow's durable state.
//
// Example:
//
//	err := dbos.CloseStream(ctx, "my-stream")
//	if err != nil {
//	    return err
//	}
func CloseStream(ctx DBOSContext, key string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.CloseStream(key)
}

func (c *dbosContext) Sleep(duration time.Duration) (time.Duration, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return 0, newStepExecutionError("", "DBOS.sleep", fmt.Errorf("workflow state not found in context: are you running this step within a workflow?"))
	}
	if wfState.isWithinStep {
		return 0, newStepExecutionError(wfState.workflowID, "DBOS.sleep", fmt.Errorf("cannot call Sleep within a step"))
	}
	return retryWithResult(c, func() (time.Duration, error) {
		return c.kernel.sleep(c, sleepInput{duration: duration, skipSleep: false})
	}, withRetrierLogger(c.logger))
}

// Sleep pauses workflow execution for the specified duration.
// This is a durable sleep - if the workflow is recovered during the sleep period,
// it will continue sleeping for the remaining time.
// Returns the actual duration slept.
//
// Example:
//
//	actualDuration, err := dbos.Sleep(ctx, 5*time.Second)
//	if err != nil {
//	    return err
//	}
func Sleep(ctx DBOSContext, duration time.Duration) (time.Duration, error) {
	if ctx == nil {
		return 0, errors.New("ctx cannot be nil")
	}
	return ctx.Sleep(duration)
}

const _DBOS_PATCH_PREFIX = "DBOS.patch-"

func (c *dbosContext) Patch(patchName string) (bool, error) {
	if !c.config.EnablePatching {
		return false, newPatchingNotEnabledError()
	}

	if patchName == "" {
		return false, errors.New("patch name cannot be empty")
	}

	// Get workflow state to determine current step ID
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return false, errors.New("patch can only be called within a workflow")
	}

	if wfState.isWithinStep {
		return false, newStepExecutionError(wfState.workflowID, patchName, fmt.Errorf("cannot call Patch within a step"))
	}

	// Automatically prefix the patch name with _DBOS_PATCH_PREFIX
	prefixedPatchName := _DBOS_PATCH_PREFIX + patchName

	patched, err := retryWithResult(c, func() (bool, error) {
		return c.kernel.patch(c, patchDBInput{
			workflowID: wfState.workflowID,
			stepID:     wfState.stepID + 1, // We are checking if the upcoming step should use the patched code
			patchName:  prefixedPatchName,
		})
	}, withRetrierLogger(c.logger))

	if patched && err == nil {
		// The patch take its own step ID
		wfState.nextStepID()
	}

	return patched, err
}

// Patch checks if the current workflow should use patched code.
// Returns true if the workflow should use new code, false if it should use old code.
//
// The patch system allows modifying code while long-lived workflows are running:
// - Existing workflows that already passed this patch point continue with old code
// - New workflows use new code
// - Workflows that started but haven't reached this point yet use new code
//
// Example:
//
//	if dbos.Patch(ctx, "my-patch") {
//	    // New code path
//	} else {
//	    // Old code path
//	}
func Patch(ctx DBOSContext, patchName string) (bool, error) {
	if ctx == nil {
		return false, errors.New("ctx cannot be nil")
	}
	return ctx.Patch(patchName)
}

func (c *dbosContext) DeprecatePatch(patchName string) error {
	if !c.config.EnablePatching {
		return newPatchingNotEnabledError()
	}

	if patchName == "" {
		return errors.New("patch name cannot be empty")
	}

	// Get workflow state to determine current step ID
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return errors.New("deprecate patch can only be called within a workflow")
	}

	if wfState.isWithinStep {
		return newStepExecutionError(wfState.workflowID, patchName, fmt.Errorf("cannot call DeprecatePatch within a step"))
	}

	// Automatically prefix the patch name with _DBOS_PATCH_PREFIX
	prefixedPatchName := _DBOS_PATCH_PREFIX + patchName

	patchNameFromDB, err := retryWithResult(c, func() (string, error) {
		return c.kernel.doesPatchExists(c, patchDBInput{
			workflowID: wfState.workflowID,
			stepID:     wfState.stepID + 1,
			patchName:  prefixedPatchName,
		})
	}, withRetrierLogger(c.logger))

	// If patch doesn't exist, it's already deprecated (or never existed)
	if patchNameFromDB != prefixedPatchName || err == pgx.ErrNoRows {
		return nil
	}

	// If there was an error checking, return it
	if err != nil {
		return err
	}

	// Patch exists, deprecate it by incrementing step ID
	wfState.nextStepID()
	return nil
}

// DeprecatePatch allows removing patches from code while ensuring the correct history
// of workflows that were executing before the patch was deprecated.
//
// Example:
//
// err := dbos.DeprecatePatch(ctx, "my-patch")
//
//	if err != nil {
//	    return err
//	}
//
// // New code path
func DeprecatePatch(ctx DBOSContext, patchName string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.DeprecatePatch(patchName)
}

/***********************************/
/******* WORKFLOW MANAGEMENT *******/
/***********************************/

func (c *dbosContext) GetWorkflowID() (string, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return "", errors.New("not within a DBOS workflow context")
	}
	return wfState.workflowID, nil
}

func (c *dbosContext) GetStepID() (int, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return -1, errors.New("not within a DBOS workflow context")
	}
	return wfState.stepID, nil
}

// GetWorkflowID retrieves the workflow ID from the context if called within a DBOS workflow.
// Returns an error if not called from within a workflow context.
//
// Example:
//
//	workflowID, err := dbos.GetWorkflowID(ctx)
//	if err != nil {
//	    log.Printf("Not within a workflow context")
//	} else {
//	    log.Printf("Current workflow ID: %s", workflowID)
//	}
func GetWorkflowID(ctx DBOSContext) (string, error) {
	if ctx == nil {
		return "", errors.New("ctx cannot be nil")
	}
	return ctx.GetWorkflowID()
}

// GetStepID retrieves the current step ID from the context if called within a DBOS workflow.
// Returns -1 and an error if not called from within a workflow context.
//
// Example:
//
//	stepID, err := dbos.GetStepID(ctx)
//	if err != nil {
//	    log.Printf("Not within a workflow context")
//	} else {
//	    log.Printf("Current step ID: %d", stepID)
//	}
func GetStepID(ctx DBOSContext) (int, error) {
	if ctx == nil {
		return -1, errors.New("ctx cannot be nil")
	}
	return ctx.GetStepID()
}

func (c *dbosContext) RetrieveWorkflow(workflowID string) (*WorkflowHandle[any], error) {
	loadInput := false
	loadOutput := false
	if c.launched.Load() {
		loadInput = false
		loadOutput = false
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var workflowStatus []WorkflowStatus
	var err error
	if isWithinWorkflow {
		workflowStatus, err = Run(c, func(ctx context.Context) ([]WorkflowStatus, error) {
			return retryWithResult(ctx, func() ([]WorkflowStatus, error) {
				return c.kernel.listWorkflows(ctx, listWorkflowsDBInput{
					workflowIDs: []string{workflowID},
					loadInput:   loadInput,
					loadOutput:  loadOutput,
				})
			}, withRetrierLogger(c.logger))
		}, WithStepName("DBOS.retrieveWorkflow"))
	} else {
		workflowStatus, err = retryWithResult(c, func() ([]WorkflowStatus, error) {
			return c.kernel.listWorkflows(c, listWorkflowsDBInput{
				workflowIDs: []string{workflowID},
				loadInput:   loadInput,
				loadOutput:  loadOutput,
			})
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve workflow status: %w", err)
	}
	if len(workflowStatus) == 0 {
		return nil, newNonExistentWorkflowError(workflowID)
	}
	return newWorkflowHandle[any](c, workflowID), nil
}

// RetrieveWorkflow returns a typed handle to an existing workflow.
// The handle can be used to check status and wait for results.
// The type parameter R must match the workflow's actual return type.
//
// Example:
//
//	handle, err := dbos.RetrieveWorkflow[int](ctx, "workflow-id")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	result, err := handle.GetResult()
//	if err != nil {
//	    log.Printf("Workflow failed: %v", err)
//	} else {
//	    log.Printf("Result: %d", result)
//	}
func RetrieveWorkflow[R any](ctx DBOSContext, workflowID string) (*WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("dbosCtx cannot be nil")
	}

	// Call the interface method
	handle, err := ctx.(*dbosContext).RetrieveWorkflow(workflowID)
	if err != nil {
		return nil, err
	}

	// Convert to typed polling handle
	return newWorkflowHandle[R](ctx, handle.GetWorkflowID()), nil
}

func (c *dbosContext) CancelWorkflow(workflowID string) error {
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var found []string
	var err error
	if isWithinWorkflow {
		found, err = runAsTxn(c, func(ctx context.Context, tx Tx) ([]string, error) {
			return c.kernel.cancelWorkflows(ctx, cancelWorkflowsDBInput{workflowIDs: []string{workflowID}, tx: tx})
		}, WithStepName("DBOS.cancelWorkflow"))
	} else {
		found, err = retryWithResult(c, func() ([]string, error) {
			return c.kernel.cancelWorkflows(c, cancelWorkflowsDBInput{workflowIDs: []string{workflowID}})
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return err
	}
	if len(found) == 0 {
		return newNonExistentWorkflowError(workflowID)
	}
	return nil
}

// CancelWorkflow cancels a running or enqueued workflow by setting its status to CANCELLED and removing it from the queue.
// Once cancelled, the workflow will stop executing at the start of the next step. Executing steps will not be interrupted.
//
// Parameters:
//   - ctx: DBOS context for the operation
//   - workflowID: The unique identifier of the workflow to cancel
//
// Returns an error if the workflow does not exist or if the cancellation operation fails.
//
// Example:
//
//	err := dbos.CancelWorkflow(ctx, "workflow-to-cancel")
//	if err != nil {
//	    log.Printf("Failed to cancel workflow: %v", err)
//	}
func CancelWorkflow(ctx DBOSContext, workflowID string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).CancelWorkflow(workflowID)
}

func (c *dbosContext) CancelWorkflows(workflowIDs []string) error {
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		_, err := runAsTxn(c, func(ctx context.Context, tx Tx) ([]string, error) {
			return c.kernel.cancelWorkflows(ctx, cancelWorkflowsDBInput{workflowIDs: workflowIDs, tx: tx})
		}, WithStepName("DBOS.cancelWorkflows"))
		return err
	}
	_, err := retryWithResult(c, func() ([]string, error) {
		return c.kernel.cancelWorkflows(c, cancelWorkflowsDBInput{workflowIDs: workflowIDs})
	}, withRetrierLogger(c.logger))
	return err
}

// CancelWorkflows cancels multiple workflows in a single database round-trip.
// Each workflow that exists and is not already in a terminal state (SUCCESS, ERROR, CANCELLED)
// is moved to CANCELLED and removed from its queue. Missing or already-terminal IDs are silently
// skipped. Unlike the singular CancelWorkflow, this function does not return
// NonExistentWorkflowError when some IDs are missing.
//
// Example:
//
//	err := dbos.CancelWorkflows(ctx, []string{"wf-1", "wf-2"})
//	if err != nil {
//	    log.Fatal(err)
//	}
func CancelWorkflows(ctx DBOSContext, workflowIDs []string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).CancelWorkflows(workflowIDs)
}

// SetWorkflowDelayOption configures how the delay is set on a workflow.
type SetWorkflowDelayOption func(*setWorkflowDelayOptions)

type setWorkflowDelayOptions struct {
	delay      time.Duration
	delayUntil time.Time
}

// WithDelayDuration sets a relative delay from now.
func WithDelayDuration(d time.Duration) SetWorkflowDelayOption {
	return func(o *setWorkflowDelayOptions) {
		o.delay = d
	}
}

// WithDelayUntil sets an absolute time until which the workflow should remain delayed.
func WithDelayUntil(t time.Time) SetWorkflowDelayOption {
	return func(o *setWorkflowDelayOptions) {
		o.delayUntil = t
	}
}

func resolveDelayUntil(opts []SetWorkflowDelayOption) (time.Time, error) {
	params := &setWorkflowDelayOptions{}
	for _, opt := range opts {
		opt(params)
	}
	hasDelay := params.delay > 0
	hasUntil := !params.delayUntil.IsZero()
	if hasDelay && hasUntil {
		return time.Time{}, errors.New("specify either WithDelayDuration or WithDelayUntil, not both")
	}
	if !hasDelay && !hasUntil {
		return time.Time{}, errors.New("must specify either WithDelayDuration or WithDelayUntil")
	}
	if hasDelay {
		return time.Now().Add(params.delay), nil
	}
	return params.delayUntil, nil
}

func (c *dbosContext) SetWorkflowDelay(workflowID string, opts ...SetWorkflowDelayOption) error {
	delayUntil, err := resolveDelayUntil(opts)
	if err != nil {
		return err
	}
	input := setWorkflowDelayDBInput{workflowID: workflowID, delayUntil: delayUntil}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		_, err := runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
			input.tx = tx
			return nil, c.kernel.setWorkflowDelay(ctx, input)
		}, WithStepName("DBOS.setWorkflowDelay"))
		return err
	}
	return retry(c, func() error {
		return c.kernel.setWorkflowDelay(c, input)
	}, withRetrierLogger(c.logger))
}

// SetWorkflowDelay sets or updates the delay on a DELAYED workflow.
// Provide exactly one of WithDelayDuration (relative) or WithDelayUntil (absolute).
// Only affects workflows in the DELAYED status.
//
// Example:
//
//	err := dbos.SetWorkflowDelay(ctx, workflowID, dbos.WithDelayDuration(5*time.Second))
//	err := dbos.SetWorkflowDelay(ctx, workflowID, dbos.WithDelayUntil(time.Now().Add(10*time.Minute)))
func SetWorkflowDelay(ctx DBOSContext, workflowID string, opts ...SetWorkflowDelayOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).SetWorkflowDelay(workflowID, opts...)
}

func (c *dbosContext) DeleteWorkflows(workflowIDs []string, opts ...DeleteWorkflowOption) error {
	// Process options
	params := &deleteWorkflowOptions{}
	for _, opt := range opts {
		opt(params)
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		_, err := runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
			err := c.kernel.deleteWorkflows(ctx, deleteWorkflowsDBInput{
				workflowIDs:    workflowIDs,
				deleteChildren: params.deleteChildren,
				tx:             tx,
			})
			return "", err
		}, WithStepName("DBOS.deleteWorkflows"))
		return err
	} else {
		return retry(c, func() error {
			return c.kernel.deleteWorkflows(c, deleteWorkflowsDBInput{
				workflowIDs:    workflowIDs,
				deleteChildren: params.deleteChildren,
			})
		}, withRetrierLogger(c.logger))
	}
}

// deleteWorkflowOptions holds configuration parameters for deleting workflows.
type deleteWorkflowOptions struct {
	deleteChildren bool
}

// DeleteWorkflowOption is a functional option for configuring workflow deletion.
type DeleteWorkflowOption func(*deleteWorkflowOptions)

// WithDeleteChildren enables recursive deletion of child workflows.
// When set, all child workflows (and their children, recursively) will be deleted
// along with the parent workflow.
func WithDeleteChildren() DeleteWorkflowOption {
	return func(o *deleteWorkflowOptions) {
		o.deleteChildren = true
	}
}

// DeleteWorkflows permanently deletes one or more workflows and all their associated data
// from the database, regardless of their current status. This includes active (PENDING, ENQUEUED) workflows.
//
// This operation is irreversible and removes the workflow status, operation outputs,
// events, event history, and streams associated with each workflow.
//
// Options:
//   - WithDeleteChildren: Also delete all child workflows recursively
//
// Parameters:
//   - ctx: DBOS context for the operation
//   - workflowIDs: The unique identifiers of the workflows to delete
//
// Example:
//
//	// Delete a single workflow
//	err := dbos.DeleteWorkflows(ctx, []string{"workflow-to-delete"})
//
//	// Delete workflows and all their children
//	err := dbos.DeleteWorkflows(ctx, []string{"wf1", "wf2"}, dbos.WithDeleteChildren())
func DeleteWorkflows(ctx DBOSContext, workflowIDs []string, opts ...DeleteWorkflowOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).DeleteWorkflows(workflowIDs, opts...)
}

// resumeWorkflowOptions holds configuration parameters for resuming workflows.
type resumeWorkflowOptions struct {
	queueName string
}

// ResumeWorkflowOption is a functional option for configuring workflow resumption.
type ResumeWorkflowOption func(*resumeWorkflowOptions)

// WithResumeQueue re-enqueues the resumed workflow(s) on the specified queue instead of the internal queue.
func WithResumeQueue(queueName string) ResumeWorkflowOption {
	return func(o *resumeWorkflowOptions) {
		o.queueName = queueName
	}
}

func (c *dbosContext) ResumeWorkflow(workflowID string, opts ...ResumeWorkflowOption) (*WorkflowHandle[any], error) {
	handles, err := c.ResumeWorkflows([]string{workflowID}, opts...)
	if err != nil {
		return nil, err
	}
	if len(handles) == 0 {
		return nil, newNonExistentWorkflowError(workflowID)
	}
	return handles[0], nil
}

func (c *dbosContext) ResumeWorkflows(workflowIDs []string, opts ...ResumeWorkflowOption) ([]*WorkflowHandle[any], error) {
	params := &resumeWorkflowOptions{}
	for _, opt := range opts {
		opt(params)
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var foundIDs []string
	var err error
	if isWithinWorkflow {
		foundIDs, err = runAsTxn(c, func(ctx context.Context, tx Tx) ([]string, error) {
			return c.kernel.resumeWorkflows(ctx, resumeWorkflowsDBInput{
				workflowIDs: workflowIDs,
				queueName:   params.queueName,
				tx:          tx,
			})
		}, WithStepName("DBOS.resumeWorkflow"))
	} else {
		foundIDs, err = retryWithResult(c, func() ([]string, error) {
			return c.kernel.resumeWorkflows(c, resumeWorkflowsDBInput{
				workflowIDs: workflowIDs,
				queueName:   params.queueName,
			})
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, err
	}

	handles := make([]*WorkflowHandle[any], 0, len(foundIDs))
	for _, id := range foundIDs {
		handles = append(handles, newWorkflowHandle[any](c, id))
	}
	return handles, nil
}

// ResumeWorkflow resumes a workflow by starting it from its last completed step.
// You can use this to resume workflows that are cancelled or have exceeded their maximum
// recovery attempts. You can also use this to start an enqueued workflow immediately,
// bypassing its queue.
// If the workflow is already completed, this is a no-op.
// Returns a handle that can be used to wait for completion and retrieve results.
// Returns an error if the workflow does not exist or if the operation fails.
//
// Options:
//   - WithResumeQueue: re-enqueue the workflow on a named queue instead of the internal queue.
//
// Example:
//
//	handle, err := dbos.ResumeWorkflow[int](ctx, "workflow-id")
//	if err != nil {
//	    log.Printf("Failed to resume workflow: %v", err)
//	} else {
//	    result, err := handle.GetResult()
//	    if err != nil {
//	        log.Printf("Workflow failed: %v", err)
//	    } else {
//	        log.Printf("Result: %d", result)
//	    }
//	}
func ResumeWorkflow[R any](ctx DBOSContext, workflowID string, opts ...ResumeWorkflowOption) (*WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	_, err := ctx.(*dbosContext).ResumeWorkflow(workflowID, opts...)
	if err != nil {
		return nil, err
	}
	return newWorkflowHandle[R](ctx, workflowID), nil
}

// ResumeWorkflows resumes multiple workflows in a single database round-trip. Each workflow
// that exists and is not in a terminal state is re-enqueued; completed or missing workflows
// are skipped.
//
// Unlike the singular ResumeWorkflow, this function does not return NonExistentWorkflowError
// when some IDs are missing.
//
// Options:
//   - WithResumeQueue: re-enqueue the workflows on a named queue instead of the internal queue.
//
// Example:
//
//	handles, err := dbos.ResumeWorkflows[int](ctx, []string{"wf-1", "wf-2"}, dbos.WithResumeQueue("priority"))
//	if err != nil {
//	    log.Fatal(err)
//	}
func ResumeWorkflows[R any](ctx DBOSContext, workflowIDs []string, opts ...ResumeWorkflowOption) ([]*WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	anyHandles, err := ctx.(*dbosContext).ResumeWorkflows(workflowIDs, opts...)
	if err != nil {
		return nil, err
	}
	handles := make([]*WorkflowHandle[R], 0, len(anyHandles))
	for _, h := range anyHandles {
		handles = append(handles, newWorkflowHandle[R](ctx, h.GetWorkflowID()))
	}
	return handles, nil
}

// ForkWorkflowInput holds configuration parameters for forking workflows.
// OriginalWorkflowID is required. Other fields are optional.
type ForkWorkflowInput struct {
	OriginalWorkflowID string // Required: The UUID of the original workflow to fork from
	ForkedWorkflowID   string // Optional: Custom workflow ID for the forked workflow (auto-generated if empty)
	StartStep          uint   // Optional: Step to start the forked workflow from (default: 0)
	ApplicationVersion string // Optional: Application version for the forked workflow (inherits from original if empty)
	QueueName          string // Optional: Queue to enqueue the forked workflow on (defaults to the internal queue)
	QueuePartitionKey  string // Optional: Partition key when enqueueing the forked workflow onto a partitioned queue
}

func (c *dbosContext) ForkWorkflow(input ForkWorkflowInput) (*WorkflowHandle[any], error) {
	if input.OriginalWorkflowID == "" {
		return nil, errors.New("original workflow ID cannot be empty")
	}
	if input.QueuePartitionKey != "" && input.QueueName == "" {
		return nil, errors.New("queue partition key requires a queue name")
	}

	// Create input for system database
	if input.StartStep > uint(math.MaxInt) {
		return nil, fmt.Errorf("start step too large: %d", input.StartStep)
	}
	dbInput := forkWorkflowDBInput{
		originalWorkflowID: input.OriginalWorkflowID,
		forkedWorkflowID:   input.ForkedWorkflowID,
		startStep:          int(input.StartStep),
		applicationVersion: input.ApplicationVersion,
		queueName:          input.QueueName,
		queuePartitionKey:  input.QueuePartitionKey,
	}

	// Call system database method
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var forkedWorkflowID string
	var err error
	if isWithinWorkflow {
		forkedWorkflowID, err = runAsTxn(c, func(ctx context.Context, tx Tx) (string, error) {
			dbInput.tx = tx
			return c.kernel.forkWorkflow(ctx, dbInput)
		}, WithStepName("DBOS.forkWorkflow"))
	} else {
		forkedWorkflowID, err = retryWithResult(c, func() (string, error) {
			return c.kernel.forkWorkflow(c, dbInput)
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, err
	}

	return newWorkflowHandle[any](c, forkedWorkflowID), nil
}

// ForkWorkflow creates a new workflow instance by copying an existing workflow from a specific step.
// The forked workflow will have a new UUID and will execute from the specified StartStep.
// If StartStep > 0, the forked workflow will reuse the operation outputs from steps 0 to StartStep-1
// copied from the original workflow.
//
// Parameters:
//   - ctx: DBOS context for the operation
//   - input: Configuration parameters for the forked workflow
//
// Returns a typed workflow handle for the newly created forked workflow.
//
// Example usage:
//
//	// Basic fork from step 5
//	handle, err := dbos.ForkWorkflow[MyResultType](ctx, dbos.ForkWorkflowInput{
//	    OriginalWorkflowID: "original-workflow-id",
//	    StartStep:          5,
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Fork with custom workflow ID and application version
//	handle, err := dbos.ForkWorkflow[MyResultType](ctx, dbos.ForkWorkflowInput{
//	    OriginalWorkflowID: "original-workflow-id",
//	    ForkedWorkflowID:   "my-custom-fork-id",
//	    StartStep:          3,
//	    ApplicationVersion: "v2.0.0",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Fork onto a named queue instead of the internal queue.
//	handle, err := dbos.ForkWorkflow[MyResultType](ctx, dbos.ForkWorkflowInput{
//	    OriginalWorkflowID: "original-workflow-id",
//	    QueueName:          "priority",
//	})
func ForkWorkflow[R any](ctx DBOSContext, input ForkWorkflowInput) (*WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	handle, err := ctx.(*dbosContext).ForkWorkflow(input)
	if err != nil {
		return nil, err
	}
	return newWorkflowHandle[R](ctx, handle.GetWorkflowID()), nil
}

// listWorkflowsOptions holds configuration parameters for listing workflows
type listWorkflowsOptions struct {
	workflowIDs      []string
	status           []WorkflowStatusType
	startTime        time.Time
	endTime          time.Time
	name             []string
	appVersion       []string
	user             []string
	limit            *int
	offset           *int
	sortDesc         bool
	workflowIDPrefix []string
	loadInput        bool
	loadOutput       bool
	queueName        []string
	queuesOnly       bool
	executorIDs      []string
	forkedFrom       []string
	parentWorkflowID []string
	deduplicationID  []string
	completedAfter   time.Time
	completedBefore  time.Time
	dequeuedAfter    time.Time
	dequeuedBefore   time.Time
	wasForkedFrom    *bool
	hasParent        *bool
}

// ListWorkflowsOption is a functional option for configuring workflow listing parameters.
type ListWorkflowsOption func(*listWorkflowsOptions)

// WithWorkflowIDs filters workflows by the specified workflow IDs.
func WithWorkflowIDs(workflowIDs []string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.workflowIDs = workflowIDs
	}
}

// WithStatus filters workflows by the specified list of statuses.
func WithStatus(status []WorkflowStatusType) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.status = status
	}
}

// WithStartTime filters workflows created after the specified time.
func WithStartTime(startTime time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.startTime = startTime
	}
}

// WithEndTime filters workflows created before the specified time.
func WithEndTime(endTime time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.endTime = endTime
	}
}

// WithName filters workflows by the specified workflow function name(s).
func WithName(name ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.name = name
	}
}

// WithAppVersion filters workflows by the specified application version(s).
func WithAppVersion(appVersion ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.appVersion = appVersion
	}
}

// WithUser filters workflows by the specified authenticated user(s).
func WithUser(user ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.user = user
	}
}

// WithLimit limits the number of workflows returned.
func WithLimit(limit int) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.limit = &limit
	}
}

// WithOffset sets the offset for pagination.
func WithOffset(offset int) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.offset = &offset
	}
}

// WithSortDesc enables descending sort by creation time (default is ascending).
func WithSortDesc() ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.sortDesc = true
	}
}

// WithWorkflowIDPrefix filters workflows by workflow ID prefix(es).
func WithWorkflowIDPrefix(prefix ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.workflowIDPrefix = prefix
	}
}

// WithLoadInput controls whether to load workflow input data (default: true).
func WithLoadInput(loadInput bool) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.loadInput = loadInput
	}
}

// WithLoadOutput controls whether to load workflow output data (default: true).
func WithLoadOutput(loadOutput bool) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.loadOutput = loadOutput
	}
}

// WithExecutorIDs filters workflows by the specified executor IDs.
func WithExecutorIDs(executorIDs []string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.executorIDs = executorIDs
	}
}

// WithForkedFrom filters workflows by the specified forked_from workflow ID(s).
func WithForkedFrom(forkedFrom ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.forkedFrom = forkedFrom
	}
}

// WithParentWorkflowID filters workflows by the specified parent workflow ID(s).
func WithParentWorkflowID(parentWorkflowID ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.parentWorkflowID = parentWorkflowID
	}
}

// WithFilterDeduplicationID filters workflows by the specified deduplication ID(s).
func WithFilterDeduplicationID(deduplicationID ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.deduplicationID = deduplicationID
	}
}

// WithCompletedAfter filters workflows that reached a terminal state at or after the specified time.
func WithCompletedAfter(completedAfter time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.completedAfter = completedAfter
	}
}

// WithCompletedBefore filters workflows that reached a terminal state at or before the specified time.
func WithCompletedBefore(completedBefore time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.completedBefore = completedBefore
	}
}

// WithDequeuedAfter filters workflows that started executing at or after the specified time.
func WithDequeuedAfter(dequeuedAfter time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.dequeuedAfter = dequeuedAfter
	}
}

// WithDequeuedBefore filters workflows that started executing at or before the specified time.
func WithDequeuedBefore(dequeuedBefore time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.dequeuedBefore = dequeuedBefore
	}
}

// WithWasForkedFrom filters workflows by whether they have been forked from (true) or not (false).
func WithWasForkedFrom(wasForkedFrom bool) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.wasForkedFrom = &wasForkedFrom
	}
}

// WithHasParent filters workflows by whether they have a parent workflow (true) or not (false).
func WithHasParent(hasParent bool) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.hasParent = &hasParent
	}
}

func (c *dbosContext) ListWorkflows(opts ...ListWorkflowsOption) ([]WorkflowStatus, error) {
	// Initialize parameters with defaults
	loadInput := true
	loadOutput := true
	if !c.launched.Load() {
		loadInput = false
		loadOutput = false
	}
	params := &listWorkflowsOptions{
		loadInput:  loadInput,
		loadOutput: loadOutput,
	}

	// Apply all provided options
	for _, opt := range opts {
		opt(params)
	}

	// If we are asked to retrieve only queue workflows with no status, only fetch ENQUEUED, PENDING, and DELAYED tasks
	if params.queuesOnly && len(params.status) == 0 {
		params.status = []WorkflowStatusType{WorkflowStatusEnqueued, WorkflowStatusPending, WorkflowStatusDelayed}
	}

	// Convert to system database input structure
	dbInput := listWorkflowsDBInput{
		workflowIDs:        params.workflowIDs,
		status:             params.status,
		startTime:          params.startTime,
		endTime:            params.endTime,
		workflowName:       params.name,
		applicationVersion: params.appVersion,
		authenticatedUser:  params.user,
		limit:              params.limit,
		offset:             params.offset,
		sortDesc:           params.sortDesc,
		workflowIDPrefix:   params.workflowIDPrefix,
		loadInput:          params.loadInput,
		loadOutput:         params.loadOutput,
		queueName:          params.queueName,
		queuesOnly:         params.queuesOnly,
		executorIDs:        params.executorIDs,
		forkedFrom:         params.forkedFrom,
		parentWorkflowID:   params.parentWorkflowID,
		deduplicationID:    params.deduplicationID,
		completedAfter:     params.completedAfter,
		completedBefore:    params.completedBefore,
		dequeuedAfter:      params.dequeuedAfter,
		dequeuedBefore:     params.dequeuedBefore,
		wasForkedFrom:      params.wasForkedFrom,
		hasParent:          params.hasParent,
	}

	// Call the context method to list workflows
	var workflows []WorkflowStatus
	var err error
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		workflows, err = Run(c, func(ctx context.Context) ([]WorkflowStatus, error) {
			return retryWithResult(ctx, func() ([]WorkflowStatus, error) {
				return c.kernel.listWorkflows(ctx, dbInput)
			}, withRetrierLogger(c.logger))
		}, WithStepName("DBOS.listWorkflows"))
	} else {
		workflows, err = retryWithResult(c, func() ([]WorkflowStatus, error) {
			return c.kernel.listWorkflows(c, dbInput)
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, err
	}

	// Deserialize Input and Output fields if they were loaded
	if params.loadInput || params.loadOutput {
		for i := range workflows {
			if params.loadInput && workflows[i].Input != nil {
				encodedInput, ok := workflows[i].Input.(*string)
				if !ok {
					return nil, fmt.Errorf("workflow input must be encoded string, got %T", workflows[i].Input)
				}
				if encodedInput == nil || *encodedInput == nilMarker {
					workflows[i].Input = nil
				} else if workflows[i].Serialization == PortableSerializerName {
					// Portable inputs are stored as plain JSON (possibly with envelope from other languages).
					// Return the raw JSON string as-is.
					workflows[i].Input = *encodedInput
				} else if c.serializer != nil {
					decoded, err := c.serializer.Decode(encodedInput)
					if err != nil {
						return nil, fmt.Errorf("failed to decode workflow input for %s: %w", workflows[i].ID, err)
					}
					workflows[i].Input = decoded
				} else {
					decodedBytes, err := base64.StdEncoding.DecodeString(*encodedInput)
					if err != nil {
						return nil, fmt.Errorf("failed to decode base64 workflow input for %s: %w", workflows[i].ID, err)
					}
					workflows[i].Input = string(decodedBytes)
				}
			}
			if params.loadOutput && workflows[i].Output != nil {
				encodedOutput, ok := workflows[i].Output.(*string)
				if !ok {
					return nil, fmt.Errorf("workflow output must be encoded *string, got %T", workflows[i].Output)
				}
				if encodedOutput == nil || *encodedOutput == nilMarker {
					workflows[i].Output = nil
				} else if workflows[i].Serialization == PortableSerializerName {
					// Portable outputs are stored as plain JSON. Return raw string.
					workflows[i].Output = *encodedOutput
				} else if c.serializer != nil {
					decoded, err := c.serializer.Decode(encodedOutput)
					if err != nil {
						return nil, fmt.Errorf("failed to decode workflow output for %s: %w", workflows[i].ID, err)
					}
					workflows[i].Output = decoded
				} else {
					decodedBytes, err := base64.StdEncoding.DecodeString(*encodedOutput)
					if err != nil {
						return nil, fmt.Errorf("failed to decode base64 workflow output for %s: %w", workflows[i].ID, err)
					}
					workflows[i].Output = string(decodedBytes)
				}
			}
			if params.loadOutput && workflows[i].Error != nil {
				s := workflows[i].Error.Error()
				workflows[i].Error = deserializeWorkflowError(&s, nil, workflows[i].Serialization)
			}
		}
	}

	return workflows, nil
}

// ListWorkflows retrieves a list of workflows based on the provided filters.
//
// The function supports filtering by workflow IDs, status, time ranges, names, application versions,
// workflow ID prefixes, and more. It also supports pagination through
// limit/offset parameters and sorting control (ascending by default, or descending with WithSortDesc).
//
// By default, both input and output data are loaded for each workflow. This can be controlled
// using WithLoadInput(false) and WithLoadOutput(false) options for better performance when
// the data is not needed.
//
// Parameters:
//   - opts: Functional options to configure the query filters and parameters
//
// Returns a slice of WorkflowStatus structs containing the workflow information.
//
// Example usage:
//
//	// List all successful workflows from the last 24 hours
//	workflows, err := dbos.ListWorkflows(
//	    dbos.WithStatus([]dbos.WorkflowStatusType{dbos.WorkflowStatusSuccess}),
//	    dbos.WithStartTime(time.Now().Add(-24*time.Hour)),
//	    dbos.WithLimit(100))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// List workflows by specific IDs without loading input/output data
//	workflows, err := dbos.ListWorkflows(
//	    dbos.WithWorkflowIDs([]string{"workflow1", "workflow2"}),
//	    dbos.WithLoadInput(false),
//	    dbos.WithLoadOutput(false))
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// List workflows with pagination
//	workflows, err := dbos.ListWorkflows(
//	    dbos.WithUser("john.doe"),
//	    dbos.WithOffset(50),
//	    dbos.WithLimit(25),
//	    dbos.WithSortDesc()
//	if err != nil {
//	    log.Fatal(err)
//	}
func ListWorkflows(ctx DBOSContext, opts ...ListWorkflowsOption) ([]WorkflowStatus, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).ListWorkflows(opts...)
}

type StepInfo struct {
	StepID      int       // The sequential ID of the step within the workflow
	StepName    string    // The name of the step function
	Output      any       // The output returned by the step (if any)
	Error       error     // The error returned by the step (if any)
	StartedAt   time.Time // When the step execution started
	CompletedAt time.Time // When the step execution completed
}

// getWorkflowStepsOptions holds optional parameters for GetWorkflowSteps.
type getWorkflowStepsOptions struct {
	loadOutput *bool
}

// GetWorkflowStepsOption is a functional option for GetWorkflowSteps.
type GetWorkflowStepsOption func(*getWorkflowStepsOptions)

// WithStepsLoadOutput controls whether to load step output data.
// When unset, output is loaded only if the DBOS context has been launched.
func WithStepsLoadOutput(loadOutput bool) GetWorkflowStepsOption {
	return func(o *getWorkflowStepsOptions) {
		o.loadOutput = &loadOutput
	}
}

func (c *dbosContext) GetWorkflowSteps(workflowID string, opts ...GetWorkflowStepsOption) ([]StepInfo, error) {
	options := getWorkflowStepsOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	loadOutput := c.launched.Load()
	if options.loadOutput != nil {
		loadOutput = *options.loadOutput
	}
	getWorkflowStepsInput := getWorkflowStepsInput{
		workflowID: workflowID,
		loadOutput: loadOutput,
	}

	var steps []stepInfo
	var err error
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		steps, err = Run(c, func(ctx context.Context) ([]stepInfo, error) {
			return retryWithResult(ctx, func() ([]stepInfo, error) {
				return c.kernel.getWorkflowSteps(ctx, getWorkflowStepsInput)
			}, withRetrierLogger(c.logger))
		}, WithStepName("DBOS.getWorkflowSteps"))
	} else {
		steps, err = retryWithResult(c, func() ([]stepInfo, error) {
			return c.kernel.getWorkflowSteps(c, getWorkflowStepsInput)
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, err
	}
	stepInfos := make([]StepInfo, len(steps))
	for i, step := range steps {
		var stepErr error
		if step.Error != nil {
			s := step.Error.Error()
			stepErr = deserializeWorkflowError(&s, nil, step.Serialization)
		}
		stepInfos[i] = StepInfo{
			StepID:      step.StepID,
			StepName:    step.StepName,
			Error:       stepErr,
			StartedAt:   step.StartedAt,
			CompletedAt: step.CompletedAt,
		}
	}

	// Deserialize outputs if asked to
	if loadOutput {
		for i := range steps {
			encodedOutput := steps[i].Output
			if encodedOutput == nil || *encodedOutput == nilMarker {
				stepInfos[i].Output = nil
				continue
			}
			if steps[i].Serialization == PortableSerializerName {
				// Portable outputs are plain JSON — return raw string as-is.
				stepInfos[i].Output = *encodedOutput
			} else if c.serializer != nil {
				// Custom serializer: fully decode using the serializer
				decoded, err := c.serializer.Decode(encodedOutput)
				if err != nil {
					return nil, fmt.Errorf("failed to decode step output for step %d: %w", steps[i].StepID, err)
				}
				stepInfos[i].Output = decoded
			} else {
				// Default JSON: base64 decode to get the JSON string
				decodedBytes, err := base64.StdEncoding.DecodeString(*encodedOutput)
				if err != nil {
					return nil, fmt.Errorf("failed to decode base64 step output for step %d: %w", steps[i].StepID, err)
				}
				stepInfos[i].Output = string(decodedBytes)
			}
		}
	}

	return stepInfos, nil
}

// GetWorkflowSteps retrieves the execution steps of a workflow.
// Returns a list of step information including step IDs, names, outputs, errors, and child workflow IDs.
// The list is sorted by step ID in ascending order.
//
// Parameters:
//   - ctx: DBOS context for the operation
//   - workflowID: The unique identifier of the workflow
//
// Returns a slice of StepInfo structs containing information about each executed step.
//
// Example:
//
//	steps, err := dbos.GetWorkflowSteps(ctx, "workflow-id")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for _, step := range steps {
//	    log.Printf("Step %d: %s", step.StepID, step.StepName)
//	}
func GetWorkflowSteps(ctx DBOSContext, workflowID string, opts ...GetWorkflowStepsOption) ([]StepInfo, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).GetWorkflowSteps(workflowID, opts...)
}

// GetWorkflowAggregatesInput is the input to GetWorkflowAggregates.
//
// At least one of the GroupBy* flags must be true, or TimeBucketSize must be > 0.
type GetWorkflowAggregatesInput struct {
	GroupByStatus             bool
	GroupByName               bool
	GroupByQueueName          bool
	GroupByExecutorID         bool
	GroupByApplicationVersion bool

	// When non-zero, groups results by created_at time bucket of this size.
	TimeBucketSize time.Duration

	// Filters
	Status             []WorkflowStatusType
	StartTime          time.Time
	EndTime            time.Time
	Name               []string
	ApplicationVersion []string
	ExecutorID         []string
	QueueName          []string
	WorkflowIDPrefix   []string
}

func (c *dbosContext) GetWorkflowAggregates(input GetWorkflowAggregatesInput) ([]WorkflowAggregateRow, error) {
	if input.TimeBucketSize < 0 {
		return nil, errors.New("TimeBucketSize must be >= 0")
	}
	dbInput := getWorkflowAggregatesDBInput{
		groupByStatus:             input.GroupByStatus,
		groupByName:               input.GroupByName,
		groupByQueueName:          input.GroupByQueueName,
		groupByExecutorID:         input.GroupByExecutorID,
		groupByApplicationVersion: input.GroupByApplicationVersion,
		timeBucketSizeMs:          input.TimeBucketSize.Milliseconds(),
		status:                    input.Status,
		startTime:                 input.StartTime,
		endTime:                   input.EndTime,
		workflowName:              input.Name,
		applicationVersion:        input.ApplicationVersion,
		executorID:                input.ExecutorID,
		queueName:                 input.QueueName,
		workflowIDPrefix:          input.WorkflowIDPrefix,
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		return runAsTxn(c, func(ctx context.Context, tx Tx) ([]WorkflowAggregateRow, error) {
			in := dbInput
			in.tx = tx
			return c.kernel.getWorkflowAggregates(ctx, in)
		}, WithStepName("DBOS.getWorkflowAggregates"))
	}
	return retryWithResult(c, func() ([]WorkflowAggregateRow, error) {
		return c.kernel.getWorkflowAggregates(c, dbInput)
	}, withRetrierLogger(c.logger))
}

// GetWorkflowAggregates returns aggregate counts of workflows grouped by one or more
// columns and/or by created_at time bucket.
//
// At least one GroupBy* flag in the input must be true, or TimeBucketSize must be > 0.
// Filter fields (Status, StartTime, EndTime, Name, ApplicationVersion, ExecutorID,
// QueueName, WorkflowIDPrefix) narrow which workflows are counted before grouping.
//
// Returns one WorkflowAggregateRow per non-empty group. Each row's Group map contains an
// entry per enabled grouping column ("status", "name", "queue_name", "executor_id",
// "application_version", "time_bucket"). Map values are pointers to allow representing
// NULL grouping values (e.g. workflows without a queue_name).
//
// Example:
//
//	rows, err := dbos.GetWorkflowAggregates(ctx, dbos.GetWorkflowAggregatesInput{
//	    GroupByStatus: true,
//	    StartTime:     time.Now().Add(-24 * time.Hour),
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for _, r := range rows {
//	    log.Printf("status=%s count=%d", *r.Group["status"], r.Count)
//	}
func GetWorkflowAggregates(ctx DBOSContext, input GetWorkflowAggregatesInput) ([]WorkflowAggregateRow, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).GetWorkflowAggregates(input)
}

// GetStepAggregatesInput is the input to GetStepAggregates.
//
// At least one of the GroupBy* flags must be true, or TimeBucketSize must be > 0.
// At least one of the Select* flags must be true.
type GetStepAggregatesInput struct {
	GroupByFunctionName bool
	GroupByStatus       bool

	SelectCount         bool
	SelectMaxDurationMs bool

	// When non-zero, groups results by completed_at time bucket of this size.
	TimeBucketSize time.Duration

	// Filters
	Status           []string
	FunctionName     []string
	WorkflowIDPrefix []string
	CompletedAfter   time.Time
	CompletedBefore  time.Time
}

func (c *dbosContext) GetStepAggregates(input GetStepAggregatesInput) ([]StepAggregateRow, error) {
	if input.TimeBucketSize < 0 {
		return nil, errors.New("TimeBucketSize must be >= 0")
	}
	dbInput := getStepAggregatesDBInput{
		groupByFunctionName: input.GroupByFunctionName,
		groupByStatus:       input.GroupByStatus,
		selectCount:         input.SelectCount,
		selectMaxDurationMs: input.SelectMaxDurationMs,
		timeBucketSizeMs:    input.TimeBucketSize.Milliseconds(),
		status:              input.Status,
		functionName:        input.FunctionName,
		workflowIDPrefix:    input.WorkflowIDPrefix,
		completedAfter:      input.CompletedAfter,
		completedBefore:     input.CompletedBefore,
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		return runAsTxn(c, func(ctx context.Context, tx Tx) ([]StepAggregateRow, error) {
			in := dbInput
			in.tx = tx
			return c.kernel.getStepAggregates(ctx, in)
		}, WithStepName("DBOS.getStepAggregates"))
	}
	return retryWithResult(c, func() ([]StepAggregateRow, error) {
		return c.kernel.getStepAggregates(c, dbInput)
	}, withRetrierLogger(c.logger))
}

// GetStepAggregates returns aggregate counts and/or max durations of steps grouped by
// function name and/or derived status, optionally bucketed by completed_at time.
//
// At least one GroupBy* flag must be true, or TimeBucketSize must be > 0. At least one
// Select* flag must be true. Step status is derived from operation_outputs: steps with no
// recorded error are "SUCCESS", otherwise "ERROR".
//
// Returns one StepAggregateRow per non-empty group. Each row's Group map contains an entry
// per enabled grouping column ("function_name", "status", "time_bucket"). Count and
// MaxDurationMs are populated only for the corresponding enabled Select* flag.
func GetStepAggregates(ctx DBOSContext, input GetStepAggregatesInput) ([]StepAggregateRow, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).GetStepAggregates(input)
}

// listRegisteredWorkflowsOptions holds configuration parameters for listing registered workflows
type listRegisteredWorkflowsOptions struct {
	scheduledOnly bool
}

// ListRegisteredWorkflowsOption is a functional option for configuring registered workflow listing parameters.
type ListRegisteredWorkflowsOption func(*listRegisteredWorkflowsOptions)

// WithScheduledOnly filters to only return scheduled workflows (those with a cron schedule).
func WithScheduledOnly() ListRegisteredWorkflowsOption {
	return func(p *listRegisteredWorkflowsOptions) {
		p.scheduledOnly = true
	}
}

// ListRegisteredWorkflows returns information about workflows registered with DBOS.
// Each WorkflowRegistryEntry contains:
// - MaxRetries: Maximum number of retry attempts for workflow recovery
// - Name: Custom name if provided during registration, otherwise empty
// - FQN: Fully qualified name of the workflow function (always present)
// - CronSchedule: Empty string for non-scheduled workflows
//
// The function supports filtering using functional options:
// - WithScheduledOnly(): Return only scheduled workflows
//
// Example:
//
//	// List all registered workflows
//	workflows, err := dbos.ListRegisteredWorkflows(ctx)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// List only scheduled workflows
//	scheduled, err := dbos.ListRegisteredWorkflows(ctx, dbos.WithScheduledOnly())
//	if err != nil {
//	    log.Fatal(err)
//	}
func ListRegisteredWorkflows(ctx DBOSContext, opts ...ListRegisteredWorkflowsOption) ([]WorkflowRegistryEntry, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).ListRegisteredWorkflows(opts...)
}

/*******************************/
/******* SCHEDULE MANAGEMENT ********/
/*******************************/

// validateScheduledWorkflowFn ensures fn has signature
// func(DBOSContext, ScheduledWorkflowInput) (any, error). Used by
// ApplySchedules where each entry's WorkflowFn is type-erased.
func validateScheduledWorkflowFn(fn any) error {
	t := reflect.TypeOf(fn)
	if t == nil || t.Kind() != reflect.Func {
		return errors.New("workflow function must be a function")
	}
	if t.NumIn() < 2 {
		return errors.New("workflow function must accept (DBOSContext, ScheduledWorkflowInput)")
	}
	if t.In(1) != reflect.TypeFor[ScheduledWorkflowInput]() {
		return fmt.Errorf("scheduled workflow function must accept a ScheduledWorkflowInput as input, got %v", t.In(1))
	}
	return nil
}

func (c *dbosContext) CreateSchedule(fn ScheduledWorkflowFunc, input CreateScheduleRequest, opts ...CreateScheduleOption) error {
	if input.ScheduleName == "" {
		return errors.New("schedule_name is required")
	}

	workflowName, err := c.resolveWorkflowName(fn)
	if err != nil {
		return err
	}
	var o createScheduleOptions
	for _, opt := range opts {
		opt(&o)
	}

	if err := validateCronSchedule(input.Schedule, o.cronTimezone); err != nil {
		return err
	}

	contextJSON, err := json.Marshal(o.context)
	if err != nil {
		return fmt.Errorf("failed to serialize context: %w", err)
	}

	scheduleID := uuid.New().String()
	dbInput := createScheduleDBInput{
		ScheduleID:        scheduleID,
		ScheduleName:      input.ScheduleName,
		WorkflowName:      workflowName,
		WorkflowClassName: o.workflowClassName,
		Schedule:          input.Schedule,
		Context:           string(contextJSON),
		Status:            ScheduleStatusActive,
		AutomaticBackfill: o.automaticBackfill,
		CronTimezone:      o.cronTimezone,
	}

	if state, inWorkflow := c.Value(workflowStateKey).(*workflowState); inWorkflow && state != nil {
		_, err := runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
			input := dbInput
			input.tx = tx
			return nil, c.kernel.createSchedule(ctx, input)
		}, WithStepName("DBOS.createSchedule"))
		return err
	}

	return retry(c, func() error {
		return c.kernel.createSchedule(c, dbInput)
	}, withRetrierLogger(c.logger))
}

// CreateScheduleRequest carries the mandatory fields for CreateSchedule.
// Optional settings are configured via CreateScheduleOption.
type CreateScheduleRequest struct {
	ScheduleName string
	Schedule     string
}

type createScheduleOptions struct {
	context           any
	automaticBackfill bool
	cronTimezone      string
	workflowClassName string
}

type CreateScheduleOption func(*createScheduleOptions)

// WithScheduleContext attaches a user-defined context (serialized as JSON)
// passed to each scheduled invocation.
func WithScheduleContext(context any) CreateScheduleOption {
	return func(o *createScheduleOptions) { o.context = context }
}

// WithAutomaticBackfill enables backfilling missed ticks when the schedule is
// reloaded after downtime.
func WithAutomaticBackfill(enabled bool) CreateScheduleOption {
	return func(o *createScheduleOptions) { o.automaticBackfill = enabled }
}

// WithCronTimezone sets the IANA timezone used to interpret the cron
// expression.
func WithCronTimezone(tz string) CreateScheduleOption {
	return func(o *createScheduleOptions) { o.cronTimezone = tz }
}

// WithScheduleWorkflowClassName records a class/namespace name on the schedule
// for cross-language dispatch. Use this when the scheduled workflow is owned
// by a non-Go runtime (e.g. a Python class-based workflow) so the stored
// schedule and Conductor view carry the correct class name.
func WithScheduleWorkflowClassName(name string) CreateScheduleOption {
	return func(o *createScheduleOptions) { o.workflowClassName = name }
}

// listSchedulesOptions holds configuration parameters for listing schedules.
type listSchedulesOptions struct {
	statuses             []ScheduleStatus
	workflowNames        []string
	scheduleNamePrefixes []string
}

// CreateSchedule creates a new schedule for a workflow. The reconciler loop
// picks the new schedule up on its next tick and installs it in the cron
// scheduler. The fn must already be registered via NewWorkflow.
//
// Example:
//
//	err := dbos.CreateSchedule(ctx, myWorkflow, dbos.CreateScheduleRequest{
//	    ScheduleName: "my-schedule",
//	    Schedule:     "*/5 * * * *",
//	}, dbos.WithScheduleContext("my context"))
func CreateSchedule(ctx DBOSContext, fn ScheduledWorkflowFunc, input CreateScheduleRequest, opts ...CreateScheduleOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	if fn == nil {
		return errors.New("workflow function cannot be nil")
	}
	return ctx.(*dbosContext).CreateSchedule(fn, input, opts...)
}

func (c *dbosContext) ApplySchedules(schedules []ApplySchedulesRequest) error {
	if state, ok := c.Value(workflowStateKey).(*workflowState); ok && state != nil {
		return errors.New("DBOS.ApplySchedules cannot be called from within a workflow")
	}

	if len(schedules) == 0 {
		return nil
	}

	for _, req := range schedules {
		if req.ScheduleName == "" {
			return errors.New("schedule_name is required")
		}
		if err := validateCronSchedule(req.Schedule, req.CronTimezone); err != nil {
			return err
		}
		if err := validateScheduledWorkflowFn(req.WorkflowFn); err != nil {
			return err
		}
	}

	return retry(c, func() error {
		tx, err := c.kernel.pool.BeginTx(c, TxOptions{})
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(c)

		for _, req := range schedules {
			workflowName, err := c.resolveWorkflowName(req.WorkflowFn)
			if err != nil {
				return err
			}

			contextJSON, err := json.Marshal(req.Context)
			if err != nil {
				return fmt.Errorf("failed to serialize context: %w", err)
			}

			// Delete any existing schedule with this name, then create the new one.
			if err := c.kernel.deleteSchedule(c, deleteScheduleDBInput{
				ScheduleName: req.ScheduleName,
				tx:           tx,
			}); err != nil {
				return fmt.Errorf("failed to delete existing schedule: %w", err)
			}

			scheduleID := uuid.New().String()
			if err := c.kernel.createSchedule(c, createScheduleDBInput{
				ScheduleID:        scheduleID,
				ScheduleName:      req.ScheduleName,
				WorkflowName:      workflowName,
				Schedule:          req.Schedule,
				Context:           string(contextJSON),
				Status:            ScheduleStatusActive,
				AutomaticBackfill: req.AutomaticBackfill,
				CronTimezone:      req.CronTimezone,
				tx:                tx,
			}); err != nil {
				return fmt.Errorf("failed to create schedule: %w", err)
			}
		}

		if err := tx.Commit(c); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		return nil
	}, withRetrierLogger(c.logger))
}

// ApplySchedules applies a list of schedules, creating new ones or updating existing ones.
// This is useful for defining a set of static schedules to be created on program start.
//
// Example:
//
//	err := dbos.ApplySchedules(ctx, []dbos.ApplySchedulesRequest{
//	    {ScheduleName: "schedule-a", WorkflowFn: workflowA, Schedule: "*/10 * * * *"},
//	    {ScheduleName: "schedule-b", WorkflowFn: workflowB, Schedule: "0 0 * * *"},
//	})
func ApplySchedules(ctx DBOSContext, schedules []ApplySchedulesRequest) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).ApplySchedules(schedules)
}

func (c *dbosContext) PauseSchedule(scheduleName string) error {
	if scheduleName == "" {
		return errors.New("schedule_name is required")
	}

	existing, err := c.GetSchedule(scheduleName)
	if err != nil {
		return fmt.Errorf("failed to get schedule: %w", err)
	}
	if existing == nil {
		return fmt.Errorf("schedule not found: %s", scheduleName)
	}

	dbInput := updateScheduleDBInput{
		ScheduleName: scheduleName,
		Status:       ScheduleStatusPaused,
	}

	if state, inWorkflow := c.Value(workflowStateKey).(*workflowState); inWorkflow && state != nil {
		_, err := runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
			in := dbInput
			in.tx = tx
			return nil, c.kernel.updateSchedule(ctx, in)
		}, WithStepName("DBOS.pauseSchedule"))
		return err
	}

	return retry(c, func() error {
		return c.kernel.updateSchedule(c, dbInput)
	}, withRetrierLogger(c.logger))
}

// PauseSchedule pauses a schedule so it stops firing.
//
// Example:
//
//	err := dbos.PauseSchedule(ctx, "my-schedule")
func PauseSchedule(ctx DBOSContext, scheduleName string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).PauseSchedule(scheduleName)
}

func (c *dbosContext) ResumeSchedule(scheduleName string) error {
	if scheduleName == "" {
		return errors.New("schedule_name is required")
	}

	existing, err := c.GetSchedule(scheduleName)
	if err != nil {
		return fmt.Errorf("failed to get schedule: %w", err)
	}
	if existing == nil {
		return fmt.Errorf("schedule not found: %s", scheduleName)
	}

	dbInput := updateScheduleDBInput{
		ScheduleName: scheduleName,
		Status:       ScheduleStatusActive,
	}

	if state, inWorkflow := c.Value(workflowStateKey).(*workflowState); inWorkflow && state != nil {
		_, err := runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
			in := dbInput
			in.tx = tx
			return nil, c.kernel.updateSchedule(ctx, in)
		}, WithStepName("DBOS.resumeSchedule"))
		return err
	}

	return retry(c, func() error {
		return c.kernel.updateSchedule(c, dbInput)
	}, withRetrierLogger(c.logger))
}

// ResumeSchedule resumes a paused schedule.
//
// Example:
//
//	err := dbos.ResumeSchedule(ctx, "my-schedule")
func ResumeSchedule(ctx DBOSContext, scheduleName string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).ResumeSchedule(scheduleName)
}

func (c *dbosContext) DeleteSchedule(scheduleName string) error {
	if scheduleName == "" {
		return errors.New("schedule_name is required")
	}

	if state, inWorkflow := c.Value(workflowStateKey).(*workflowState); inWorkflow && state != nil {
		_, err := runAsTxn(c, func(ctx context.Context, tx Tx) (any, error) {
			return nil, c.kernel.deleteSchedule(ctx, deleteScheduleDBInput{ScheduleName: scheduleName, tx: tx})
		}, WithStepName("DBOS.deleteSchedule"))
		return err
	}

	return retry(c, func() error {
		return c.kernel.deleteSchedule(c, deleteScheduleDBInput{ScheduleName: scheduleName})
	}, withRetrierLogger(c.logger))
}

// DeleteSchedule deletes a schedule.
//
// Example:
//
//	err := dbos.DeleteSchedule(ctx, "my-schedule")
func DeleteSchedule(ctx DBOSContext, scheduleName string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).DeleteSchedule(scheduleName)
}

// Potentially we could return an error here, if helpful to the user, if the schedule is not found.
func (c *dbosContext) GetSchedule(scheduleName string) (*WorkflowSchedule, error) {
	if scheduleName == "" {
		return nil, errors.New("schedule_name is required")
	}

	dbInput := listSchedulesDBInput{ScheduleNamePrefixes: []string{scheduleName}}

	var schedules []WorkflowSchedule
	var err error
	if state, inWorkflow := c.Value(workflowStateKey).(*workflowState); inWorkflow && state != nil {
		schedules, err = runAsTxn(c, func(ctx context.Context, tx Tx) ([]WorkflowSchedule, error) {
			in := dbInput
			in.tx = tx
			return c.kernel.listSchedules(ctx, in)
		}, WithStepName("DBOS.getSchedule"))
	} else {
		schedules, err = retryWithResult(c, func() ([]WorkflowSchedule, error) {
			return c.kernel.listSchedules(c, dbInput)
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, err
	}
	for i := range schedules {
		if schedules[i].ScheduleName == scheduleName {
			return &schedules[i], nil
		}
	}
	return nil, nil
}

// GetSchedule gets a schedule by name.
//
// Example:
//
//	schedule, err := dbos.GetSchedule(ctx, "my-schedule")
func GetSchedule(ctx DBOSContext, scheduleName string) (*WorkflowSchedule, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).GetSchedule(scheduleName)
}

func (c *dbosContext) ListSchedules(opts ...ListSchedulesOption) ([]WorkflowSchedule, error) {
	var o listSchedulesOptions
	for _, opt := range opts {
		opt(&o)
	}
	dbInput := listSchedulesDBInput{
		Statuses:             o.statuses,
		WorkflowNames:        o.workflowNames,
		ScheduleNamePrefixes: o.scheduleNamePrefixes,
	}
	if state, inWorkflow := c.Value(workflowStateKey).(*workflowState); inWorkflow && state != nil {
		return runAsTxn(c, func(ctx context.Context, tx Tx) ([]WorkflowSchedule, error) {
			in := dbInput
			in.tx = tx
			return c.kernel.listSchedules(ctx, in)
		}, WithStepName("DBOS.listSchedules"))
	}
	return retryWithResult(c, func() ([]WorkflowSchedule, error) {
		return c.kernel.listSchedules(c, dbInput)
	}, withRetrierLogger(c.logger))
}

// ListSchedulesOption is a functional option for configuring schedule listing parameters.
type ListSchedulesOption func(*listSchedulesOptions)

// WithScheduleStatuses filters schedules by the specified status(es).
func WithScheduleStatuses(statuses ...ScheduleStatus) ListSchedulesOption {
	return func(o *listSchedulesOptions) { o.statuses = statuses }
}

// WithScheduleWorkflowNames filters schedules by the specified workflow name(s).
func WithScheduleWorkflowNames(names ...string) ListSchedulesOption {
	return func(o *listSchedulesOptions) { o.workflowNames = names }
}

// WithScheduleNamePrefixes filters schedules by schedule name prefix(es).
func WithScheduleNamePrefixes(prefixes ...string) ListSchedulesOption {
	return func(o *listSchedulesOptions) { o.scheduleNamePrefixes = prefixes }
}

// ListSchedules lists schedules, optionally filtered by the supplied options.
// Pass no options to return all schedules.
//
// Example:
//
//	schedules, err := dbos.ListSchedules(ctx, dbos.WithScheduleStatuses(dbos.ScheduleStatusActive))
func ListSchedules(ctx DBOSContext, opts ...ListSchedulesOption) ([]WorkflowSchedule, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).ListSchedules(opts...)
}

func (c *dbosContext) BackfillSchedule(scheduleName string, start time.Time, end time.Time) ([]string, error) {
	if state, ok := c.Value(workflowStateKey).(*workflowState); ok && state != nil {
		return nil, errors.New("DBOS.BackfillSchedule cannot be called from within a workflow")
	}
	if scheduleName == "" {
		return nil, errors.New("schedule_name is required")
	}

	existing, err := c.GetSchedule(scheduleName)
	if err != nil {
		return nil, fmt.Errorf("failed to get schedule: %w", err)
	}
	if existing == nil {
		return nil, fmt.Errorf("schedule not found: %s", scheduleName)
	}

	var ids []string
	err = retry(c, func() error {
		var bfErr error
		ids, bfErr = c.kernel.backfillSchedule(c, backfillScheduleDBInput{
			ScheduleName: scheduleName,
			Schedule:     existing.Schedule,
			StartTime:    start,
			EndTime:      end,
		})
		return bfErr
	}, withRetrierLogger(c.logger))
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// BackfillSchedule backfills a schedule, executing it for each time slot in the range.
// Already-executed times are automatically skipped. Returns the IDs of the
// workflows enqueued for the backfilled time slots.
//
// Example:
//
//	ids, err := dbos.BackfillSchedule(ctx, "my-schedule", startTime, endTime)
func BackfillSchedule(ctx DBOSContext, scheduleName string, start, end time.Time) ([]string, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).BackfillSchedule(scheduleName, start, end)
}

func (c *dbosContext) TriggerSchedule(scheduleName string) (*WorkflowHandle[any], error) {
	if scheduleName == "" {
		return nil, errors.New("schedule_name is required")
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	if ok && workflowState != nil {
		return nil, errors.New("DBOS.TriggerSchedule cannot be called from within a workflow")
	}

	workflowID, err := c.kernel.triggerSchedule(c, scheduleName)
	if err != nil {
		return nil, err
	}
	return newWorkflowHandle[any](c, workflowID), nil
}

// TriggerSchedule triggers a schedule immediately, returning a handle to the
// enqueued workflow.
//
// Example:
//
//	handle, err := dbos.TriggerSchedule(ctx, "my-schedule")
func TriggerSchedule(ctx DBOSContext, scheduleName string) (*WorkflowHandle[any], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).TriggerSchedule(scheduleName)
}

// ListApplicationVersions returns every registered application version ordered
// by timestamp (newest first).
func (c *dbosContext) ListApplicationVersions() ([]VersionInfo, error) {
	return retryWithResult(c, func() ([]VersionInfo, error) {
		return c.kernel.listApplicationVersions(c)
	}, withRetrierLogger(c.logger))
}

// ListApplicationVersions is the package-level wrapper for DBOSContext.ListApplicationVersions.
func ListApplicationVersions(ctx DBOSContext) ([]VersionInfo, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).ListApplicationVersions()
}

// GetLatestApplicationVersion returns the application version with the most
// recent timestamp.
func (c *dbosContext) GetLatestApplicationVersion() (*VersionInfo, error) {
	return retryWithResult(c, func() (*VersionInfo, error) {
		return c.kernel.getLatestApplicationVersion(c)
	}, withRetrierLogger(c.logger))
}

// GetLatestApplicationVersion is the package-level wrapper for DBOSContext.GetLatestApplicationVersion.
func GetLatestApplicationVersion(ctx DBOSContext) (*VersionInfo, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).GetLatestApplicationVersion()
}

// SetLatestApplicationVersion marks the named application version as latest by
// updating its timestamp to the current time.
func (c *dbosContext) SetLatestApplicationVersion(versionName string) error {
	if versionName == "" {
		return errors.New("version_name is required")
	}
	return retry(c, func() error {
		return c.kernel.updateApplicationVersionTimestamp(c, versionName, time.Now().UnixMilli())
	}, withRetrierLogger(c.logger))
}

// SetLatestApplicationVersion is the package-level wrapper for DBOSContext.SetLatestApplicationVersion.
func SetLatestApplicationVersion(ctx DBOSContext, versionName string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).SetLatestApplicationVersion(versionName)
}
