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

type WorkflowStatusType string

const (
	WorkflowStatusPending                     WorkflowStatusType = "PENDING"
	WorkflowStatusEnqueued                    WorkflowStatusType = "ENQUEUED"
	WorkflowStatusDelayed                     WorkflowStatusType = "DELAYED"
	WorkflowStatusSuccess                     WorkflowStatusType = "SUCCESS"
	WorkflowStatusError                       WorkflowStatusType = "ERROR"
	WorkflowStatusCancelled                   WorkflowStatusType = "CANCELLED"
	WorkflowStatusMaxRecoveryAttemptsExceeded WorkflowStatusType = "MAX_RECOVERY_ATTEMPTS_EXCEEDED"
)

type WorkflowStatus struct {
	Id                 string             `json:"workflow_uuid"`
	Status             WorkflowStatusType `json:"status"`
	Name               string             `json:"name"`
	AuthenticatedUser  string             `json:"authenticated_user,omitempty"`
	AssumedRole        string             `json:"assumed_role,omitempty"`
	AuthenticatedRoles []string           `json:"authenticated_roles,omitempty"`
	Output             any                `json:"output,omitempty"`
	Error              error              `json:"error,omitempty"`
	ExecutorId         string             `json:"executor_id"`
	CreatedAt          time.Time          `json:"created_at"`
	UpdatedAt          time.Time          `json:"updated_at"`
	ApplicationVersion string             `json:"application_version"`
	ApplicationId      string             `json:"application_id,omitempty"`
	Attempts           int                `json:"attempts"`
	QueueName          string             `json:"queue_name,omitempty"`
	Timeout            time.Duration      `json:"timeout,omitempty"`
	Deadline           time.Time          `json:"deadline"`
	StartedAt          time.Time          `json:"started_at"`
	DeduplicationId    string             `json:"deduplication_id,omitempty"`
	Input              any                `json:"input,omitempty"`
	Priority           int                `json:"priority,omitempty"`
	QueuePartitionKey  string             `json:"queue_partition_key,omitempty"`
	ForkedFrom         string             `json:"forked_from,omitempty"`
	WasForkedFrom      bool               `json:"was_forked_from,omitempty"`
	ParentWorkflowId   string             `json:"parent_workflow_id,omitempty"`
	CompletedAt        time.Time          `json:"completed_at,omitempty"`
	ClassName          string             `json:"class_name,omitempty"`
	ConfigName         *string            `json:"config_name,omitempty"`
	Serialization      string             `json:"serialization,omitempty"`
	DelayUntil         time.Time          `json:"delay_until,omitempty"`
	DefinitionDigest   string             `json:"definition_digest,omitempty"`
}

type workflowState struct {
	workflowId         string
	stepId             int
	isWithinStep       bool
	isPortableWorkflow bool

	authenticatedUser  string
	assumedRole        string
	authenticatedRoles []string
}

func (ws *workflowState) nextStepId() int {
	ws.stepId++
	return ws.stepId
}

type stepCheckpointedOutcome struct {
	value         any
	serialization string
}

type workflowHandle struct {
	workflowId  string
	dbosContext Context
}

type GetResultOption func(*getResultOptions)

type getResultOptions struct {
	timeout      time.Duration
	pollInterval time.Duration
}

func defaultGetResultOptions() *getResultOptions {
	return &getResultOptions{pollInterval: _dbRetryInterval}
}

func WithHandleTimeout(timeout time.Duration) GetResultOption {
	return func(opts *getResultOptions) {
		opts.timeout = timeout
	}
}

func WithHandlePollingInterval(interval time.Duration) GetResultOption {
	return func(opts *getResultOptions) {
		if interval > 0 {
			opts.pollInterval = interval
		}
	}
}

func (h *workflowHandle) GetStatus() (WorkflowStatus, error) {
	loadInput := false
	loadOutput := false
	if h.dbosContext.(*dbosContext).started.Load() {
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
					workflowIds: []string{h.workflowId},
					loadInput:   loadInput,
					loadOutput:  loadOutput,
				})
			}, withRetrierLogger(c.logger))
		}, WithStepName("DBOS.getStatus"))
	} else {
		workflowStatuses, err = retryWithResult(c, func() ([]WorkflowStatus, error) {
			return c.kernel.listWorkflows(c, listWorkflowsDBInput{
				workflowIds: []string{h.workflowId},
				loadInput:   loadInput,
				loadOutput:  loadOutput,
			})
		})
	}
	if err != nil {
		return WorkflowStatus{}, fmt.Errorf("failed to get workflow status: %w", err)
	}
	if len(workflowStatuses) == 0 {
		return WorkflowStatus{}, newNonExistentWorkflowError(h.workflowId)
	}
	return workflowStatuses[0], nil
}

func (h *workflowHandle) GetWorkflowId() string {
	return h.workflowId
}

func newWorkflowHandle[R any](ctx Context, workflowId string) *WorkflowHandle[R] {
	return &WorkflowHandle[R]{
		workflowHandle: workflowHandle{
			workflowId:  workflowId,
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
		return h.dbosContext.(*dbosContext).kernel.awaitWorkflowResult(ctx, h.workflowId, options.pollInterval)
	}, withRetrierLogger(h.dbosContext.(*dbosContext).logger))

	if awaitErr != nil && options.timeout > 0 && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return *new(R), fmt.Errorf("workflow result timeout after %v: %w", options.timeout, context.DeadlineExceeded)
	}
	err := awaitErr
	if awaitResult != nil && awaitResult.errStr != nil {
		if awaitErr == nil {
			err = deserializeWorkflowError(awaitResult.errStr, awaitResult.errEncoded, awaitResult.serialization)
		} else if dbosErr, ok := awaitErr.(*DbosError); ok && dbosErr.Code == AwaitedWorkflowCancelled {

			dbosErr.wrappedErr = deserializeWorkflowError(awaitResult.errStr, awaitResult.errEncoded, awaitResult.serialization)
		}
	}

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

func storeWorkflowRegistryEntry(ctx Context, entry WorkflowRegistryEntry, customName string) {

	c, ok := ctx.(*dbosContext)
	if !ok {
		return
	}

	if c.started.Load() {
		panic("Cannot register workflow after DBOS has started")
	}

	entry.Name = customName
	entry.CronSchedule = ""
	entry.Retention = _defaultWorkflowRetention

	workflowName := entry.FQN
	if customName != "" {
		workflowName = customName
	}
	if _, exists := c.workflowRegistry.LoadOrStore(workflowName, entry); exists {
		c.logger.Error("workflow function already registered", "workflow_name", workflowName, "fqn", entry.FQN)
		panic(newConflictingRegistrationError(workflowName))
	}
}

func registerScheduledWorkflow(ctx Context, workflowFQN, customName string, fn WorkflowFunc, cronSchedule string) {

	c, ok := ctx.(*dbosContext)
	if !ok {
		return
	}

	if c.started.Load() {
		panic("Cannot register scheduled workflow after DBOS has started")
	}

	workflowName := workflowFQN
	if customName != "" {
		workflowName = customName
	}
	if !c.workflowRegistry.SetCronSchedule(workflowName, cronSchedule) {
		panic(fmt.Sprintf("workflow %s must be registered before scheduling", workflowFQN))
	}

	name := workflowName
	scheduled := ScheduledWorkflowFunc(func(ctx Context, input ScheduledWorkflowInput) (any, error) {
		scheduledTime := input.ScheduledTime
		wfId := fmt.Sprintf("sched-%s-%s", name, scheduledTime)

		ser := resolveEncoder(ctx)
		encodedInput, err := ser.Encode(scheduledTime)
		if err != nil {
			return nil, fmt.Errorf("failed to encode scheduled workflow input: %w", err)
		}
		opts := []WorkflowOption{
			withWorkflowId(wfId),
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
	_defaultMaxRecoveryAttempts = 100
	_defaultWorkflowRetention   = 24 * time.Hour

	_defaultStepBaseInterval  = 100 * time.Millisecond
	_defaultStepMaxInterval   = 5 * time.Second
	_defaultStepBackoffFactor = 2.0
)

func WithMaxRetries(maxRetries int) WorkflowOption {
	return func(p *workflowOptions) {
		p.MaxRetries = maxRetries
	}
}

func WithSchedule(schedule string) WorkflowOption {
	return func(p *workflowOptions) {
		p.CronSchedule = schedule
	}
}

func WithWorkflowName(name string) WorkflowOption {
	return func(p *workflowOptions) {
		p.WorkflowName = name
	}
}

func WithGlobalConcurrency(concurrency int) WorkflowOption {
	return func(p *workflowOptions) {
		p.GlobalConcurrency = &concurrency
	}
}

func WithRateLimit(limit int, period time.Duration) WorkflowOption {
	return func(p *workflowOptions) {
		p.RateLimit = &rateLimiter{limit: limit, period: period}
	}
}

func WithWorkflowRetention(retention time.Duration) WorkflowOption {
	return func(p *workflowOptions) {
		p.Retention = &retention
	}
}

type Workflow[P any, R any] func(ctx Context, input P, opts ...WorkflowOption) (*WorkflowHandle[R], error)

// newWorkflow is the registration primitive: its whole behavior is turning a WorkflowFn
// into a durable, named workflow and returning a typed invoker for it (plus the resolved
// name). It knows nothing about debouncing or any other composed behavior.
func newWorkflow[P any, R any](ctx Context, fn WorkflowFn[P, R], opts ...WorkflowOption) (Workflow[P, R], string) {
	c, ok := ctx.(*dbosContext)
	if !ok {
		panic("ctx must be a DBOS context")
	}

	params := workflowOptions{}
	for _, opt := range opts {
		opt(&params)
	}
	retention := _defaultWorkflowRetention
	if params.Retention != nil {
		retention = *params.Retention
	}
	if retention <= 0 {
		panic("workflow retention must be greater than 0")
	}

	registerWorkflow(ctx, fn, opts...)

	name := params.workflowFQN
	if name == "" {
		name = resolveWorkflowFunctionName(fn)
	}
	if params.WorkflowName != "" {
		name = params.WorkflowName
	} else if resolved, exists := c.workflowRegistry.ResolveName(name); exists {
		name = resolved
	}
	if !c.workflowRegistry.SetExecutionPolicies(name, params.GlobalConcurrency, params.RateLimit, retention) {
		panic(fmt.Sprintf("workflow %s must be registered before assigning execution policies", name))
	}

	invoker := func(ctx Context, input P, workflowOpts ...WorkflowOption) (*WorkflowHandle[R], error) {
		if ctx == nil {
			return nil, fmt.Errorf("ctx cannot be nil")
		}

		workflowOpts = append(workflowOpts, withWorkflowName(name))

		typedErasedWorkflow := WorkflowFunc(func(ctx Context, input any) (any, error) {
			return fn(ctx, input.(P))
		})

		handle, err := ctx.RunWorkflow(typedErasedWorkflow, input, workflowOpts...)
		if err != nil {
			return nil, err
		}

		return newWorkflowHandle[R](handle.dbosContext, handle.workflowId), nil
	}
	return invoker, name
}

// NewWorkflow is the user-facing factory. It registers fn via the newWorkflow primitive
// and composes configured behaviors on top — currently a debounce window workflow, used
// when a call passes WithDebounce.
func NewWorkflow[P any, R any](ctx Context, fn WorkflowFn[P, R], opts ...WorkflowOption) Workflow[P, R] {
	target, name := newWorkflow(ctx, fn, opts...)
	window, _ := newWorkflow(ctx, debounceWindow(target), withWorkflowFQN(name+_DEBOUNCER_NAME_SUFFIX))

	return func(ctx Context, input P, callOpts ...WorkflowOption) (*WorkflowHandle[R], error) {
		if ctx == nil {
			return nil, fmt.Errorf("ctx cannot be nil")
		}

		callParams := workflowOptions{}
		for _, opt := range callOpts {
			opt(&callParams)
		}

		if callParams.debounce {
			return startDebounced(ctx, window, input, callParams)
		}

		return target(ctx, input, callOpts...)
	}
}

func resolveWorkflowFunctionName[P any, R any](fn WorkflowFn[P, R]) string {
	ptr := reflect.ValueOf(fn).Pointer()
	fqn := runtime.FuncForPC(ptr).Name()

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

func registerWorkflow[P any, R any](ctx Context, fn WorkflowFn[P, R], opts ...WorkflowOption) {
	if ctx == nil {
		panic("ctx cannot be nil")
	}

	if fn == nil {
		panic("workflow function cannot be nil")
	}

	var p P

	registrationParams := workflowOptions{
		MaxRetries: _defaultMaxRecoveryAttempts,
	}

	for _, opt := range opts {
		opt(&registrationParams)
	}

	fqn := registrationParams.workflowFQN
	if fqn == "" {
		fqn = resolveWorkflowFunctionName(fn)
	}

	// Input will always come, encoded, from the database, so we decode it into the target type (captured by this wrapped closure)

	typedErasedWorkflow := func(ctx Context, input any, inputSerialization string) (any, error) {
		workflowId, err := GetWorkflowId(ctx)
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
				return *new(R), newWorkflowExecutionError(workflowId, resolveErr)
			}
			typedInput, err = inputDecoder.Decode(encodedInput)
		}
		if err != nil {
			return *new(R), newWorkflowExecutionError(workflowId, err)
		}
		return fn(ctx, typedInput)
	}

	typeErasedWrapper := wrappedWorkflowFunc(func(ctx Context, input any, inputSerialization string, opts ...WorkflowOption) (*WorkflowHandle[any], error) {
		wfFunc := WorkflowFunc(func(ctx Context, input any) (any, error) {
			return typedErasedWorkflow(ctx, input, inputSerialization)
		})
		opts = append(opts, withWorkflowName(fqn), withAlreadyEncodedInput())
		if inputSerialization == PortableSerializerName {
			opts = append(opts, WithPortableWorkflow())
		}
		handle, err := ctx.RunWorkflow(wfFunc, input, opts...)
		if err != nil {
			return nil, err
		}
		return newWorkflowHandle[any](ctx, handle.GetWorkflowId()), nil
	})
	storeWorkflowRegistryEntry(ctx, WorkflowRegistryEntry{
		wrappedFunction: typeErasedWrapper,
		FQN:             fqn,
		MaxRetries:      registrationParams.MaxRetries,
		InputSchema:     reflect.TypeFor[P]().String(),
		OutputSchema:    reflect.TypeFor[R]().String(),
		DebounceDelay:   registrationParams.debounceDelay,
		DebounceTimeout: registrationParams.debounceTimeout,
	}, registrationParams.WorkflowName)

	if registrationParams.CronSchedule != "" {
		if reflect.TypeOf(p) != reflect.TypeFor[time.Time]() {
			panic(fmt.Sprintf("scheduled workflow function must accept a time.Time as input, got %T", p))
		}
		scheduledWfFunc := WorkflowFunc(func(ctx Context, input any) (any, error) {
			return typedErasedWorkflow(ctx, input, resolveEncoder(ctx).Name())
		})
		registerScheduledWorkflow(ctx, fqn, registrationParams.WorkflowName, scheduledWfFunc, registrationParams.CronSchedule)
	}
}

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

type dbosContextKey string

const workflowStateKey dbosContextKey = "workflowState"

// All workflow functions must accept a Context as their first parameter.
type WorkflowFn[P any, R any] func(ctx Context, input P) (R, error)

type WorkflowFunc func(ctx Context, input any) (any, error)

type activeWorkflowEntry struct{}

type workflowOptions struct {
	WorkflowName        string
	CronSchedule        string
	GlobalConcurrency   *int
	RateLimit           *rateLimiter
	Retention           *time.Duration
	WorkflowId          string
	QueueName           string
	ApplicationVersion  string
	MaxRetries          int
	DeduplicationId     string
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
	workflowFQN         string
	alreadyEncodedInput bool
	isDequeue           bool
	isRecovery          bool
	isPortableWorkflow  bool
}

type WorkflowOption func(*workflowOptions)

// withWorkflowId sets a custom workflow ID instead of generating one

func withWorkflowId(id string) WorkflowOption {
	return func(p *workflowOptions) {
		p.WorkflowId = id
	}
}

func WithApplicationVersion(version string) WorkflowOption {
	return func(p *workflowOptions) {
		p.ApplicationVersion = version
	}
}

func WithDeduplicationId(id string) WorkflowOption {
	return func(p *workflowOptions) {
		p.DeduplicationId = id
	}
}

func WithPriority(priority uint) WorkflowOption {
	return func(p *workflowOptions) {
		p.Priority = priority
	}
}

func WithQueuePartitionKey(partitionKey string) WorkflowOption {
	return func(p *workflowOptions) {
		p.QueuePartitionKey = partitionKey
	}
}

// Must be used together with WithQueue.
func WithDelay(delay time.Duration) WorkflowOption {
	return func(p *workflowOptions) {
		p.DelayDuration = delay
	}
}

func withWorkflowName(name string) WorkflowOption {
	return func(p *workflowOptions) {
		if p.WorkflowName == "" {
			p.WorkflowName = name
		}
	}
}

// An internal option that overrides the FQN derived from the function pointer. Needed for
// workflows built from closures (e.g. debounce windows), whose runtime function names are
// not unique per registration.
func withWorkflowFQN(fqn string) WorkflowOption {
	return func(p *workflowOptions) {
		p.workflowFQN = fqn
	}
}

// An internal option we use to indicate that the input is already encoded, so we don't need to encode it again
func withAlreadyEncodedInput() WorkflowOption {
	return func(p *workflowOptions) {
		p.alreadyEncodedInput = true
	}
}

func withIsDequeue() WorkflowOption {
	return func(p *workflowOptions) {
		p.isDequeue = true
	}
}

func withIsRecovery() WorkflowOption {
	return func(p *workflowOptions) {
		p.isRecovery = true
	}
}

func WithPortableWorkflow() WorkflowOption {
	return func(p *workflowOptions) {
		p.isPortableWorkflow = true
	}
}

func WithAuthenticatedUser(user string) WorkflowOption {
	return func(p *workflowOptions) {
		p.AuthenticatedUser = user
	}
}

func WithAssumedRole(role string) WorkflowOption {
	return func(p *workflowOptions) {
		p.AssumedRole = role
	}
}

func WithAuthenticatedRoles(roles []string) WorkflowOption {
	return func(p *workflowOptions) {
		p.AuthenticatedRoles = roles
	}
}

func WithDebounce(key string, delay time.Duration) WorkflowOption {
	return func(p *workflowOptions) {
		p.debounce = true
		p.debounceKey = key
		p.debounceDelay = delay
	}
}

func WithDebounceTimeout(timeout time.Duration) WorkflowOption {
	return func(p *workflowOptions) {
		p.debounceTimeout = timeout
	}
}

// workflowClaim carries a durably recorded workflow invocation from the recording
// (scheduling) phase to the enactment (execution) phase. A non-nil pollingHandle means
// this process must not execute the workflow (enqueued, deduplicated, already finished,
// owned by another executor, or already running locally) and should hand the handle back.
type workflowClaim struct {
	workflowId    string
	params        workflowOptions
	insertResult  *insertWorkflowResult
	pollingHandle *WorkflowHandle[any]
}

// RunWorkflow durably records a workflow invocation and, when this process is the one
// responsible for executing it, enacts it locally. Direct calls record and enact;
// enqueued calls record only (a worker enacts later via the dequeue path); dequeue and
// recovery calls re-enter here to claim the existing record and enact it.
func (c *dbosContext) RunWorkflow(fn WorkflowFunc, input any, opts ...WorkflowOption) (*WorkflowHandle[any], error) {
	claim, err := c.recordWorkflow(input, opts...)
	if err != nil {
		return nil, err
	}
	if claim.pollingHandle != nil {
		return claim.pollingHandle, nil
	}
	return c.enactWorkflow(claim, fn, input), nil
}

// recordWorkflow is the scheduling half: resolve the registered name, insert the status
// row (or claim an existing one when dequeuing or recovering), and resolve deduplication.
// It performs no execution.
func (c *dbosContext) recordWorkflow(input any, opts ...WorkflowOption) (*workflowClaim, error) {

	params := workflowOptions{
		ApplicationVersion: c.GetApplicationVersion(),
	}
	for _, opt := range opts {
		opt(&params)
	}

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

	// Invocations only schedule: the workflow is recorded as ENQUEUED and a worker claims
	// and enacts it via the dequeue path, which is also the enforcement point for execution
	// policies (rate limits, global concurrency). Only dequeue and recovery re-entries
	// proceed to local enactment.
	enqueue := !params.isDequeue && !params.isRecovery

	if params.DelayDuration > 0 && !enqueue {
		return nil, newWorkflowExecutionError("", fmt.Errorf("delay can only be applied when enqueuing a workflow"))
	}

	parentWorkflowState, ok := c.Value(workflowStateKey).(*workflowState)
	hasParentWorkflow := ok && parentWorkflowState != nil

	var workflowId string
	if params.WorkflowId == "" {
		workflowId = uuid.New().String()
	} else {
		workflowId = params.WorkflowId
	}

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

	deadline, ok := c.Deadline()
	if !ok {
		deadline = time.Time{}
	}
	var timeout time.Duration
	if !deadline.IsZero() {
		timeout = time.Until(deadline)

		if timeout < 0 {
			timeout = 1 * time.Millisecond
		}
	}

	if status == WorkflowStatusEnqueued || status == WorkflowStatusDelayed {
		deadline = time.Time{}
	}

	if params.Priority > uint(math.MaxInt) {
		c.logger.Error("priority exceeds maximum allowed value", "workflow_name", params.WorkflowName, "priority", params.Priority, "max_allowed_value", math.MaxInt)
		return nil, fmt.Errorf("priority %d exceeds maximum allowed value %d", params.Priority, math.MaxInt)
	}

	var encodedInput any
	if params.alreadyEncodedInput {
		encodedInput = input
	} else if params.isPortableWorkflow {
		var serErr error
		encodedInput, serErr = encodePortableArgs(input)
		if serErr != nil {
			c.logger.Error("failed to serialize portable workflow input", "error", serErr, "workflow_id", workflowId)
			return nil, newWorkflowExecutionError(workflowId, fmt.Errorf("failed to serialize portable workflow input: %w", serErr))
		}
	} else {
		var serErr error
		encodedInput, serErr = resolveEncoder(c).Encode(input)
		if serErr != nil {
			c.logger.Error("failed to serialize workflow input", "error", serErr, "workflow_id", workflowId)
			return nil, newWorkflowExecutionError(workflowId, fmt.Errorf("failed to serialize workflow input: %w", serErr))
		}
	}

	workflowStatus := WorkflowStatus{
		Name:               params.WorkflowName,
		ApplicationVersion: params.ApplicationVersion,
		ExecutorId:         c.GetExecutorId(),
		Status:             status,
		Id:                 workflowId,
		CreatedAt:          time.Now(),
		Deadline:           deadline,
		Timeout:            timeout,
		Input:              encodedInput,
		ApplicationId:      c.GetApplicationId(),
		DeduplicationId:    params.DeduplicationId,
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
		workflowStatus.ParentWorkflowId = parentWorkflowState.workflowId
	}
	// Pin the instance to the definition that admitted it.
	workflowStatus.DefinitionDigest = registeredWorkflow.Digest

	var earlyReturnPollingHandle *WorkflowHandle[any]
	var insertStatusResult *insertWorkflowResult

	insertWorkflowStatusTx := func() error {
		tx, err := c.kernel.pool.BeginTx(uncancellableCtx, pgx.TxOptions{})
		if err != nil {
			return newWorkflowExecutionError(workflowId, fmt.Errorf("failed to begin transaction: %w", err))
		}
		defer tx.Rollback(uncancellableCtx)

		ownerXId := uuid.New().String()
		insertInput := insertWorkflowStatusDBInput{
			status:            workflowStatus,
			maxRetries:        params.MaxRetries,
			tx:                tx,
			ownerXId:          &ownerXId,
			incrementAttempts: params.isDequeue || params.isRecovery,
		}
		insertStatusResult, err = c.kernel.insertWorkflowStatus(uncancellableCtx, insertInput)
		if err != nil {
			if !errors.Is(err, errDeduplicationCollision) {
				c.logger.Error("failed to insert workflow status", "error", err, "workflow_id", workflowId)
			}
			return newWorkflowExecutionError(workflowId, fmt.Errorf("failed to insert workflow status: %w", err))
		}

		var loaded bool
		if c.activeWorkflowIds != nil {
			_, loaded = c.activeWorkflowIds.Load(workflowId)
		}

		shouldSkip :=
			enqueue ||
				insertStatusResult.status == WorkflowStatusSuccess ||
				insertStatusResult.status == WorkflowStatusError ||
				(!params.isDequeue && !params.isRecovery && insertStatusResult.ownerXId != ownerXId) ||
				loaded

		if shouldSkip {

			if err := tx.Commit(uncancellableCtx); err != nil {
				return newWorkflowExecutionError(workflowId, fmt.Errorf("failed to commit transaction: %w", err))
			}
			earlyReturnPollingHandle = newWorkflowHandle[any](uncancellableCtx, workflowStatus.Id)
			return nil
		}

		// Commit the transaction. This must happen before we start the goroutine to ensure the workflow is found by steps in the database
		if err := tx.Commit(uncancellableCtx); err != nil {
			return newWorkflowExecutionError(workflowId, fmt.Errorf("failed to commit transaction: %w", err))
		}

		return nil
	}

	for {
		err := retry(c, insertWorkflowStatusTx, withRetrierLogger(c.logger))
		if err == nil {

			break
		}
		// Now handle the case where the insert failed because the deduplication ID is already held by another workflow.
		if !errors.Is(err, errDeduplicationCollision) {
			return nil, err
		}
		existingId, lookupErr := retryWithResult(uncancellableCtx, func() (*string, error) {
			return c.kernel.getDeduplicatedWorkflow(uncancellableCtx, params.WorkflowName, params.DeduplicationId)
		}, withRetrierLogger(c.logger))
		if lookupErr != nil {
			return nil, newWorkflowExecutionError(workflowId, fmt.Errorf("looking up deduplicated workflow: %w", lookupErr))
		}
		if existingId == nil {
			continue
		}
		c.logger.Info("returning handle to existing deduplicated workflow", "workflow_name", params.WorkflowName, "queue_name", params.QueueName, "deduplication_id", params.DeduplicationId, "existing_workflow_id", *existingId)
		return &workflowClaim{pollingHandle: newWorkflowHandle[any](uncancellableCtx, *existingId)}, nil
	}
	if earlyReturnPollingHandle != nil {
		return &workflowClaim{pollingHandle: earlyReturnPollingHandle}, nil
	}

	return &workflowClaim{
		workflowId:   workflowId,
		params:       params,
		insertResult: insertStatusResult,
	}, nil
}

// enactWorkflow is the execution half: build the workflow context for a claimed record,
// arm the durable deadline, and run fn in a goroutine that records the outcome. The
// returned handle can be awaited immediately.
func (c *dbosContext) enactWorkflow(claim *workflowClaim, fn WorkflowFunc, input any) *WorkflowHandle[any] {
	workflowId := claim.workflowId
	params := claim.params
	insertStatusResult := claim.insertResult
	uncancellableCtx := WithoutCancel(c)

	wfState := &workflowState{
		workflowId:         workflowId,
		stepId:             -1,
		isPortableWorkflow: params.isPortableWorkflow,
		authenticatedUser:  params.AuthenticatedUser,
		assumedRole:        params.AssumedRole,
		authenticatedRoles: params.AuthenticatedRoles,
	}
	workflowCtx := WithValue(c, workflowStateKey, wfState)

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

		workflowCancelFunction := func() {
			c.logger.Info("Cancelling workflow", "workflow_id", workflowId)
			err := retry(c, func() error {
				_, err := c.kernel.cancelWorkflows(uncancellableCtx, cancelWorkflowsDBInput{workflowIds: []string{workflowId}})
				return err
			}, withRetrierLogger(c.logger))
			if err != nil {
				c.logger.Error("Failed to cancel workflow", "error", err)
			}
			close(cancelFuncCompleted)
		}
		stopFunc = context.AfterFunc(workflowCtx, workflowCancelFunction)
	}

	c.workflowsWg.Add(1)
	go func() {
		defer c.workflowsWg.Done()

		if c.activeWorkflowIds != nil {
			_, loaded := c.activeWorkflowIds.LoadOrStore(workflowId, activeWorkflowEntry{})
			if loaded {
				c.logger.Error("UNREACHABLE: workflow already running on this context", "workflow_id", workflowId)
			}
			defer c.activeWorkflowIds.Delete(workflowId)
		}

		var result any
		var err error

		result, err = fn(workflowCtx, input)

		if errors.Is(err, &DbosError{Code: ConflictingIdError}) {
			c.logger.Warn("Workflow ID conflict detected. Waiting for existing workflow to complete", "workflow_id", workflowId)
			_, awaitErr := retryWithResult(c, func() (*awaitWorkflowResultOutput, error) {
				return c.kernel.awaitWorkflowResult(uncancellableCtx, workflowId, _dbRetryInterval)
			}, withRetrierLogger(c.logger))
			if awaitErr != nil {
				c.logger.Error("Error awaiting conflicting workflow", "workflow_id", workflowId, "error", awaitErr)
			}
			return
		}
		status := WorkflowStatusSuccess

		if err != nil {
			status = WorkflowStatusError
		}

		if stopFunc != nil && !stopFunc() {
			c.logger.Info("Workflow was cancelled. Waiting for cancel function to complete", "workflow_id", workflowId)
			<-cancelFuncCompleted
			status = WorkflowStatusCancelled
		}

		encodedOutput, serErr := resolveEncoder(workflowCtx).Encode(result)
		if serErr != nil {
			c.logger.Error("Failed to serialize workflow output", "workflow_id", workflowId, "error", serErr)
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
				workflowId: workflowId,
				status:     status,
				errStr:     serializedErr,
				errEncoded: encodedErr,
				output:     encodedOutput,
			})
		}, withRetrierLogger(c.logger))
		if recordErr != nil {
			c.logger.Error("Error recording workflow outcome", "workflow_id", workflowId, "error", recordErr)
		}
	}()

	return newWorkflowHandle[any](uncancellableCtx, workflowId)
}

type StepFunc func(ctx context.Context) (any, error)

type Step[R any] func(ctx context.Context) (R, error)

type txnFunc func(ctx context.Context, tx pgx.Tx) (any, error)

type txn[R any] func(ctx context.Context, tx pgx.Tx) (R, error)

type stepOptions struct {
	maxRetries         int
	backoffFactor      float64
	baseInterval       time.Duration
	maxInterval        time.Duration
	stepName           string
	preGeneratedStepId *int
	txIsoLevel         *pgx.TxIsoLevel
}

func (opts *stepOptions) setDefaults() {
	if opts.backoffFactor == 0 {
		opts.backoffFactor = _defaultStepBackoffFactor
	}
	if opts.baseInterval == 0 {
		opts.baseInterval = _defaultStepBaseInterval
	}
	if opts.maxInterval == 0 {
		opts.maxInterval = _defaultStepMaxInterval
	}
}

type StepOption func(*stepOptions)

func WithStepName(name string) StepOption {
	return func(opts *stepOptions) {
		if opts.stepName == "" {
			opts.stepName = name
		}
	}
}

func WithStepMaxRetries(maxRetries int) StepOption {
	return func(opts *stepOptions) {
		opts.maxRetries = maxRetries
	}
}

func WithBackoffFactor(factor float64) StepOption {
	return func(opts *stepOptions) {
		opts.backoffFactor = factor
	}
}

func WithBaseInterval(interval time.Duration) StepOption {
	return func(opts *stepOptions) {
		opts.baseInterval = interval
	}
}

func WithMaxInterval(interval time.Duration) StepOption {
	return func(opts *stepOptions) {
		opts.maxInterval = interval
	}
}

func WithNextStepId(stepId int) StepOption {
	return func(opts *stepOptions) {
		opts.preGeneratedStepId = &stepId
	}
}

type StepOutcome[R any] struct {
	Result R     `json:"result"`
	Err    error `json:"err"`
}

type StreamValue[R any] struct {
	Value  R
	Err    error
	Closed bool
}

func convertStepResult[R any](ctx Context, result any) (R, error) {
	var typedResult R

	if _, ok := ctx.(*dbosContext); ok {

		if checkpointed, ok := result.(stepCheckpointedOutcome); ok {

			encodedOutput, ok := checkpointed.value.(*string)
			if !ok {
				workflowId, _ := GetWorkflowId(ctx)
				return *new(R), newWorkflowExecutionError(workflowId, fmt.Errorf("checkpointed outcome value is not *string, got %T", checkpointed.value))
			}
			var decodeErr error
			stepDecoder, resolveErr := resolveDecoder[R](checkpointed.serialization, getCustomSerializerFromCtx(ctx))
			if resolveErr != nil {
				workflowId, err := GetWorkflowId(ctx)
				if err != nil {
					return *new(R), fmt.Errorf("getting workflow ID from context: %w; original error: %v", err, resolveErr)
				}
				return *new(R), newWorkflowExecutionError(workflowId, resolveErr)
			}
			typedResult, decodeErr = stepDecoder.Decode(encodedOutput)
			if decodeErr != nil {
				workflowId, _ := GetWorkflowId(ctx)
				return *new(R), newWorkflowExecutionError(workflowId, fmt.Errorf("decoding step result to expected type %T: %w", *new(R), decodeErr))
			}
		} else if typedRes, ok := result.(R); ok {

			typedResult = typedRes
		} else {
			workflowId, _ := GetWorkflowId(ctx) // Must be within a workflow so we can ignore the error
			return *new(R), newWorkflowUnexpectedResultType(workflowId, fmt.Sprintf("%T", *new(R)), fmt.Sprintf("%T", result))
		}
	} else {

		if typedRes, ok := result.(R); ok {
			typedResult = typedRes
		} else {
			workflowId, _ := GetWorkflowId(ctx)
			return *new(R), newWorkflowUnexpectedResultType(workflowId, fmt.Sprintf("%T", *new(R)), fmt.Sprintf("%T", result))
		}
	}
	return typedResult, nil
}

type preparedStep struct {
	WorkflowId   string
	StepOpts     *stepOptions
	StepState    *workflowState
	IsWithinStep bool
}

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
		return &preparedStep{WorkflowId: wfState.workflowId, StepOpts: stepOpts, StepState: nil, IsWithinStep: true}, nil
	}

	var stepId int
	if stepOpts.preGeneratedStepId != nil {
		stepId = *stepOpts.preGeneratedStepId
	} else {
		stepId = wfState.nextStepId()
	}
	stepState := workflowState{
		workflowId:   wfState.workflowId,
		stepId:       stepId,
		isWithinStep: true,
	}
	return &preparedStep{WorkflowId: wfState.workflowId, StepOpts: stepOpts, StepState: &stepState, IsWithinStep: false}, nil
}

func executeStepWithRetry(c *dbosContext, workflowId string, stepOpts *stepOptions, runOnce func() (any, error)) (stepOutput any, stepError error) {
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
			return nil, newStepExecutionError(workflowId, stepOpts.stepName, fmt.Errorf("context cancelled during retry: %w", c.Err()))
		case <-time.After(delay):
		}
		stepOutput, stepError = runOnce()
		if stepError == nil {
			return stepOutput, stepError
		}
		joinedErrors = errors.Join(joinedErrors, stepError)
		if retry == stepOpts.maxRetries {
			stepError = newMaxStepRetriesExceededError(workflowId, stepOpts.stepName, stepOpts.maxRetries, joinedErrors)
			break
		}
	}
	return stepOutput, stepError
}

// result is returned instead of re-executing the function.

// Note that the function passed to Run must accept a context.Context as its first parameter

func Run[R any](ctx Context, fn Step[R], opts ...StepOption) (R, error) {
	if ctx == nil {
		return *new(R), newStepExecutionError("", "", fmt.Errorf("ctx cannot be nil"))
	}

	if fn == nil {
		return *new(R), newStepExecutionError("", "", fmt.Errorf("step function cannot be nil"))
	}

	// Append WithStepName option to ensure the step name is set. This will not erase a user-provided step name
	stepName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	opts = append(opts, WithStepName(stepName))

	typeErasedFn := StepFunc(func(ctx context.Context) (any, error) { return fn(ctx) })

	result, err := ctx.RunAsStep(typeErasedFn, opts...)

	if result == nil {
		return *new(R), err
	}
	typedResult, convertErr := convertStepResult[R](ctx, result)
	if convertErr != nil {
		return *new(R), convertErr
	}
	return typedResult, err
}

func Uuid(ctx Context) (string, error) {
	newUuid := func(context.Context) (string, error) {
		id, err := uuid.NewV7()
		if err != nil {
			return "", err
		}
		return id.String(), nil
	}
	if workflowState, ok := ctx.Value(workflowStateKey).(*workflowState); !ok || workflowState == nil {
		return newUuid(ctx)
	}
	return Run(ctx, newUuid, WithStepName("DBOS.uuid"))
}

func (c *dbosContext) RunAsStep(fn StepFunc, opts ...StepOption) (any, error) {
	prep, err := prepareStepExecution(c, opts)
	if err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, newStepExecutionError(prep.WorkflowId, prep.StepOpts.stepName, fmt.Errorf("step function cannot be nil"))
	}
	if prep.IsWithinStep {
		return fn(c)
	}

	uncancellableCtx := WithoutCancel(c)
	stepState := prep.StepState
	stepOpts := prep.StepOpts

	recordedOutput, err := retryWithResult(c, func() (*recordedResult, error) {
		return c.kernel.checkOperationExecution(uncancellableCtx, checkOperationExecutionDBInput{
			workflowId: stepState.workflowId,
			stepId:     stepState.stepId,
			stepName:   stepOpts.stepName,
		})
	}, withRetrierLogger(c.logger))
	if err != nil {
		return nil, newStepExecutionError(stepState.workflowId, stepOpts.stepName, fmt.Errorf("checking operation execution: %w", err))
	}
	if recordedOutput != nil {

		return stepCheckpointedOutcome{value: recordedOutput.output, serialization: recordedOutput.serialization}, deserializeWorkflowError(recordedOutput.errStr, recordedOutput.errEncoded, recordedOutput.serialization)
	}

	stepCtx := WithValue(c, workflowStateKey, stepState)
	stepStartTime := time.Now()
	stepOutput, stepError := executeStepWithRetry(c, stepState.workflowId, stepOpts, func() (any, error) { return fn(stepCtx) })

	ser := resolveEncoder(c)
	encodedStepOutput, serErr := ser.Encode(stepOutput)
	if serErr != nil {
		return nil, newStepExecutionError(stepState.workflowId, stepOpts.stepName, fmt.Errorf("failed to serialize step output: %w", serErr))
	}

	stepCompletedTime := time.Now()
	var serializedStepErr *string
	var encodedStepErr *string
	if stepError != nil {
		s := serializeWorkflowError(stepError, ser.Name())
		serializedStepErr = &s
		encodedStepErr = encodeWorkflowError(stepError)
	}
	dbInput := recordOperationResultDBInput{
		workflowId:    stepState.workflowId,
		stepName:      stepOpts.stepName,
		stepId:        stepState.stepId,
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
		return nil, newStepExecutionError(stepState.workflowId, stepOpts.stepName, recErr)
	}

	return stepOutput, stepError
}

func runAsTxn[R any](ctx Context, fn txn[R], opts ...StepOption) (R, error) {
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

	typeErasedFn := txnFunc(func(ctx context.Context, tx pgx.Tx) (any, error) { return fn(ctx, tx) })

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
		return nil, newStepExecutionError(prep.WorkflowId, prep.StepOpts.stepName, fmt.Errorf("step function cannot be nil"))
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

	txOpts := pgx.TxOptions{IsoLevel: pgx.ReadCommitted}
	if stepOpts.txIsoLevel != nil {
		txOpts.IsoLevel = *stepOpts.txIsoLevel
	}
	return retryWithResult(c, func() (any, error) {
		tx, err := pool.BeginTx(uncancellableCtx, txOpts)
		if err != nil {
			return nil, newStepExecutionError(stepState.workflowId, stepOpts.stepName, fmt.Errorf("failed to begin transaction: %w", err))
		}
		defer tx.Rollback(uncancellableCtx)

		recordedOutput, err := c.kernel.checkOperationExecution(uncancellableCtx, checkOperationExecutionDBInput{
			workflowId: stepState.workflowId,
			stepId:     stepState.stepId,
			stepName:   stepOpts.stepName,
			tx:         tx,
		})
		if err != nil {
			return nil, newStepExecutionError(stepState.workflowId, stepOpts.stepName, fmt.Errorf("checking operation execution: %w", err))
		}
		if recordedOutput != nil {
			return stepCheckpointedOutcome{value: recordedOutput.output, serialization: recordedOutput.serialization}, deserializeWorkflowError(recordedOutput.errStr, recordedOutput.errEncoded, recordedOutput.serialization)
		}

		stepOutput, stepError := executeStepWithRetry(c, stepState.workflowId, stepOpts, func() (any, error) { return fn(stepCtx, tx) })

		txnSer := resolveEncoder(c)
		encodedStepOutput, serErr := txnSer.Encode(stepOutput)
		if serErr != nil {
			return nil, newStepExecutionError(stepState.workflowId, stepOpts.stepName, fmt.Errorf("failed to serialize step output: %w", serErr))
		}

		var serializedTxnErr *string
		var encodedTxnErr *string
		if stepError != nil {
			s := serializeWorkflowError(stepError, txnSer.Name())
			serializedTxnErr = &s
			encodedTxnErr = encodeWorkflowError(stepError)
		}
		dbInput := recordOperationResultDBInput{
			workflowId:    stepState.workflowId,
			stepName:      stepOpts.stepName,
			stepId:        stepState.stepId,
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
			return nil, newStepExecutionError(stepState.workflowId, stepOpts.stepName, recErr)
		}
		if err := tx.Commit(uncancellableCtx); err != nil {
			return nil, newStepExecutionError(stepState.workflowId, stepOpts.stepName, fmt.Errorf("failed to commit transaction: %w", err))
		}
		return stepOutput, stepError
	}, withRetrierLogger(c.logger))
}

func Go[R any](ctx Context, fn Step[R], opts ...StepOption) (chan StepOutcome[R], error) {
	if ctx == nil {
		return nil, newStepExecutionError("", "", errors.New("ctx cannot be nil"))
	}

	if fn == nil {
		return nil, newStepExecutionError("", "", errors.New("step function cannot be nil"))
	}

	// Append WithStepName option to ensure the step name is set. This will not erase a user-provided step name
	stepName := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	opts = append(opts, WithStepName(stepName))

	typeErasedFn := StepFunc(func(ctx context.Context) (any, error) { return fn(ctx) })

	result, err := ctx.Go(typeErasedFn, opts...)
	if err != nil {
		return nil, err
	}

	outcomeChan := make(chan StepOutcome[R], 1)

	go func() {
		defer close(outcomeChan)

		outcome := <-result

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

	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return nil, newStepExecutionError("", "", errors.New("workflow state not found in context: are you running this step within a workflow?"))
	}
	opts = append(opts, WithNextStepId(wfState.nextStepId()))

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

// It checkpoints the selected channel index and value so that workflow replay produces deterministic results.

func Select[R any](ctx Context, channels []<-chan StepOutcome[R]) (R, error) {
	if ctx == nil {
		var zero R
		return zero, errors.New("ctx cannot be nil")
	}

	if len(channels) == 0 {
		if c, ok := ctx.(*dbosContext); ok {
			c.logger.Warn("Select called with empty channels slice, returning zero value")
		}
		var zero R
		return zero, nil
	}

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

						return
					}
					select {
					case anyCh <- StepOutcome[any]{
						Result: outcome.Result,
						Err:    outcome.Err,
					}:
					case <-selectCtx.Done():

						return
					}
				}
			}
		}()
		anyChannels[i] = anyCh
	}

	result, err := ctx.Select(anyChannels)

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

	if len(channels) == 0 {
		c.logger.Warn("Select called with empty channels slice, returning zero value")
		return nil, nil
	}

	result, err := c.RunAsStep(func(ctx context.Context) (any, error) {

		cases := make([]reflect.SelectCase, 0, len(channels)+1)

		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ctx.Done()),
		})

		for _, ch := range channels {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ch),
			})
		}

		chosen, value, ok := reflect.Select(cases)

		if chosen == 0 {
			return nil, ctx.Err()
		}

		if !ok {

			selectedIndex := chosen - 1
			// If context was cancelled, return cancellation error instead of channel closed error
			// This handles the race condition after a closed channel (due to cancellation) is selected
			// instead of context.Done() (both are eligible to be selected).
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("channel at index %d was closed", selectedIndex)
		}

		outcomeValue := value.Interface()
		outcome, ok := outcomeValue.(StepOutcome[any])
		if !ok {

			selectedIndex := chosen - 1
			return nil, fmt.Errorf("unexpected value type from channel at index %d: expected StepOutcome[any], got %T", selectedIndex, outcomeValue)
		}

		return outcome.Result, outcome.Err
	}, WithStepName("DBOS.select"))

	return result, err
}

type sendOptions struct {
	usePortableSerializer bool
}

type SendOption func(*sendOptions)

func WithPortableSend() SendOption {
	return func(opts *sendOptions) {
		opts.usePortableSerializer = true
	}
}

func (c *dbosContext) Send(destinationId string, message any, topic string, opts ...SendOption) error {
	// Send cannot be sent from within a step if used within a workflow
	isWithinWorkflow := false
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if ok && wfState != nil {
		isWithinWorkflow = true
		if wfState.isWithinStep {
			return newStepExecutionError(wfState.workflowId, "DBOS.send", fmt.Errorf("cannot call Send within a step"))
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
		DestinationId: destinationId,
		Message:       encodedMessage,
		Topic:         topic,
		serialization: sendSer.Name(),
	}

	if isWithinWorkflow {
		_, err = runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
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

func Send[P any](ctx Context, destinationId string, message P, topic string, opts ...SendOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.Send(destinationId, message, topic, opts...)
}

type recvInput struct {
	Topic         string
	Timeout       time.Duration
	serialization string
}

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
		return nil, newStepExecutionError(wfState.workflowId, "DBOS.recv", fmt.Errorf("cannot call Recv within a step"))
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

func Recv[R any](ctx Context, topic string, timeout time.Duration) (R, error) {
	if ctx == nil {
		return *new(R), errors.New("ctx cannot be nil")
	}
	msg, err := ctx.Recv(topic, timeout)
	if err != nil {
		return *new(R), err
	}

	if msg == nil {
		return *new(R), nil
	}

	result, ok := msg.(*recvResult)
	if !ok {
		workflowId, _ := GetWorkflowId(ctx) // Must be within a workflow so we can ignore the error
		return *new(R), newWorkflowUnexpectedResultType(workflowId, "*recvResult", fmt.Sprintf("%T", msg))
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

type setEventOptions struct {
	usePortableSerializer bool
}

type SetEventOption func(*setEventOptions)

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

	_, err = runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
		return nil, c.kernel.setEvent(ctx, WorkflowSetEventInput{
			Key:           key,
			Message:       encodedMessage,
			tx:            tx,
			serialization: evtSer.Name(),
		})
	}, WithStepName("DBOS.setEvent"))
	return err
}

func SetEvent[P any](ctx Context, key string, message P, opts ...SetEventOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.SetEvent(key, message, opts...)
}

type getEventInput struct {
	TargetWorkflowId string
	Key              string
	Timeout          time.Duration
	serialization    string
}

type getEventResult struct {
	value         *string
	serialization string
}

func (c *dbosContext) GetEvent(targetWorkflowId, key string, timeout time.Duration) (any, error) {
	input := getEventInput{
		TargetWorkflowId: targetWorkflowId,
		Key:              key,
		Timeout:          timeout,
		serialization:    resolveEncoder(c).Name(),
	}
	return retryWithResult(c, func() (*getEventResult, error) {
		return c.kernel.getEvent(c, input)
	}, withRetrierLogger(c.logger))
}

func GetEvent[R any](ctx Context, targetWorkflowId, key string, timeout time.Duration) (R, error) {
	if ctx == nil {
		return *new(R), errors.New("ctx cannot be nil")
	}
	value, err := ctx.GetEvent(targetWorkflowId, key, timeout)
	if err != nil {
		return *new(R), err
	}
	if value == nil {
		return *new(R), nil
	}

	var typedValue R

	if _, ok := ctx.(*dbosContext); ok {
		result, ok := value.(*getEventResult)
		if !ok {
			workflowId, _ := GetWorkflowId(ctx) // Must be within a workflow so we can ignore the error
			return *new(R), newWorkflowUnexpectedResultType(workflowId, "*getEventResult", fmt.Sprintf("%T", value))
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
			workflowId, _ := GetWorkflowId(ctx) // Must be within a workflow so we can ignore the error
			return *new(R), newWorkflowUnexpectedResultType(workflowId, fmt.Sprintf("%T", new(R)), fmt.Sprintf("%T", value))
		}
	}
	return typedValue, nil
}

type writeStreamOptions struct {
	usePortableSerializer bool
}

type WriteStreamOption func(*writeStreamOptions)

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

	_, err = runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
		return "", c.kernel.writeStream(ctx, writeStreamDBInput{
			Key:           key,
			Value:         encodedValue,
			tx:            tx,
			serialization: ser.Name(),
		})
	}, WithStepName("DBOS.writeStream"))
	return err
}

func WriteStream[P any](ctx Context, key string, value P, opts ...WriteStreamOption) error {
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

// values have been drained, instead of blocking until the stream is closed or

func WithReadStreamSnapshot(fromOffset int) ReadStreamOption {
	return func(o *readStreamOptions) {
		o.snapshot = true
		o.fromOffset = fromOffset
	}
}

func (c *dbosContext) readStream(workflowId string, key string, snapshot bool, fromOffset int) <-chan StreamValue[any] {
	ch := make(chan StreamValue[any], 1)

	go func() {
		defer close(ch)

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

		for {

			input := readStreamDBInput{
				WorkflowId: workflowId,
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

			for _, entry := range entries {
				if !send(StreamValue[any]{Value: streamEntryWithSerialization{value: entry.Value, serialization: entry.Serialization}}) {
					return
				}
				currentOffset = entry.Offset + 1
			}

			if closed {
				send(StreamValue[any]{Closed: true})
				return
			}

			// so stop here instead of polling for more.
			if snapshot {
				return
			}

			status, err := retryWithResult(c, func() (WorkflowStatusType, error) {
				workflows, err := c.kernel.listWorkflows(c, listWorkflowsDBInput{
					workflowIds: []string{workflowId},
					loadInput:   false,
					loadOutput:  false,
				})
				if err != nil {
					return "", err
				}
				if len(workflows) == 0 {
					return "", newNonExistentWorkflowError(workflowId)
				}
				return workflows[0].Status, nil
			}, withRetrierLogger(c.logger))

			if err != nil {
				send(StreamValue[any]{Err: err})
				return
			}

			if status != WorkflowStatusPending && status != WorkflowStatusEnqueued {
				send(StreamValue[any]{Closed: true})
				return
			}

			if len(entries) == 0 {
				select {
				case <-c.Done():
					send(StreamValue[any]{Err: c.Err()})
					return
				case <-time.After(_dbRetryInterval):

				}
			}
		}
	}()

	return ch
}

type streamEntryWithSerialization struct {
	value         string
	serialization string
}

func (c *dbosContext) ReadStream(workflowId string, key string, opts ...ReadStreamOption) ([]any, bool, error) {
	var o readStreamOptions
	for _, opt := range opts {
		opt(&o)
	}

	var allValues []any
	closed := false

	ch := c.readStream(workflowId, key, o.snapshot, o.fromOffset)

	for streamValue := range ch {
		if streamValue.Err != nil {
			return nil, false, streamValue.Err
		}

		if streamValue.Closed {
			closed = true
			break
		}

		allValues = append(allValues, streamValue.Value)
	}

	return allValues, closed, nil
}

func ReadStream[R any](ctx Context, workflowId string, key string, opts ...ReadStreamOption) ([]R, bool, error) {
	if ctx == nil {
		return nil, false, errors.New("ctx cannot be nil")
	}
	values, closed, err := ctx.ReadStream(workflowId, key, opts...)
	if err != nil {
		return nil, false, err
	}

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

func (c *dbosContext) ReadStreamAsync(workflowId string, key string) (<-chan StreamValue[any], error) {
	return c.readStream(workflowId, key, false, 0), nil
}

func ReadStreamAsync[R any](ctx Context, workflowId string, key string) (<-chan StreamValue[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	anyCh, err := ctx.ReadStreamAsync(workflowId, key)
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
	_, err := runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
		sentinel := _dbosStreamClosedSentinel
		return "", c.kernel.writeStream(ctx, writeStreamDBInput{
			Key:   key,
			Value: &sentinel,
			tx:    tx,
		})
	}, WithStepName("DBOS.closeStream"))
	return err
}

func CloseStream(ctx Context, key string) error {
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
		return 0, newStepExecutionError(wfState.workflowId, "DBOS.sleep", fmt.Errorf("cannot call Sleep within a step"))
	}
	return retryWithResult(c, func() (time.Duration, error) {
		return c.kernel.sleep(c, sleepInput{duration: duration, skipSleep: false})
	}, withRetrierLogger(c.logger))
}

func Sleep(ctx Context, duration time.Duration) (time.Duration, error) {
	if ctx == nil {
		return 0, errors.New("ctx cannot be nil")
	}
	return ctx.Sleep(duration)
}

const _dbosPatchPrefix = "DBOS.patch-"

func (c *dbosContext) Patch(patchName string) (bool, error) {
	if !c.config.EnablePatching {
		return false, newPatchingNotEnabledError()
	}

	if patchName == "" {
		return false, errors.New("patch name cannot be empty")
	}

	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return false, errors.New("patch can only be called within a workflow")
	}

	if wfState.isWithinStep {
		return false, newStepExecutionError(wfState.workflowId, patchName, fmt.Errorf("cannot call Patch within a step"))
	}

	prefixedPatchName := _dbosPatchPrefix + patchName

	patched, err := retryWithResult(c, func() (bool, error) {
		return c.kernel.patch(c, patchDBInput{
			workflowId: wfState.workflowId,
			stepId:     wfState.stepId + 1,
			patchName:  prefixedPatchName,
		})
	}, withRetrierLogger(c.logger))

	if patched && err == nil {

		wfState.nextStepId()
	}

	return patched, err
}

func Patch(ctx Context, patchName string) (bool, error) {
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

	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return errors.New("deprecate patch can only be called within a workflow")
	}

	if wfState.isWithinStep {
		return newStepExecutionError(wfState.workflowId, patchName, fmt.Errorf("cannot call DeprecatePatch within a step"))
	}

	prefixedPatchName := _dbosPatchPrefix + patchName

	patchNameFromDB, err := retryWithResult(c, func() (string, error) {
		return c.kernel.doesPatchExists(c, patchDBInput{
			workflowId: wfState.workflowId,
			stepId:     wfState.stepId + 1,
			patchName:  prefixedPatchName,
		})
	}, withRetrierLogger(c.logger))

	// If patch doesn't exist, it's already deprecated (or never existed)
	if patchNameFromDB != prefixedPatchName || err == pgx.ErrNoRows {
		return nil
	}

	if err != nil {
		return err
	}

	wfState.nextStepId()
	return nil
}

func DeprecatePatch(ctx Context, patchName string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.DeprecatePatch(patchName)
}

func (c *dbosContext) GetWorkflowId() (string, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return "", errors.New("not within a DBOS workflow context")
	}
	return wfState.workflowId, nil
}

func (c *dbosContext) GetStepId() (int, error) {
	wfState, ok := c.Value(workflowStateKey).(*workflowState)
	if !ok || wfState == nil {
		return -1, errors.New("not within a DBOS workflow context")
	}
	return wfState.stepId, nil
}

func GetWorkflowId(ctx Context) (string, error) {
	if ctx == nil {
		return "", errors.New("ctx cannot be nil")
	}
	return ctx.GetWorkflowId()
}

func GetStepId(ctx Context) (int, error) {
	if ctx == nil {
		return -1, errors.New("ctx cannot be nil")
	}
	return ctx.GetStepId()
}

func (c *dbosContext) RetrieveWorkflow(workflowId string) (*WorkflowHandle[any], error) {
	loadInput := false
	loadOutput := false
	if c.started.Load() {
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
					workflowIds: []string{workflowId},
					loadInput:   loadInput,
					loadOutput:  loadOutput,
				})
			}, withRetrierLogger(c.logger))
		}, WithStepName("DBOS.retrieveWorkflow"))
	} else {
		workflowStatus, err = retryWithResult(c, func() ([]WorkflowStatus, error) {
			return c.kernel.listWorkflows(c, listWorkflowsDBInput{
				workflowIds: []string{workflowId},
				loadInput:   loadInput,
				loadOutput:  loadOutput,
			})
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve workflow status: %w", err)
	}
	if len(workflowStatus) == 0 {
		return nil, newNonExistentWorkflowError(workflowId)
	}
	return newWorkflowHandle[any](c, workflowId), nil
}

// The type parameter R must match the workflow's actual return type.

func RetrieveWorkflow[R any](ctx Context, workflowId string) (*WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("dbosCtx cannot be nil")
	}

	handle, err := ctx.(*dbosContext).RetrieveWorkflow(workflowId)
	if err != nil {
		return nil, err
	}

	return newWorkflowHandle[R](ctx, handle.GetWorkflowId()), nil
}

func (c *dbosContext) CancelWorkflow(workflowId string) error {
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var found []string
	var err error
	if isWithinWorkflow {
		found, err = runAsTxn(c, func(ctx context.Context, tx pgx.Tx) ([]string, error) {
			return c.kernel.cancelWorkflows(ctx, cancelWorkflowsDBInput{workflowIds: []string{workflowId}, tx: tx})
		}, WithStepName("DBOS.cancelWorkflow"))
	} else {
		found, err = retryWithResult(c, func() ([]string, error) {
			return c.kernel.cancelWorkflows(c, cancelWorkflowsDBInput{workflowIds: []string{workflowId}})
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return err
	}
	if len(found) == 0 {
		return newNonExistentWorkflowError(workflowId)
	}
	return nil
}

// Returns an error if the workflow does not exist or if the cancellation operation fails.

func CancelWorkflow(ctx Context, workflowId string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).CancelWorkflow(workflowId)
}

func (c *dbosContext) CancelWorkflows(workflowIds []string) error {
	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		_, err := runAsTxn(c, func(ctx context.Context, tx pgx.Tx) ([]string, error) {
			return c.kernel.cancelWorkflows(ctx, cancelWorkflowsDBInput{workflowIds: workflowIds, tx: tx})
		}, WithStepName("DBOS.cancelWorkflows"))
		return err
	}
	_, err := retryWithResult(c, func() ([]string, error) {
		return c.kernel.cancelWorkflows(c, cancelWorkflowsDBInput{workflowIds: workflowIds})
	}, withRetrierLogger(c.logger))
	return err
}

// skipped. Unlike the singular CancelWorkflow, this function does not return

func CancelWorkflows(ctx Context, workflowIds []string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).CancelWorkflows(workflowIds)
}

type SetWorkflowDelayOption func(*setWorkflowDelayOptions)

type setWorkflowDelayOptions struct {
	delay      time.Duration
	delayUntil time.Time
}

func WithDelayDuration(d time.Duration) SetWorkflowDelayOption {
	return func(o *setWorkflowDelayOptions) {
		o.delay = d
	}
}

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

func (c *dbosContext) SetWorkflowDelay(workflowId string, opts ...SetWorkflowDelayOption) error {
	delayUntil, err := resolveDelayUntil(opts)
	if err != nil {
		return err
	}
	input := setWorkflowDelayDBInput{workflowId: workflowId, delayUntil: delayUntil}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		_, err := runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
			input.tx = tx
			return nil, c.kernel.setWorkflowDelay(ctx, input)
		}, WithStepName("DBOS.setWorkflowDelay"))
		return err
	}
	return retry(c, func() error {
		return c.kernel.setWorkflowDelay(c, input)
	}, withRetrierLogger(c.logger))
}

func SetWorkflowDelay(ctx Context, workflowId string, opts ...SetWorkflowDelayOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).SetWorkflowDelay(workflowId, opts...)
}

func (c *dbosContext) DeleteWorkflows(workflowIds []string, opts ...DeleteWorkflowOption) error {

	params := &deleteWorkflowOptions{}
	for _, opt := range opts {
		opt(params)
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		_, err := runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
			err := c.kernel.deleteWorkflows(ctx, deleteWorkflowsDBInput{
				workflowIds:    workflowIds,
				deleteChildren: params.deleteChildren,
				tx:             tx,
			})
			return "", err
		}, WithStepName("DBOS.deleteWorkflows"))
		return err
	} else {
		return retry(c, func() error {
			return c.kernel.deleteWorkflows(c, deleteWorkflowsDBInput{
				workflowIds:    workflowIds,
				deleteChildren: params.deleteChildren,
			})
		}, withRetrierLogger(c.logger))
	}
}

type deleteWorkflowOptions struct {
	deleteChildren bool
}

type DeleteWorkflowOption func(*deleteWorkflowOptions)

func WithDeleteChildren() DeleteWorkflowOption {
	return func(o *deleteWorkflowOptions) {
		o.deleteChildren = true
	}
}

func DeleteWorkflows(ctx Context, workflowIds []string, opts ...DeleteWorkflowOption) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).DeleteWorkflows(workflowIds, opts...)
}

type resumeWorkflowOptions struct {
	queueName string
}

type ResumeWorkflowOption func(*resumeWorkflowOptions)

// WithResumeQueue re-enqueues the resumed workflow(s) on the specified queue instead of the internal queue.
func WithResumeQueue(queueName string) ResumeWorkflowOption {
	return func(o *resumeWorkflowOptions) {
		o.queueName = queueName
	}
}

func (c *dbosContext) ResumeWorkflow(workflowId string, opts ...ResumeWorkflowOption) (*WorkflowHandle[any], error) {
	handles, err := c.ResumeWorkflows([]string{workflowId}, opts...)
	if err != nil {
		return nil, err
	}
	if len(handles) == 0 {
		return nil, newNonExistentWorkflowError(workflowId)
	}
	return handles[0], nil
}

func (c *dbosContext) ResumeWorkflows(workflowIds []string, opts ...ResumeWorkflowOption) ([]*WorkflowHandle[any], error) {
	params := &resumeWorkflowOptions{}
	for _, opt := range opts {
		opt(params)
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var foundIds []string
	var err error
	if isWithinWorkflow {
		foundIds, err = runAsTxn(c, func(ctx context.Context, tx pgx.Tx) ([]string, error) {
			return c.kernel.resumeWorkflows(ctx, resumeWorkflowsDBInput{
				workflowIds: workflowIds,
				queueName:   params.queueName,
				tx:          tx,
			})
		}, WithStepName("DBOS.resumeWorkflow"))
	} else {
		foundIds, err = retryWithResult(c, func() ([]string, error) {
			return c.kernel.resumeWorkflows(c, resumeWorkflowsDBInput{
				workflowIds: workflowIds,
				queueName:   params.queueName,
			})
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, err
	}

	handles := make([]*WorkflowHandle[any], 0, len(foundIds))
	for _, id := range foundIds {
		handles = append(handles, newWorkflowHandle[any](c, id))
	}
	return handles, nil
}

// Returns an error if the workflow does not exist or if the operation fails.

//   - WithResumeQueue: re-enqueue the workflow on a named queue instead of the internal queue.

func ResumeWorkflow[R any](ctx Context, workflowId string, opts ...ResumeWorkflowOption) (*WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	_, err := ctx.(*dbosContext).ResumeWorkflow(workflowId, opts...)
	if err != nil {
		return nil, err
	}
	return newWorkflowHandle[R](ctx, workflowId), nil
}

// Unlike the singular ResumeWorkflow, this function does not return NonExistentWorkflowError

//   - WithResumeQueue: re-enqueue the workflows on a named queue instead of the internal queue.

func ResumeWorkflows[R any](ctx Context, workflowIds []string, opts ...ResumeWorkflowOption) ([]*WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	anyHandles, err := ctx.(*dbosContext).ResumeWorkflows(workflowIds, opts...)
	if err != nil {
		return nil, err
	}
	handles := make([]*WorkflowHandle[R], 0, len(anyHandles))
	for _, h := range anyHandles {
		handles = append(handles, newWorkflowHandle[R](ctx, h.GetWorkflowId()))
	}
	return handles, nil
}

type ForkWorkflowInput struct {
	OriginalWorkflowId string
	ForkedWorkflowId   string
	StartStep          uint
	ApplicationVersion string
	QueueName          string
	QueuePartitionKey  string
}

func (c *dbosContext) ForkWorkflow(input ForkWorkflowInput) (*WorkflowHandle[any], error) {
	if input.OriginalWorkflowId == "" {
		return nil, errors.New("original workflow ID cannot be empty")
	}
	if input.QueuePartitionKey != "" && input.QueueName == "" {
		return nil, errors.New("queue partition key requires a queue name")
	}

	if input.StartStep > uint(math.MaxInt) {
		return nil, fmt.Errorf("start step too large: %d", input.StartStep)
	}
	dbInput := forkWorkflowDBInput{
		originalWorkflowId: input.OriginalWorkflowId,
		forkedWorkflowId:   input.ForkedWorkflowId,
		startStep:          int(input.StartStep),
		applicationVersion: input.ApplicationVersion,
		queueName:          input.QueueName,
		queuePartitionKey:  input.QueuePartitionKey,
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	var forkedWorkflowId string
	var err error
	if isWithinWorkflow {
		forkedWorkflowId, err = runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (string, error) {
			dbInput.tx = tx
			return c.kernel.forkWorkflow(ctx, dbInput)
		}, WithStepName("DBOS.forkWorkflow"))
	} else {
		forkedWorkflowId, err = retryWithResult(c, func() (string, error) {
			return c.kernel.forkWorkflow(c, dbInput)
		}, withRetrierLogger(c.logger))
	}
	if err != nil {
		return nil, err
	}

	return newWorkflowHandle[any](c, forkedWorkflowId), nil
}

//	// Fork onto a named queue instead of the internal queue.

func ForkWorkflow[R any](ctx Context, input ForkWorkflowInput) (*WorkflowHandle[R], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}

	handle, err := ctx.(*dbosContext).ForkWorkflow(input)
	if err != nil {
		return nil, err
	}
	return newWorkflowHandle[R](ctx, handle.GetWorkflowId()), nil
}

type listWorkflowsOptions struct {
	workflowIds      []string
	status           []WorkflowStatusType
	startTime        time.Time
	endTime          time.Time
	name             []string
	appVersion       []string
	user             []string
	limit            *int
	offset           *int
	sortDesc         bool
	workflowIdPrefix []string
	loadInput        bool
	loadOutput       bool
	queueName        []string
	queuesOnly       bool
	executorIds      []string
	forkedFrom       []string
	parentWorkflowId []string
	deduplicationId  []string
	completedAfter   time.Time
	completedBefore  time.Time
	dequeuedAfter    time.Time
	dequeuedBefore   time.Time
	wasForkedFrom    *bool
	hasParent        *bool
}

type ListWorkflowsOption func(*listWorkflowsOptions)

func WithWorkflowIds(workflowIds []string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.workflowIds = workflowIds
	}
}

func WithStatus(status []WorkflowStatusType) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.status = status
	}
}

func WithStartTime(startTime time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.startTime = startTime
	}
}

func WithEndTime(endTime time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.endTime = endTime
	}
}

func WithName(name ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.name = name
	}
}

func WithAppVersion(appVersion ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.appVersion = appVersion
	}
}

func WithUser(user ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.user = user
	}
}

func WithLimit(limit int) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.limit = &limit
	}
}

func WithOffset(offset int) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.offset = &offset
	}
}

func WithSortDesc() ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.sortDesc = true
	}
}

func WithWorkflowIdPrefix(prefix ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.workflowIdPrefix = prefix
	}
}

func WithLoadInput(loadInput bool) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.loadInput = loadInput
	}
}

func WithLoadOutput(loadOutput bool) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.loadOutput = loadOutput
	}
}

func WithExecutorIds(executorIds []string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.executorIds = executorIds
	}
}

func WithForkedFrom(forkedFrom ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.forkedFrom = forkedFrom
	}
}

func WithParentWorkflowId(parentWorkflowId ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.parentWorkflowId = parentWorkflowId
	}
}

func WithFilterDeduplicationId(deduplicationId ...string) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.deduplicationId = deduplicationId
	}
}

func WithCompletedAfter(completedAfter time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.completedAfter = completedAfter
	}
}

func WithCompletedBefore(completedBefore time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.completedBefore = completedBefore
	}
}

func WithDequeuedAfter(dequeuedAfter time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.dequeuedAfter = dequeuedAfter
	}
}

func WithDequeuedBefore(dequeuedBefore time.Time) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.dequeuedBefore = dequeuedBefore
	}
}

func WithWasForkedFrom(wasForkedFrom bool) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.wasForkedFrom = &wasForkedFrom
	}
}

func WithHasParent(hasParent bool) ListWorkflowsOption {
	return func(p *listWorkflowsOptions) {
		p.hasParent = &hasParent
	}
}

func (c *dbosContext) ListWorkflows(opts ...ListWorkflowsOption) ([]WorkflowStatus, error) {

	loadInput := true
	loadOutput := true
	if !c.started.Load() {
		loadInput = false
		loadOutput = false
	}
	params := &listWorkflowsOptions{
		loadInput:  loadInput,
		loadOutput: loadOutput,
	}

	for _, opt := range opts {
		opt(params)
	}

	if params.queuesOnly && len(params.status) == 0 {
		params.status = []WorkflowStatusType{WorkflowStatusEnqueued, WorkflowStatusPending, WorkflowStatusDelayed}
	}

	dbInput := listWorkflowsDBInput{
		workflowIds:        params.workflowIds,
		status:             params.status,
		startTime:          params.startTime,
		endTime:            params.endTime,
		workflowName:       params.name,
		applicationVersion: params.appVersion,
		authenticatedUser:  params.user,
		limit:              params.limit,
		offset:             params.offset,
		sortDesc:           params.sortDesc,
		workflowIdPrefix:   params.workflowIdPrefix,
		loadInput:          params.loadInput,
		loadOutput:         params.loadOutput,
		queueName:          params.queueName,
		queuesOnly:         params.queuesOnly,
		executorIds:        params.executorIds,
		forkedFrom:         params.forkedFrom,
		parentWorkflowId:   params.parentWorkflowId,
		deduplicationId:    params.deduplicationId,
		completedAfter:     params.completedAfter,
		completedBefore:    params.completedBefore,
		dequeuedAfter:      params.dequeuedAfter,
		dequeuedBefore:     params.dequeuedBefore,
		wasForkedFrom:      params.wasForkedFrom,
		hasParent:          params.hasParent,
	}

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

					workflows[i].Input = *encodedInput
				} else if c.serializer != nil {
					decoded, err := c.serializer.Decode(encodedInput)
					if err != nil {
						return nil, fmt.Errorf("failed to decode workflow input for %s: %w", workflows[i].Id, err)
					}
					workflows[i].Input = decoded
				} else {
					decodedBytes, err := base64.StdEncoding.DecodeString(*encodedInput)
					if err != nil {
						return nil, fmt.Errorf("failed to decode base64 workflow input for %s: %w", workflows[i].Id, err)
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

					workflows[i].Output = *encodedOutput
				} else if c.serializer != nil {
					decoded, err := c.serializer.Decode(encodedOutput)
					if err != nil {
						return nil, fmt.Errorf("failed to decode workflow output for %s: %w", workflows[i].Id, err)
					}
					workflows[i].Output = decoded
				} else {
					decodedBytes, err := base64.StdEncoding.DecodeString(*encodedOutput)
					if err != nil {
						return nil, fmt.Errorf("failed to decode base64 workflow output for %s: %w", workflows[i].Id, err)
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

//	// List workflows by specific IDs without loading input/output data

func ListWorkflows(ctx Context, opts ...ListWorkflowsOption) ([]WorkflowStatus, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).ListWorkflows(opts...)
}

type StepInfo struct {
	StepId      int
	StepName    string
	Output      any
	Error       error
	StartedAt   time.Time
	CompletedAt time.Time
}

type getWorkflowStepsOptions struct {
	loadOutput *bool
}

type GetWorkflowStepsOption func(*getWorkflowStepsOptions)

// When unset, output is loaded only if the DBOS context has been started.
func WithStepsLoadOutput(loadOutput bool) GetWorkflowStepsOption {
	return func(o *getWorkflowStepsOptions) {
		o.loadOutput = &loadOutput
	}
}

func (c *dbosContext) GetWorkflowSteps(workflowId string, opts ...GetWorkflowStepsOption) ([]StepInfo, error) {
	options := getWorkflowStepsOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	loadOutput := c.started.Load()
	if options.loadOutput != nil {
		loadOutput = *options.loadOutput
	}
	getWorkflowStepsInput := getWorkflowStepsInput{
		workflowId: workflowId,
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
			StepId:      step.StepId,
			StepName:    step.StepName,
			Error:       stepErr,
			StartedAt:   step.StartedAt,
			CompletedAt: step.CompletedAt,
		}
	}

	if loadOutput {
		for i := range steps {
			encodedOutput := steps[i].Output
			if encodedOutput == nil || *encodedOutput == nilMarker {
				stepInfos[i].Output = nil
				continue
			}
			if steps[i].Serialization == PortableSerializerName {

				stepInfos[i].Output = *encodedOutput
			} else if c.serializer != nil {

				decoded, err := c.serializer.Decode(encodedOutput)
				if err != nil {
					return nil, fmt.Errorf("failed to decode step output for step %d: %w", steps[i].StepId, err)
				}
				stepInfos[i].Output = decoded
			} else {

				decodedBytes, err := base64.StdEncoding.DecodeString(*encodedOutput)
				if err != nil {
					return nil, fmt.Errorf("failed to decode base64 step output for step %d: %w", steps[i].StepId, err)
				}
				stepInfos[i].Output = string(decodedBytes)
			}
		}
	}

	return stepInfos, nil
}

func GetWorkflowSteps(ctx Context, workflowId string, opts ...GetWorkflowStepsOption) ([]StepInfo, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).GetWorkflowSteps(workflowId, opts...)
}

// At least one of the GroupBy* flags must be true, or TimeBucketSize must be > 0.
type GetWorkflowAggregatesInput struct {
	GroupByStatus             bool
	GroupByName               bool
	GroupByQueueName          bool
	GroupByExecutorId         bool
	GroupByApplicationVersion bool

	TimeBucketSize time.Duration

	Status             []WorkflowStatusType
	StartTime          time.Time
	EndTime            time.Time
	Name               []string
	ApplicationVersion []string
	ExecutorId         []string
	QueueName          []string
	WorkflowIdPrefix   []string
}

func (c *dbosContext) GetWorkflowAggregates(input GetWorkflowAggregatesInput) ([]WorkflowAggregateRow, error) {
	if input.TimeBucketSize < 0 {
		return nil, errors.New("TimeBucketSize must be >= 0")
	}
	dbInput := getWorkflowAggregatesDBInput{
		groupByStatus:             input.GroupByStatus,
		groupByName:               input.GroupByName,
		groupByQueueName:          input.GroupByQueueName,
		groupByExecutorId:         input.GroupByExecutorId,
		groupByApplicationVersion: input.GroupByApplicationVersion,
		timeBucketSizeMs:          input.TimeBucketSize.Milliseconds(),
		status:                    input.Status,
		startTime:                 input.StartTime,
		endTime:                   input.EndTime,
		workflowName:              input.Name,
		applicationVersion:        input.ApplicationVersion,
		executorId:                input.ExecutorId,
		queueName:                 input.QueueName,
		workflowIdPrefix:          input.WorkflowIdPrefix,
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		return runAsTxn(c, func(ctx context.Context, tx pgx.Tx) ([]WorkflowAggregateRow, error) {
			in := dbInput
			in.tx = tx
			return c.kernel.getWorkflowAggregates(ctx, in)
		}, WithStepName("DBOS.getWorkflowAggregates"))
	}
	return retryWithResult(c, func() ([]WorkflowAggregateRow, error) {
		return c.kernel.getWorkflowAggregates(c, dbInput)
	}, withRetrierLogger(c.logger))
}

// At least one GroupBy* flag in the input must be true, or TimeBucketSize must be > 0.

// NULL grouping values (e.g. workflows without a queue_name).

func GetWorkflowAggregates(ctx Context, input GetWorkflowAggregatesInput) ([]WorkflowAggregateRow, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).GetWorkflowAggregates(input)
}

// At least one of the GroupBy* flags must be true, or TimeBucketSize must be > 0.
// At least one of the Select* flags must be true.
type GetStepAggregatesInput struct {
	GroupByFunctionName bool
	GroupByStatus       bool

	SelectCount         bool
	SelectMaxDurationMs bool

	TimeBucketSize time.Duration

	Status           []string
	FunctionName     []string
	WorkflowIdPrefix []string
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
		workflowIdPrefix:    input.WorkflowIdPrefix,
		completedAfter:      input.CompletedAfter,
		completedBefore:     input.CompletedBefore,
	}

	workflowState, ok := c.Value(workflowStateKey).(*workflowState)
	isWithinWorkflow := ok && workflowState != nil
	if isWithinWorkflow {
		return runAsTxn(c, func(ctx context.Context, tx pgx.Tx) ([]StepAggregateRow, error) {
			in := dbInput
			in.tx = tx
			return c.kernel.getStepAggregates(ctx, in)
		}, WithStepName("DBOS.getStepAggregates"))
	}
	return retryWithResult(c, func() ([]StepAggregateRow, error) {
		return c.kernel.getStepAggregates(c, dbInput)
	}, withRetrierLogger(c.logger))
}

// At least one GroupBy* flag must be true, or TimeBucketSize must be > 0. At least one
// Select* flag must be true. Step status is derived from operation_outputs: steps with no
// recorded error are "SUCCESS", otherwise "ERROR".

func GetStepAggregates(ctx Context, input GetStepAggregatesInput) ([]StepAggregateRow, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).GetStepAggregates(input)
}

type listRegisteredWorkflowsOptions struct {
	scheduledOnly bool
}

type ListRegisteredWorkflowsOption func(*listRegisteredWorkflowsOptions)

func WithScheduledOnly() ListRegisteredWorkflowsOption {
	return func(p *listRegisteredWorkflowsOptions) {
		p.scheduledOnly = true
	}
}

// - Name: Custom name if provided during registration, otherwise empty

func ListRegisteredWorkflows(ctx Context, opts ...ListRegisteredWorkflowsOption) ([]WorkflowRegistryEntry, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).ListRegisteredWorkflows(opts...)
}

func validateScheduledWorkflowFn(fn any) error {
	t := reflect.TypeOf(fn)
	if t == nil || t.Kind() != reflect.Func {
		return errors.New("workflow function must be a function")
	}
	if t.NumIn() < 2 {
		return errors.New("workflow function must accept (DbosContext, ScheduledWorkflowInput)")
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

	contextJson, err := json.Marshal(o.context)
	if err != nil {
		return fmt.Errorf("failed to serialize context: %w", err)
	}

	scheduleId := uuid.New().String()
	dbInput := createScheduleDBInput{
		ScheduleId:        scheduleId,
		ScheduleName:      input.ScheduleName,
		WorkflowName:      workflowName,
		WorkflowClassName: o.workflowClassName,
		Schedule:          input.Schedule,
		Context:           string(contextJson),
		Status:            ScheduleStatusActive,
		AutomaticBackfill: o.automaticBackfill,
		CronTimezone:      o.cronTimezone,
	}

	if state, inWorkflow := c.Value(workflowStateKey).(*workflowState); inWorkflow && state != nil {
		_, err := runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
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

func WithScheduleContext(context any) CreateScheduleOption {
	return func(o *createScheduleOptions) { o.context = context }
}

func WithAutomaticBackfill(enabled bool) CreateScheduleOption {
	return func(o *createScheduleOptions) { o.automaticBackfill = enabled }
}

func WithCronTimezone(tz string) CreateScheduleOption {
	return func(o *createScheduleOptions) { o.cronTimezone = tz }
}

func WithScheduleWorkflowClassName(name string) CreateScheduleOption {
	return func(o *createScheduleOptions) { o.workflowClassName = name }
}

type listSchedulesOptions struct {
	statuses             []ScheduleStatus
	workflowNames        []string
	scheduleNamePrefixes []string
}

// scheduler. The fn must already be registered via NewWorkflow.

func CreateSchedule(ctx Context, fn ScheduledWorkflowFunc, input CreateScheduleRequest, opts ...CreateScheduleOption) error {
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
		tx, err := c.kernel.pool.BeginTx(c, pgx.TxOptions{})
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		defer tx.Rollback(c)

		for _, req := range schedules {
			workflowName, err := c.resolveWorkflowName(req.WorkflowFn)
			if err != nil {
				return err
			}

			contextJson, err := json.Marshal(req.Context)
			if err != nil {
				return fmt.Errorf("failed to serialize context: %w", err)
			}

			if err := c.kernel.deleteSchedule(c, deleteScheduleDBInput{
				ScheduleName: req.ScheduleName,
				tx:           tx,
			}); err != nil {
				return fmt.Errorf("failed to delete existing schedule: %w", err)
			}

			scheduleId := uuid.New().String()
			if err := c.kernel.createSchedule(c, createScheduleDBInput{
				ScheduleId:        scheduleId,
				ScheduleName:      req.ScheduleName,
				WorkflowName:      workflowName,
				Schedule:          req.Schedule,
				Context:           string(contextJson),
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

func ApplySchedules(ctx Context, schedules []ApplySchedulesRequest) error {
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
		_, err := runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
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

func PauseSchedule(ctx Context, scheduleName string) error {
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
		_, err := runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
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

func ResumeSchedule(ctx Context, scheduleName string) error {
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
		_, err := runAsTxn(c, func(ctx context.Context, tx pgx.Tx) (any, error) {
			return nil, c.kernel.deleteSchedule(ctx, deleteScheduleDBInput{ScheduleName: scheduleName, tx: tx})
		}, WithStepName("DBOS.deleteSchedule"))
		return err
	}

	return retry(c, func() error {
		return c.kernel.deleteSchedule(c, deleteScheduleDBInput{ScheduleName: scheduleName})
	}, withRetrierLogger(c.logger))
}

func DeleteSchedule(ctx Context, scheduleName string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).DeleteSchedule(scheduleName)
}

func (c *dbosContext) GetSchedule(scheduleName string) (*WorkflowSchedule, error) {
	if scheduleName == "" {
		return nil, errors.New("schedule_name is required")
	}

	dbInput := listSchedulesDBInput{ScheduleNamePrefixes: []string{scheduleName}}

	var schedules []WorkflowSchedule
	var err error
	if state, inWorkflow := c.Value(workflowStateKey).(*workflowState); inWorkflow && state != nil {
		schedules, err = runAsTxn(c, func(ctx context.Context, tx pgx.Tx) ([]WorkflowSchedule, error) {
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

func GetSchedule(ctx Context, scheduleName string) (*WorkflowSchedule, error) {
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
		return runAsTxn(c, func(ctx context.Context, tx pgx.Tx) ([]WorkflowSchedule, error) {
			in := dbInput
			in.tx = tx
			return c.kernel.listSchedules(ctx, in)
		}, WithStepName("DBOS.listSchedules"))
	}
	return retryWithResult(c, func() ([]WorkflowSchedule, error) {
		return c.kernel.listSchedules(c, dbInput)
	}, withRetrierLogger(c.logger))
}

type ListSchedulesOption func(*listSchedulesOptions)

func WithScheduleStatuses(statuses ...ScheduleStatus) ListSchedulesOption {
	return func(o *listSchedulesOptions) { o.statuses = statuses }
}

func WithScheduleWorkflowNames(names ...string) ListSchedulesOption {
	return func(o *listSchedulesOptions) { o.workflowNames = names }
}

func WithScheduleNamePrefixes(prefixes ...string) ListSchedulesOption {
	return func(o *listSchedulesOptions) { o.scheduleNamePrefixes = prefixes }
}

func ListSchedules(ctx Context, opts ...ListSchedulesOption) ([]WorkflowSchedule, error) {
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

func BackfillSchedule(ctx Context, scheduleName string, start, end time.Time) ([]string, error) {
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

	workflowId, err := c.kernel.triggerSchedule(c, scheduleName)
	if err != nil {
		return nil, err
	}
	return newWorkflowHandle[any](c, workflowId), nil
}

func TriggerSchedule(ctx Context, scheduleName string) (*WorkflowHandle[any], error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).TriggerSchedule(scheduleName)
}

func (c *dbosContext) ListApplicationVersions() ([]VersionInfo, error) {
	return retryWithResult(c, func() ([]VersionInfo, error) {
		return c.kernel.listApplicationVersions(c)
	}, withRetrierLogger(c.logger))
}

func ListApplicationVersions(ctx Context) ([]VersionInfo, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).ListApplicationVersions()
}

func (c *dbosContext) GetLatestApplicationVersion() (*VersionInfo, error) {
	return retryWithResult(c, func() (*VersionInfo, error) {
		return c.kernel.getLatestApplicationVersion(c)
	}, withRetrierLogger(c.logger))
}

func GetLatestApplicationVersion(ctx Context) (*VersionInfo, error) {
	if ctx == nil {
		return nil, errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).GetLatestApplicationVersion()
}

func (c *dbosContext) SetLatestApplicationVersion(versionName string) error {
	if versionName == "" {
		return errors.New("version_name is required")
	}
	return retry(c, func() error {
		return c.kernel.updateApplicationVersionTimestamp(c, versionName, time.Now().UnixMilli())
	}, withRetrierLogger(c.logger))
}

func SetLatestApplicationVersion(ctx Context, versionName string) error {
	if ctx == nil {
		return errors.New("ctx cannot be nil")
	}
	return ctx.(*dbosContext).SetLatestApplicationVersion(versionName)
}
