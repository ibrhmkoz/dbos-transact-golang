package dbos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DbosAdminConfig struct {
	DatabaseUrl    string
	SystemDBPool   *pgxpool.Pool
	Kernel         *Kernel // Shared system database. When set, DbosAdmin does not own its lifecycle.
	DatabaseSchema string
	Logger         *slog.Logger
	Serializer     Serializer[any]
}

type DbosAdmin interface {
	Enqueue(queueName, workflowName string, input any, opts ...EnqueueOption) (*WorkflowHandle[any], error)
	ListWorkflows(opts ...ListWorkflowsOption) ([]WorkflowStatus, error)
	Send(destinationId string, message any, topic string, opts ...SendOption) error
	GetEvent(targetWorkflowId, key string, timeout time.Duration) (any, error)
	RetrieveWorkflow(workflowId string) (*WorkflowHandle[any], error)
	CancelWorkflow(workflowId string) error
	CancelWorkflows(workflowIds []string) error
	SetWorkflowDelay(workflowId string, opts ...SetWorkflowDelayOption) error
	DeleteWorkflows(workflowIds []string, opts ...DeleteWorkflowOption) error
	ResumeWorkflow(workflowId string, opts ...ResumeWorkflowOption) (*WorkflowHandle[any], error)
	ResumeWorkflows(workflowIds []string, opts ...ResumeWorkflowOption) ([]*WorkflowHandle[any], error)
	ForkWorkflow(input ForkWorkflowInput) (*WorkflowHandle[any], error)
	GetWorkflowSteps(workflowId string) ([]StepInfo, error)
	ReadStream(workflowId string, key string, opts ...ReadStreamOption) ([]any, bool, error)
	ReadStreamAsync(workflowId string, key string) (<-chan StreamValue[any], error)

	CreateSchedule(input AdminScheduleInput) error
	ApplySchedules(schedules []AdminScheduleInput) error
	GetSchedule(scheduleName string) (*WorkflowSchedule, error)
	ListSchedules(opts ...ListSchedulesOption) ([]WorkflowSchedule, error)
	PauseSchedule(scheduleName string) error
	ResumeSchedule(scheduleName string) error
	DeleteSchedule(scheduleName string) error
	BackfillSchedule(scheduleName string, start, end time.Time) ([]string, error)
	TriggerSchedule(scheduleName string) (*WorkflowHandle[any], error)

	ListApplicationVersions() ([]VersionInfo, error)
	GetLatestApplicationVersion() (*VersionInfo, error)
	SetLatestApplicationVersion(versionName string) error

	Shutdown(timeout time.Duration)
}

type dbosAdmin struct {
	dbosCtx *dbosContext
}

func NewDbosAdmin(ctx context.Context, config DbosAdminConfig) (DbosAdmin, error) {
	dbosCtx, err := NewDbosContext(ctx, Config{
		DatabaseUrl:    config.DatabaseUrl,
		DatabaseSchema: config.DatabaseSchema,
		AppName:        "dbos-admin",
		Logger:         config.Logger,
		SystemDBPool:   config.SystemDBPool,
		Kernel:         config.Kernel,
		Serializer:     config.Serializer,
	})
	if err != nil {
		return nil, err
	}

	asDbosCtx := dbosCtx.(*dbosContext)
	if asDbosCtx.ownsSystemDB {
		asDbosCtx.kernel.Start()
	}

	return &dbosAdmin{dbosCtx: asDbosCtx}, nil
}

type EnqueueOption func(*enqueueOptions)

// WithEnqueueWorkflowID sets a custom workflow ID instead of generating one automatically.
func WithEnqueueWorkflowId(id string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.workflowId = id
	}
}

func WithEnqueueApplicationVersion(version string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.applicationVersion = version
	}
}

func WithEnqueueDeduplicationId(id string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.deduplicationId = id
	}
}

func WithEnqueuePriority(priority uint) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.priority = priority
	}
}

func WithEnqueueTimeout(timeout time.Duration) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.workflowTimeout = timeout
	}
}

func WithEnqueueQueuePartitionKey(partitionKey string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.queuePartitionKey = partitionKey
	}
}

// This is required when enqueueing to Python, TypeScript, or Java targets, which

func WithEnqueueClassName(className string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.className = className
	}
}

// This is required when enqueueing to Python, TypeScript, or Java targets that

func WithEnqueueConfigName(configName string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.configName = &configName
	}
}

func WithEnqueueDelay(delay time.Duration) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.delayDuration = delay
	}
}

func WithEnqueueAuthenticatedUser(user string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.authenticatedUser = user
	}
}

func WithEnqueueAssumedRole(role string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.assumedRole = role
	}
}

func WithEnqueueAuthenticatedRoles(roles []string) EnqueueOption {
	return func(opts *enqueueOptions) {
		opts.authenticatedRoles = roles
	}
}

type enqueueOptions struct {
	workflowName       string
	workflowId         string
	applicationVersion string
	deduplicationId    string
	priority           uint
	workflowTimeout    time.Duration
	workflowInput      any
	queuePartitionKey  string
	className          string
	configName         *string
	delayDuration      time.Duration
	authenticatedUser  string
	assumedRole        string
	authenticatedRoles []string
}

func (c *dbosAdmin) Enqueue(queueName, workflowName string, input any, opts ...EnqueueOption) (*WorkflowHandle[any], error) {

	dbosCtx := c.dbosCtx

	params := &enqueueOptions{
		workflowName:       workflowName,
		applicationVersion: dbosCtx.GetApplicationVersion(),
		workflowInput:      input,
	}
	for _, opt := range opts {
		opt(params)
	}

	if len(queueName) == 0 {
		return nil, fmt.Errorf("queue name is required")
	}

	if len(workflowName) == 0 {
		return nil, fmt.Errorf("workflow name is required")
	}

	if len(params.queuePartitionKey) > 0 && len(params.deduplicationId) > 0 {
		return nil, fmt.Errorf("partition key and deduplication ID cannot be used together")
	}

	workflowId := params.workflowId
	if workflowId == "" {
		workflowId = uuid.New().String()
	}

	var deadline time.Time
	if params.workflowTimeout > 0 {
		deadline = time.Now().Add(params.workflowTimeout)
	}

	if params.priority > uint(math.MaxInt) {
		return nil, fmt.Errorf("priority %d exceeds maximum allowed value %d", params.priority, math.MaxInt)
	}

	var encodedInput *string
	var serialization string
	if _, ok := input.(PortableWorkflowArgs); ok {
		ser := newPortableSerializer[any]()
		var err error
		encodedInput, err = ser.Encode(input)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize portable workflow input: %w", err)
		}
		serialization = PortableSerializerName
	} else {
		ser := resolveEncoder(dbosCtx)
		var err error
		encodedInput, err = ser.Encode(input)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize workflow input: %w", err)
		}
		serialization = ser.Name()
	}

	var wfStatus WorkflowStatusType
	var delayUntil time.Time
	if params.delayDuration > 0 {
		wfStatus = WorkflowStatusDelayed
		delayUntil = time.Now().Add(params.delayDuration)
	} else {
		wfStatus = WorkflowStatusEnqueued
	}

	status := WorkflowStatus{
		Name:               params.workflowName,
		ApplicationVersion: params.applicationVersion,
		Status:             wfStatus,
		Id:                 workflowId,
		CreatedAt:          time.Now(),
		Deadline:           deadline,
		Timeout:            params.workflowTimeout,
		Input:              encodedInput,
		QueueName:          queueName,
		DeduplicationId:    params.deduplicationId,
		Priority:           int(params.priority),
		QueuePartitionKey:  params.queuePartitionKey,
		ClassName:          params.className,
		ConfigName:         params.configName,
		Serialization:      serialization,
		DelayUntil:         delayUntil,
		AuthenticatedUser:  params.authenticatedUser,
		AssumedRole:        params.assumedRole,
		AuthenticatedRoles: params.authenticatedRoles,
	}

	uncancellableCtx := WithoutCancel(dbosCtx)
	for {
		tx, err := dbosCtx.kernel.pool.BeginTx(uncancellableCtx, pgx.TxOptions{})
		if err != nil {
			return nil, newWorkflowExecutionError(workflowId, fmt.Errorf("failed to begin transaction: %v", err))
		}

		insertInput := insertWorkflowStatusDBInput{
			status: status,
			tx:     tx,
		}
		_, err = dbosCtx.kernel.insertWorkflowStatus(uncancellableCtx, insertInput)
		if err != nil {
			if rbErr := tx.Rollback(uncancellableCtx); rbErr != nil {
				dbosCtx.logger.Warn("failed to roll back transaction", "error", rbErr, "workflow_id", workflowId)
			}
			if errors.Is(err, errDeduplicationCollision) {
				existingId, lookupErr := dbosCtx.kernel.getDeduplicatedWorkflow(uncancellableCtx, workflowName, params.deduplicationId)
				if lookupErr != nil {
					return nil, newWorkflowExecutionError(workflowId, fmt.Errorf("looking up deduplicated workflow: %w", lookupErr))
				}
				if existingId != nil {
					return newWorkflowHandle[any](uncancellableCtx, *existingId), nil
				}

				continue
			}
			dbosCtx.logger.Error("failed to insert workflow status", "error", err, "workflow_id", workflowId)
			return nil, err
		}

		if err := tx.Commit(uncancellableCtx); err != nil {
			if rbErr := tx.Rollback(uncancellableCtx); rbErr != nil {
				dbosCtx.logger.Warn("failed to roll back transaction", "error", rbErr, "workflow_id", workflowId)
			}
			return nil, fmt.Errorf("failed to commit transaction: %w", err)
		}

		return newWorkflowHandle[any](uncancellableCtx, workflowId), nil
	}
}

func Enqueue[P any, R any](c DbosAdmin, queueName, workflowName string, input P, opts ...EnqueueOption) (*WorkflowHandle[R], error) {
	if c == nil {
		return nil, errors.New("dbosAdmin cannot be nil")
	}

	handle, err := c.Enqueue(queueName, workflowName, input, opts...)
	if err != nil {
		return nil, err
	}

	return newWorkflowHandle[R](c.(*dbosAdmin).dbosCtx, handle.GetWorkflowId()), nil
}

func (c *dbosAdmin) ListWorkflows(opts ...ListWorkflowsOption) ([]WorkflowStatus, error) {
	return c.dbosCtx.ListWorkflows(opts...)
}

func (c *dbosAdmin) Send(destinationId string, message any, topic string, opts ...SendOption) error {
	return c.dbosCtx.Send(destinationId, message, topic, opts...)
}

func (c *dbosAdmin) GetEvent(targetWorkflowId, key string, timeout time.Duration) (any, error) {
	result, err := c.dbosCtx.GetEvent(targetWorkflowId, key, timeout)
	if err != nil {
		return nil, err
	}

	if evtResult, ok := result.(*getEventResult); ok {
		return evtResult.value, nil
	}
	return result, nil
}

func (c *dbosAdmin) RetrieveWorkflow(workflowId string) (*WorkflowHandle[any], error) {
	return c.dbosCtx.RetrieveWorkflow(workflowId)
}

func (c *dbosAdmin) CancelWorkflow(workflowId string) error {
	return c.dbosCtx.CancelWorkflow(workflowId)
}

func (c *dbosAdmin) CancelWorkflows(workflowIds []string) error {
	return c.dbosCtx.CancelWorkflows(workflowIds)
}

func (c *dbosAdmin) SetWorkflowDelay(workflowId string, opts ...SetWorkflowDelayOption) error {
	return c.dbosCtx.SetWorkflowDelay(workflowId, opts...)
}

func (c *dbosAdmin) DeleteWorkflows(workflowIds []string, opts ...DeleteWorkflowOption) error {
	return c.dbosCtx.DeleteWorkflows(workflowIds, opts...)
}

func (c *dbosAdmin) ResumeWorkflow(workflowId string, opts ...ResumeWorkflowOption) (*WorkflowHandle[any], error) {
	return c.dbosCtx.ResumeWorkflow(workflowId, opts...)
}

func (c *dbosAdmin) ResumeWorkflows(workflowIds []string, opts ...ResumeWorkflowOption) ([]*WorkflowHandle[any], error) {
	return c.dbosCtx.ResumeWorkflows(workflowIds, opts...)
}

func (c *dbosAdmin) ForkWorkflow(input ForkWorkflowInput) (*WorkflowHandle[any], error) {
	return c.dbosCtx.ForkWorkflow(input)
}

func (c *dbosAdmin) GetWorkflowSteps(workflowId string) ([]StepInfo, error) {
	return c.dbosCtx.GetWorkflowSteps(workflowId)
}

func (c *dbosAdmin) ReadStream(workflowId string, key string, opts ...ReadStreamOption) ([]any, bool, error) {
	return c.dbosCtx.ReadStream(workflowId, key, opts...)
}

func AdminReadStream[R any](c DbosAdmin, workflowId string, key string, opts ...ReadStreamOption) ([]R, bool, error) {
	if c == nil {
		return nil, false, errors.New("dbosAdmin cannot be nil")
	}
	values, closed, err := c.ReadStream(workflowId, key, opts...)
	if err != nil {
		return nil, false, err
	}

	customSer := c.(*dbosAdmin).dbosCtx.serializer
	typedValues := make([]R, len(values))
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

	return typedValues, closed, nil
}

func (c *dbosAdmin) ReadStreamAsync(workflowId string, key string) (<-chan StreamValue[any], error) {
	return c.dbosCtx.ReadStreamAsync(workflowId, key)
}

func AdminReadStreamAsync[R any](c DbosAdmin, workflowId string, key string) (<-chan StreamValue[R], error) {
	if c == nil {
		return nil, errors.New("dbosAdmin cannot be nil")
	}

	anyCh, err := c.ReadStreamAsync(workflowId, key)
	if err != nil {
		return nil, err
	}

	typedCh := make(chan StreamValue[R], 1)
	dbosCtx := c.(*dbosAdmin).dbosCtx

	go func() {
		defer close(typedCh)

		send := func(v StreamValue[R]) bool {
			select {
			case typedCh <- v:
				return true
			case <-dbosCtx.Done():
				return false
			}
		}

		customSer := dbosCtx.serializer

		for streamValue := range anyCh {
			if streamValue.Err != nil {
				send(StreamValue[R]{Err: streamValue.Err})
				return
			}

			if streamValue.Closed {
				send(StreamValue[R]{Closed: true})
				return
			}

			entry, ok := streamValue.Value.(streamEntryWithSerialization)
			if !ok {
				send(StreamValue[R]{Err: fmt.Errorf("stream value is not streamEntryWithSerialization, got %T", streamValue.Value)})
				return
			}

			decoder, resolveErr := resolveDecoder[R](entry.serialization, customSer)
			if resolveErr != nil {
				send(StreamValue[R]{Err: resolveErr})
				return
			}

			decodedValue, decodeErr := decoder.Decode(&entry.value)
			if decodeErr != nil {
				send(StreamValue[R]{Err: fmt.Errorf("decoding stream value to type %T: %w", *new(R), decodeErr)})
				return
			}

			if !send(StreamValue[R]{Value: decodedValue}) {
				return
			}
		}
	}()

	return typedCh, nil
}

type AdminScheduleInput struct {
	ScheduleName      string
	WorkflowName      string
	WorkflowClassName string
	Schedule          string
	Context           any
	AutomaticBackfill bool
	CronTimezone      string
	QueueName         string
}

func (c *dbosAdmin) CreateSchedule(input AdminScheduleInput) error {
	if input.ScheduleName == "" {
		return errors.New("schedule_name is required")
	}
	if input.WorkflowName == "" {
		return errors.New("workflow_name is required")
	}
	if err := validateCronSchedule(input.Schedule, input.CronTimezone); err != nil {
		return err
	}

	dbosCtx := c.dbosCtx

	scheduleId := uuid.New().String()
	contextJson, err := json.Marshal(input.Context)
	if err != nil {
		return fmt.Errorf("failed to serialize context: %w", err)
	}

	return dbosCtx.kernel.createSchedule(dbosCtx, createScheduleDBInput{
		ScheduleId:        scheduleId,
		ScheduleName:      input.ScheduleName,
		WorkflowName:      input.WorkflowName,
		WorkflowClassName: input.WorkflowClassName,
		Schedule:          input.Schedule,
		Context:           string(contextJson),
		Status:            ScheduleStatusActive,
		AutomaticBackfill: input.AutomaticBackfill,
		CronTimezone:      input.CronTimezone,
		QueueName:         input.QueueName,
	})
}

func (c *dbosAdmin) ApplySchedules(schedules []AdminScheduleInput) error {
	if len(schedules) == 0 {
		return nil
	}

	for i, req := range schedules {
		if req.ScheduleName == "" {
			return fmt.Errorf("schedule entry %d is missing required field 'schedule_name'", i)
		}
		if req.WorkflowName == "" {
			return fmt.Errorf("schedule entry %d is missing required field 'workflow_name'", i)
		}
		if err := validateCronSchedule(req.Schedule, req.CronTimezone); err != nil {
			return fmt.Errorf("schedule entry %d: %w", i, err)
		}
	}

	dbosCtx := c.dbosCtx

	tx, err := dbosCtx.kernel.pool.BeginTx(dbosCtx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(dbosCtx)

	for _, req := range schedules {
		contextJson, err := json.Marshal(req.Context)
		if err != nil {
			return fmt.Errorf("failed to serialize context: %w", err)
		}

		queueName := req.QueueName
		if queueName == "" {
			queueName = _dbosInternalQueueName
		}

		if err := dbosCtx.kernel.deleteSchedule(dbosCtx, deleteScheduleDBInput{
			ScheduleName: req.ScheduleName,
			tx:           tx,
		}); err != nil {
			return fmt.Errorf("failed to delete existing schedule: %w", err)
		}

		if err := dbosCtx.kernel.createSchedule(dbosCtx, createScheduleDBInput{
			ScheduleId:        uuid.New().String(),
			ScheduleName:      req.ScheduleName,
			WorkflowName:      req.WorkflowName,
			WorkflowClassName: req.WorkflowClassName,
			Schedule:          req.Schedule,
			Context:           string(contextJson),
			Status:            ScheduleStatusActive,
			AutomaticBackfill: req.AutomaticBackfill,
			CronTimezone:      req.CronTimezone,
			QueueName:         queueName,
			tx:                tx,
		}); err != nil {
			return fmt.Errorf("failed to create schedule: %w", err)
		}
	}

	if err := tx.Commit(dbosCtx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (c *dbosAdmin) GetSchedule(scheduleName string) (*WorkflowSchedule, error) {
	return c.dbosCtx.GetSchedule(scheduleName)
}

func (c *dbosAdmin) ListSchedules(opts ...ListSchedulesOption) ([]WorkflowSchedule, error) {
	return c.dbosCtx.ListSchedules(opts...)
}

func (c *dbosAdmin) PauseSchedule(scheduleName string) error {
	return c.dbosCtx.PauseSchedule(scheduleName)
}

func (c *dbosAdmin) ResumeSchedule(scheduleName string) error {
	return c.dbosCtx.ResumeSchedule(scheduleName)
}

func (c *dbosAdmin) DeleteSchedule(scheduleName string) error {
	return c.dbosCtx.DeleteSchedule(scheduleName)
}

func (c *dbosAdmin) BackfillSchedule(scheduleName string, start, end time.Time) ([]string, error) {
	return c.dbosCtx.BackfillSchedule(scheduleName, start, end)
}

func (c *dbosAdmin) TriggerSchedule(scheduleName string) (*WorkflowHandle[any], error) {
	return c.dbosCtx.TriggerSchedule(scheduleName)
}

func (c *dbosAdmin) ListApplicationVersions() ([]VersionInfo, error) {
	return c.dbosCtx.ListApplicationVersions()
}

func (c *dbosAdmin) GetLatestApplicationVersion() (*VersionInfo, error) {
	return c.dbosCtx.GetLatestApplicationVersion()
}

func (c *dbosAdmin) SetLatestApplicationVersion(versionName string) error {
	return c.dbosCtx.SetLatestApplicationVersion(versionName)
}

func (c *dbosAdmin) Shutdown(timeout time.Duration) {

	dbosCtx := c.dbosCtx

	dbosCtx.ctxCancelFunc(errors.New("dbosAdmin shutdown initiated"))

	// Close the system database only when this dbosAdmin created it.
	if dbosCtx.kernel != nil && dbosCtx.ownsSystemDB {
		dbosCtx.logger.Debug("Shutting down system database")
		// dbosCtx is already cancelled, so the shutdown deadline must come
		// from a fresh context.
		kernelCtx, cancel := context.WithTimeout(context.Background(), timeout)
		if err := dbosCtx.kernel.Shutdown(kernelCtx); err != nil {
			dbosCtx.logger.Warn("Kernel shutdown did not complete in time", "error", err)
		}
		cancel()
	}
}
