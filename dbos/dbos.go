package dbos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

const (
	_defaultAdminServerPort = 3001
	_defaultSystemDbSchema  = "dbos"
)

type Config struct {
	AppName                  string
	DatabaseUrl              string
	SystemDBPool             *pgxpool.Pool
	Kernel                   *Kernel
	DatabaseSchema           string
	Logger                   *slog.Logger
	AdminServer              bool
	AdminServerPort          int
	ApplicationVersion       string
	ExecutorId               string
	EnablePatching           bool
	Serializer               Serializer[any]
	SchedulerPollingInterval time.Duration
	// WorkerPollingInterval is the base interval at which workers poll for enqueued
	// workflows to enact. Defaults to 1s.
	WorkerPollingInterval time.Duration
}

func processConfig(inputConfig *Config) (*Config, error) {
	// First check required fields
	if len(inputConfig.DatabaseUrl) == 0 && inputConfig.SystemDBPool == nil && inputConfig.Kernel == nil {
		return nil, fmt.Errorf("one of databaseURL, systemDBPool, or kernel must be provided")
	}
	if inputConfig.Kernel != nil && (inputConfig.DatabaseUrl != "" || inputConfig.SystemDBPool != nil) {
		return nil, fmt.Errorf("kernel is mutually exclusive with databaseURL and systemDBPool")
	}
	if len(inputConfig.AppName) == 0 {
		return nil, fmt.Errorf("missing required config field: appName")
	}
	if inputConfig.SystemDBPool == nil && inputConfig.Kernel == nil {
		if err := validateDatabaseUrl(inputConfig.DatabaseUrl); err != nil {
			return nil, err
		}
	}
	if inputConfig.AdminServerPort == 0 {
		inputConfig.AdminServerPort = _defaultAdminServerPort
	}

	dbosConfig := &Config{
		DatabaseUrl:              inputConfig.DatabaseUrl,
		AppName:                  inputConfig.AppName,
		DatabaseSchema:           inputConfig.DatabaseSchema,
		Logger:                   inputConfig.Logger,
		AdminServer:              inputConfig.AdminServer,
		AdminServerPort:          inputConfig.AdminServerPort,
		ApplicationVersion:       inputConfig.ApplicationVersion,
		ExecutorId:               inputConfig.ExecutorId,
		SystemDBPool:             inputConfig.SystemDBPool,
		Kernel:                   inputConfig.Kernel,
		EnablePatching:           inputConfig.EnablePatching,
		Serializer:               inputConfig.Serializer,
		SchedulerPollingInterval: inputConfig.SchedulerPollingInterval,
		WorkerPollingInterval:    inputConfig.WorkerPollingInterval,
	}

	if dbosConfig.Logger == nil {
		dbosConfig.Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}
	if dbosConfig.DatabaseSchema == "" {
		dbosConfig.DatabaseSchema = _defaultSystemDbSchema
	}

	if dbosConfig.EnablePatching && dbosConfig.ApplicationVersion == "" {
		dbosConfig.ApplicationVersion = "PATCHING_ENABLED"
	}

	if envAppVersion := os.Getenv("DBOS__APPVERSION"); envAppVersion != "" {
		dbosConfig.ApplicationVersion = envAppVersion
	}
	if envExecutorId := os.Getenv("DBOS__VMID"); envExecutorId != "" {
		dbosConfig.ExecutorId = envExecutorId
	}

	if dbosConfig.ApplicationVersion == "" {
		dbosConfig.ApplicationVersion = computeApplicationVersion()
	}
	if dbosConfig.ExecutorId == "" {
		dbosConfig.ExecutorId = "local"
	}

	return dbosConfig, nil
}

type Context interface {
	context.Context

	Start() error
	Shutdown(timeout time.Duration)

	RunAsStep(fn StepFunc, opts ...StepOption) (any, error)
	RunWorkflow(fn WorkflowFunc, input any, opts ...WorkflowOption) (*WorkflowHandle[any], error)
	Go(fn StepFunc, opts ...StepOption) (chan StepOutcome[any], error)
	Select(channels []<-chan StepOutcome[any]) (any, error)
	Send(destinationId string, message any, topic string, opts ...SendOption) error
	Recv(topic string, timeout time.Duration) (any, error)
	SetEvent(key string, message any, opts ...SetEventOption) error
	GetEvent(targetWorkflowId string, key string, timeout time.Duration) (any, error)
	WriteStream(key string, value any, opts ...WriteStreamOption) error
	CloseStream(key string) error
	ReadStream(workflowId string, key string, opts ...ReadStreamOption) ([]any, bool, error)
	ReadStreamAsync(workflowId string, key string) (<-chan StreamValue[any], error)
	Sleep(duration time.Duration) (time.Duration, error)
	Patch(patchName string) (bool, error)
	DeprecatePatch(patchName string) error
	GetWorkflowId() (string, error)
	GetStepId() (int, error)

	GetApplicationVersion() string
	GetExecutorId() string
	GetApplicationId() string

	From(ctx context.Context) Context
	WithoutCancel() Context
	WithTimeout(timeout time.Duration) (Context, context.CancelFunc)
	WithValue(key, val any) Context
	WithCancel() (Context, context.CancelFunc)
	WithCancelCause() (Context, context.CancelCauseFunc)
}

type dbosContext struct {
	ctx           context.Context
	ctxCancelFunc context.CancelCauseFunc

	started atomic.Bool

	kernel       *Kernel
	ownsSystemDB bool
	adminServer  *adminServer
	config       *Config

	worker *worker

	applicationVersion string
	applicationId      string
	executorId         string

	workflowsWg *sync.WaitGroup

	workflowRegistry *WorkflowRegistry

	activeWorkflowIds *sync.Map

	workflowScheduler *cron.Cron

	scheduleMu sync.Mutex

	scheduleEntryIds map[string]cron.EntryID

	scheduleInstalledIds map[string]string

	logger *slog.Logger

	serializer Serializer[any]
}

func (c *dbosContext) ClearRegistries() {
	c.workflowRegistry.Clear()
}

func (c *dbosContext) Deadline() (deadline time.Time, ok bool) {
	return c.ctx.Deadline()
}

func (c *dbosContext) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *dbosContext) Err() error {
	return c.ctx.Err()
}

func (c *dbosContext) Value(key any) any {
	return c.ctx.Value(key)
}

// The provided context must be a child of a context.Context that was provided by DBOS (e.g., the first argument of RunWorkflow or Run)
// That is because such context embeds important metadata necessary for DBOS to function correctly.
func (c *dbosContext) From(ctx context.Context) Context {
	if ctx == nil {
		return nil
	}
	started := c.started.Load()
	childCtx := &dbosContext{
		ctx:                ctx,
		config:             c.config,
		logger:             c.logger,
		kernel:             c.kernel,
		workflowsWg:        c.workflowsWg,
		workflowRegistry:   c.workflowRegistry,
		activeWorkflowIds:  c.activeWorkflowIds,
		applicationVersion: c.applicationVersion,
		executorId:         c.executorId,
		applicationId:      c.applicationId,
		worker:             c.worker,
		serializer:         c.serializer,
	}
	childCtx.started.Store(started)
	return childCtx
}

func From(dbosCtx Context, ctx context.Context) Context {
	if dbosCtx == nil {
		return nil
	}
	return dbosCtx.From(ctx)
}

func WithValue(ctx Context, key, val any) Context {
	if ctx == nil {
		return nil
	}
	return ctx.WithValue(key, val)
}

func (c *dbosContext) WithValue(key, val any) Context {
	started := c.started.Load()
	childCtx := &dbosContext{
		ctx:                context.WithValue(c.ctx, key, val),
		config:             c.config,
		logger:             c.logger,
		kernel:             c.kernel,
		workflowsWg:        c.workflowsWg,
		workflowRegistry:   c.workflowRegistry,
		activeWorkflowIds:  c.activeWorkflowIds,
		applicationVersion: c.applicationVersion,
		executorId:         c.executorId,
		applicationId:      c.applicationId,
		worker:             c.worker,
		serializer:         c.serializer,
	}
	childCtx.started.Store(started)
	return childCtx
}

func (c *dbosContext) WithoutCancel() Context {
	started := c.started.Load()
	childCtx := &dbosContext{
		ctx:                context.WithoutCancel(c.ctx),
		config:             c.config,
		logger:             c.logger,
		kernel:             c.kernel,
		workflowsWg:        c.workflowsWg,
		workflowRegistry:   c.workflowRegistry,
		activeWorkflowIds:  c.activeWorkflowIds,
		applicationVersion: c.applicationVersion,
		executorId:         c.executorId,
		applicationId:      c.applicationId,
		worker:             c.worker,
		serializer:         c.serializer,
	}
	childCtx.started.Store(started)
	return childCtx
}

func WithoutCancel(ctx Context) Context {
	if ctx == nil {
		return nil
	}
	return ctx.WithoutCancel()
}

func (c *dbosContext) WithCancel() (Context, context.CancelFunc) {
	started := c.started.Load()
	newCtx, cancelFunc := context.WithCancel(c.ctx)
	childCtx := &dbosContext{
		ctx:                newCtx,
		logger:             c.logger,
		kernel:             c.kernel,
		workflowsWg:        c.workflowsWg,
		workflowRegistry:   c.workflowRegistry,
		activeWorkflowIds:  c.activeWorkflowIds,
		applicationVersion: c.applicationVersion,
		executorId:         c.executorId,
		applicationId:      c.applicationId,
		worker:             c.worker,
		serializer:         c.serializer,
	}
	childCtx.started.Store(started)
	return childCtx, cancelFunc
}

// The returned CancelFunc must be called when the derived context is no longer needed,

func WithCancel(ctx Context) (Context, context.CancelFunc) {
	if ctx == nil {
		return nil, func() {}
	}
	return ctx.WithCancel()
}

func (c *dbosContext) WithCancelCause() (Context, context.CancelCauseFunc) {
	started := c.started.Load()
	newCtx, cancelCauseFunc := context.WithCancelCause(c.ctx)
	childCtx := &dbosContext{
		ctx:                newCtx,
		logger:             c.logger,
		kernel:             c.kernel,
		workflowsWg:        c.workflowsWg,
		workflowRegistry:   c.workflowRegistry,
		activeWorkflowIds:  c.activeWorkflowIds,
		applicationVersion: c.applicationVersion,
		executorId:         c.executorId,
		applicationId:      c.applicationId,
		worker:             c.worker,
		serializer:         c.serializer,
	}
	childCtx.started.Store(started)
	return childCtx, cancelCauseFunc
}

func WithCancelCause(ctx Context) (Context, context.CancelCauseFunc) {
	if ctx == nil {
		return nil, func(error) {}
	}
	return ctx.WithCancelCause()
}

func (c *dbosContext) WithTimeout(timeout time.Duration) (Context, context.CancelFunc) {
	started := c.started.Load()
	newCtx, cancelFunc := context.WithTimeoutCause(c.ctx, timeout, errors.New("DBOS context timeout"))
	childCtx := &dbosContext{
		ctx:                newCtx,
		config:             c.config,
		logger:             c.logger,
		kernel:             c.kernel,
		workflowsWg:        c.workflowsWg,
		workflowRegistry:   c.workflowRegistry,
		activeWorkflowIds:  c.activeWorkflowIds,
		applicationVersion: c.applicationVersion,
		executorId:         c.executorId,
		applicationId:      c.applicationId,
		worker:             c.worker,
		serializer:         c.serializer,
	}
	childCtx.started.Store(started)
	return childCtx, cancelFunc
}

func WithTimeout(ctx Context, timeout time.Duration) (Context, context.CancelFunc) {
	if ctx == nil {
		return nil, func() {}
	}
	return ctx.WithTimeout(timeout)
}

func (c *dbosContext) getWorkflowScheduler() *cron.Cron {
	if c.workflowScheduler == nil {
		c.workflowScheduler = cron.New(cron.WithSeconds())
		c.scheduleEntryIds = make(map[string]cron.EntryID)
		c.scheduleInstalledIds = make(map[string]string)
	}
	return c.workflowScheduler
}

func (c *dbosContext) GetApplicationVersion() string {
	return c.applicationVersion
}

func (c *dbosContext) GetExecutorId() string {
	return c.executorId
}

func (c *dbosContext) GetApplicationId() string {
	return c.applicationId
}

func (c *dbosContext) ListRegisteredWorkflows(opts ...ListRegisteredWorkflowsOption) ([]WorkflowRegistryEntry, error) {

	params := &listRegisteredWorkflowsOptions{}

	for _, opt := range opts {
		opt(params)
	}

	return c.workflowRegistry.List(params.scheduledOnly), nil
}

// The context must be started with Start() for workflow execution and should be shut down with Shutdown().

func NewDbosContext(ctx context.Context, inputConfig Config) (Context, error) {
	dbosBaseCtx, cancelFunc := context.WithCancelCause(ctx)
	initExecutor := &dbosContext{
		workflowsWg:       &sync.WaitGroup{},
		ctx:               dbosBaseCtx,
		ctxCancelFunc:     cancelFunc,
		workflowRegistry:  NewWorkflowRegistry(),
		activeWorkflowIds: &sync.Map{},
	}

	config, err := processConfig(&inputConfig)
	if err != nil {
		return nil, newInitializationError(err.Error())
	}
	initExecutor.config = config

	initExecutor.logger = config.Logger
	initExecutor.logger.Info("Initializing DBOS context", "app_name", config.AppName, "dbos_version", getDbosVersion())

	initExecutor.applicationVersion = config.ApplicationVersion
	initExecutor.executorId = config.ExecutorId

	initExecutor.applicationId = os.Getenv("DBOS__APPID")
	initExecutor.serializer = config.Serializer

	kernelConfig := KernelConfig{
		DatabaseUrl:     config.DatabaseUrl,
		DatabaseSchema:  config.DatabaseSchema,
		SystemDBPool:    config.SystemDBPool,
		Logger:          initExecutor.logger,
		ApplicationName: config.AppName,
	}

	if config.Kernel != nil {
		initExecutor.kernel = config.Kernel
	} else {

		kernel, err := NewKernel(initExecutor, kernelConfig)
		if err != nil {
			initExecutor.logger.Error("failed to create system database", "error", err)
			return nil, newInitializationError(err.Error())
		}
		initExecutor.kernel = kernel
		initExecutor.ownsSystemDB = true
	}
	initExecutor.logger.Debug("System database initialized")

	initExecutor.worker = newWorker(initExecutor.logger)

	return initExecutor, nil
}

func (c *dbosContext) Start() error {
	if c.started.Load() {
		return newInitializationError("DBOS is already started")
	}

	if c.ownsSystemDB {
		c.kernel.Start()
	}

	if err := retry(c, func() error {
		return c.kernel.createApplicationVersion(c, c.applicationVersion)
	}, withRetrierLogger(c.logger)); err != nil {
		c.logger.Warn("Failed to register application version", "version", c.applicationVersion, "error", err)
	} else if latest, err := retryWithResult(c, func() (*VersionInfo, error) {
		return c.kernel.getLatestApplicationVersion(c)
	}, withRetrierLogger(c.logger)); err != nil {
		c.logger.Warn("Failed to fetch latest application version", "error", err)
	} else if latest.Name != c.applicationVersion {
		c.logger.Warn("Current application version is not the latest",
			"current", c.applicationVersion, "latest", latest.Name)
	}

	if c.config.AdminServer {
		adminServer := newAdminServer(c, c.config.AdminServerPort)
		err := adminServer.Start()
		if err != nil {
			c.logger.Error("Failed to start admin server", "error", err)
			return newInitializationError(fmt.Sprintf("failed to start admin server: %v", err))
		}
		c.logger.Debug("Admin server started", "port", c.config.AdminServerPort)
		c.adminServer = adminServer
	}

	if err := c.persistWorkflowDefinitions(); err != nil {
		return newInitializationError(err.Error())
	}

	go func() {
		c.worker.run(c)
	}()
	c.logger.Debug("Worker started")

	c.getWorkflowScheduler().Start()
	c.logger.Debug("Workflow scheduler started")

	go c.runScheduleReconciler()

	recoveryHandles, err := recoverPendingWorkflows(c, []string{c.executorId})
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to recover pending workflows during start: %v", err))
	}
	if len(recoveryHandles) > 0 {
		c.logger.Info("Recovered pending workflows", "count", len(recoveryHandles))
	} else {
		c.logger.Debug("No pending workflows to recover")
	}

	c.logger.Info("DBOS started", "app_version", c.applicationVersion, "executor_id", c.executorId)
	c.started.Store(true)
	return nil
}

// Each step respects the provided timeout. If any component doesn't shut down within the timeout,

func (c *dbosContext) Shutdown(timeout time.Duration) {
	c.logger.Debug("Shutting down DBOS context")

	c.ctxCancelFunc(errors.New("DBOS cancellation initiated"))

	// waiting on the WaitGroup before they finish races with those Adds.

	if c.worker != nil && c.started.Load() {
		c.logger.Debug("Waiting for queue runner to complete")
		select {
		case <-c.worker.completionChan:
			c.logger.Debug("Queue runner completed")
		case <-time.After(timeout):
			c.logger.Warn("Timeout waiting for queue runner to complete", "timeout", timeout)
		}
	}

	if c.workflowScheduler != nil && c.started.Load() {
		c.logger.Debug("Stopping workflow scheduler")
		ctx := c.workflowScheduler.Stop()

		select {
		case <-ctx.Done():
			c.logger.Debug("All scheduled jobs completed")
			c.workflowScheduler = nil
		case <-time.After(timeout):
			c.logger.Warn("Timeout waiting for jobs to complete. Moving on", "timeout", timeout)
		}
	}

	if c.adminServer != nil && c.started.Load() {
		c.logger.Debug("Shutting down admin server")
		err := c.adminServer.Shutdown(timeout)
		if err != nil {
			c.logger.Error("Failed to shutdown admin server", "error", err)
		} else {
			c.logger.Debug("Admin server shutdown complete")
		}
	}

	c.logger.Debug("Waiting for all workflows to finish")
	done := make(chan struct{})
	go func() {
		c.workflowsWg.Wait()
		close(done)
	}()
	select {
	case <-done:
		c.logger.Debug("All workflows completed")
	case <-time.After(timeout):
		c.logger.Warn("Timeout waiting for workflows to complete", "timeout", timeout)
	}

	if c.kernel != nil && c.ownsSystemDB {
		c.logger.Debug("Shutting down system database")
		// c is already cancelled at this point, so the shutdown deadline must
		// come from a fresh context.
		kernelCtx, cancel := context.WithTimeout(context.Background(), timeout)
		if err := c.kernel.Shutdown(kernelCtx); err != nil {
			c.logger.Warn("Kernel shutdown did not complete in time", "error", err)
		}
		cancel()
	}

	c.started.Store(false)
}

// This is used for application versioning to ensure workflow compatibility across deployments.
// Returns the hexadecimal representation of the hash or an error if the executable cannot be read.
func getBinaryHash() (string, error) {
	execPath, err := os.Executable()
	if err != nil {
		return "", err
	}

	execPath, err = filepath.EvalSymlinks(execPath)
	if err != nil {
		return "", fmt.Errorf("resolve self path: %w", err)
	}

	fi, err := os.Lstat(execPath)
	if err != nil {
		return "", err
	}
	if !fi.Mode().IsRegular() {
		return "", fmt.Errorf("executable is not a regular file")
	}

	file, err := os.Open(execPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func computeApplicationVersion() string {
	hash, err := getBinaryHash()
	if err != nil {
		fmt.Printf("DBOS: Failed to compute binary hash: %v\n", err)
		return ""
	}
	return hash
}

func getDbosVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if dep.Path == "github.com/dbos-inc/dbos-transact-golang" {
				return dep.Version
			}
		}

		if info.Main.Path == "github.com/dbos-inc/dbos-transact-golang" {
			return info.Main.Version
		}
	}
	return "unknown"
}

func Start(ctx Context) error {
	if ctx == nil {
		return fmt.Errorf("ctx cannot be nil")
	}
	return ctx.Start()
}

func Shutdown(ctx Context, timeout time.Duration) {
	if ctx == nil {
		return
	}
	ctx.Shutdown(timeout)
}

func ClearRegistries(ctx Context) {
	c, ok := ctx.(*dbosContext)
	if !ok {
		return
	}
	c.ClearRegistries()
}
