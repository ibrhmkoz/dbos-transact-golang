package dbos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/robfig/cron/v3"
)

const (
	_defaultAdminServerPort = 3001
	_defaultSystemDbSchema  = "dbos"
	_dbosDomain             = "cloud.dbos.dev"
)

type Config struct {
	AppName                   string
	DatabaseUrl               string
	SystemDBPool              *pgxpool.Pool
	Kernel                    *Kernel
	DatabaseSchema            string
	Logger                    *slog.Logger
	AdminServer               bool
	AdminServerPort           int
	ConductorUrl              string
	ConductorApiKey           string
	ConductorExecutorMetadata map[string]any // Metadata associated with this executor that may be used to identify it on the Conductor dashboard. Must be JSON-serializable.
	ApplicationVersion        string
	ExecutorId                string
	EnablePatching            bool
	Serializer                Serializer[any]
	SchedulerPollingInterval  time.Duration
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
		DatabaseUrl:               inputConfig.DatabaseUrl,
		AppName:                   inputConfig.AppName,
		DatabaseSchema:            inputConfig.DatabaseSchema,
		Logger:                    inputConfig.Logger,
		AdminServer:               inputConfig.AdminServer,
		AdminServerPort:           inputConfig.AdminServerPort,
		ConductorUrl:              inputConfig.ConductorUrl,
		ConductorApiKey:           inputConfig.ConductorApiKey,
		ConductorExecutorMetadata: inputConfig.ConductorExecutorMetadata,
		ApplicationVersion:        inputConfig.ApplicationVersion,
		ExecutorId:                inputConfig.ExecutorId,
		SystemDBPool:              inputConfig.SystemDBPool,
		Kernel:                    inputConfig.Kernel,
		EnablePatching:            inputConfig.EnablePatching,
		Serializer:                inputConfig.Serializer,
		SchedulerPollingInterval:  inputConfig.SchedulerPollingInterval,
	}

	if dbosConfig.ConductorExecutorMetadata != nil {
		if _, err := json.Marshal(dbosConfig.ConductorExecutorMetadata); err != nil {
			return nil, fmt.Errorf("conductorExecutorMetadata must be JSON-serializable: %w", err)
		}
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

type AlertHandler func(name string, message string, metadata map[string]string)

type DbosContext interface {
	context.Context

	Launch() error
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

	From(ctx context.Context) DbosContext
	WithoutCancel() DbosContext
	WithTimeout(timeout time.Duration) (DbosContext, context.CancelFunc)
	WithValue(key, val any) DbosContext
	WithCancel() (DbosContext, context.CancelFunc)
	WithCancelCause() (DbosContext, context.CancelCauseFunc)

	SetAlertHandler(handler AlertHandler)
}

type dbosContext struct {
	ctx           context.Context
	ctxCancelFunc context.CancelCauseFunc

	launched atomic.Bool

	kernel       *Kernel
	ownsSystemDB bool
	adminServer  *adminServer
	config       *Config

	worker *worker

	conductor *conductor

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

	alertHandler AlertHandler
}

// Must be called before Launch(). Only one handler is allowed per context.
func (c *dbosContext) SetAlertHandler(handler AlertHandler) {
	if handler == nil {
		panic("alert handler cannot be nil")
	}
	if c.launched.Load() {
		panic("cannot set alert handler after Launch()")
	}
	if c.alertHandler != nil {
		panic("alert handler is already registered")
	}
	c.alertHandler = handler
}

// Must be called before Launch(). Only one handler is allowed per context.
func SetAlertHandler(ctx DbosContext, handler AlertHandler) {
	if ctx == nil {
		panic("ctx cannot be nil")
	}
	ctx.SetAlertHandler(handler)
}

func (c *dbosContext) ClearRegistries() {
	c.workflowRegistry.Clear()
	c.alertHandler = nil
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
func (c *dbosContext) From(ctx context.Context) DbosContext {
	if ctx == nil {
		return nil
	}
	launched := c.launched.Load()
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
	childCtx.launched.Store(launched)
	return childCtx
}

func From(dbosCtx DbosContext, ctx context.Context) DbosContext {
	if dbosCtx == nil {
		return nil
	}
	return dbosCtx.From(ctx)
}

func WithValue(ctx DbosContext, key, val any) DbosContext {
	if ctx == nil {
		return nil
	}
	return ctx.WithValue(key, val)
}

func (c *dbosContext) WithValue(key, val any) DbosContext {
	launched := c.launched.Load()
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
	childCtx.launched.Store(launched)
	return childCtx
}

func (c *dbosContext) WithoutCancel() DbosContext {
	launched := c.launched.Load()
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
	childCtx.launched.Store(launched)
	return childCtx
}

func WithoutCancel(ctx DbosContext) DbosContext {
	if ctx == nil {
		return nil
	}
	return ctx.WithoutCancel()
}

func (c *dbosContext) WithCancel() (DbosContext, context.CancelFunc) {
	launched := c.launched.Load()
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
	childCtx.launched.Store(launched)
	return childCtx, cancelFunc
}

// The returned CancelFunc must be called when the derived context is no longer needed,

func WithCancel(ctx DbosContext) (DbosContext, context.CancelFunc) {
	if ctx == nil {
		return nil, func() {}
	}
	return ctx.WithCancel()
}

func (c *dbosContext) WithCancelCause() (DbosContext, context.CancelCauseFunc) {
	launched := c.launched.Load()
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
	childCtx.launched.Store(launched)
	return childCtx, cancelCauseFunc
}

func WithCancelCause(ctx DbosContext) (DbosContext, context.CancelCauseFunc) {
	if ctx == nil {
		return nil, func(error) {}
	}
	return ctx.WithCancelCause()
}

func (c *dbosContext) WithTimeout(timeout time.Duration) (DbosContext, context.CancelFunc) {
	launched := c.launched.Load()
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
	childCtx.launched.Store(launched)
	return childCtx, cancelFunc
}

func WithTimeout(ctx DbosContext, timeout time.Duration) (DbosContext, context.CancelFunc) {
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

// The context must be launched with Launch() for workflow execution and should be shut down with Shutdown().

func NewDbosContext(ctx context.Context, inputConfig Config) (DbosContext, error) {
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

	newKernelInputs := newKernelInput{
		databaseUrl:     config.DatabaseUrl,
		databaseSchema:  config.DatabaseSchema,
		customPool:      config.SystemDBPool,
		logger:          initExecutor.logger,
		applicationName: config.AppName,
	}

	if config.Kernel != nil {
		initExecutor.kernel = config.Kernel
	} else {

		kernel, err := newKernel(initExecutor, newKernelInputs)
		if err != nil {
			initExecutor.logger.Error("failed to create system database", "error", err)
			return nil, newInitializationError(err.Error())
		}
		initExecutor.kernel = kernel
		initExecutor.ownsSystemDB = true
	}
	initExecutor.logger.Debug("System database initialized")

	initExecutor.worker = newWorker(initExecutor.logger)

	// This allows a client to debounce workflow and the server side to run them, even without knowing the actual workflow types
	registerWorkflow(initExecutor, internalDebouncerWF[any, any])

	if config.ConductorApiKey != "" {
		initExecutor.executorId = uuid.NewString()
		if config.ConductorUrl == "" {
			dbosDomain := os.Getenv("DBOS_DOMAIN")
			if dbosDomain == "" {
				dbosDomain = _dbosDomain
			}
			config.ConductorUrl = fmt.Sprintf("wss://%s/conductor/v1alpha1", dbosDomain)
		}
		conductorConfig := conductorConfig{
			url:              config.ConductorUrl,
			apiKey:           config.ConductorApiKey,
			appName:          config.AppName,
			executorMetadata: config.ConductorExecutorMetadata,
		}
		conductor, err := newConductor(initExecutor, conductorConfig)
		if err != nil {
			return nil, newInitializationError(fmt.Sprintf("failed to initialize conductor: %v", err))
		}
		initExecutor.conductor = conductor
		initExecutor.logger.Debug("Conductor initialized")
	}

	return initExecutor, nil
}

func (c *dbosContext) Launch() error {
	if c.launched.Load() {
		return newInitializationError("DBOS is already launched")
	}

	if c.ownsSystemDB {
		c.kernel.launch(c)
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

	if c.conductor != nil {
		c.conductor.launch()
		c.logger.Debug("Conductor started")
	}

	recoveryHandles, err := recoverPendingWorkflows(c, []string{c.executorId})
	if err != nil {
		return newInitializationError(fmt.Sprintf("failed to recover pending workflows during launch: %v", err))
	}
	if len(recoveryHandles) > 0 {
		c.logger.Info("Recovered pending workflows", "count", len(recoveryHandles))
	} else {
		c.logger.Debug("No pending workflows to recover")
	}

	c.logger.Info("DBOS launched", "app_version", c.applicationVersion, "executor_id", c.executorId)
	c.launched.Store(true)
	return nil
}

// Each step respects the provided timeout. If any component doesn't shut down within the timeout,

func (c *dbosContext) Shutdown(timeout time.Duration) {
	c.logger.Debug("Shutting down DBOS context")

	c.ctxCancelFunc(errors.New("DBOS cancellation initiated"))

	// waiting on the WaitGroup before they finish races with those Adds.

	if c.worker != nil && c.launched.Load() {
		c.logger.Debug("Waiting for queue runner to complete")
		select {
		case <-c.worker.completionChan:
			c.logger.Debug("Queue runner completed")
		case <-time.After(timeout):
			c.logger.Warn("Timeout waiting for queue runner to complete", "timeout", timeout)
		}
	}

	if c.workflowScheduler != nil && c.launched.Load() {
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

	if c.conductor != nil {
		c.logger.Debug("Shutting down conductor")
		c.conductor.shutdown(timeout)
	}

	if c.adminServer != nil && c.launched.Load() {
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
		c.kernel.shutdown(c, timeout)
	}

	c.launched.Store(false)
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

func Launch(ctx DbosContext) error {
	if ctx == nil {
		return fmt.Errorf("ctx cannot be nil")
	}
	return ctx.Launch()
}

func Shutdown(ctx DbosContext, timeout time.Duration) {
	if ctx == nil {
		return
	}
	ctx.Shutdown(timeout)
}

func ClearRegistries(ctx DbosContext) {
	c, ok := ctx.(*dbosContext)
	if !ok {
		return
	}
	c.ClearRegistries()
}
