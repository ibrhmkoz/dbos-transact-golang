package dbos

import (
	"context"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	_dbosInternalQueueName       = "_dbos_internal_queue"
	_defaultMaxTasksPerIteration = 100
	_defaultBasePollingInterval  = 1 * time.Second
	_defaultMaxPollingInterval   = 120 * time.Second
)

type worker struct {
	logger *slog.Logger

	backoffFactor   float64
	scalebackFactor float64
	jitterMin       float64
	jitterMax       float64

	policyGoroutinesWg sync.WaitGroup

	completionChan chan struct{}
}

func newWorker(logger *slog.Logger) *worker {
	return &worker{
		backoffFactor:   2.0,
		scalebackFactor: 0.9,
		jitterMin:       0.95,
		jitterMax:       1.05,
		completionChan:  make(chan struct{}, 1),
		logger:          logger.With("service", "worker"),
	}
}

func (w *worker) run(ctx *dbosContext) {
	for _, entry := range ctx.workflowRegistry.List(false) {
		w.policyGoroutinesWg.Add(1)
		go w.runWorkflow(ctx, entry.Name)
	}

	w.policyGoroutinesWg.Wait()
	w.logger.Debug("Worker stopped")
	w.completionChan <- struct{}{}
}

func (w *worker) runWorkflow(ctx *dbosContext, workflowName string) {
	defer w.policyGoroutinesWg.Done()

	workerLogger := w.logger.With("workflow_name", workflowName)

	currentPollingInterval := _defaultBasePollingInterval

	for {
		hasBackoffError := false

		if err := ctx.kernel.transitionDelayedWorkflows(ctx); err != nil {
			workerLogger.Warn("Exception transitioning delayed workflows", "error", err)
		}

		dequeuedWorkflows, _ := w.dequeueWorkflows(ctx, workflowName, &hasBackoffError)

		if len(dequeuedWorkflows) > 0 {
			workerLogger.Debug("Claimed workflows", "workflows", len(dequeuedWorkflows))
		}
		for _, workflow := range dequeuedWorkflows {
			registeredWorkflow, exists := ctx.workflowRegistry.Load(workflow.name)
			if !exists {
				workerLogger.Error("workflow function not found in registry", "workflow_name", workflow.name)
				continue
			}

			_, err := registeredWorkflow.wrappedFunction(ctx, workflow.input, workflow.serialization, withWorkflowId(workflow.id), withIsDequeue())
			if err != nil {
				workerLogger.Error("Error running claimed workflow", "error", err)
			}
		}

		if hasBackoffError {

			newInterval := time.Duration(float64(currentPollingInterval) * w.backoffFactor)
			currentPollingInterval = min(newInterval, _defaultMaxPollingInterval)
		} else {

			newInterval := time.Duration(float64(currentPollingInterval) * w.scalebackFactor)
			currentPollingInterval = max(newInterval, _defaultBasePollingInterval)
		}

		jitter := w.jitterMin + rand.Float64()*(w.jitterMax-w.jitterMin)
		sleepDuration := time.Duration(float64(currentPollingInterval) * jitter)

		select {
		case <-ctx.Done():
			workerLogger.Debug("Worker claim loop stopping due to context cancellation", "cause", context.Cause(ctx))
			return
		case <-time.After(sleepDuration):

		}
	}
}

func (w *worker) dequeueWorkflows(ctx *dbosContext, workflowName string, hasBackoffError *bool) ([]dequeuedWorkflow, bool) {
	dequeuedWorkflows, err := retryWithResult(ctx, func() ([]dequeuedWorkflow, error) {
		return ctx.kernel.dequeueWorkflows(ctx, dequeueWorkflowsInput{
			workflowName:       workflowName,
			executorId:         ctx.executorId,
			applicationVersion: ctx.applicationVersion,
		})
	}, withRetrierLogger(w.logger))

	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			switch pgErr.Code {
			case pgerrcode.SerializationFailure, pgerrcode.LockNotAvailable:
				*hasBackoffError = true
			}
		} else {
			w.logger.Error("Error claiming workflows", "workflow_name", workflowName, "error", err)
		}
		return nil, true
	}

	return dequeuedWorkflows, false
}
