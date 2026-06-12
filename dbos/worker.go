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
	_DBOS_INTERNAL_QUEUE_NAME        = "_dbos_internal_queue"
	_DEFAULT_MAX_TASKS_PER_ITERATION = 100
	_DEFAULT_BASE_POLLING_INTERVAL   = 1 * time.Second
	_DEFAULT_MAX_POLLING_INTERVAL    = 120 * time.Second
)

type worker struct {
	logger *slog.Logger

	// Claim-loop iteration parameters
	backoffFactor   float64
	scalebackFactor float64
	jitterMin       float64
	jitterMax       float64

	// WaitGroup to track policy claim loops.
	policyGoroutinesWg sync.WaitGroup

	// Channel to signal completion back to the DBOS context
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

// run starts one claim loop per worker-dispatched workflow.
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
	// Current polling interval starts at the base interval and adjusts based on errors
	currentPollingInterval := _DEFAULT_BASE_POLLING_INTERVAL

	for {
		hasBackoffError := false
		// Transition any DELAYED workflows whose delay has expired to ENQUEUED.
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

			// Pass encoded input directly - decoding will happen in workflow wrapper when we know the target type
			_, err := registeredWorkflow.wrappedFunction(ctx, workflow.input, workflow.serialization, withWorkflowID(workflow.id), withIsDequeue())
			if err != nil {
				workerLogger.Error("Error running claimed workflow", "error", err)
			}
		}

		// Adjust polling interval for this policy based on errors
		if hasBackoffError {
			// Increase polling interval using exponential backoff, but never exceed maxPollingInterval
			newInterval := time.Duration(float64(currentPollingInterval) * w.backoffFactor)
			currentPollingInterval = min(newInterval, _DEFAULT_MAX_POLLING_INTERVAL)
		} else {
			// Scale back polling interval on successful iteration, but never go below base interval
			newInterval := time.Duration(float64(currentPollingInterval) * w.scalebackFactor)
			currentPollingInterval = max(newInterval, _DEFAULT_BASE_POLLING_INTERVAL)
		}

		// Apply jitter to this policy's polling interval
		jitter := w.jitterMin + rand.Float64()*(w.jitterMax-w.jitterMin) // #nosec G404 -- non-crypto jitter; acceptable
		sleepDuration := time.Duration(float64(currentPollingInterval) * jitter)

		// Sleep with jittered interval, but allow early exit on context cancellation
		select {
		case <-ctx.Done():
			workerLogger.Debug("Worker claim loop stopping due to context cancellation", "cause", context.Cause(ctx))
			return
		case <-time.After(sleepDuration):
			// Continue to next iteration
		}
	}
}

// dequeueWorkflows dequeues workflows from a specific partition and handles errors.
// Returns the dequeued workflows and a boolean indicating whether to continue to the next iteration.
func (w *worker) dequeueWorkflows(ctx *dbosContext, workflowName string, hasBackoffError *bool) ([]dequeuedWorkflow, bool) {
	dequeuedWorkflows, err := retryWithResult(ctx, func() ([]dequeuedWorkflow, error) {
		return ctx.kernel.dequeueWorkflows(ctx, dequeueWorkflowsInput{
			workflowName:       workflowName,
			executorID:         ctx.executorID,
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
		return nil, true // Indicate to continue to next iteration
	}

	return dequeuedWorkflows, false // Success, don't continue
}
