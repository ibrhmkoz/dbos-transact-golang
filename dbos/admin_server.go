package dbos

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	_healthcheckPattern      = "GET /dbos-healthz"
	_workflowRecoveryPattern = "POST /dbos-workflow-recovery"
	_deactivatePattern       = "GET /deactivate"
	_garbageCollectPattern   = "POST /dbos-garbage-collect"
	_globalTimeoutPattern    = "POST /dbos-global-timeout"
	_workflowsPattern        = "POST /workflows"
	_workflowPattern         = "GET /workflows/{id}"
	_workflowStepsPattern    = "GET /workflows/{id}/steps"
	_workflowCancelPattern   = "POST /workflows/{id}/cancel"
	_workflowResumePattern   = "POST /workflows/{id}/resume"
	_workflowForkPattern     = "POST /workflows/{id}/fork"

	_adminServerReadHeaderTimeout = 5 * time.Second
)

type listWorkflowsRequest struct {
	WorkflowUuids      []string   `json:"workflow_uuids"`
	AuthenticatedUser  *string    `json:"authenticated_user"`
	StartTime          *time.Time `json:"start_time"`
	EndTime            *time.Time `json:"end_time"`
	Status             string     `json:"status"`
	ApplicationVersion *string    `json:"application_version"`
	WorkflowName       *string    `json:"workflow_name"`
	Limit              *int       `json:"limit"`
	Offset             *int       `json:"offset"`
	SortDesc           *bool      `json:"sort_desc"`
	WorkflowIdPrefix   *string    `json:"workflow_id_prefix"`
	LoadInput          *bool      `json:"load_input"`
	LoadOutput         *bool      `json:"load_output"`
}

func (req *listWorkflowsRequest) toListWorkflowsOptions() []ListWorkflowsOption {
	var opts []ListWorkflowsOption
	if len(req.WorkflowUuids) > 0 {
		opts = append(opts, WithWorkflowIds(req.WorkflowUuids))
	}
	if req.AuthenticatedUser != nil {
		opts = append(opts, WithUser(*req.AuthenticatedUser))
	}
	if req.StartTime != nil {
		opts = append(opts, WithStartTime(*req.StartTime))
	}
	if req.EndTime != nil {
		opts = append(opts, WithEndTime(*req.EndTime))
	}
	if len(req.Status) > 0 {
		statuses := make([]WorkflowStatusType, 1)
		statuses[0] = WorkflowStatusType(req.Status)
		opts = append(opts, WithStatus(statuses))
	}
	if req.ApplicationVersion != nil {
		opts = append(opts, WithAppVersion(*req.ApplicationVersion))
	}
	if req.WorkflowName != nil {
		opts = append(opts, WithName(*req.WorkflowName))
	}
	if req.Limit != nil {
		opts = append(opts, WithLimit(*req.Limit))
	}
	if req.Offset != nil {
		opts = append(opts, WithOffset(*req.Offset))
	}
	if req.SortDesc != nil {
		opts = append(opts, WithSortDesc())
	}
	if req.WorkflowIdPrefix != nil {
		opts = append(opts, WithWorkflowIdPrefix(*req.WorkflowIdPrefix))
	}
	if req.LoadInput != nil {
		opts = append(opts, WithLoadInput(*req.LoadInput))
	}
	if req.LoadOutput != nil {
		opts = append(opts, WithLoadOutput(*req.LoadOutput))
	}
	return opts
}

type adminServer struct {
	server        *http.Server
	logger        *slog.Logger
	port          int
	isDeactivated atomic.Int32
	wg            sync.WaitGroup
}

func toListWorkflowResponse(ws WorkflowStatus) (map[string]any, error) {
	result := map[string]any{
		"WorkflowUUID":       ws.Id,
		"Status":             ws.Status,
		"WorkflowName":       ws.Name,
		"AuthenticatedUser":  ws.AuthenticatedUser,
		"AssumedRole":        ws.AssumedRole,
		"AuthenticatedRoles": ws.AuthenticatedRoles,
		"Output":             ws.Output,
		"ExecutorID":         ws.ExecutorId,
		"ApplicationVersion": ws.ApplicationVersion,
		"ApplicationID":      ws.ApplicationId,
		"Attempts":           ws.Attempts,
		"QueueName":          ws.QueueName,
		"Timeout":            ws.Timeout,
		"DeduplicationID":    ws.DeduplicationId,
		"Priority":           ws.Priority,
		"QueuePartitionKey":  ws.QueuePartitionKey,
		"Input":              ws.Input,
	}

	formatEpochMs := func(t time.Time) any {
		if t.IsZero() {
			return nil
		}
		return strconv.FormatInt(t.UTC().UnixMilli(), 10)
	}

	result["CreatedAt"] = formatEpochMs(ws.CreatedAt)
	result["UpdatedAt"] = formatEpochMs(ws.UpdatedAt)
	result["WorkflowDeadlineEpochMS"] = formatEpochMs(ws.Deadline)
	result["StartedAt"] = formatEpochMs(ws.StartedAt)

	if ws.Input != nil {

		jsonInput, ok := ws.Input.(string)
		if ok {
			result["Input"] = jsonInput
		} else {
			result["Input"] = ""
		}
	}

	if ws.Output != nil {
		jsonOutput, ok := ws.Output.(string)
		if ok {
			result["Output"] = jsonOutput
		} else {
			result["Output"] = ""
		}
	}

	if ws.Error != nil {

		errStr := ws.Error.Error()
		bytes, err := json.Marshal(errStr)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal error: %w", err)
		}
		result["Error"] = string(bytes)
	} else {
		result["Error"] = ""
	}

	return result, nil
}

func newAdminServer(ctx *dbosContext, port int) *adminServer {
	as := &adminServer{
		logger: ctx.logger,
		port:   port,
	}

	mux := http.NewServeMux()

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _healthcheckPattern)
	mux.HandleFunc(_healthcheckPattern, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"status":"healthy"}`))
		if err != nil {
			ctx.logger.Error("Error writing health check response", "error", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _workflowRecoveryPattern)
	mux.HandleFunc(_workflowRecoveryPattern, func(w http.ResponseWriter, r *http.Request) {
		var executorIds []string
		if err := json.NewDecoder(r.Body).Decode(&executorIds); err != nil {
			http.Error(w, "Invalid JSON body", http.StatusBadRequest)
			return
		}

		ctx.logger.Info("Recovering workflows for executors", "executors", executorIds)

		handles, err := recoverPendingWorkflows(ctx, executorIds)
		if err != nil {
			ctx.logger.Error("Error recovering workflows", "error", err)
			http.Error(w, fmt.Sprintf("Recovery failed: %v", err), http.StatusInternalServerError)
			return
		}

		workflowIds := make([]string, len(handles))
		for i, handle := range handles {
			workflowIds[i] = handle.GetWorkflowId()
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(workflowIds); err != nil {
			ctx.logger.Error("Error encoding response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
			return
		}
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _deactivatePattern)
	mux.HandleFunc(_deactivatePattern, func(w http.ResponseWriter, r *http.Request) {
		if as.isDeactivated.CompareAndSwap(0, 1) {
			ctx.logger.Info("Deactivating DBOS executor", "executor_id", ctx.executorId, "app_version", ctx.applicationVersion)

			if ctx.workflowScheduler != nil {
				ctx.workflowScheduler.Stop()
			}
		}

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("deactivated")); err != nil {
			ctx.logger.Error("Error writing deactivate response", "error", err)
		}
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _garbageCollectPattern)
	mux.HandleFunc(_garbageCollectPattern, func(w http.ResponseWriter, r *http.Request) {
		var inputs struct {
			CutoffEpochTimestampMs *int64 `json:"cutoff_epoch_timestamp_ms"`
			RowsThreshold          *int   `json:"rows_threshold"`
		}

		if err := json.NewDecoder(r.Body).Decode(&inputs); err != nil {
			http.Error(w, "Invalid JSON body", http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _globalTimeoutPattern)
	mux.HandleFunc(_globalTimeoutPattern, func(w http.ResponseWriter, r *http.Request) {
		var inputs struct {
			CutoffEpochTimestampMs int64 `json:"cutoff_epoch_timestamp_ms"`
		}

		if err := json.NewDecoder(r.Body).Decode(&inputs); err != nil {
			http.Error(w, "Invalid JSON body", http.StatusBadRequest)
			return
		}

		cutoffTime := time.UnixMilli(inputs.CutoffEpochTimestampMs)
		ctx.logger.Info("Global timeout request", "cutoff_time", cutoffTime)

		err := retry(ctx, func() error {
			return ctx.kernel.cancelAllBefore(ctx, cutoffTime)
		}, withRetrierLogger(ctx.logger))
		if err != nil {
			ctx.logger.Error("Global timeout failed", "error", err)
			http.Error(w, fmt.Sprintf("Global timeout failed: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _workflowsPattern)
	mux.HandleFunc(_workflowsPattern, func(w http.ResponseWriter, r *http.Request) {
		var req listWorkflowsRequest
		if r.ContentLength > 0 {
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, fmt.Sprintf("Invalid JSON input: %v", err), http.StatusBadRequest)
				return
			}
		}

		workflows, err := ListWorkflows(ctx, req.toListWorkflowsOptions()...)
		if err != nil {
			ctx.logger.Error("Failed to list workflows", "error", err)
			http.Error(w, fmt.Sprintf("Failed to list workflows: %v", err), http.StatusInternalServerError)
			return
		}

		responseWorkflows := make([]map[string]any, len(workflows))
		for i, wf := range workflows {
			responseWorkflows[i], err = toListWorkflowResponse(wf)
			if err != nil {
				ctx.logger.Error("Error transforming workflow response", "error", err)
				http.Error(w, fmt.Sprintf("Failed to format workflow response: %v", err), http.StatusInternalServerError)
				return
			}
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(responseWorkflows); err != nil {
			ctx.logger.Error("Error encoding workflows response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _workflowPattern)
	mux.HandleFunc(_workflowPattern, func(w http.ResponseWriter, r *http.Request) {
		workflowId := r.PathValue("id")

		opts := []ListWorkflowsOption{WithWorkflowIds([]string{workflowId})}
		workflows, err := ListWorkflows(ctx, opts...)
		if err != nil {
			ctx.logger.Error("Failed to get workflow", "workflow_id", workflowId, "error", err)
			http.Error(w, fmt.Sprintf("Failed to get workflow: %v", err), http.StatusInternalServerError)
			return
		}

		if len(workflows) == 0 {
			http.Error(w, "Workflow not found", http.StatusNotFound)
			return
		}

		workflow, err := toListWorkflowResponse(workflows[0])
		if err != nil {
			ctx.logger.Error("Error transforming workflow response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to format workflow response: %v", err), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(workflow); err != nil {
			ctx.logger.Error("Error encoding workflow response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _workflowStepsPattern)
	mux.HandleFunc(_workflowStepsPattern, func(w http.ResponseWriter, r *http.Request) {
		workflowId := r.PathValue("id")

		steps, err := GetWorkflowSteps(ctx, workflowId)
		if err != nil {
			ctx.logger.Error("Failed to list workflow steps", "workflow_id", workflowId, "error", err)
			http.Error(w, fmt.Sprintf("Failed to list steps: %v", err), http.StatusInternalServerError)
			return
		}

		formattedSteps := make([]map[string]any, len(steps))
		for i, step := range steps {
			formattedStep := map[string]any{
				"function_id":   step.StepId,
				"function_name": step.StepName,
			}

			if !step.StartedAt.IsZero() {
				formattedStep["started_at_epoch_ms"] = step.StartedAt.UnixMilli()
			}
			if !step.CompletedAt.IsZero() {
				formattedStep["completed_at_epoch_ms"] = step.CompletedAt.UnixMilli()
			}

			if step.Output != nil {

				jsonOutput, ok := step.Output.(string)
				if ok {
					formattedStep["output"] = jsonOutput
				} else {
					formattedStep["output"] = ""
				}
			} else {
				formattedStep["output"] = ""
			}

			if step.Error != nil {

				errStr := step.Error.Error()
				bytes, err := json.Marshal(errStr)
				if err != nil {
					ctx.logger.Error("Failed to marshal step error", "error", err)
					http.Error(w, fmt.Sprintf("Failed to format step error: %v", err), http.StatusInternalServerError)
					return
				}
				formattedStep["error"] = string(bytes)
			}

			formattedSteps[i] = formattedStep
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(formattedSteps); err != nil {
			ctx.logger.Error("Error encoding steps response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _workflowCancelPattern)
	mux.HandleFunc(_workflowCancelPattern, func(w http.ResponseWriter, r *http.Request) {
		workflowId := r.PathValue("id")
		ctx.logger.Info("Cancelling workflow", "workflow_id", workflowId)

		err := ctx.CancelWorkflow(workflowId)
		if err != nil {
			ctx.logger.Error("Failed to cancel workflow", "workflow_id", workflowId, "error", err)
			http.Error(w, fmt.Sprintf("Failed to cancel workflow: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _workflowResumePattern)
	mux.HandleFunc(_workflowResumePattern, func(w http.ResponseWriter, r *http.Request) {
		workflowId := r.PathValue("id")
		ctx.logger.Info("Resuming workflow", "workflow_id", workflowId)

		_, err := ctx.ResumeWorkflow(workflowId)
		if err != nil {
			ctx.logger.Error("Failed to resume workflow", "workflow_id", workflowId, "error", err)
			http.Error(w, fmt.Sprintf("Failed to resume workflow: %v", err), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	ctx.logger.Debug("Registering admin server endpoint", "pattern", _workflowForkPattern)
	mux.HandleFunc(_workflowForkPattern, func(w http.ResponseWriter, r *http.Request) {
		workflowId := r.PathValue("id")
		var data struct {
			StartStep          *uint   `json:"start_step"`
			ForkedWorkflowId   *string `json:"new_workflow_id"`
			ApplicationVersion *string `json:"application_version"`
		}

		if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON input: %v", err), http.StatusBadRequest)
			return
		}

		input := ForkWorkflowInput{
			OriginalWorkflowId: workflowId,
		}
		if data.StartStep != nil {
			input.StartStep = *data.StartStep
		}
		if data.ForkedWorkflowId != nil {
			input.ForkedWorkflowId = *data.ForkedWorkflowId
		}
		if data.ApplicationVersion != nil {
			input.ApplicationVersion = *data.ApplicationVersion
		}

		ctx.logger.Info("Forking workflow", "workflow_id", workflowId, "start_step", input.StartStep)

		handle, err := ctx.ForkWorkflow(input)
		if err != nil {
			ctx.logger.Error("Failed to fork workflow", "workflow_id", workflowId, "error", err)
			http.Error(w, fmt.Sprintf("Failed to fork workflow: %v", err), http.StatusInternalServerError)
			return
		}

		response := map[string]string{
			"workflow_id": handle.GetWorkflowId(),
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			ctx.logger.Error("Error encoding fork response", "error", err)
			http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
		}
	})

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: _adminServerReadHeaderTimeout,
	}

	as.server = server
	return as
}

func (as *adminServer) Start() error {
	as.logger.Info("Starting admin server", "port", as.port)

	as.wg.Add(1)
	go func() {
		defer as.wg.Done()
		if err := as.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			as.logger.Error("Admin server error", "error", err)
		}
	}()

	return nil
}

func (as *adminServer) Shutdown(timeout time.Duration) error {
	as.logger.Info("Shutting down admin server")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := as.server.Shutdown(ctx); err != nil {
		as.logger.Error("Admin server shutdown error", "error", err)
		return fmt.Errorf("failed to shutdown admin server: %w", err)
	}

	done := make(chan struct{})
	go func() {
		as.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		as.logger.Info("Admin server shutdown complete")
	case <-ctx.Done():
		as.logger.Warn("Admin server shutdown timed out")
	}

	return nil
}
