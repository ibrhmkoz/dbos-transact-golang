package dbos

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	_pingInterval         = 20 * time.Second
	_pingTimeout          = 30 * time.Second
	_initialReconnectWait = 1 * time.Second
	_maxReconnectWait     = 30 * time.Second
	_handshakeTimeout     = 10 * time.Second
	_writeDeadline        = 5 * time.Second
)

type conductorConfig struct {
	url              string
	apiKey           string
	appName          string
	executorMetadata map[string]any
}

type conductor struct {
	dbosCtx *dbosContext
	logger  *slog.Logger

	conn           *websocket.Conn
	needsReconnect atomic.Bool
	wg             sync.WaitGroup
	stopOnce       sync.Once
	writeMu        sync.Mutex

	url           url.URL
	pingInterval  time.Duration
	pingTimeout   time.Duration
	reconnectWait time.Duration

	executorMetadata map[string]any

	pingCancel context.CancelFunc
}

func (c *conductor) launch() {
	c.logger.Info("Launching conductor")
	c.wg.Add(1)
	go c.run()
}

func newConductor(dbosCtx *dbosContext, config conductorConfig) (*conductor, error) {
	if config.apiKey == "" {
		return nil, fmt.Errorf("conductor API key is required")
	}
	if config.url == "" {
		return nil, fmt.Errorf("conductor URL is required")
	}

	baseUrl, err := url.Parse(config.url)
	if err != nil {
		return nil, fmt.Errorf("invalid conductor URL: %w", err)
	}

	wsUrl := url.URL{
		Scheme: baseUrl.Scheme,
		Host:   baseUrl.Host,
		Path:   baseUrl.JoinPath("websocket", config.appName, config.apiKey).Path,
	}

	c := &conductor{
		dbosCtx:          dbosCtx,
		url:              wsUrl,
		pingInterval:     _pingInterval,
		pingTimeout:      _pingTimeout,
		reconnectWait:    _initialReconnectWait,
		logger:           dbosCtx.logger.With("service", "conductor"),
		executorMetadata: config.executorMetadata,
	}

	// Start with needsReconnect set to true so we connect on first run
	c.needsReconnect.Store(true)

	return c, nil
}

func (c *conductor) shutdown(timeout time.Duration) {
	c.stopOnce.Do(func() {
		if c.pingCancel != nil {
			c.pingCancel()
		}

		c.closeConn()

		done := make(chan struct{})
		go func() {
			c.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			c.logger.Info("Conductor shut down")
		case <-time.After(timeout):
			c.logger.Warn("Timeout waiting for conductor to shut down", "timeout", timeout)
		}
	})
}

// reconnectWaitWithJitter adds random jitter to the reconnect wait time to prevent thundering herd
func (c *conductor) reconnectWaitWithJitter() time.Duration {

	jitter := 0.5 + rand.Float64() // #nosec G404 -- jitter for backoff doesn't need crypto-secure randomness
	return time.Duration(float64(c.reconnectWait) * jitter)
}

func (c *conductor) closeConn() {

	if c.pingCancel != nil {
		c.pingCancel()
		c.pingCancel = nil
	}

	// Acquire write mutex to ensure no concurrent writes during close
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn != nil {
		if err := c.conn.SetWriteDeadline(time.Now().Add(_writeDeadline)); err != nil {
			c.logger.Warn("Failed to set write deadline", "error", err)
		}
		err := c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutting down"))
		if err != nil {
			c.logger.Warn("Failed to send close message", "error", err)
		}
		err = c.conn.Close()
		if err != nil {
			c.logger.Warn("Failed to close connection", "error", err)
		}
		c.conn = nil
	}

	c.needsReconnect.Store(true)
}

func (c *conductor) run() {
	defer c.wg.Done()

	for {

		select {
		case <-c.dbosCtx.Done():
			c.logger.Info("DBOS context done, stopping conductor", "cause", context.Cause(c.dbosCtx))
			c.closeConn()
			return
		default:
		}

		if c.needsReconnect.Load() {
			if err := c.connect(); err != nil {
				c.logger.Warn("Failed to connect to conductor", "error", err)
				select {
				case <-c.dbosCtx.Done():
					c.logger.Info("DBOS context done, stopping conductor", "cause", context.Cause(c.dbosCtx))
					return
				case <-time.After(c.reconnectWaitWithJitter()):

					if c.reconnectWait < _maxReconnectWait {
						c.reconnectWait *= 2
						if c.reconnectWait > _maxReconnectWait {
							c.reconnectWait = _maxReconnectWait
						}
					}
					continue
				}
			}

			c.reconnectWait = _initialReconnectWait
			c.needsReconnect.Store(false)
		}

		if c.conn == nil {
			c.needsReconnect.Store(true)
			continue
		}

		messageType, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.logger.Warn("Unexpected WebSocket close", "error", err)
			} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				c.logger.Debug("Read deadline reached", "error", err)
			} else {
				c.logger.Debug("Connection closed", "error", err)
			}

			c.closeConn()
			continue
		}

		if messageType != websocket.TextMessage {
			c.logger.Warn("Received unexpected message type, forcing reconnection", "type", messageType)
			c.closeConn()
			continue
		}

		ht := time.Now()
		if err := c.handleMessage(message); err != nil {
			c.logger.Error("Failed to handle message", "error", err)
		}
		c.logger.Debug("Handled message", "message", messageType, "latency_us", time.Since(ht).Microseconds())
	}
}

func (c *conductor) connect() error {
	c.logger.Debug("Connecting to conductor")

	dialer := websocket.Dialer{
		HandshakeTimeout: _handshakeTimeout,
	}

	conn, resp, err := dialer.Dial(c.url.String(), nil)
	if err != nil {

		baseErr := fmt.Errorf("failed to dial conductor: %w", err)
		if resp != nil {

			body := ""
			if resp.Body != nil {
				bodyBytes, readErr := io.ReadAll(resp.Body)
				if closeErr := resp.Body.Close(); closeErr != nil {
					c.logger.Debug("Failed to close response body", "error", closeErr)
				}
				if readErr == nil && len(bodyBytes) > 0 {
					body = string(bodyBytes)
				}
			}
			return fmt.Errorf("%w (%s)", baseErr, body)
		}
		return baseErr
	}

	if err := conn.SetReadDeadline(time.Now().Add(c.pingTimeout)); err != nil {
		cErr := conn.Close()
		if cErr != nil {
			c.logger.Warn("Failed to close connection", "error", cErr)
		}
		return fmt.Errorf("failed to set read deadline: %w", err)
	}

	conn.SetPongHandler(func(appData string) error {
		c.logger.Debug("Received pong from conductor")
		return conn.SetReadDeadline(time.Now().Add(c.pingTimeout))
	})

	c.conn = conn

	pingCtx, pingCancel := context.WithCancel(c.dbosCtx)
	c.pingCancel = pingCancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.pingInterval)
		defer ticker.Stop()

		for {
			select {
			case <-pingCtx.Done():
				c.logger.Debug("Exiting Conductor ping goroutine", "cause", context.Cause(pingCtx))
				return
			case <-ticker.C:
				if err := c.ping(); err != nil {
					c.logger.Warn("Ping failed, signaling reconnection", "error", err)

					c.needsReconnect.Store(true)
					return
				}
			}
		}
	}()

	c.logger.Info("Connected to DBOS conductor")
	return nil
}

func (c *conductor) ping() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	c.logger.Debug("Sending ping to conductor")

	if err := c.conn.SetWriteDeadline(time.Now().Add(_writeDeadline)); err != nil {
		c.logger.Warn("Failed to set write deadline for ping", "error", err)
	}
	if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
		return fmt.Errorf("failed to send ping: %w", err)
	}
	if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
		c.logger.Warn("Failed to clear write deadline", "error", err)
	}

	return nil
}

func (c *conductor) handleMessage(data []byte) error {
	var base baseMessage
	if err := json.Unmarshal(data, &base); err != nil {
		c.logger.Error("Failed to parse message", "error", err)
		return fmt.Errorf("failed to parse base message: %w", err)
	}
	c.logger.Debug("Received message", "type", base.Type, "request_id", base.RequestId)

	switch base.Type {
	case executorInfo:
		return c.handleExecutorInfoRequest(data, base.RequestId)
	case recoveryMessage:
		return c.handleRecoveryRequest(data, base.RequestId)
	case cancelWorkflowMessage:
		return c.handleCancelWorkflowRequest(data, base.RequestId)
	case resumeWorkflowMessage:
		return c.handleResumeWorkflowRequest(data, base.RequestId)
	case listWorkflowsMessage:
		return c.handleListWorkflowsRequest(data, base.RequestId)
	case listStepsMessage:
		return c.handleListStepsRequest(data, base.RequestId)
	case getWorkflowMessage:
		return c.handleGetWorkflowRequest(data, base.RequestId)
	case forkWorkflowMessage:
		return c.handleForkWorkflowRequest(data, base.RequestId)
	case existPendingWorkflowsMessage:
		return c.handleExistPendingWorkflowsRequest(data, base.RequestId)
	case retentionMessage:
		return c.handleRetentionRequest(data, base.RequestId)
	case getMetricsMessage:
		return c.handleGetMetricsRequest(data, base.RequestId)
	case exportWorkflowMessage:
		return c.handleExportWorkflowRequest(data, base.RequestId)
	case importWorkflowMessage:
		return c.handleImportWorkflowRequest(data, base.RequestId)
	case deleteWorkflowMessage:
		return c.handleDeleteWorkflowRequest(data, base.RequestId)
	case alertMessage:
		return c.handleAlertRequest(data, base.RequestId)
	case listSchedulesMessage:
		return c.handleListSchedulesRequest(data, base.RequestId)
	case getScheduleMessage:
		return c.handleGetScheduleRequest(data, base.RequestId)
	case pauseScheduleMessage:
		return c.handlePauseScheduleRequest(data, base.RequestId)
	case resumeScheduleMessage:
		return c.handleResumeScheduleRequest(data, base.RequestId)
	case backfillScheduleMessage:
		return c.handleBackfillScheduleRequest(data, base.RequestId)
	case triggerScheduleMessage:
		return c.handleTriggerScheduleRequest(data, base.RequestId)
	case getWorkflowEventsMessage:
		return c.handleGetWorkflowEventsRequest(data, base.RequestId)
	case getWorkflowNotificationsMsg:
		return c.handleGetWorkflowNotificationsRequest(data, base.RequestId)
	case getWorkflowStreamsMessage:
		return c.handleGetWorkflowStreamsRequest(data, base.RequestId)
	case getWorkflowAggregatesMessage:
		return c.handleGetWorkflowAggregatesRequest(data, base.RequestId)
	case getStepAggregatesMessage:
		return c.handleGetStepAggregatesRequest(data, base.RequestId)
	case listAppVersionsMessage:
		return c.handleListApplicationVersionsRequest(data, base.RequestId)
	case setLatestAppVersionMessage:
		return c.handleSetLatestApplicationVersionRequest(data, base.RequestId)
	default:
		c.logger.Warn("Unknown message type", "type", base.Type)
		return c.handleUnknownMessageType(base.RequestId, base.Type, "Unknown message type")
	}
}

func (c *conductor) handleExecutorInfoRequest(data []byte, requestId string) error {
	var req executorInfoRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse executor info request", "error", err)
		return fmt.Errorf("failed to parse executor info request: %w", err)
	}
	c.logger.Debug("Handling executor info request", "request_id", req)

	hostname, err := os.Hostname()
	if err != nil {
		c.logger.Error("Failed to get hostname", "error", err)
		return fmt.Errorf("failed to get hostname: %w", err)
	}

	response := executorInfoResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      executorInfo,
				RequestId: requestId,
			},
		},
		ExecutorId:         c.dbosCtx.GetExecutorId(),
		ApplicationVersion: c.dbosCtx.GetApplicationVersion(),
		Hostname:           &hostname,
		DbosVersion:        getDbosVersion(),
		Language:           "go",
		ExecutorMetadata:   c.executorMetadata,
	}

	return c.sendResponse(response, string(executorInfo))
}

func (c *conductor) handleRecoveryRequest(data []byte, requestId string) error {
	var req recoveryConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse recovery request", "error", err)
		return fmt.Errorf("failed to parse recovery request: %w", err)
	}
	c.logger.Debug("Handling recovery request", "executor_ids", req.ExecutorIds, "request_id", requestId)

	success := true
	var errorMsg *string

	_, err := recoverPendingWorkflows(c.dbosCtx, req.ExecutorIds)
	if err != nil {
		c.logger.Error("Failed to recover pending workflows", "executor_ids", req.ExecutorIds, "error", err)
		errStr := fmt.Sprintf("failed to recover pending workflows: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.logger.Info("Successfully recovered pending workflows", "executor_ids", req.ExecutorIds)
	}

	response := recoveryConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      recoveryMessage,
				RequestId: requestId,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(recoveryMessage))
}

func (c *conductor) handleCancelWorkflowRequest(data []byte, requestId string) error {
	var req cancelWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse cancel workflow request", "error", err)
		return fmt.Errorf("failed to parse cancel workflow request: %w", err)
	}
	workflowIds := req.WorkflowIds
	if len(workflowIds) == 0 && req.WorkflowId != "" {
		workflowIds = []string{req.WorkflowId}
	}
	c.logger.Debug("Handling cancel workflow request", "workflow_ids", workflowIds, "request_id", requestId)

	success := true
	var errorMsg *string

	if err := c.dbosCtx.CancelWorkflows(workflowIds); err != nil {
		c.logger.Error("Failed to cancel workflows", "workflow_ids", workflowIds, "error", err)
		errStr := fmt.Sprintf("failed to cancel workflows: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.logger.Info("Successfully cancelled workflows", "workflow_ids", workflowIds)
	}

	response := cancelWorkflowConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      cancelWorkflowMessage,
				RequestId: requestId,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(cancelWorkflowMessage))
}

func (c *conductor) handleResumeWorkflowRequest(data []byte, requestId string) error {
	var req resumeWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse resume workflow request", "error", err)
		return fmt.Errorf("failed to parse resume workflow request: %w", err)
	}
	workflowIds := req.WorkflowIds
	if len(workflowIds) == 0 && req.WorkflowId != "" {
		workflowIds = []string{req.WorkflowId}
	}
	c.logger.Debug("Handling resume workflow request", "workflow_ids", workflowIds, "request_id", requestId)

	success := true
	var errorMsg *string

	var resumeOpts []ResumeWorkflowOption
	if req.QueueName != nil {
		resumeOpts = append(resumeOpts, WithResumeQueue(*req.QueueName))
	}
	_, err := c.dbosCtx.ResumeWorkflows(workflowIds, resumeOpts...)
	if err != nil {
		c.logger.Error("Failed to resume workflows", "workflow_ids", workflowIds, "error", err)
		errStr := fmt.Sprintf("failed to resume workflows: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.logger.Info("Successfully resumed workflows", "workflow_ids", workflowIds)
	}

	response := resumeWorkflowConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      resumeWorkflowMessage,
				RequestId: requestId,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(resumeWorkflowMessage))
}

func (c *conductor) handleRetentionRequest(data []byte, requestId string) error {
	var req retentionConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse retention request", "error", err)
		return fmt.Errorf("failed to parse retention request: %w", err)
	}
	c.logger.Debug("Handling retention request", "request", req, "request_id", requestId)

	success := true
	var errorMsg *string

	// Always run GC so an otherwise empty retention request enforces workflow-definition retention.
	var cutoffMs *int64
	if req.Body.GCCutoffEpochMs != nil {
		ms := int64(*req.Body.GCCutoffEpochMs)
		cutoffMs = &ms
	}

	var rowsThreshold *int
	if req.Body.GCRowsThreshold != nil {
		rowsThreshold = req.Body.GCRowsThreshold
	}

	input := garbageCollectWorkflowsInput{
		cutoffEpochTimestampMs: cutoffMs,
		rowsThreshold:          rowsThreshold,
	}

	err := retry(c.dbosCtx, func() error {
		return c.dbosCtx.kernel.garbageCollectWorkflows(c.dbosCtx, input)
	}, withRetrierLogger(c.logger))
	if err != nil {
		c.logger.Error("Failed to garbage collect workflows", "error", err)
		errStr := fmt.Sprintf("failed to garbage collect workflows: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.logger.Info("Successfully garbage collected workflows", "cutoff_ms", cutoffMs, "rows_threshold", rowsThreshold)
	}

	if success && req.Body.TimeoutCutoffEpochMs != nil {
		cutoffTime := time.UnixMilli(int64(*req.Body.TimeoutCutoffEpochMs))
		err := retry(c.dbosCtx, func() error {
			return c.dbosCtx.kernel.cancelAllBefore(c.dbosCtx, cutoffTime)
		}, withRetrierLogger(c.logger))
		if err != nil {
			c.logger.Error("Failed to timeout workflows", "cutoff_ms", *req.Body.TimeoutCutoffEpochMs, "error", err)
			errStr := fmt.Sprintf("failed to timeout workflows: %v", err)
			errorMsg = &errStr
			success = false
		} else {
			c.logger.Info("Successfully timed out workflows", "cutoff_ms", *req.Body.TimeoutCutoffEpochMs)
		}
	}

	response := retentionConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      retentionMessage,
				RequestId: requestId,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(retentionMessage))
}

func (c *conductor) handleGetMetricsRequest(data []byte, requestId string) error {
	var req getMetricsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get metrics request", "error", err)
		return fmt.Errorf("failed to parse get metrics request: %w", err)
	}
	c.logger.Debug("Handling get metrics request",
		"start_time", req.StartTime,
		"end_time", req.EndTime,
		"metric_class", req.MetricClass,
		"request_id", requestId)

	var errorMsg *string
	var metricsData []metricData

	if req.MetricClass == "workflow_step_count" {
		var err error
		metricsData, err = retryWithResult(c.dbosCtx, func() ([]metricData, error) {
			return c.dbosCtx.kernel.getMetrics(c.dbosCtx, req.StartTime, req.EndTime)
		}, withRetrierLogger(c.logger))
		if err != nil {
			c.logger.Error("Failed to get metrics", "error", err)
			errStr := fmt.Sprintf("Exception encountered when getting metrics: %v", err)
			errorMsg = &errStr
		}
	} else {
		errStr := fmt.Sprintf("Unexpected metric class: %s", req.MetricClass)
		errorMsg = &errStr
		c.logger.Warn("Unexpected metric class", "metric_class", req.MetricClass)
	}

	response := getMetricsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      getMetricsMessage,
				RequestId: requestId,
			},
			ErrorMessage: errorMsg,
		},
		Metrics: metricsData,
	}

	return c.sendResponse(response, string(getMetricsMessage))
}

func (c *conductor) handleListWorkflowsRequest(data []byte, requestId string) error {
	var req listWorkflowsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list workflows request", "error", err)
		return fmt.Errorf("failed to parse list workflows request: %w", err)
	}
	c.logger.Debug("Handling list workflows request", "request", req)

	var opts []ListWorkflowsOption
	opts = append(opts, WithLoadInput(req.Body.LoadInput))
	opts = append(opts, WithLoadOutput(req.Body.LoadOutput))
	if req.Body.SortDesc {
		opts = append(opts, WithSortDesc())
	}
	if len(req.Body.WorkflowUuids) > 0 {
		opts = append(opts, WithWorkflowIds(req.Body.WorkflowUuids))
	}
	if len(req.Body.WorkflowName) > 0 {
		opts = append(opts, WithName(req.Body.WorkflowName.toSlice()...))
	}
	if len(req.Body.AuthenticatedUser) > 0 {
		opts = append(opts, WithUser(req.Body.AuthenticatedUser.toSlice()...))
	}
	if len(req.Body.ApplicationVersion) > 0 {
		opts = append(opts, WithAppVersion(req.Body.ApplicationVersion.toSlice()...))
	}
	if req.Body.Limit != nil {
		opts = append(opts, WithLimit(*req.Body.Limit))
	}
	if req.Body.Offset != nil {
		opts = append(opts, WithOffset(*req.Body.Offset))
	}
	if req.Body.StartTime != nil {
		opts = append(opts, WithStartTime(*req.Body.StartTime))
	}
	if req.Body.EndTime != nil {
		opts = append(opts, WithEndTime(*req.Body.EndTime))
	}
	if req.Body.CompletedAfter != nil {
		opts = append(opts, WithCompletedAfter(*req.Body.CompletedAfter))
	}
	if req.Body.CompletedBefore != nil {
		opts = append(opts, WithCompletedBefore(*req.Body.CompletedBefore))
	}
	if req.Body.DequeuedAfter != nil {
		opts = append(opts, WithDequeuedAfter(*req.Body.DequeuedAfter))
	}
	if req.Body.DequeuedBefore != nil {
		opts = append(opts, WithDequeuedBefore(*req.Body.DequeuedBefore))
	}
	if len(req.Body.Status) > 0 {
		statuses := make([]WorkflowStatusType, len(req.Body.Status))
		for i, s := range req.Body.Status {
			statuses[i] = WorkflowStatusType(s)
		}
		opts = append(opts, WithStatus(statuses))
	}
	if len(req.Body.ForkedFrom) > 0 {
		opts = append(opts, WithForkedFrom(req.Body.ForkedFrom.toSlice()...))
	}
	if len(req.Body.ParentWorkflowId) > 0 {
		opts = append(opts, WithParentWorkflowId(req.Body.ParentWorkflowId.toSlice()...))
	}
	if req.Body.WasForkedFrom != nil {
		opts = append(opts, WithWasForkedFrom(*req.Body.WasForkedFrom))
	}
	if req.Body.HasParent != nil {
		opts = append(opts, WithHasParent(*req.Body.HasParent))
	}
	if len(req.Body.WorkflowIdPrefix) > 0 {
		opts = append(opts, WithWorkflowIdPrefix(req.Body.WorkflowIdPrefix.toSlice()...))
	}
	if len(req.Body.ExecutorId) > 0 {
		opts = append(opts, WithExecutorIds(req.Body.ExecutorId.toSlice()))
	}

	workflows, err := c.dbosCtx.ListWorkflows(opts...)
	if err != nil {
		c.logger.Error("Failed to list workflows", "error", err)
		errorMsg := fmt.Sprintf("failed to list workflows: %v", err)
		response := listWorkflowsConductorResponse{
			baseResponse: baseResponse{
				baseMessage: baseMessage{
					Type:      listWorkflowsMessage,
					RequestId: requestId,
				},
				ErrorMessage: &errorMsg,
			},
			Output: []listWorkflowsConductorResponseBody{},
		}
		return c.sendResponse(response, "list workflows response")
	}

	formattedWorkflows := make([]listWorkflowsConductorResponseBody, len(workflows))
	for i, wf := range workflows {
		formattedWorkflows[i] = formatListWorkflowsResponseBody(wf)
	}

	response := listWorkflowsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      listWorkflowsMessage,
				RequestId: requestId,
			},
		},
		Output: formattedWorkflows,
	}

	return c.sendResponse(response, string(listWorkflowsMessage))
}
func (c *conductor) handleListStepsRequest(data []byte, requestId string) error {
	var req listStepsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list steps request", "error", err)
		return fmt.Errorf("failed to parse list steps request: %w", err)
	}
	c.logger.Debug("Handling list steps request", "request", req)

	steps, err := GetWorkflowSteps(c.dbosCtx, req.WorkflowId, WithStepsLoadOutput(req.LoadOutput))
	if err != nil {
		c.logger.Error("Failed to list workflow steps", "workflow_id", req.WorkflowId, "error", err)
		errorMsg := fmt.Sprintf("failed to list workflow steps: %v", err)
		response := listStepsConductorResponse{
			baseResponse: baseResponse{
				baseMessage: baseMessage{
					Type:      listStepsMessage,
					RequestId: requestId,
				},
				ErrorMessage: &errorMsg,
			},
			Output: nil,
		}
		return c.sendResponse(response, string(listStepsMessage))
	}

	var formattedSteps *[]workflowStepsConductorResponseBody
	if steps != nil {
		stepsList := make([]workflowStepsConductorResponseBody, len(steps))
		for i, step := range steps {
			stepsList[i] = formatWorkflowStepsResponseBody(step)
		}
		formattedSteps = &stepsList
	}

	response := listStepsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      listStepsMessage,
				RequestId: requestId,
			},
		},
		Output: formattedSteps,
	}

	return c.sendResponse(response, string(listStepsMessage))
}

func (c *conductor) handleGetWorkflowRequest(data []byte, requestId string) error {
	var req getWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get workflow request", "error", err)
		return fmt.Errorf("failed to parse get workflow request: %w", err)
	}
	c.logger.Debug("Handling get workflow request", "workflow_id", req.WorkflowId)

	workflows, err := c.dbosCtx.ListWorkflows(
		WithWorkflowIds([]string{req.WorkflowId}),
		WithLoadInput(req.LoadInput),
		WithLoadOutput(req.LoadOutput))
	if err != nil {
		c.logger.Error("Failed to get workflow", "workflow_id", req.WorkflowId, "error", err)
		errorMsg := fmt.Sprintf("failed to get workflow: %v", err)
		response := getWorkflowConductorResponse{
			baseResponse: baseResponse{
				baseMessage: baseMessage{
					Type:      getWorkflowMessage,
					RequestId: requestId,
				},
				ErrorMessage: &errorMsg,
			},
			Output: nil,
		}
		return c.sendResponse(response, "get workflow response")
	}

	var formattedWorkflow *listWorkflowsConductorResponseBody
	if len(workflows) > 0 {
		formatted := formatListWorkflowsResponseBody(workflows[0])
		formattedWorkflow = &formatted
	}

	response := getWorkflowConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      getWorkflowMessage,
				RequestId: requestId,
			},
		},
		Output: formattedWorkflow,
	}

	return c.sendResponse(response, string(getWorkflowMessage))
}

func (c *conductor) handleForkWorkflowRequest(data []byte, requestId string) error {
	var req forkWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse fork workflow request", "error", err)
		return fmt.Errorf("failed to parse fork workflow request: %w", err)
	}
	c.logger.Debug("Handling fork workflow request", "request", req)

	// Validate StartStep to prevent integer overflow
	if req.Body.StartStep < 0 {
		return fmt.Errorf("invalid StartStep: cannot be negative")
	}
	if req.Body.StartStep > math.MaxInt32/2 {
		return fmt.Errorf("invalid StartStep: cannot be greater than %d", math.MaxInt32/2)
	}
	input := ForkWorkflowInput{
		OriginalWorkflowId: req.Body.WorkflowId,
		StartStep:          uint(req.Body.StartStep),
	}

	if req.Body.NewWorkflowId != nil {
		input.ForkedWorkflowId = *req.Body.NewWorkflowId
	}
	if req.Body.ApplicationVersion != nil {
		input.ApplicationVersion = *req.Body.ApplicationVersion
	}
	if req.Body.QueueName != nil {
		input.QueueName = *req.Body.QueueName
	}
	if req.Body.QueuePartitionKey != nil {
		input.QueuePartitionKey = *req.Body.QueuePartitionKey
	}

	handle, err := c.dbosCtx.ForkWorkflow(input)
	var newWorkflowId *string
	var errorMsg *string

	if err != nil {
		c.logger.Error("Failed to fork workflow", "original_workflow_id", req.Body.WorkflowId, "error", err)
		errStr := fmt.Sprintf("failed to fork workflow: %v", err)
		errorMsg = &errStr
	} else {
		workflowId := handle.GetWorkflowId()
		newWorkflowId = &workflowId
		c.logger.Info("Successfully forked workflow", "original_workflow_id", req.Body.WorkflowId, "new_workflow_id", workflowId)
	}

	response := forkWorkflowConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      forkWorkflowMessage,
				RequestId: requestId,
			},
			ErrorMessage: errorMsg,
		},
		NewWorkflowId: newWorkflowId,
	}

	return c.sendResponse(response, string(forkWorkflowMessage))
}

func (c *conductor) handleExistPendingWorkflowsRequest(data []byte, requestId string) error {
	var req existPendingWorkflowsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse exist pending workflows request", "error", err)
		return fmt.Errorf("failed to parse exist pending workflows request: %w", err)
	}
	c.logger.Debug("Handling exist pending workflows request", "executor_id", req.ExecutorId, "application_version", req.ApplicationVersion)

	opts := []ListWorkflowsOption{
		WithStatus([]WorkflowStatusType{WorkflowStatusPending}),
		WithLimit(1),
		WithExecutorIds([]string{req.ExecutorId}),
		WithAppVersion(req.ApplicationVersion),
	}

	workflows, err := c.dbosCtx.ListWorkflows(opts...)
	var errorMsg *string
	if err != nil {
		c.logger.Error("Failed to check for pending workflows", "executor_id", req.ExecutorId, "application_version", req.ApplicationVersion, "error", err)
		errStr := fmt.Sprintf("failed to check for pending workflows: %v", err)
		errorMsg = &errStr
	}

	response := existPendingWorkflowsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      existPendingWorkflowsMessage,
				RequestId: requestId,
			},
			ErrorMessage: errorMsg,
		},
		Exist: len(workflows) > 0,
	}

	return c.sendResponse(response, string(existPendingWorkflowsMessage))
}

func (c *conductor) handleAlertRequest(data []byte, requestId string) error {
	var req alertRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse alert request", "error", err)
		return fmt.Errorf("failed to parse alert request: %w", err)
	}
	c.logger.Debug("Handling alert request", "name", req.Name, "request_id", requestId)

	success := true
	var errorMsg *string

	handler := c.dbosCtx.alertHandler
	if handler != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					errStr := fmt.Sprintf("panic in alert handler: %v", r)
					c.logger.Error(errStr)
					errorMsg = &errStr
					success = false
				}
			}()
			handler(req.Name, req.Message, req.Metadata)
		}()
	} else {
		c.logger.Info("Alert received (no handler registered)", "name", req.Name, "message", req.Message, "metadata", req.Metadata)
	}

	response := alertConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      alertMessage,
				RequestId: requestId,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(alertMessage))
}

func (c *conductor) handleUnknownMessageType(requestId string, msgType messageType, errorMsg string) error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	response := baseResponse{
		baseMessage: baseMessage{
			Type:      msgType,
			RequestId: requestId,
		},
		ErrorMessage: &errorMsg,
	}

	return c.sendResponse(response, "unknown message type response")
}

func (c *conductor) handleExportWorkflowRequest(data []byte, requestId string) error {
	var req exportWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse export workflow request", "error", err)
		return fmt.Errorf("failed to parse export workflow request: %w", err)
	}
	c.logger.Debug("Handling export workflow request", "workflow_id", req.WorkflowId, "export_children", req.ExportChildren)

	var serializedWorkflow *string
	var errorMsg *string

	exported, err := retryWithResult(c.dbosCtx, func() ([]ExportedWorkflow, error) {
		return c.dbosCtx.kernel.exportWorkflow(c.dbosCtx, req.WorkflowId, req.ExportChildren)
	}, withRetrierLogger(c.logger))
	if err != nil {
		c.logger.Error("Failed to export workflow", "workflow_id", req.WorkflowId, "error", err)
		errStr := fmt.Sprintf("Exception encountered when exporting workflow %s: %v", req.WorkflowId, err)
		errorMsg = &errStr
	} else {
		jsonData, err := json.Marshal(exported)
		if err != nil {
			errStr := fmt.Sprintf("Failed to marshal exported workflow: %v", err)
			errorMsg = &errStr
		} else {
			var buf bytes.Buffer
			gz := gzip.NewWriter(&buf)
			if _, err := gz.Write(jsonData); err != nil {
				errStr := fmt.Sprintf("Failed to gzip exported workflow: %v", err)
				errorMsg = &errStr
			} else if err := gz.Close(); err != nil {
				errStr := fmt.Sprintf("Failed to close gzip writer: %v", err)
				errorMsg = &errStr
			} else {
				encoded := base64.StdEncoding.EncodeToString(buf.Bytes())
				serializedWorkflow = &encoded
			}
		}
	}

	response := exportWorkflowConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      exportWorkflowMessage,
				RequestId: requestId,
			},
			ErrorMessage: errorMsg,
		},
		SerializedWorkflow: serializedWorkflow,
	}

	return c.sendResponse(response, string(exportWorkflowMessage))
}

func (c *conductor) handleImportWorkflowRequest(data []byte, requestId string) error {
	var req importWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse import workflow request", "error", err)
		return fmt.Errorf("failed to parse import workflow request: %w", err)
	}
	c.logger.Debug("Handling import workflow request")

	success := true
	var errorMsg *string

	compressed, err := base64.StdEncoding.DecodeString(req.SerializedWorkflow)
	if err != nil {
		errStr := fmt.Sprintf("Failed to base64 decode serialized workflow: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		gz, err := gzip.NewReader(bytes.NewReader(compressed))
		if err != nil {
			errStr := fmt.Sprintf("Failed to create gzip reader: %v", err)
			errorMsg = &errStr
			success = false
		} else {
			jsonData, err := io.ReadAll(gz)
			if closeErr := gz.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
			if err != nil {
				errStr := fmt.Sprintf("Failed to decompress workflow data: %v", err)
				errorMsg = &errStr
				success = false
			} else {
				var workflows []ExportedWorkflow
				if err := json.Unmarshal(jsonData, &workflows); err != nil {
					errStr := fmt.Sprintf("Failed to unmarshal workflow data: %v", err)
					errorMsg = &errStr
					success = false
				} else {
					err := retry(c.dbosCtx, func() error {
						return c.dbosCtx.kernel.importWorkflow(c.dbosCtx, workflows)
					}, withRetrierLogger(c.logger))
					if err != nil {
						errStr := fmt.Sprintf("Exception encountered when importing workflow: %v", err)
						errorMsg = &errStr
						success = false
					}
				}
			}
		}
	}

	response := importWorkflowConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      importWorkflowMessage,
				RequestId: requestId,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(importWorkflowMessage))
}

func (c *conductor) handleDeleteWorkflowRequest(data []byte, requestId string) error {
	var req deleteWorkflowConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse delete workflow request", "error", err)
		return fmt.Errorf("failed to parse delete workflow request: %w", err)
	}
	workflowIds := req.WorkflowIds
	if len(workflowIds) == 0 && req.WorkflowId != "" {
		workflowIds = []string{req.WorkflowId}
	}
	c.logger.Debug("Handling delete workflow request", "workflow_ids", workflowIds, "delete_children", req.DeleteChildren, "request_id", requestId)

	success := true
	var errorMsg *string

	err := retry(c.dbosCtx, func() error {
		return c.dbosCtx.kernel.deleteWorkflows(c.dbosCtx, deleteWorkflowsDBInput{
			workflowIds:    workflowIds,
			deleteChildren: req.DeleteChildren,
		})
	}, withRetrierLogger(c.logger))
	if err != nil {
		c.logger.Error("Failed to delete workflows", "workflow_ids", workflowIds, "error", err)
		errStr := fmt.Sprintf("failed to delete workflows: %v", err)
		errorMsg = &errStr
		success = false
	} else {
		c.logger.Info("Successfully deleted workflows", "workflow_ids", workflowIds)
	}

	response := deleteWorkflowConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{
				Type:      deleteWorkflowMessage,
				RequestId: requestId,
			},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}

	return c.sendResponse(response, string(deleteWorkflowMessage))
}

func (c *conductor) decodeStoredValueForConductor(value, serialization string) (string, error) {
	decoder, err := resolveDecoder[any](serialization, getCustomSerializerFromCtx(c.dbosCtx))
	if err != nil {
		return "", err
	}
	decoded, err := decoder.Decode(&value)
	if err != nil {
		return "", err
	}
	out, err := json.Marshal(decoded)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func (c *conductor) handleGetWorkflowEventsRequest(data []byte, requestId string) error {
	var req getWorkflowEventsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get workflow events request", "error", err)
		return fmt.Errorf("failed to parse get workflow events request: %w", err)
	}
	c.logger.Debug("Handling get workflow events request", "workflow_id", req.WorkflowId, "request_id", requestId)

	resp := getWorkflowEventsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{Type: getWorkflowEventsMessage, RequestId: requestId},
		},
	}

	records, err := c.dbosCtx.kernel.getAllEvents(c.dbosCtx, req.WorkflowId)
	if err != nil {
		c.logger.Error("Failed to get workflow events", "workflow_id", req.WorkflowId, "error", err)
		errStr := fmt.Sprintf("failed to get workflow events: %v", err)
		resp.ErrorMessage = &errStr
		return c.sendResponse(resp, string(getWorkflowEventsMessage))
	}

	events := make([]eventOutput, 0, len(records))
	for _, r := range records {
		value, err := c.decodeStoredValueForConductor(r.Value, r.Serialization)
		if err != nil {
			c.logger.Error("Failed to decode workflow event", "workflow_id", req.WorkflowId, "key", r.Key, "error", err)
			errStr := fmt.Sprintf("failed to decode event %q: %v", r.Key, err)
			resp.ErrorMessage = &errStr
			resp.Events = nil
			return c.sendResponse(resp, string(getWorkflowEventsMessage))
		}
		events = append(events, eventOutput{Key: r.Key, Value: value})
	}
	resp.Events = events
	return c.sendResponse(resp, string(getWorkflowEventsMessage))
}

func (c *conductor) handleGetWorkflowNotificationsRequest(data []byte, requestId string) error {
	var req getWorkflowNotificationsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get workflow notifications request", "error", err)
		return fmt.Errorf("failed to parse get workflow notifications request: %w", err)
	}
	c.logger.Debug("Handling get workflow notifications request", "workflow_id", req.WorkflowId, "request_id", requestId)

	resp := getWorkflowNotificationsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{Type: getWorkflowNotificationsMsg, RequestId: requestId},
		},
	}

	records, err := c.dbosCtx.kernel.getAllNotifications(c.dbosCtx, req.WorkflowId)
	if err != nil {
		c.logger.Error("Failed to get workflow notifications", "workflow_id", req.WorkflowId, "error", err)
		errStr := fmt.Sprintf("failed to get workflow notifications: %v", err)
		resp.ErrorMessage = &errStr
		return c.sendResponse(resp, string(getWorkflowNotificationsMsg))
	}

	notifs := make([]notificationOutput, 0, len(records))
	for _, r := range records {
		msg, err := c.decodeStoredValueForConductor(r.Message, r.Serialization)
		if err != nil {
			c.logger.Error("Failed to decode notification message", "workflow_id", req.WorkflowId, "error", err)
			errStr := fmt.Sprintf("failed to decode notification: %v", err)
			resp.ErrorMessage = &errStr
			resp.Notifications = nil
			return c.sendResponse(resp, string(getWorkflowNotificationsMsg))
		}
		notifs = append(notifs, notificationOutput{
			Topic:            r.Topic,
			Message:          msg,
			CreatedAtEpochMs: r.CreatedAtEpochMs,
			Consumed:         r.Consumed,
		})
	}
	resp.Notifications = notifs
	return c.sendResponse(resp, string(getWorkflowNotificationsMsg))
}

func (c *conductor) handleGetWorkflowStreamsRequest(data []byte, requestId string) error {
	var req getWorkflowStreamsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get workflow streams request", "error", err)
		return fmt.Errorf("failed to parse get workflow streams request: %w", err)
	}
	c.logger.Debug("Handling get workflow streams request", "workflow_id", req.WorkflowId, "request_id", requestId)

	resp := getWorkflowStreamsConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{Type: getWorkflowStreamsMessage, RequestId: requestId},
		},
	}

	records, err := c.dbosCtx.kernel.getAllStreamEntries(c.dbosCtx, req.WorkflowId)
	if err != nil {
		c.logger.Error("Failed to get workflow streams", "workflow_id", req.WorkflowId, "error", err)
		errStr := fmt.Sprintf("failed to get workflow streams: %v", err)
		resp.ErrorMessage = &errStr
		return c.sendResponse(resp, string(getWorkflowStreamsMessage))
	}

	var streams []streamEntryOutput
	var current *streamEntryOutput
	for _, r := range records {
		value, err := c.decodeStoredValueForConductor(r.Value, r.Serialization)
		if err != nil {
			c.logger.Error("Failed to decode stream value", "workflow_id", req.WorkflowId, "key", r.Key, "error", err)
			errStr := fmt.Sprintf("failed to decode stream %q: %v", r.Key, err)
			resp.ErrorMessage = &errStr
			resp.Streams = nil
			return c.sendResponse(resp, string(getWorkflowStreamsMessage))
		}
		if current == nil || current.Key != r.Key {
			streams = append(streams, streamEntryOutput{Key: r.Key, Values: []string{value}})
			current = &streams[len(streams)-1]
			continue
		}
		current.Values = append(current.Values, value)
	}
	resp.Streams = streams
	return c.sendResponse(resp, string(getWorkflowStreamsMessage))
}

func (c *conductor) handleGetWorkflowAggregatesRequest(data []byte, requestId string) error {
	var req getWorkflowAggregatesConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get workflow aggregates request", "error", err)
		return fmt.Errorf("failed to parse get workflow aggregates request: %w", err)
	}
	c.logger.Debug("Handling get workflow aggregates request", "request_id", requestId)

	input := GetWorkflowAggregatesInput{
		GroupByStatus:             req.Body.GroupByStatus,
		GroupByName:               req.Body.GroupByName,
		GroupByQueueName:          req.Body.GroupByQueueName,
		GroupByExecutorId:         req.Body.GroupByExecutorId,
		GroupByApplicationVersion: req.Body.GroupByApplicationVersion,
		Name:                      req.Body.Name.toSlice(),
		ApplicationVersion:        req.Body.AppVersion.toSlice(),
		ExecutorId:                req.Body.ExecutorId.toSlice(),
		QueueName:                 req.Body.QueueName.toSlice(),
		WorkflowIdPrefix:          req.Body.WorkflowIdPrefix.toSlice(),
	}
	if req.Body.TimeBucketSizeMs != nil {
		input.TimeBucketSize = time.Duration(*req.Body.TimeBucketSizeMs) * time.Millisecond
	}
	if len(req.Body.Status) > 0 {
		statuses := make([]WorkflowStatusType, len(req.Body.Status))
		for i, s := range req.Body.Status {
			statuses[i] = WorkflowStatusType(s)
		}
		input.Status = statuses
	}
	if req.Body.StartTime != nil {
		input.StartTime = *req.Body.StartTime
	}
	if req.Body.EndTime != nil {
		input.EndTime = *req.Body.EndTime
	}

	resp := getWorkflowAggregatesConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{Type: getWorkflowAggregatesMessage, RequestId: requestId},
		},
		Output: []WorkflowAggregateRow{},
	}

	rows, err := c.dbosCtx.GetWorkflowAggregates(input)
	if err != nil {
		c.logger.Error("Failed to get workflow aggregates", "error", err)
		errStr := fmt.Sprintf("failed to get workflow aggregates: %v", err)
		resp.ErrorMessage = &errStr
		return c.sendResponse(resp, string(getWorkflowAggregatesMessage))
	}

	resp.Output = rows
	return c.sendResponse(resp, string(getWorkflowAggregatesMessage))
}

func (c *conductor) handleGetStepAggregatesRequest(data []byte, requestId string) error {
	var req getStepAggregatesConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get step aggregates request", "error", err)
		return fmt.Errorf("failed to parse get step aggregates request: %w", err)
	}
	c.logger.Debug("Handling get step aggregates request", "request_id", requestId)

	input := GetStepAggregatesInput{
		GroupByFunctionName: req.Body.GroupByFunctionName,
		GroupByStatus:       req.Body.GroupByStatus,
		SelectCount:         req.Body.SelectCount,
		SelectMaxDurationMs: req.Body.SelectMaxDurationMs,
		Status:              req.Body.Status.toSlice(),
		FunctionName:        req.Body.FunctionName.toSlice(),
		WorkflowIdPrefix:    req.Body.WorkflowIdPrefix.toSlice(),
	}
	if req.Body.TimeBucketSizeMs != nil {
		input.TimeBucketSize = time.Duration(*req.Body.TimeBucketSizeMs) * time.Millisecond
	}
	if req.Body.CompletedAfter != nil {
		input.CompletedAfter = *req.Body.CompletedAfter
	}
	if req.Body.CompletedBefore != nil {
		input.CompletedBefore = *req.Body.CompletedBefore
	}

	resp := getStepAggregatesConductorResponse{
		baseResponse: baseResponse{
			baseMessage: baseMessage{Type: getStepAggregatesMessage, RequestId: requestId},
		},
		Output: []StepAggregateRow{},
	}

	rows, err := c.dbosCtx.GetStepAggregates(input)
	if err != nil {
		c.logger.Error("Failed to get step aggregates", "error", err)
		errStr := fmt.Sprintf("Exception encountered when getting step aggregates: %v", err)
		resp.ErrorMessage = &errStr
		return c.sendResponse(resp, string(getStepAggregatesMessage))
	}

	resp.Output = rows
	return c.sendResponse(resp, string(getStepAggregatesMessage))
}

func (c *conductor) sendResponse(response any, responseType string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	data, err := json.Marshal(response)
	if err != nil {
		return fmt.Errorf("failed to marshal %s: %w", responseType, err)
	}

	c.logger.Debug("Sending response", "type", responseType, "len", len(data))

	if err := c.conn.SetWriteDeadline(time.Now().Add(_writeDeadline)); err != nil {
		c.logger.Warn("Failed to set write deadline", "type", responseType, "error", err)
	}
	if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		c.logger.Error("Failed to send response", "type", responseType, "error", err)
		return fmt.Errorf("failed to send message: %w", err)
	}
	if err := c.conn.SetWriteDeadline(time.Time{}); err != nil {
		c.logger.Warn("Failed to clear write deadline", "type", responseType, "error", err)
	}

	return nil
}

// When loadContext is true, Context is JSON-encoded into a string; otherwise it is omitted.
func toScheduleConductorOutput(s WorkflowSchedule, loadContext bool) scheduleConductorOutput {
	out := scheduleConductorOutput{
		ScheduleId:        s.ScheduleId,
		ScheduleName:      s.ScheduleName,
		WorkflowName:      s.WorkflowName,
		Schedule:          s.Schedule,
		Status:            string(s.Status),
		AutomaticBackfill: s.AutomaticBackfill,
	}
	if s.WorkflowClassName != "" {
		v := s.WorkflowClassName
		out.WorkflowClassName = &v
	}
	if s.LastFiredAt != nil {
		v := s.LastFiredAt.Format(time.RFC3339Nano)
		out.LastFiredAt = &v
	}
	if s.CronTimezone != "" {
		v := s.CronTimezone
		out.CronTimezone = &v
	}
	if s.QueueName != "" {
		v := s.QueueName
		out.QueueName = &v
	}
	if loadContext && s.Context != nil {
		if b, err := json.Marshal(s.Context); err == nil {
			str := string(b)
			out.Context = &str
		}
	}
	return out
}

func (c *conductor) handleListSchedulesRequest(data []byte, requestId string) error {
	var req listSchedulesConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list schedules request", "error", err)
		return fmt.Errorf("failed to parse list schedules request: %w", err)
	}

	loadContext := true
	if req.Body.LoadContext != nil {
		loadContext = *req.Body.LoadContext
	}

	var opts []ListSchedulesOption
	if len(req.Body.Status) > 0 {
		statuses := make([]ScheduleStatus, len(req.Body.Status))
		for i, s := range req.Body.Status {
			statuses[i] = ScheduleStatus(s)
		}
		opts = append(opts, WithScheduleStatuses(statuses...))
	}
	if len(req.Body.WorkflowName) > 0 {
		opts = append(opts, WithScheduleWorkflowNames(req.Body.WorkflowName.toSlice()...))
	}
	if len(req.Body.ScheduleNamePrefix) > 0 {
		opts = append(opts, WithScheduleNamePrefixes(req.Body.ScheduleNamePrefix.toSlice()...))
	}

	schedules, err := c.dbosCtx.ListSchedules(opts...)
	output := []scheduleConductorOutput{}
	var errorMsg *string
	if err != nil {
		c.logger.Error("Failed to list schedules", "error", err)
		msg := fmt.Sprintf("failed to list schedules: %v", err)
		errorMsg = &msg
	} else {
		output = make([]scheduleConductorOutput, len(schedules))
		for i := range schedules {
			output[i] = toScheduleConductorOutput(schedules[i], loadContext)
		}
	}

	resp := listSchedulesConductorResponse{
		baseResponse: baseResponse{
			baseMessage:  baseMessage{Type: listSchedulesMessage, RequestId: requestId},
			ErrorMessage: errorMsg,
		},
		Output: output,
	}
	return c.sendResponse(resp, string(listSchedulesMessage))
}

func (c *conductor) handleGetScheduleRequest(data []byte, requestId string) error {
	var req getScheduleConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse get schedule request", "error", err)
		return fmt.Errorf("failed to parse get schedule request: %w", err)
	}

	loadContext := true
	if req.LoadContext != nil {
		loadContext = *req.LoadContext
	}

	schedule, err := c.dbosCtx.GetSchedule(req.ScheduleName)
	var errorMsg *string
	var output *scheduleConductorOutput
	if err != nil {
		c.logger.Error("Failed to get schedule", "schedule_name", req.ScheduleName, "error", err)
		msg := fmt.Sprintf("failed to get schedule '%s': %v", req.ScheduleName, err)
		errorMsg = &msg
	} else if schedule != nil {
		o := toScheduleConductorOutput(*schedule, loadContext)
		output = &o
	}

	resp := getScheduleConductorResponse{
		baseResponse: baseResponse{
			baseMessage:  baseMessage{Type: getScheduleMessage, RequestId: requestId},
			ErrorMessage: errorMsg,
		},
		Output: output,
	}
	return c.sendResponse(resp, string(getScheduleMessage))
}

func (c *conductor) handlePauseScheduleRequest(data []byte, requestId string) error {
	var req pauseScheduleConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse pause schedule request", "error", err)
		return fmt.Errorf("failed to parse pause schedule request: %w", err)
	}

	success := true
	var errorMsg *string
	if err := c.dbosCtx.PauseSchedule(req.ScheduleName); err != nil {
		c.logger.Error("Failed to pause schedule", "schedule_name", req.ScheduleName, "error", err)
		msg := fmt.Sprintf("failed to pause schedule '%s': %v", req.ScheduleName, err)
		errorMsg = &msg
		success = false
	}

	resp := pauseScheduleConductorResponse{
		baseResponse: baseResponse{
			baseMessage:  baseMessage{Type: pauseScheduleMessage, RequestId: requestId},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}
	return c.sendResponse(resp, string(pauseScheduleMessage))
}

func (c *conductor) handleResumeScheduleRequest(data []byte, requestId string) error {
	var req resumeScheduleConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse resume schedule request", "error", err)
		return fmt.Errorf("failed to parse resume schedule request: %w", err)
	}

	success := true
	var errorMsg *string
	if err := c.dbosCtx.ResumeSchedule(req.ScheduleName); err != nil {
		c.logger.Error("Failed to resume schedule", "schedule_name", req.ScheduleName, "error", err)
		msg := fmt.Sprintf("failed to resume schedule '%s': %v", req.ScheduleName, err)
		errorMsg = &msg
		success = false
	}

	resp := resumeScheduleConductorResponse{
		baseResponse: baseResponse{
			baseMessage:  baseMessage{Type: resumeScheduleMessage, RequestId: requestId},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}
	return c.sendResponse(resp, string(resumeScheduleMessage))
}

func (c *conductor) handleBackfillScheduleRequest(data []byte, requestId string) error {
	var req backfillScheduleConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse backfill schedule request", "error", err)
		return fmt.Errorf("failed to parse backfill schedule request: %w", err)
	}

	var errorMsg *string
	var workflowIds []string

	start, err := time.Parse(time.RFC3339Nano, req.Start)
	if err != nil {
		start, err = time.Parse(time.RFC3339, req.Start)
	}
	if err != nil {
		msg := fmt.Sprintf("failed to parse start time '%s': %v", req.Start, err)
		errorMsg = &msg
	} else {
		end, errEnd := time.Parse(time.RFC3339Nano, req.End)
		if errEnd != nil {
			end, errEnd = time.Parse(time.RFC3339, req.End)
		}
		if errEnd != nil {
			msg := fmt.Sprintf("failed to parse end time '%s': %v", req.End, errEnd)
			errorMsg = &msg
		} else {
			schedule, errGet := c.dbosCtx.GetSchedule(req.ScheduleName)
			if errGet != nil {
				msg := fmt.Sprintf("failed to get schedule '%s': %v", req.ScheduleName, errGet)
				errorMsg = &msg
			} else if schedule == nil {
				msg := fmt.Sprintf("schedule not found: %s", req.ScheduleName)
				errorMsg = &msg
			} else {
				ids, errBf := c.dbosCtx.kernel.backfillSchedule(c.dbosCtx, backfillScheduleDBInput{
					ScheduleName: req.ScheduleName,
					Schedule:     schedule.Schedule,
					StartTime:    start,
					EndTime:      end,
				})
				if errBf != nil {
					msg := fmt.Sprintf("failed to backfill schedule '%s': %v", req.ScheduleName, errBf)
					errorMsg = &msg
				} else {
					workflowIds = ids
				}
			}
		}
	}

	if workflowIds == nil {
		workflowIds = []string{}
	}
	resp := backfillScheduleConductorResponse{
		baseResponse: baseResponse{
			baseMessage:  baseMessage{Type: backfillScheduleMessage, RequestId: requestId},
			ErrorMessage: errorMsg,
		},
		WorkflowIds: workflowIds,
	}
	return c.sendResponse(resp, string(backfillScheduleMessage))
}

func (c *conductor) handleTriggerScheduleRequest(data []byte, requestId string) error {
	var req triggerScheduleConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse trigger schedule request", "error", err)
		return fmt.Errorf("failed to parse trigger schedule request: %w", err)
	}

	var errorMsg *string
	var workflowId *string
	id, err := c.dbosCtx.kernel.triggerSchedule(c.dbosCtx, req.ScheduleName)
	if err != nil {
		c.logger.Error("Failed to trigger schedule", "schedule_name", req.ScheduleName, "error", err)
		msg := fmt.Sprintf("failed to trigger schedule '%s': %v", req.ScheduleName, err)
		errorMsg = &msg
	} else {
		workflowId = &id
	}

	resp := triggerScheduleConductorResponse{
		baseResponse: baseResponse{
			baseMessage:  baseMessage{Type: triggerScheduleMessage, RequestId: requestId},
			ErrorMessage: errorMsg,
		},
		WorkflowId: workflowId,
	}
	return c.sendResponse(resp, string(triggerScheduleMessage))
}

func (c *conductor) handleListApplicationVersionsRequest(data []byte, requestId string) error {
	var req listApplicationVersionsConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse list application versions request", "error", err)
		return fmt.Errorf("failed to parse list application versions request: %w", err)
	}

	var errorMsg *string
	output := []applicationVersionOutput{}
	versions, err := retryWithResult(c.dbosCtx, func() ([]VersionInfo, error) {
		return c.dbosCtx.kernel.listApplicationVersions(c.dbosCtx)
	}, withRetrierLogger(c.logger))
	if err != nil {
		c.logger.Error("Failed to list application versions", "error", err)
		msg := fmt.Sprintf("failed to list application versions: %v", err)
		errorMsg = &msg
	} else {
		for _, v := range versions {
			output = append(output, formatApplicationVersionOutput(v))
		}
	}

	resp := listApplicationVersionsConductorResponse{
		baseResponse: baseResponse{
			baseMessage:  baseMessage{Type: listAppVersionsMessage, RequestId: requestId},
			ErrorMessage: errorMsg,
		},
		Output: output,
	}
	return c.sendResponse(resp, string(listAppVersionsMessage))
}

func (c *conductor) handleSetLatestApplicationVersionRequest(data []byte, requestId string) error {
	var req setLatestApplicationVersionConductorRequest
	if err := json.Unmarshal(data, &req); err != nil {
		c.logger.Error("Failed to parse set latest application version request", "error", err)
		return fmt.Errorf("failed to parse set latest application version request: %w", err)
	}

	success := true
	var errorMsg *string
	if err := retry(c.dbosCtx, func() error {
		return c.dbosCtx.kernel.updateApplicationVersionTimestamp(c.dbosCtx, req.VersionName, time.Now().UnixMilli())
	}, withRetrierLogger(c.logger)); err != nil {
		c.logger.Error("Failed to set latest application version", "version_name", req.VersionName, "error", err)
		msg := fmt.Sprintf("failed to set latest application version '%s': %v", req.VersionName, err)
		errorMsg = &msg
		success = false
	}

	resp := setLatestApplicationVersionConductorResponse{
		baseResponse: baseResponse{
			baseMessage:  baseMessage{Type: setLatestAppVersionMessage, RequestId: requestId},
			ErrorMessage: errorMsg,
		},
		Success: success,
	}
	return c.sendResponse(resp, string(setLatestAppVersionMessage))
}
