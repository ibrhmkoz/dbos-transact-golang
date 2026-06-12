package dbos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultAdminClientTimeout = 5 * time.Second

type AdminClient struct {
	baseUrl    *url.URL
	httpClient *http.Client
}

type AdminClientError struct {
	StatusCode int
	Body       string
}

func (e *AdminClientError) Error() string {
	return fmt.Sprintf("admin API returned HTTP %d: %s", e.StatusCode, e.Body)
}

type AdminEpochMillis int64

func (m *AdminEpochMillis) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		*m = 0
		return nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("admin epoch milliseconds must be a JSON string or null: %w", err)
	}
	millis, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid admin epoch milliseconds %q: %w", value, err)
	}
	*m = AdminEpochMillis(millis)
	return nil
}

func (m AdminEpochMillis) Time() time.Time {
	return time.UnixMilli(int64(m))
}

type AdminHealthResponse struct {
	Status string `json:"status"`
}

type AdminListWorkflowsRequest struct {
	WorkflowUuids      []string           `json:"workflow_uuids,omitempty"`
	AuthenticatedUser  *string            `json:"authenticated_user,omitempty"`
	StartTime          *time.Time         `json:"start_time,omitempty"`
	EndTime            *time.Time         `json:"end_time,omitempty"`
	Status             WorkflowStatusType `json:"status,omitempty"`
	ApplicationVersion *string            `json:"application_version,omitempty"`
	WorkflowName       *string            `json:"workflow_name,omitempty"`
	Limit              *int               `json:"limit,omitempty"`
	Offset             *int               `json:"offset,omitempty"`
	SortDesc           *bool              `json:"sort_desc,omitempty"`
	WorkflowIdPrefix   *string            `json:"workflow_id_prefix,omitempty"`
	LoadInput          *bool              `json:"load_input,omitempty"`
	LoadOutput         *bool              `json:"load_output,omitempty"`
	QueueName          *string            `json:"queue_name,omitempty"`
}

type AdminWorkflow struct {
	WorkflowUuid            string             `json:"WorkflowUUID"`
	Status                  WorkflowStatusType `json:"Status"`
	WorkflowName            string             `json:"WorkflowName"`
	AuthenticatedUser       string             `json:"AuthenticatedUser"`
	AssumedRole             string             `json:"AssumedRole"`
	AuthenticatedRoles      []string           `json:"AuthenticatedRoles"`
	Output                  string             `json:"Output"`
	Error                   string             `json:"Error"`
	ExecutorId              string             `json:"ExecutorID"`
	ApplicationVersion      string             `json:"ApplicationVersion"`
	ApplicationId           string             `json:"ApplicationID"`
	Attempts                int                `json:"Attempts"`
	QueueName               string             `json:"QueueName"`
	Timeout                 time.Duration      `json:"Timeout"`
	DeduplicationId         string             `json:"DeduplicationID"`
	Priority                int                `json:"Priority"`
	QueuePartitionKey       string             `json:"QueuePartitionKey"`
	Input                   string             `json:"Input"`
	CreatedAt               AdminEpochMillis   `json:"CreatedAt"`
	UpdatedAt               AdminEpochMillis   `json:"UpdatedAt"`
	WorkflowDeadlineEpochMS AdminEpochMillis   `json:"WorkflowDeadlineEpochMS"`
	StartedAt               AdminEpochMillis   `json:"StartedAt"`
}

type AdminWorkflowStep struct {
	FunctionId         int    `json:"function_id"`
	FunctionName       string `json:"function_name"`
	StartedAtEpochMS   int64  `json:"started_at_epoch_ms"`
	CompletedAtEpochMS int64  `json:"completed_at_epoch_ms"`
	Output             string `json:"output"`
	Error              string `json:"error"`
}

type AdminForkWorkflowRequest struct {
	StartStep          *uint   `json:"start_step,omitempty"`
	NewWorkflowId      *string `json:"new_workflow_id,omitempty"`
	ApplicationVersion *string `json:"application_version,omitempty"`
}

type AdminForkWorkflowResponse struct {
	WorkflowId string `json:"workflow_id"`
}

type AdminGarbageCollectRequest struct {
	CutoffEpochTimestampMS *int64 `json:"cutoff_epoch_timestamp_ms,omitempty"`
	RowsThreshold          *int   `json:"rows_threshold,omitempty"`
}

type AdminGlobalTimeoutRequest struct {
	CutoffEpochTimestampMS int64 `json:"cutoff_epoch_timestamp_ms"`
}

func NewAdminClient(rawBaseUrl string) (*AdminClient, error) {
	if rawBaseUrl == "" {
		return nil, errors.New("admin client base URL is required")
	}
	baseUrl, err := url.Parse(rawBaseUrl)
	if err != nil {
		return nil, fmt.Errorf("invalid admin client base URL: %w", err)
	}
	if baseUrl.Scheme == "" || baseUrl.Host == "" {
		return nil, errors.New("admin client base URL must include scheme and host")
	}

	return &AdminClient{
		baseUrl:    baseUrl,
		httpClient: &http.Client{Timeout: defaultAdminClientTimeout},
	}, nil
}

func (c *AdminClient) Health(ctx context.Context) (AdminHealthResponse, error) {
	return doAdminRequest[AdminHealthResponse](c, ctx, http.MethodGet, "/dbos-healthz", nil)
}

func (c *AdminClient) RecoverWorkflows(ctx context.Context, executorIds []string) ([]string, error) {
	return doAdminRequest[[]string](c, ctx, http.MethodPost, "/dbos-workflow-recovery", executorIds)
}

func (c *AdminClient) GarbageCollect(ctx context.Context, request AdminGarbageCollectRequest) error {
	return doAdminRequestWithoutResponse(c, ctx, http.MethodPost, "/dbos-garbage-collect", request)
}

func (c *AdminClient) GlobalTimeout(ctx context.Context, cutoffTime time.Time) error {
	return doAdminRequestWithoutResponse(c, ctx, http.MethodPost, "/dbos-global-timeout", AdminGlobalTimeoutRequest{
		CutoffEpochTimestampMS: cutoffTime.UnixMilli(),
	})
}

func (c *AdminClient) ListWorkflows(ctx context.Context, request AdminListWorkflowsRequest) ([]AdminWorkflow, error) {
	return doAdminRequest[[]AdminWorkflow](c, ctx, http.MethodPost, "/workflows", request)
}

func (c *AdminClient) GetWorkflow(ctx context.Context, workflowId string) (AdminWorkflow, error) {
	return doAdminRequest[AdminWorkflow](c, ctx, http.MethodGet, "/workflows/"+url.PathEscape(workflowId), nil)
}

func (c *AdminClient) GetWorkflowSteps(ctx context.Context, workflowId string) ([]AdminWorkflowStep, error) {
	return doAdminRequest[[]AdminWorkflowStep](c, ctx, http.MethodGet, "/workflows/"+url.PathEscape(workflowId)+"/steps", nil)
}

func (c *AdminClient) CancelWorkflow(ctx context.Context, workflowId string) error {
	return doAdminRequestWithoutResponse(c, ctx, http.MethodPost, "/workflows/"+url.PathEscape(workflowId)+"/cancel", nil)
}

func (c *AdminClient) ResumeWorkflow(ctx context.Context, workflowId string) error {
	return doAdminRequestWithoutResponse(c, ctx, http.MethodPost, "/workflows/"+url.PathEscape(workflowId)+"/resume", nil)
}

func (c *AdminClient) ForkWorkflow(ctx context.Context, workflowId string, request AdminForkWorkflowRequest) (AdminForkWorkflowResponse, error) {
	return doAdminRequest[AdminForkWorkflowResponse](c, ctx, http.MethodPost, "/workflows/"+url.PathEscape(workflowId)+"/fork", request)
}

func (c *AdminClient) Deactivate(ctx context.Context) error {
	return doAdminRequestWithoutResponse(c, ctx, http.MethodGet, "/deactivate", nil)
}

func doAdminRequest[T any](client *AdminClient, ctx context.Context, method, path string, body any) (T, error) {
	var result T
	response, err := client.do(ctx, method, path, body)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return result, fmt.Errorf("decoding admin API response: %w", err)
	}
	return result, nil
}

func doAdminRequestWithoutResponse(client *AdminClient, ctx context.Context, method, path string, body any) error {
	response, err := client.do(ctx, method, path, body)
	if err != nil {
		return err
	}
	return response.Body.Close()
}

func (c *AdminClient) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var requestBody io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encoding admin API request: %w", err)
		}
		requestBody = bytes.NewReader(encoded)
	}

	requestUrl := c.baseUrl.JoinPath(strings.TrimPrefix(path, "/"))
	request, err := http.NewRequestWithContext(ctx, method, requestUrl.String(), requestBody)
	if err != nil {
		return nil, fmt.Errorf("creating admin API request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("calling admin API: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		defer response.Body.Close()
		responseBody, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			return nil, fmt.Errorf("reading admin API error response: %w", readErr)
		}
		return nil, &AdminClientError{StatusCode: response.StatusCode, Body: strings.TrimSpace(string(responseBody))}
	}
	return response, nil
}
