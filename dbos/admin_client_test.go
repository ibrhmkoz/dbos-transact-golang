package dbos

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminClientListWorkflows(t *testing.T) {
	startTime := time.Date(2026, time.June, 8, 12, 0, 0, 123456789, time.UTC)
	limit := 10

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/workflows", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		var request AdminListWorkflowsRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.NotNil(t, request.StartTime)
		assert.Equal(t, startTime, *request.StartTime)
		require.NotNil(t, request.Limit)
		assert.Equal(t, limit, *request.Limit)

		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`[{
			"WorkflowUUID":"workflow-id",
			"Status":"SUCCESS",
			"WorkflowName":"example.workflow",
			"CreatedAt":"1780920000123",
			"UpdatedAt":"1780920000456",
			"WorkflowDeadlineEpochMS":null,
			"StartedAt":"1780920000234"
		}]`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	client, err := NewAdminClient(server.URL)
	require.NoError(t, err)

	workflows, err := client.ListWorkflows(context.Background(), AdminListWorkflowsRequest{
		StartTime: &startTime,
		Limit:     &limit,
	})
	require.NoError(t, err)
	require.Len(t, workflows, 1)
	assert.Equal(t, "workflow-id", workflows[0].WorkflowUuid)
	assert.Equal(t, WorkflowStatusSuccess, workflows[0].Status)
	assert.Equal(t, AdminEpochMillis(1780920000123), workflows[0].CreatedAt)
	assert.Equal(t, time.UnixMilli(1780920000123), workflows[0].CreatedAt.Time())
	assert.Zero(t, workflows[0].WorkflowDeadlineEpochMS)
}

func TestAdminClientError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "workflow not found", http.StatusNotFound)
	}))
	t.Cleanup(server.Close)

	client, err := NewAdminClient(server.URL)
	require.NoError(t, err)

	_, err = client.GetWorkflow(context.Background(), "missing")
	require.Error(t, err)

	var adminErr *AdminClientError
	require.True(t, errors.As(err, &adminErr))
	assert.Equal(t, http.StatusNotFound, adminErr.StatusCode)
	assert.Equal(t, "workflow not found", adminErr.Body)
}

func TestNewAdminClientValidation(t *testing.T) {
	_, err := NewAdminClient("")
	require.EqualError(t, err, "admin client base URL is required")

	_, err = NewAdminClient("localhost:3001")
	require.EqualError(t, err, "admin client base URL must include scheme and host")

}

func TestNewAdminClientDefaults(t *testing.T) {
	client, err := NewAdminClient("http://localhost:3001")
	require.NoError(t, err)
	assert.Equal(t, defaultAdminClientTimeout, client.httpClient.Timeout)
}
