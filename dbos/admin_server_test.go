package dbos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStepResult is a custom struct for testing step outputs
type TestStepResult struct {
	Message string `json:"message"`
	Count   int    `json:"count"`
	Success bool   `json:"success"`
}

func TestAdminServer(t *testing.T) {
	defer verifyNoLeaks(t)

	t.Run("Admin server is not started by default", func(t *testing.T) {
		databaseURL := backendDatabaseURL(t)
		resetTestDatabase(t, databaseURL)
		ctx, err := NewDBOSContext(context.Background(), Config{
			DatabaseURL: databaseURL,
			AppName:     "test-app",
		})
		require.NoError(t, err)

		err = Launch(ctx)
		require.NoError(t, err)
		// Ensure cleanup
		defer func() {
			if ctx != nil {
				Shutdown(ctx, 1*time.Minute)
			}
		}()

		// Verify admin server is not running
		client := &http.Client{Timeout: 1 * time.Second}
		_, err = client.Get(fmt.Sprintf("http://localhost:%d/%s", _DEFAULT_ADMIN_SERVER_PORT, strings.TrimPrefix(_HEALTHCHECK_PATTERN, "GET /")))
		require.Error(t, err, "Expected request to fail when admin server is not started")

		// Verify the DBOS executor doesn't have an admin server instance
		require.NotNil(t, ctx, "Expected DBOS instance to be created")

		exec, ok := ctx.(*dbosContext)
		require.True(t, ok, "Expected ctx to be of type *dbosContext")
		require.Nil(t, exec.adminServer, "Expected admin server to be nil when not configured")
	})

	t.Run("Admin server endpoints", func(t *testing.T) {
		databaseURL := backendDatabaseURL(t)
		resetTestDatabase(t, databaseURL)
		// Launch DBOS with admin server once for all endpoint tests
		ctx, err := NewDBOSContext(context.Background(), Config{
			DatabaseURL:     databaseURL,
			AppName:         "test-app",
			AdminServer:     true,
			AdminServerPort: _DEFAULT_ADMIN_SERVER_PORT,
		})
		require.NoError(t, err)

		err = Launch(ctx)
		require.NoError(t, err)

		// Ensure cleanup
		defer func() {
			if ctx != nil {
				Shutdown(ctx, 1*time.Minute)
			}
		}()

		// Give the server a moment to start
		time.Sleep(100 * time.Millisecond)

		// Verify the DBOS executor has an admin server instance
		require.NotNil(t, ctx, "Expected DBOS instance to be created")

		exec := ctx.(*dbosContext)
		require.NotNil(t, exec.adminServer, "Expected admin server to be created in DBOS instance")

		client := &http.Client{Timeout: 5 * time.Second}

		type adminServerTestCase struct {
			name           string
			method         string
			endpoint       string
			body           io.Reader
			contentType    string
			expectedStatus int
			validateResp   func(t *testing.T, resp *http.Response)
		}

		tests := []adminServerTestCase{
			{
				name:           "Health endpoint responds correctly",
				method:         "GET",
				endpoint:       fmt.Sprintf("http://localhost:%d/%s", _DEFAULT_ADMIN_SERVER_PORT, strings.TrimPrefix(_HEALTHCHECK_PATTERN, "GET /")),
				expectedStatus: http.StatusOK,
			},
			{
				name:           "Recovery endpoint responds correctly with valid JSON",
				method:         "POST",
				endpoint:       fmt.Sprintf("http://localhost:%d/%s", _DEFAULT_ADMIN_SERVER_PORT, strings.TrimPrefix(_WORKFLOW_RECOVERY_PATTERN, "POST /")),
				body:           bytes.NewBuffer(mustMarshal([]string{"executor1", "executor2"})),
				contentType:    "application/json",
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var workflowIDs []string
					err := json.NewDecoder(resp.Body).Decode(&workflowIDs)
					require.NoError(t, err, "Failed to decode response as JSON array")
					assert.NotNil(t, workflowIDs, "Expected non-nil workflow IDs array")
				},
			},
			{
				name:           "Recovery endpoint rejects invalid JSON",
				method:         "POST",
				endpoint:       fmt.Sprintf("http://localhost:%d/%s", _DEFAULT_ADMIN_SERVER_PORT, strings.TrimPrefix(_WORKFLOW_RECOVERY_PATTERN, "POST /")),
				body:           strings.NewReader(`{"invalid": json}`),
				contentType:    "application/json",
				expectedStatus: http.StatusBadRequest,
			},
			{
				name:     "Workflows endpoint accepts all filters without error",
				method:   "POST",
				endpoint: fmt.Sprintf("http://localhost:%d/%s", _DEFAULT_ADMIN_SERVER_PORT, strings.TrimPrefix(_WORKFLOWS_PATTERN, "POST /")),
				body: bytes.NewBuffer(mustMarshal(map[string]any{
					"workflow_uuids":      []string{"test-id-1", "test-id-2"},
					"authenticated_user":  "test-user",
					"start_time":          time.Now().Add(-24 * time.Hour).Format(time.RFC3339Nano),
					"end_time":            time.Now().Format(time.RFC3339Nano),
					"status":              "PENDING",
					"application_version": "v1.0.0",
					"workflow_name":       "testWorkflow",
					"limit":               100,
					"offset":              0,
					"sort_desc":           true,
					"workflow_id_prefix":  "test-",
					"load_input":          true,
					"load_output":         true,
					"queue_name":          "test-queue",
				})),
				contentType:    "application/json",
				expectedStatus: http.StatusOK,
				validateResp: func(t *testing.T, resp *http.Response) {
					var workflows []map[string]any
					err := json.NewDecoder(resp.Body).Decode(&workflows)
					require.NoError(t, err, "Failed to decode workflows response")
					// We expect an empty array -- there's no workflow in the db
					assert.NotNil(t, workflows, "Expected non-nil workflows array")
					assert.Empty(t, workflows, "Expected empty workflows array")
				},
			},
			{
				name:           "Get single workflow returns 404 for non-existent workflow",
				method:         "GET",
				endpoint:       fmt.Sprintf("http://localhost:%d/workflow/non-existent-workflow-id", _DEFAULT_ADMIN_SERVER_PORT),
				expectedStatus: http.StatusNotFound,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var req *http.Request
				var err error

				if tt.body != nil {
					req, err = http.NewRequest(tt.method, tt.endpoint, tt.body)
				} else {
					req, err = http.NewRequest(tt.method, tt.endpoint, nil)
				}
				require.NoError(t, err, "Failed to create request")

				if tt.contentType != "" {
					req.Header.Set("Content-Type", tt.contentType)
				}

				resp, err := client.Do(req)
				require.NoError(t, err, "Failed to make request")
				defer resp.Body.Close()

				assert.Equal(t, tt.expectedStatus, resp.StatusCode)

				if tt.validateResp != nil {
					tt.validateResp(t, resp)
				}
			})
		}
	})

	t.Run("List workflows input/output values", func(t *testing.T) {
		databaseURL := backendDatabaseURL(t)
		resetTestDatabase(t, databaseURL)
		ctx, err := NewDBOSContext(context.Background(), Config{
			DatabaseURL:     databaseURL,
			AppName:         "test-app",
			AdminServer:     true,
			AdminServerPort: _DEFAULT_ADMIN_SERVER_PORT,
		})
		require.NoError(t, err)

		// Define a custom struct for testing
		type TestStruct struct {
			Name  string `json:"name"`
			Value int    `json:"value"`
		}

		// Test workflow with int input/output
		intWorkflow := func(dbosCtx DBOSContext, input int) (int, error) {
			return input * 2, nil
		}
		intWF := NewWorkflow(ctx, intWorkflow)

		// Test workflow with empty string input/output
		emptyStringWorkflow := func(dbosCtx DBOSContext, input string) (string, error) {
			return "", nil
		}
		emptyStringWF := NewWorkflow(ctx, emptyStringWorkflow)

		// Test workflow with struct input/output
		structWorkflow := func(dbosCtx DBOSContext, input TestStruct) (TestStruct, error) {
			return TestStruct{Name: "output-" + input.Name, Value: input.Value * 2}, nil
		}
		structWF := NewWorkflow(ctx, structWorkflow)

		err = Launch(ctx)
		require.NoError(t, err)

		// Ensure cleanup
		defer func() {
			if ctx != nil {
				Shutdown(ctx, 1*time.Minute)
			}
		}()

		// Give the server a moment to start
		time.Sleep(100 * time.Millisecond)

		client := &http.Client{Timeout: 5 * time.Second}
		endpoint := fmt.Sprintf("http://localhost:%d/%s", _DEFAULT_ADMIN_SERVER_PORT, strings.TrimPrefix(_WORKFLOWS_PATTERN, "POST /"))

		// Create workflows with different input/output types
		// 1. Integer workflow
		intHandle, err := intWF(ctx, 42)
		require.NoError(t, err, "Failed to create int workflow")
		intResult, err := intHandle.GetResult()
		require.NoError(t, err, "Failed to get int workflow result")
		assert.Equal(t, 84, intResult)

		// 2. Empty string workflow
		emptyStringHandle, err := emptyStringWF(ctx, "")
		require.NoError(t, err, "Failed to create empty string workflow")
		emptyStringResult, err := emptyStringHandle.GetResult()
		require.NoError(t, err, "Failed to get empty string workflow result")
		assert.Equal(t, "", emptyStringResult)

		// 3. Struct workflow
		structInput := TestStruct{Name: "test", Value: 10}
		structHandle, err := structWF(ctx, structInput)
		require.NoError(t, err, "Failed to create struct workflow")
		structResult, err := structHandle.GetResult()
		require.NoError(t, err, "Failed to get struct workflow result")
		assert.Equal(t, TestStruct{Name: "output-test", Value: 20}, structResult)

		// Query workflows with input/output loading enabled
		// Filter by the workflow IDs we just created to avoid interference from other tests
		reqBody := map[string]any{
			"workflow_uuids": []string{
				intHandle.GetWorkflowID(),
				emptyStringHandle.GetWorkflowID(),
				structHandle.GetWorkflowID(),
			},
			"load_input":  true,
			"load_output": true,
			"limit":       10,
		}
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBuffer(mustMarshal(reqBody)))
		require.NoError(t, err, "Failed to create request")
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		require.NoError(t, err, "Failed to make request")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var workflows []map[string]any
		err = json.NewDecoder(resp.Body).Decode(&workflows)
		require.NoError(t, err, "Failed to decode workflows response")

		// Should have exactly 3 workflows
		assert.Equal(t, 3, len(workflows), "Expected exactly 3 workflows")

		// Verify each workflow's input/output marshalling
		for _, wf := range workflows {
			wfID := wf["WorkflowUUID"].(string)

			// Check input and output fields exist and are strings (JSON marshaled)
			if wfID == intHandle.GetWorkflowID() {
				// Integer workflow: input and output should be marshaled as JSON strings
				inputStr, ok := wf["Input"].(string)
				require.True(t, ok, "Int workflow Input should be a string")
				assert.Equal(t, "42", inputStr, "Int workflow input should be marshaled as '42'")

				outputStr, ok := wf["Output"].(string)
				require.True(t, ok, "Int workflow Output should be a string")
				assert.Equal(t, "84", outputStr, "Int workflow output should be marshaled as '84'")

			} else if wfID == emptyStringHandle.GetWorkflowID() {
				// Empty string workflow: both input and output are empty strings
				// According to the logic, empty strings should not have Input/Output fields
				input, hasInput := wf["Input"]
				require.Equal(t, "\"\"", input)
				require.True(t, hasInput, "Empty string workflow should have Input field")

				output, hasOutput := wf["Output"]
				require.True(t, hasOutput, "Empty string workflow should have Output field")
				require.Equal(t, "\"\"", output)

			} else if wfID == structHandle.GetWorkflowID() {
				// Struct workflow: input and output should be marshaled as JSON strings
				inputStr, ok := wf["Input"].(string)
				require.True(t, ok, "Struct workflow Input should be a string")
				var inputStruct TestStruct
				err = json.Unmarshal([]byte(inputStr), &inputStruct)
				require.NoError(t, err, "Failed to unmarshal struct workflow input")
				assert.Equal(t, structInput, inputStruct, "Struct workflow input should match")

				outputStr, ok := wf["Output"].(string)
				require.True(t, ok, "Struct workflow Output should be a string")
				var outputStruct TestStruct
				err = json.Unmarshal([]byte(outputStr), &outputStruct)
				require.NoError(t, err, "Failed to unmarshal struct workflow output")
				assert.Equal(t, TestStruct{Name: "output-test", Value: 20}, outputStruct, "Struct workflow output should match")
			}
		}
	})

	t.Run("List endpoints time filtering", func(t *testing.T) {
		databaseURL := backendDatabaseURL(t)
		resetTestDatabase(t, databaseURL)
		ctx, err := NewDBOSContext(context.Background(), Config{
			DatabaseURL:     databaseURL,
			AppName:         "test-app",
			AdminServer:     true,
			AdminServerPort: _DEFAULT_ADMIN_SERVER_PORT,
		})
		require.NoError(t, err)

		testWorkflow := func(dbosCtx DBOSContext, input string) (string, error) {
			return "result-" + input, nil
		}
		testWF := NewWorkflow(ctx, testWorkflow)

		err = Launch(ctx)
		require.NoError(t, err)

		// Ensure cleanup
		defer func() {
			if ctx != nil {
				Shutdown(ctx, 1*time.Minute)
			}
		}()

		client, err := NewAdminClient(fmt.Sprintf("http://localhost:%d", _DEFAULT_ADMIN_SERVER_PORT))
		require.NoError(t, err)

		workflowIDs := make([]string, 5)
		for i := range workflowIDs {
			input := fmt.Sprintf("workflow-%d", i)
			handle, err := testWF(ctx, input)
			require.NoError(t, err)
			result, err := handle.GetResult()
			require.NoError(t, err)
			assert.Equal(t, "result-"+input, result)
			workflowIDs[i] = handle.GetWorkflowID()
			if i < len(workflowIDs)-1 {
				time.Sleep(2 * time.Millisecond)
			}
		}

		allWorkflows, err := client.ListWorkflows(context.Background(), AdminListWorkflowsRequest{
			WorkflowUUIDs: workflowIDs,
		})
		require.NoError(t, err)
		require.Len(t, allWorkflows, len(workflowIDs))

		for i := 1; i < len(allWorkflows); i++ {
			require.Less(t, allWorkflows[i-1].CreatedAt, allWorkflows[i].CreatedAt)
		}

		thirdCreatedAt := allWorkflows[2].CreatedAt.Time()
		afterThird := thirdCreatedAt.Add(time.Millisecond)

		firstThree, err := client.ListWorkflows(context.Background(), AdminListWorkflowsRequest{
			WorkflowUUIDs: workflowIDs,
			EndTime:       &thirdCreatedAt,
		})
		require.NoError(t, err)
		require.Len(t, firstThree, 3)

		lastTwo, err := client.ListWorkflows(context.Background(), AdminListWorkflowsRequest{
			WorkflowUUIDs: workflowIDs,
			StartTime:     &afterThird,
		})
		require.NoError(t, err)
		require.Len(t, lastTwo, 2)

		filteredIDs := make([]string, 0, len(workflowIDs))
		for _, workflow := range append(firstThree, lastTwo...) {
			filteredIDs = append(filteredIDs, workflow.WorkflowUUID)
		}
		assert.ElementsMatch(t, workflowIDs, filteredIDs)
	})

	t.Run("WorkflowSteps", func(t *testing.T) {
		databaseURL := backendDatabaseURL(t)
		resetTestDatabase(t, databaseURL)
		ctx, err := NewDBOSContext(context.Background(), Config{
			DatabaseURL: databaseURL,
			AppName:     "test-app",
			AdminServer: true,
		})
		require.NoError(t, err)

		// Test workflow with multiple steps - simpler version that won't fail on serialization
		testWorkflow := func(dbosCtx DBOSContext, input string) (string, error) {
			// Step 1: Return a string
			stepResult1, err := Run(dbosCtx, func(ctx context.Context) (string, error) {
				return "step1-output", nil
			}, WithStepName("stringStep"))
			if err != nil {
				return "", err
			}

			// Step 2: Return a user-defined struct
			stepResult2, err := Run(dbosCtx, func(ctx context.Context) (TestStepResult, error) {
				return TestStepResult{
					Message: "structured data",
					Count:   100,
					Success: true,
				}, nil
			}, WithStepName("structStep"))
			if err != nil {
				return "", err
			}

			// Step 3: Return an error - but we don't abort on error to test error marshaling
			_, _ = Run(dbosCtx, func(ctx context.Context) (string, error) {
				return "", fmt.Errorf("deliberate error for testing")
			}, WithStepName("errorStep"))

			// Step 4: Return empty string (to test empty value handling)
			stepResult4, err := Run(dbosCtx, func(ctx context.Context) (string, error) {
				return "", nil
			}, WithStepName("emptyStep"))
			if err != nil {
				return "", err
			}

			// Combine results
			return fmt.Sprintf("workflow complete: %s, struct(%s,%d,%v), %s", stepResult1, stepResult2.Message, stepResult2.Count, stepResult2.Success, stepResult4), nil
		}

		testWF := NewWorkflow(ctx, testWorkflow)

		err = Launch(ctx)
		require.NoError(t, err)

		// Ensure cleanup
		defer func() {
			if ctx != nil {
				Shutdown(ctx, 1*time.Minute)
			}
		}()

		// Give the server a moment to start
		time.Sleep(100 * time.Millisecond)

		client := &http.Client{Timeout: 5 * time.Second}

		// Create and run the workflow
		handle, err := testWF(ctx, "test-input")
		require.NoError(t, err, "Failed to create workflow")

		// Wait for workflow to complete
		result, err := handle.GetResult()
		require.NoError(t, err, "Workflow should complete successfully")
		t.Logf("Workflow result: %s", result)

		// Call the workflow steps endpoint
		workflowID := handle.GetWorkflowID()
		endpoint := fmt.Sprintf("http://localhost:%d/workflows/%s/steps", _DEFAULT_ADMIN_SERVER_PORT, workflowID)
		req, err := http.NewRequest("GET", endpoint, nil)
		require.NoError(t, err, "Failed to create request")

		resp, err := client.Do(req)
		require.NoError(t, err, "Failed to make request")
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK from steps endpoint")

		// Decode the response
		var steps []map[string]any
		err = json.NewDecoder(resp.Body).Decode(&steps)
		require.NoError(t, err, "Failed to decode steps response")

		// Should have 4 steps
		assert.Equal(t, 4, len(steps), "Expected exactly 4 steps")

		// Verify each step's output/error is properly marshaled
		for i, step := range steps {
			functionName, ok := step["function_name"].(string)
			require.True(t, ok, "function_name should be a string for step %d", i)

			// Verify timestamps are present
			_, hasStartedAt := step["started_at_epoch_ms"]
			assert.True(t, hasStartedAt, "Step %d should have started_at_epoch_ms field", i)
			_, hasCompletedAt := step["completed_at_epoch_ms"]
			assert.True(t, hasCompletedAt, "Step %d should have completed_at_epoch_ms field", i)

			t.Logf("Step %d (%s): output=%v, error=%v", i, functionName, step["output"], step["error"])

			switch functionName {
			case "stringStep":
				// String output should be marshaled as JSON string
				outputStr, ok := step["output"].(string)
				require.True(t, ok, "String step output should be a JSON string")

				var unmarshaledOutput string
				err = json.Unmarshal([]byte(outputStr), &unmarshaledOutput)
				require.NoError(t, err, "Failed to unmarshal string step output")
				assert.Equal(t, "step1-output", unmarshaledOutput, "String step output should match")

				assert.Nil(t, step["error"], "String step should have no error")

			case "structStep":
				// Struct output should be marshaled as JSON string
				outputStr, ok := step["output"].(string)
				require.True(t, ok, "Struct step output should be a JSON string")

				var unmarshaledOutput TestStepResult
				err = json.Unmarshal([]byte(outputStr), &unmarshaledOutput)
				require.NoError(t, err, "Failed to unmarshal struct step output")
				assert.Equal(t, TestStepResult{
					Message: "structured data",
					Count:   100,
					Success: true,
				}, unmarshaledOutput, "Struct step output should match")

				assert.Nil(t, step["error"], "Struct step should have no error")

			case "errorStep":
				// Error step should have error marshaled as JSON string
				errorStr, ok := step["error"].(string)
				require.True(t, ok, "Error step error should be a JSON string")

				var unmarshaledError string
				err = json.Unmarshal([]byte(errorStr), &unmarshaledError)
				require.NoError(t, err, "Failed to unmarshal error step error")
				assert.Contains(t, unmarshaledError, "deliberate error for testing", "Error message should be preserved")

			case "emptyStep":
				// Empty string is returned as an empty JSON string
				output := step["output"]
				require.Equal(t, "\"\"", output, "Empty step output should be an empty string")
				assert.Nil(t, step["error"], "Empty step should have no error")
			}
		}
	})

	t.Run("TestDeactivate", func(t *testing.T) {
		databaseURL := backendDatabaseURL(t)
		resetTestDatabase(t, databaseURL)
		ctx, err := NewDBOSContext(context.Background(), Config{
			DatabaseURL:     databaseURL,
			AppName:         "test-app",
			AdminServer:     true,
			AdminServerPort: _DEFAULT_ADMIN_SERVER_PORT,
		})
		require.NoError(t, err)

		// Track scheduled workflow executions
		var executionCount atomic.Int32

		// Register a scheduled workflow that runs every second
		NewWorkflow(ctx, func(dbosCtx DBOSContext, scheduledTime time.Time) (string, error) {
			executionCount.Add(1)
			return fmt.Sprintf("executed at %v", scheduledTime), nil
		}, WithSchedule("* * * * * *")) // Every second

		err = Launch(ctx)
		require.NoError(t, err)

		client := &http.Client{Timeout: 5 * time.Second}

		// Ensure cleanup
		defer func() {
			if ctx != nil {
				Shutdown(ctx, 1*time.Minute)
			}
			if client.Transport != nil {
				client.Transport.(*http.Transport).CloseIdleConnections()
			}
		}()

		// Wait for 2-3 executions to verify scheduler is running
		require.Eventually(t, func() bool {
			return executionCount.Load() >= 2
		}, 10*time.Second, 100*time.Millisecond, "Expected at least 2 scheduled workflow executions")

		// Call deactivate endpoint
		endpoint := fmt.Sprintf("http://localhost:%d/%s", _DEFAULT_ADMIN_SERVER_PORT, strings.TrimPrefix(_DEACTIVATE_PATTERN, "GET /"))
		req, err := http.NewRequest("GET", endpoint, nil)
		require.NoError(t, err, "Failed to create deactivate request")

		resp, err := client.Do(req)
		require.NoError(t, err, "Failed to call deactivate endpoint")
		defer resp.Body.Close()

		// Verify endpoint returned 200 OK
		assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected 200 OK from deactivate endpoint")

		// Verify response body
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err, "Failed to read response body")
		assert.Equal(t, "deactivated", string(body), "Expected 'deactivated' response body")

		// Record count after deactivate and wait
		countAfterDeactivate := executionCount.Load()
		time.Sleep(4 * time.Second) // Wait long enough for multiple executions if scheduler was still running

		// Verify no new executions occurred
		finalCount := executionCount.Load()
		assert.LessOrEqual(t, finalCount, countAfterDeactivate+1,
			"Expected no new scheduled workflows after deactivate (had %d before, %d after)",
			countAfterDeactivate, finalCount)
	})
}

func mustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
