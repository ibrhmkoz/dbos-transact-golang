package dbos

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Callable debounced workflow shared by the debounce tests. Assigned in TestDebouncer setup.
var debounceTestWF Workflow[string, string]

// Helper test workflows
func debounceTestWorkflow(ctx DBOSContext, input string) (string, error) {
	return input, nil
}

// Helper workflow that debounces from within a workflow.
// Can handle both single and multiple debounce calls.
type debounceCallInput struct {
	Key    string        // Debounce key
	Delay  time.Duration // Debounce delay
	Inputs []string      // Single element for single call, multiple for multiple calls
}

func workflowThatCallsDebounce(ctx DBOSContext, input debounceCallInput) (string, error) {
	var lastHandle *WorkflowHandle[string]
	var err error

	for _, inp := range input.Inputs {
		lastHandle, err = debounceTestWF(ctx, inp,
			WithDebounce(input.Key, input.Delay),
			WithDebounceTimeout(10*time.Second),
			WithAssumedRole("test-role"))
		if err != nil {
			return "", err
		}
		if lastHandle == nil {
			return "", fmt.Errorf("expected a handle from debounce call")
		}
	}

	// Get result from the last debounce call
	result, err := lastHandle.GetResult()
	if err != nil {
		return "", err
	}
	return result, nil
}

func TestDebouncer(t *testing.T) {
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Register callable workflows before launch.
	debounceTestWF = NewWorkflow(dbosCtx, debounceTestWorkflow)
	callsDebounceWF := NewWorkflow(dbosCtx, workflowThatCallsDebounce)

	Launch(dbosCtx)
	t.Run("TestSingleDebounceCall", func(t *testing.T) {
		// Create a workflow that calls Debounce
		parentInput := debounceCallInput{
			Key:    "test-key-1",
			Delay:  500 * time.Millisecond,
			Inputs: []string{"test-input-1"},
		}

		startTime := time.Now()
		handle, err := callsDebounceWF(dbosCtx, parentInput)
		require.NoError(t, err, "failed to start workflow that calls debounce")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result")
		assert.Equal(t, "test-input-1", result, "result should match input")

		// Verify execution happened approximately 500ms after first call
		elapsed := time.Since(startTime)
		assert.GreaterOrEqual(t, elapsed, 500*time.Millisecond, "execution should take at least 450ms")
		assert.LessOrEqual(t, elapsed, 10*time.Second, "execution should take less than 10s")

		// Verify steps are generated for msg ID generation and wf ID generation
		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowID())
		require.NoError(t, err, "failed to get workflow steps")

		// Find the steps for DBOS.Debounce.assignWorkflowID and DBOS.Debounce.assignMessageID
		foundWorkflowIDStep := false
		foundMessageIDStep := false
		for _, step := range steps {
			if step.StepName == "DBOS.debounce.assignWorkflowID" {
				foundWorkflowIDStep = true
				assert.Nil(t, step.Error, "workflow ID step should not have error")
			}
			if step.StepName == "DBOS.debounce.assignMessageID" {
				foundMessageIDStep = true
				assert.Nil(t, step.Error, "message ID step should not have error")
			}
		}
		assert.True(t, foundWorkflowIDStep, "should have DBOS.debounce.assignWorkflowID step")
		assert.True(t, foundMessageIDStep, "should have DBOS.debounce.assignMessageID step")
	})

	t.Run("TestMultipleCallsPushBackAndLatestInput", func(t *testing.T) {
		// Use a long enough active window for all five calls to update the same debouncer.
		parentInput := debounceCallInput{
			Key:    "test-key-2",
			Delay:  8 * time.Second,
			Inputs: []string{"input-1", "input-2", "input-3", "input-4", "input-5"},
		}

		startTime := time.Now()
		handle, err := callsDebounceWF(dbosCtx, parentInput)
		require.NoError(t, err, "failed to start workflow that calls debounce multiple times")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result")
		assert.Equal(t, "input-5", result, "result should match latest input")

		// Verify execution happened after the final update's delay.
		elapsed := time.Since(startTime)
		assert.GreaterOrEqual(t, elapsed, 8*time.Second, "execution should take at least 8s")
		assert.LessOrEqual(t, elapsed, 15*time.Second, "execution should take less than 15s")
	})

	t.Run("TestDelayGreaterThanTimeout", func(t *testing.T) {
		// Call Debounce directly with delay=2s (greater than timeout of 200ms)
		startTime := time.Now()
		handle, err := debounceTestWF(dbosCtx, "timeout-input", WithDebounce("test-key-4", 2*time.Second), WithDebounceTimeout(200*time.Millisecond))
		require.NoError(t, err, "failed to call Debounce with delay > timeout")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result")
		assert.Equal(t, "timeout-input", result, "result should match input")

		// Verify execution happened at timeout (200ms), not delay (2s)
		elapsed := time.Since(startTime)
		assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond, "execution should take at least 200ms")
		assert.LessOrEqual(t, elapsed, 3*time.Second, "execution should take less than 3s")
	})

	t.Run("TestDelayOverride", func(t *testing.T) {
		// First call: Debounce with a very long delay (creates debouncer workflow)
		handle1, err := debounceTestWF(dbosCtx, "first-input", WithDebounce("test-key-5", 10*time.Second), WithDebounceTimeout(10*time.Second))
		require.NoError(t, err, "failed to call Debounce from outside workflow (first call)")

		// Second call: Debounce with delay=0 (should trigger immediate execution)
		startTime := time.Now()
		handle2, err := debounceTestWF(dbosCtx, "second-input", WithDebounce("test-key-5", 0), WithDebounceTimeout(10*time.Second))
		require.NoError(t, err, "failed to call Debounce from outside workflow (second call)")

		// Verify both handles refer to the same workflow ID
		assert.Equal(t, handle1.GetWorkflowID(), handle2.GetWorkflowID(), "both handles should refer to the same workflow ID")

		// Verify the second call completes immediately
		result, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result")
		assert.Equal(t, "second-input", result, "result should match latest input")

		elapsed := time.Since(startTime)
		assert.LessOrEqual(t, elapsed, 2*time.Second, "execution should happen immediately with delay=0")
	})

	t.Run("TestDifferentKeys", func(t *testing.T) {
		// Call Debounce with different keys - each should create a separate group
		handle1, err := debounceTestWF(dbosCtx, "input-key-1", WithDebounce("different-key-1", 200*time.Millisecond), WithDebounceTimeout(10*time.Second))
		require.NoError(t, err, "failed to call Debounce with first key")

		handle2, err := debounceTestWF(dbosCtx, "input-key-2", WithDebounce("different-key-2", 200*time.Millisecond), WithDebounceTimeout(10*time.Second))
		require.NoError(t, err, "failed to call Debounce with second key")

		handle3, err := debounceTestWF(dbosCtx, "input-key-3", WithDebounce("different-key-3", 200*time.Millisecond), WithDebounceTimeout(10*time.Second))
		require.NoError(t, err, "failed to call Debounce with third key")

		// All handles should have different workflow IDs
		assert.NotEqual(t, handle1.GetWorkflowID(), handle2.GetWorkflowID(), "different keys should create different workflow IDs")
		assert.NotEqual(t, handle2.GetWorkflowID(), handle3.GetWorkflowID(), "different keys should create different workflow IDs")
		assert.NotEqual(t, handle1.GetWorkflowID(), handle3.GetWorkflowID(), "different keys should create different workflow IDs")

		// Each handle should get its own input
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first handle")
		assert.Equal(t, "input-key-1", result1, "first handle should get its own input")

		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from second handle")
		assert.Equal(t, "input-key-2", result2, "second handle should get its own input")

		result3, err := handle3.GetResult()
		require.NoError(t, err, "failed to get result from third handle")
		assert.Equal(t, "input-key-3", result3, "third handle should get its own input")
	})

	t.Run("TestDifferentKeysExecuteIndependently", func(t *testing.T) {
		// Call Debounce with different keys and verify they execute independently
		handle1, err := debounceTestWF(dbosCtx, "independent-1", WithDebounce("independent-key-1", 5*time.Second), WithDebounceTimeout(10*time.Second))
		require.NoError(t, err, "failed to call Debounce with first key")

		startTime2 := time.Now()
		handle2, err := debounceTestWF(dbosCtx, "independent-2", WithDebounce("independent-key-2", 200*time.Millisecond), WithDebounceTimeout(10*time.Second))
		require.NoError(t, err, "failed to call Debounce with second key")

		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from second handle")
		assert.Equal(t, "independent-2", result2, "second handle should get its own input")

		// Verify key-2 executed independently (should complete before the 2s delay of key-1)
		elapsed2 := time.Since(startTime2)
		assert.GreaterOrEqual(t, elapsed2, 200*time.Millisecond, "key-2 should execute after its delay")
		assert.Less(t, elapsed2, 5*time.Second, "key-2 should not be affected by key-1's delay")

		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first handle")
		assert.Equal(t, "independent-1", result1, "first handle should get its own input")

	})

	t.Run("TestRecoverDebouncedWorkflow", func(t *testing.T) {
		// Call Debounce directly using the 2 second timeout debouncer
		handle1, err := debounceTestWF(dbosCtx, "recovery-input-1", WithDebounce("recovery-test-key", 200*time.Millisecond), WithDebounceTimeout(2*time.Second))
		require.NoError(t, err, "failed to call Debounce")

		// Wait for it to exit
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first run")
		assert.Equal(t, "recovery-input-1", result1, "result should match input")

		// Access kernel and manually change status to PENDING
		dbosCtxInstance, ok := dbosCtx.(*dbosContext)
		require.True(t, ok, "expected dbosContext")
		require.NotNil(t, dbosCtxInstance.kernel)

		// Sleep for a few seconds, which would push back the time computation in the debouncer workflow
		time.Sleep(3 * time.Second)

		// Find the internal debouncer workflow through the target workflow's parent metadata.
		sysDBInstance := dbosCtxInstance.kernel

		query := sysDBInstance.renderSQL(`SELECT parent_workflow_id FROM %sworkflow_status WHERE workflow_uuid = $1`, "")
		var debouncerWorkflowID string
		err = sysDBInstance.pool.QueryRow(context.Background(), query, handle1.GetWorkflowID()).Scan(&debouncerWorkflowID)
		require.NoError(t, err, "failed to find debouncer workflow from parent metadata")
		require.NotEmpty(t, debouncerWorkflowID, "debouncer workflow ID should not be empty")

		err = dbosCtxInstance.kernel.updateWorkflowOutcome(context.Background(), updateWorkflowOutcomeDBInput{
			workflowID: debouncerWorkflowID,
			status:     WorkflowStatusPending,
			output:     nil,
			tx:         nil,
		})
		require.NoError(t, err, "failed to update workflow status to PENDING")

		cleared, err := dbosCtxInstance.kernel.clearQueueAssignment(context.Background(), debouncerWorkflowID)
		require.NoError(t, err, "failed to clear queue assignment")
		require.True(t, cleared, "should have cleared queue assignment")

		debouncerWorkflowHandle := newWorkflowHandle[any](dbosCtx, debouncerWorkflowID)
		_, err = debouncerWorkflowHandle.GetResult()
		require.NoError(t, err, "shouldn't have errored")
	})
}

func TestWorkflowCannotBeRegisteredAfterLaunch(t *testing.T) {
	// Set up a new DBOS context for this test.
	dbosCtx := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Launch the context.
	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS context")

	// Registering a workflow after launch must panic.
	assert.Panics(t, func() {
		NewWorkflow(dbosCtx, debounceTestWorkflow)
	}, "registering a workflow after launch should panic")
}
