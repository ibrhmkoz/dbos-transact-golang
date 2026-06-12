package dbos

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var debounceTestWF Workflow[string, string]

func debounceTestWorkflow(ctx Context, input string) (string, error) {
	return input, nil
}

type debounceCallInput struct {
	Key    string
	Delay  time.Duration
	Inputs []string
}

func workflowThatCallsDebounce(ctx Context, input debounceCallInput) (string, error) {
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

	result, err := lastHandle.GetResult()
	if err != nil {
		return "", err
	}
	return result, nil
}

func TestDebouncer(t *testing.T) {
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	debounceTestWF = NewWorkflow(dbosCtx, debounceTestWorkflow)
	callsDebounceWF := NewWorkflow(dbosCtx, workflowThatCallsDebounce)

	Start(dbosCtx)
	t.Run("TestSingleDebounceCall", func(t *testing.T) {

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

		elapsed := time.Since(startTime)
		assert.GreaterOrEqual(t, elapsed, 500*time.Millisecond, "execution should take at least 450ms")
		assert.LessOrEqual(t, elapsed, 10*time.Second, "execution should take less than 10s")

		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps")

		uuidSteps := 0
		for _, step := range steps {
			if step.StepName == "DBOS.uuid" {
				uuidSteps++
				assert.Nil(t, step.Error, "DBOS.uuid step should not have error")
			}
		}
		assert.GreaterOrEqual(t, uuidSteps, 2, "should have DBOS.uuid steps for the target workflow and message IDs")
	})

	t.Run("TestMultipleCallsPushBackAndLatestInput", func(t *testing.T) {

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

		elapsed := time.Since(startTime)
		assert.GreaterOrEqual(t, elapsed, 8*time.Second, "execution should take at least 8s")
		assert.LessOrEqual(t, elapsed, 15*time.Second, "execution should take less than 15s")
	})

	t.Run("TestDelayGreaterThanTimeout", func(t *testing.T) {

		startTime := time.Now()
		handle, err := debounceTestWF(dbosCtx, "timeout-input", WithDebounce("test-key-4", 2*time.Second), WithDebounceTimeout(200*time.Millisecond))
		require.NoError(t, err, "failed to call Debounce with delay > timeout")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result")
		assert.Equal(t, "timeout-input", result, "result should match input")

		elapsed := time.Since(startTime)
		assert.GreaterOrEqual(t, elapsed, 200*time.Millisecond, "execution should take at least 200ms")
		assert.LessOrEqual(t, elapsed, 3*time.Second, "execution should take less than 3s")
	})

	t.Run("TestDelayOverride", func(t *testing.T) {

		handle1, err := debounceTestWF(dbosCtx, "first-input", WithDebounce("test-key-5", 10*time.Second), WithDebounceTimeout(10*time.Second))
		require.NoError(t, err, "failed to call Debounce from outside workflow (first call)")

		startTime := time.Now()
		handle2, err := debounceTestWF(dbosCtx, "second-input", WithDebounce("test-key-5", 0), WithDebounceTimeout(10*time.Second))
		require.NoError(t, err, "failed to call Debounce from outside workflow (second call)")

		assert.Equal(t, handle1.GetWorkflowId(), handle2.GetWorkflowId(), "both handles should refer to the same workflow ID")

		result, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result")
		assert.Equal(t, "second-input", result, "result should match latest input")

		elapsed := time.Since(startTime)
		assert.LessOrEqual(t, elapsed, 2*time.Second, "execution should happen immediately with delay=0")
	})

	t.Run("TestDifferentKeys", func(t *testing.T) {

		handle1, err := debounceTestWF(dbosCtx, "input-key-1", WithDebounce("different-key-1", 200*time.Millisecond), WithDebounceTimeout(10*time.Second))
		require.NoError(t, err, "failed to call Debounce with first key")

		handle2, err := debounceTestWF(dbosCtx, "input-key-2", WithDebounce("different-key-2", 200*time.Millisecond), WithDebounceTimeout(10*time.Second))
		require.NoError(t, err, "failed to call Debounce with second key")

		handle3, err := debounceTestWF(dbosCtx, "input-key-3", WithDebounce("different-key-3", 200*time.Millisecond), WithDebounceTimeout(10*time.Second))
		require.NoError(t, err, "failed to call Debounce with third key")

		assert.NotEqual(t, handle1.GetWorkflowId(), handle2.GetWorkflowId(), "different keys should create different workflow IDs")
		assert.NotEqual(t, handle2.GetWorkflowId(), handle3.GetWorkflowId(), "different keys should create different workflow IDs")
		assert.NotEqual(t, handle1.GetWorkflowId(), handle3.GetWorkflowId(), "different keys should create different workflow IDs")

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

		handle1, err := debounceTestWF(dbosCtx, "independent-1", WithDebounce("independent-key-1", 5*time.Second), WithDebounceTimeout(10*time.Second))
		require.NoError(t, err, "failed to call Debounce with first key")

		startTime2 := time.Now()
		handle2, err := debounceTestWF(dbosCtx, "independent-2", WithDebounce("independent-key-2", 200*time.Millisecond), WithDebounceTimeout(10*time.Second))
		require.NoError(t, err, "failed to call Debounce with second key")

		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from second handle")
		assert.Equal(t, "independent-2", result2, "second handle should get its own input")

		elapsed2 := time.Since(startTime2)
		assert.GreaterOrEqual(t, elapsed2, 200*time.Millisecond, "key-2 should execute after its delay")
		assert.Less(t, elapsed2, 5*time.Second, "key-2 should not be affected by key-1's delay")

		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first handle")
		assert.Equal(t, "independent-1", result1, "first handle should get its own input")

	})

	t.Run("TestRecoverDebouncedWorkflow", func(t *testing.T) {

		handle1, err := debounceTestWF(dbosCtx, "recovery-input-1", WithDebounce("recovery-test-key", 200*time.Millisecond), WithDebounceTimeout(2*time.Second))
		require.NoError(t, err, "failed to call Debounce")

		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first run")
		assert.Equal(t, "recovery-input-1", result1, "result should match input")

		dbosCtxInstance, ok := dbosCtx.(*dbosContext)
		require.True(t, ok, "expected dbosContext")
		require.NotNil(t, dbosCtxInstance.kernel)

		time.Sleep(3 * time.Second)

		sysDBInstance := dbosCtxInstance.kernel

		query := sysDBInstance.renderSql(`SELECT parent_workflow_id FROM %sworkflow_status WHERE workflow_uuid = $1`, "")
		var debouncerWorkflowId string
		err = sysDBInstance.pool.QueryRow(context.Background(), query, handle1.GetWorkflowId()).Scan(&debouncerWorkflowId)
		require.NoError(t, err, "failed to find debouncer workflow from parent metadata")
		require.NotEmpty(t, debouncerWorkflowId, "debouncer workflow ID should not be empty")

		err = dbosCtxInstance.kernel.updateWorkflowOutcome(context.Background(), updateWorkflowOutcomeDBInput{
			workflowId: debouncerWorkflowId,
			status:     WorkflowStatusPending,
			output:     nil,
			tx:         nil,
		})
		require.NoError(t, err, "failed to update workflow status to PENDING")

		cleared, err := dbosCtxInstance.kernel.clearQueueAssignment(context.Background(), debouncerWorkflowId)
		require.NoError(t, err, "failed to clear queue assignment")
		require.True(t, cleared, "should have cleared queue assignment")

		debouncerWorkflowHandle := newWorkflowHandle[any](dbosCtx, debouncerWorkflowId)
		_, err = debouncerWorkflowHandle.GetResult()
		require.NoError(t, err, "shouldn't have errored")
	})
}

func TestWorkflowCannotBeRegisteredAfterStart(t *testing.T) {

	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	err := Start(dbosCtx)
	require.NoError(t, err, "failed to start DBOS context")

	// Registering a workflow after start must panic.
	assert.Panics(t, func() {
		NewWorkflow(dbosCtx, debounceTestWorkflow)
	}, "registering a workflow after start should panic")
}
