package dbos

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testAllSerializationPaths tests workflow recovery and verifies all read paths.
// This is the unified test function that exercises:
// 1. WorkflowFn recovery: starts a workflow, blocks it, recovers it, then verifies completion
// 2. All read paths: HandleGetResult, GetWorkflowSteps, ListWorkflows, RetrieveWorkflow
// This ensures recovery paths exercise all encoding/decoding scenarios that normal workflows do.
// If input is nil, the test expects the output to be nil too.
func testAllSerializationPaths[T any](
	t *testing.T,
	executor DBOSContext,
	recoveryWorkflow Workflow[T, T],
	input T,
	workflowID string,
) {
	t.Helper()

	// Check if input is nil (for pointer types, slice, map, etc.)
	val := reflect.ValueOf(input)
	isNilExpected := false
	if !val.IsValid() {
		isNilExpected = true
	} else {
		switch val.Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
			isNilExpected = val.IsNil()
		}
	}

	// Setup events for recovery
	startEvent := NewEvent()
	blockingEvent := NewEvent()
	recoveryEventRegistry[workflowID] = struct {
		startEvent    *Event
		blockingEvent *Event
	}{startEvent, blockingEvent}
	defer delete(recoveryEventRegistry, workflowID)

	// Start the blocking workflow
	handle, err := recoveryWorkflow(executor, input, WithWorkflowID(workflowID))
	require.NoError(t, err, "failed to start blocking workflow")

	// Wait for the workflow to reach the blocking step
	startEvent.Wait()

	// Recover the pending workflow
	dbosCtx, ok := executor.(*dbosContext)
	require.True(t, ok, "expected dbosContext")
	recoveredHandles, err := recoverPendingWorkflows(dbosCtx, []string{"local"})
	require.NoError(t, err, "failed to recover pending workflows")

	// Find our workflow in the recovered handles
	var recoveredHandle *WorkflowHandle[any]
	for _, h := range recoveredHandles {
		if h.GetWorkflowID() == handle.GetWorkflowID() {
			recoveredHandle = h
			break
		}
	}
	require.NotNil(t, recoveredHandle, "expected to find recovered handle")

	// Unblock the workflow
	blockingEvent.Set()

	// Expected output - workflow returns input, so output equals input
	expectedOutput := input

	// Test read paths after completion
	t.Run("HandleGetResult", func(t *testing.T) {
		output, err := handle.GetResult()
		require.NoError(t, err)
		if isNilExpected {
			assert.Nil(t, output, "Nil result should be preserved")
		} else {
			assert.Equal(t, expectedOutput, output)
		}
	})

	t.Run("RetrieveWorkflow", func(t *testing.T) {
		h2, err := RetrieveWorkflow[T](executor, handle.GetWorkflowID())
		require.NoError(t, err)
		output, err := h2.GetResult()
		require.NoError(t, err)
		if isNilExpected {
			assert.Nil(t, output, "Retrieved workflow result should be nil")
		} else {
			assert.Equal(t, expectedOutput, output, "Retrieved workflow result should match expected output")
		}
	})

	// Check the last step output (the workflow result)
	customSer := getCustomSerializerFromCtx(executor)
	t.Run("GetWorkflowSteps", func(t *testing.T) {
		steps, err := GetWorkflowSteps(executor, handle.GetWorkflowID())
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(steps), 1, "Should have at least one step")
		if len(steps) > 0 {
			lastStep := steps[len(steps)-1]
			if isNilExpected {
				assert.Nil(t, lastStep.Output, "Step output should be nil")
			} else {
				require.NotNil(t, lastStep.Output)
				if customSer != nil {
					// Custom serializer: output is already decoded to concrete type
					assert.Equal(t, expectedOutput, lastStep.Output, "Step output should match expected output")
				} else {
					// Default JSON: output is a base64-decoded JSON string
					strValue, ok := lastStep.Output.(string)
					require.True(t, ok, "Step output should be a string")
					if strValue == "" {
						var zero T
						assert.Equal(t, zero, expectedOutput, "Step output should be the zero value of type T")
					} else {
						var decodedOutput T
						err := json.Unmarshal([]byte(strValue), &decodedOutput)
						require.NoError(t, err, "Failed to unmarshal step output to type T")
						assert.Equal(t, expectedOutput, decodedOutput, "Step output should match expected output")
					}
				}
			}
			assert.Nil(t, lastStep.Error)
		}
	})

	// Verify final state via ListWorkflows
	t.Run("ListWorkflows", func(t *testing.T) {
		wfs, err := ListWorkflows(executor,
			WithWorkflowIDs([]string{handle.GetWorkflowID()}),
			WithLoadInput(true), WithLoadOutput(true))
		require.NoError(t, err)
		require.Len(t, wfs, 1)
		wf := wfs[0]
		if isNilExpected {
			require.Nil(t, wf.Input, "Workflow input should be nil")
			require.Nil(t, wf.Output, "Workflow output should be nil")
		} else {
			require.NotNil(t, wf.Input)
			require.NotNil(t, wf.Output)

			if customSer != nil {
				// Custom serializer: input/output are already decoded to concrete types
				assert.Equal(t, input, wf.Input, "Workflow input should match input")
				assert.Equal(t, expectedOutput, wf.Output, "Workflow output should match expected output")
			} else {
				// Default JSON: input/output are base64-decoded JSON strings
				inputStr, ok := wf.Input.(string)
				require.True(t, ok, "Workflow input should be a string")
				outputStr, ok := wf.Output.(string)
				require.True(t, ok, "Workflow output should be a string")

				if inputStr == "" {
					var zero T
					assert.Equal(t, zero, input, "Workflow input should be the zero value of type T")
				} else {
					var decodedInput T
					err := json.Unmarshal([]byte(inputStr), &decodedInput)
					require.NoError(t, err, "Failed to unmarshal workflow input to type T")
					assert.Equal(t, input, decodedInput, "Workflow input should match input")
				}

				if outputStr == "" {
					var zero T
					assert.Equal(t, zero, expectedOutput, "Workflow output should be the zero value of type T")
				} else {
					var decodedOutput T
					err = json.Unmarshal([]byte(outputStr), &decodedOutput)
					require.NoError(t, err, "Failed to unmarshal workflow output to type T")
					assert.Equal(t, expectedOutput, decodedOutput, "Workflow output should match expected output")
				}
			}
		}
	})

	// If nil is expected, verify the nil marker is stored in the database
	if isNilExpected {
		t.Run("DatabaseNilMarker", func(t *testing.T) {
			// Get the database pool to query directly
			dbosCtx, ok := executor.(*dbosContext)
			require.True(t, ok, "expected dbosContext")
			Kernel := dbosCtx.kernel

			// Query the database directly to check for the marker
			ctx := context.Background()
			schemaPrefix := ""
			query := Kernel.renderSQL(`SELECT inputs, output FROM %sworkflow_status WHERE workflow_uuid = $1`, schemaPrefix)

			var inputString, outputString *string
			err := Kernel.pool.QueryRow(ctx, query, workflowID).Scan(&inputString, &outputString)
			require.NoError(t, err, "failed to query workflow status")

			// Both input and output should be the nil marker
			require.NotNil(t, inputString, "input should not be NULL in database")
			assert.Equal(t, nilMarker, *inputString, "input should be the nil marker")

			require.NotNil(t, outputString, "output should not be NULL in database")
			assert.Equal(t, nilMarker, *outputString, "output should be the nil marker")

			// Also check the step output in operation_outputs
			stepQuery := Kernel.renderSQL(`SELECT output FROM %soperation_outputs WHERE workflow_uuid = $1 ORDER BY function_id LIMIT 1`, schemaPrefix)
			var stepOutputString *string
			err = Kernel.pool.QueryRow(ctx, stepQuery, workflowID).Scan(&stepOutputString)
			require.NoError(t, err, "failed to query step output")
			require.NotNil(t, stepOutputString, "step output should not be NULL in database")
			assert.Equal(t, nilMarker, *stepOutputString, "step output should be the nil marker")
		})
	}
}

// Helper function to test Send/Recv communication
func testSendRecv[T any](
	t *testing.T,
	executor DBOSContext,
	senderWorkflow Workflow[T, T],
	receiverWorkflow Workflow[T, T],
	input T,
	senderID string,
) {
	t.Helper()

	// Start receiver workflow first (it will wait for the message)
	receiverHandle, err := receiverWorkflow(executor, input, WithWorkflowID(senderID+"-receiver"))
	require.NoError(t, err, "Receiver workflow execution failed")

	// Start sender workflow (it will send the message)
	senderHandle, err := senderWorkflow(executor, input, WithWorkflowID(senderID))
	require.NoError(t, err, "Sender workflow execution failed")

	// Get sender result
	senderResult, err := senderHandle.GetResult()
	require.NoError(t, err, "Sender workflow should complete")

	// Get receiver result
	receiverResult, err := receiverHandle.GetResult()
	require.NoError(t, err, "Receiver workflow should complete")

	// Verify the received data matches what was sent
	assert.Equal(t, input, senderResult, "Sender result should match input")
	assert.Equal(t, input, receiverResult, "Received data should match sent data")
}

// Helper function to test SetEvent/GetEvent communication
func testSetGetEvent[T any](
	t *testing.T,
	executor DBOSContext,
	setEventWorkflow Workflow[T, T],
	getEventWorkflow Workflow[string, T],
	input T,
	setEventID string,
	getEventID string,
) {
	t.Helper()

	// Start setEvent workflow
	setEventHandle, err := setEventWorkflow(executor, input, WithWorkflowID(setEventID))
	require.NoError(t, err, "SetEvent workflow execution failed")

	// Wait for setEvent to complete
	setResult, err := setEventHandle.GetResult()
	require.NoError(t, err, "SetEvent workflow should complete")

	// Start getEvent workflow (will retrieve the event)
	getEventHandle, err := getEventWorkflow(executor, setEventID, WithWorkflowID(getEventID))
	require.NoError(t, err, "GetEvent workflow execution failed")

	// Get the event result
	getResult, err := getEventHandle.GetResult()
	require.NoError(t, err, "GetEvent workflow should complete")

	// Verify the event data matches what was set
	assert.Equal(t, input, setResult, "SetEvent result should match input")
	assert.Equal(t, input, getResult, "GetEvent data should match what was set")
}

type MyInt int
type MyString string
type IntSliceSlice [][]int

type TestData struct {
	Message string
	Value   int
	Active  bool
}

type NestedTestData struct {
	Key   string
	Count int
}

type TestWorkflowData struct {
	ID           string
	Message      string
	Value        int
	Active       bool
	Data         TestData
	Metadata     map[string]string
	NestedSlice  []NestedTestData
	NestedMap    map[string]MyInt
	StringPtr    *string
	StringPtrPtr **string
}

// Typed workflow functions for testing concrete signatures
var (
	serializerWorkflow             = makeTestWorkflow[TestWorkflowData]()
	recoveryStructPtrWorkflow      = makeRecoveryWorkflow[*TestWorkflowData]()
	serializerStructWorkflow       = makeRecoveryWorkflow[TestWorkflowData]()
	recoveryIntWorkflow            = makeRecoveryWorkflow[int]()
	recoveryStringWorkflow         = makeRecoveryWorkflow[string]()
	recoveryIntPtrWorkflow         = makeRecoveryWorkflow[*int]()
	recoveryNestedIntPtrWorkflow   = makeRecoveryWorkflow[**int]()
	recoveryIntSliceWorkflow       = makeRecoveryWorkflow[[]int]()
	recoveryIntArrayWorkflow       = makeRecoveryWorkflow[[3]int]()
	recoveryByteSliceWorkflow      = makeRecoveryWorkflow[[]byte]()
	recoveryStringIntMapWorkflow   = makeRecoveryWorkflow[map[string]int]()
	recoveryMyIntWorkflow          = makeRecoveryWorkflow[MyInt]()
	recoveryMyStringWorkflow       = makeRecoveryWorkflow[MyString]()
	recoveryMyStringSliceWorkflow  = makeRecoveryWorkflow[[]MyString]()
	recoveryStringMyIntMapWorkflow = makeRecoveryWorkflow[map[string]MyInt]()
	// Additional types: empty struct, nested collections, slices of pointers
	recoveryEmptyStructWorkflow   = makeRecoveryWorkflow[struct{}]()
	recoveryIntSliceSliceWorkflow = makeRecoveryWorkflow[IntSliceSlice]()
	recoveryNestedMapWorkflow     = makeRecoveryWorkflow[map[string]map[string]int]()
	recoveryIntPtrSliceWorkflow   = makeRecoveryWorkflow[[]*int]()
	recoveryAnyWorkflow           = makeRecoveryWorkflow[any]()
)

// Typed Send/Recv workflows for various types
var (
	serializerSenderWorkflow         = makeSenderWorkflow[TestWorkflowData]()
	serializerReceiverWorkflow       = makeReceiverWorkflow[TestWorkflowData]()
	serializerIntSenderWorkflow      = makeSenderWorkflow[int]()
	serializerIntReceiverWorkflow    = makeReceiverWorkflow[int]()
	serializerIntPtrSenderWorkflow   = makeSenderWorkflow[*int]()
	serializerIntPtrReceiverWorkflow = makeReceiverWorkflow[*int]()
	serializerMyIntSenderWorkflow    = makeSenderWorkflow[MyInt]()
	serializerMyIntReceiverWorkflow  = makeReceiverWorkflow[MyInt]()
)

// Typed SetEvent/GetEvent workflows for various types
var (
	serializerSetEventWorkflow       = makeSetEventWorkflow[TestWorkflowData]()
	serializerGetEventWorkflow       = makeGetEventWorkflow[TestWorkflowData]()
	serializerIntSetEventWorkflow    = makeSetEventWorkflow[int]()
	serializerIntGetEventWorkflow    = makeGetEventWorkflow[int]()
	serializerIntPtrSetEventWorkflow = makeSetEventWorkflow[*int]()
	serializerIntPtrGetEventWorkflow = makeGetEventWorkflow[*int]()
	serializerMyIntSetEventWorkflow  = makeSetEventWorkflow[MyInt]()
	serializerMyIntGetEventWorkflow  = makeGetEventWorkflow[MyInt]()
)

// Stream workflows
var serializerStreamWorkflow = makeStreamWorkflow[TestWorkflowData]()

func makeStreamWorkflow[T any]() WorkflowFn[T, T] {
	return func(ctx DBOSContext, input T) (T, error) {
		if err := WriteStream(ctx, "test-stream", input); err != nil {
			return *new(T), fmt.Errorf("write stream failed: %w", err)
		}
		if err := CloseStream(ctx, "test-stream"); err != nil {
			return *new(T), fmt.Errorf("close stream failed: %w", err)
		}
		return input, nil
	}
}

// makeSenderWorkflow creates a generic sender workflow that sends a message to a receiver workflow.
func makeSenderWorkflow[T any]() WorkflowFn[T, T] {
	return func(ctx DBOSContext, input T) (T, error) {
		receiverWorkflowID, err := GetWorkflowID(ctx)
		if err != nil {
			return *new(T), fmt.Errorf("failed to get workflow ID: %w", err)
		}
		destID := receiverWorkflowID + "-receiver"
		err = Send(ctx, destID, input, "test-topic")
		if err != nil {
			return *new(T), fmt.Errorf("send failed: %w", err)
		}
		return input, nil
	}
}

// makeReceiverWorkflow creates a generic receiver workflow that receives a message.
func makeReceiverWorkflow[T any]() WorkflowFn[T, T] {
	return func(ctx DBOSContext, _ T) (T, error) {
		received, err := Recv[T](ctx, "test-topic", 10*time.Second)
		if err != nil {
			return *new(T), fmt.Errorf("recv failed: %w", err)
		}
		return received, nil
	}
}

// makeSetEventWorkflow creates a generic workflow that sets an event.
func makeSetEventWorkflow[T any]() WorkflowFn[T, T] {
	return func(ctx DBOSContext, input T) (T, error) {
		err := SetEvent(ctx, "test-key", input)
		if err != nil {
			return *new(T), fmt.Errorf("set event failed: %w", err)
		}
		return input, nil
	}
}

// makeGetEventWorkflow creates a generic workflow that gets an event.
func makeGetEventWorkflow[T any]() WorkflowFn[string, T] {
	return func(ctx DBOSContext, targetWorkflowID string) (T, error) {
		event, err := GetEvent[T](ctx, targetWorkflowID, "test-key", 10*time.Second)
		if err != nil {
			return *new(T), fmt.Errorf("get event failed: %w", err)
		}
		return event, nil
	}
}

// makeTestWorkflow creates a generic workflow that simply returns the input.
func makeTestWorkflow[T any]() WorkflowFn[T, T] {
	return func(ctx DBOSContext, input T) (T, error) {
		return Run(ctx, func(context context.Context) (T, error) {
			return input, nil
		})
	}
}

func serializerErrorStep(_ context.Context, _ TestWorkflowData) (TestWorkflowData, error) {
	return TestWorkflowData{}, fmt.Errorf("step error")
}

func serializerErrorWorkflow(ctx DBOSContext, input TestWorkflowData) (TestWorkflowData, error) {
	return Run(ctx, func(context context.Context) (TestWorkflowData, error) {
		return serializerErrorStep(context, input)
	})
}

// recoveryEventRegistry stores events for recovery workflows by workflow ID
var recoveryEventRegistry = make(map[string]struct {
	startEvent    *Event
	blockingEvent *Event
})

// makeRecoveryWorkflow creates a generic recovery workflow that has an initial step
// and then a blocking step that uses the output of the first step.
// This is used to test workflow recovery with various types.
// The workflow looks up events from recoveryEventRegistry using the workflow ID.
func makeRecoveryWorkflow[T any]() WorkflowFn[T, T] {
	return func(ctx DBOSContext, input T) (T, error) {
		// First step: return the input (tests encoding/decoding of type T)
		firstStepOutput, err := Run(ctx, func(context context.Context) (T, error) {
			return input, nil
		}, WithStepName("FirstStep"))
		if err != nil {
			fmt.Printf("makeRecoveryWorkflow: FirstStep error: %v\n", err)
			return *new(T), err
		}

		// Second step: blocking step that uses the first step's output
		// This tests that the first step's output is correctly decoded
		// If decoding fails or is incorrect, this step will fail
		return Run(ctx, func(context context.Context) (T, error) {
			workflowID, err := GetWorkflowID(ctx)
			if err != nil {
				return *new(T), fmt.Errorf("failed to get workflow ID: %w", err)
			}
			events, ok := recoveryEventRegistry[workflowID]
			if !ok {
				return *new(T), fmt.Errorf("no events registered for workflow ID: %s", workflowID)
			}
			events.startEvent.Set()
			events.blockingEvent.Wait()
			// Return the first step's output - this verifies correct decoding
			// If the type was decoded incorrectly, this assignment/return will fail
			return firstStepOutput, nil
		}, WithStepName("BlockingStep"))
	}
}

// TestDataProcessor is an interface for testing workflows with interface signatures
type TestDataProcessor interface {
	Process(data string) string
}

// TestStringProcessor is a concrete implementation of TestDataProcessor
type TestStringProcessor struct {
	Prefix string
}

// Process implements the TestDataProcessor interface
func (p *TestStringProcessor) Process(data string) string {
	return p.Prefix + data
}

// TestSerializer tests that workflows use the configured serializer for input/output.
//
// This test suite uses recovery-based testing as the primary approach. All tests exercise
// workflow recovery paths because:
//  1. Recovery paths exercise all encoding/decoding scenarios that normal workflows do
//  2. Recovery paths additionally test decoding from persisted state (database)
//  3. This ensures that serialization works correctly even when workflows are recovered
//     after a process restart or failure
//
// Each test:
// - Starts a workflow with a blocking step
// - Recovers the pending workflow from the database
// - Verifies all read paths: HandleGetResult, ListWorkflows, GetWorkflowSteps, RetrieveWorkflow
// - Ensures that both original and recovered handles produce correct results
//
// The suite covers: scalars, pointers, nested pointers
// slices, arrays, byte slices, maps, and custom types. It also tests Send/Recv and
// SetEvent/GetEvent communication patterns.
func TestSerializer(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Register workflows
	queuedSerializerWorkflow := NewWorkflow(executor, serializerWorkflow)
	recoveryStructPtrWorkflowD := NewWorkflow(executor, recoveryStructPtrWorkflow)
	serializerErrorWorkflowD := NewWorkflow(executor, serializerErrorWorkflow)
	serializerSenderWorkflowD := NewWorkflow(executor, serializerSenderWorkflow)
	serializerReceiverWorkflowD := NewWorkflow(executor, serializerReceiverWorkflow)
	serializerSetEventWorkflowD := NewWorkflow(executor, serializerSetEventWorkflow)
	serializerGetEventWorkflowD := NewWorkflow(executor, serializerGetEventWorkflow)
	serializerStructWorkflowD := NewWorkflow(executor, serializerStructWorkflow)

	// Register recovery workflows for all types
	recoveryIntWorkflowD := NewWorkflow(executor, recoveryIntWorkflow)
	recoveryStringWorkflowD := NewWorkflow(executor, recoveryStringWorkflow)
	recoveryIntPtrWorkflowD := NewWorkflow(executor, recoveryIntPtrWorkflow)
	recoveryNestedIntPtrWorkflowD := NewWorkflow(executor, recoveryNestedIntPtrWorkflow)
	recoveryIntSliceWorkflowD := NewWorkflow(executor, recoveryIntSliceWorkflow)
	recoveryIntArrayWorkflowD := NewWorkflow(executor, recoveryIntArrayWorkflow)
	recoveryByteSliceWorkflowD := NewWorkflow(executor, recoveryByteSliceWorkflow)
	recoveryStringIntMapWorkflowD := NewWorkflow(executor, recoveryStringIntMapWorkflow)
	recoveryMyIntWorkflowD := NewWorkflow(executor, recoveryMyIntWorkflow)
	recoveryMyStringWorkflowD := NewWorkflow(executor, recoveryMyStringWorkflow)
	recoveryMyStringSliceWorkflowD := NewWorkflow(executor, recoveryMyStringSliceWorkflow)
	recoveryStringMyIntMapWorkflowD := NewWorkflow(executor, recoveryStringMyIntMapWorkflow)
	// Register additional recovery workflows
	recoveryEmptyStructWorkflowD := NewWorkflow(executor, recoveryEmptyStructWorkflow)
	recoveryIntSliceSliceWorkflowD := NewWorkflow(executor, recoveryIntSliceSliceWorkflow)
	recoveryNestedMapWorkflowD := NewWorkflow(executor, recoveryNestedMapWorkflow)
	recoveryIntPtrSliceWorkflowD := NewWorkflow(executor, recoveryIntPtrSliceWorkflow)
	recoveryAnyWorkflowD := NewWorkflow(executor, recoveryAnyWorkflow)
	// Register typed Send/Recv workflows
	serializerIntSenderWorkflowD := NewWorkflow(executor, serializerIntSenderWorkflow)
	serializerIntReceiverWorkflowD := NewWorkflow(executor, serializerIntReceiverWorkflow)
	serializerIntPtrSenderWorkflowD := NewWorkflow(executor, serializerIntPtrSenderWorkflow)
	serializerIntPtrReceiverWorkflowD := NewWorkflow(executor, serializerIntPtrReceiverWorkflow)
	serializerMyIntSenderWorkflowD := NewWorkflow(executor, serializerMyIntSenderWorkflow)
	serializerMyIntReceiverWorkflowD := NewWorkflow(executor, serializerMyIntReceiverWorkflow)
	// Register typed SetEvent/GetEvent workflows
	serializerIntSetEventWorkflowD := NewWorkflow(executor, serializerIntSetEventWorkflow)
	serializerIntGetEventWorkflowD := NewWorkflow(executor, serializerIntGetEventWorkflow)
	serializerIntPtrSetEventWorkflowD := NewWorkflow(executor, serializerIntPtrSetEventWorkflow)
	serializerIntPtrGetEventWorkflowD := NewWorkflow(executor, serializerIntPtrGetEventWorkflow)
	serializerMyIntSetEventWorkflowD := NewWorkflow(executor, serializerMyIntSetEventWorkflow)
	serializerMyIntGetEventWorkflowD := NewWorkflow(executor, serializerMyIntGetEventWorkflow)
	serializerStreamWorkflowD := NewWorkflow(executor, serializerStreamWorkflow)

	err := Launch(executor)
	require.NoError(t, err)
	defer Shutdown(executor, 10*time.Second)

	// Test workflow with comprehensive data structure
	t.Run("StructValues", func(t *testing.T) {
		strPtr := "pointer value"
		strPtrPtr := &strPtr
		input := TestWorkflowData{
			ID:       "test-id",
			Message:  "test message",
			Value:    42,
			Active:   true,
			Data:     TestData{Message: "embedded", Value: 123, Active: false},
			Metadata: map[string]string{"key": "value"},
			NestedSlice: []NestedTestData{
				{Key: "nested1", Count: 10},
				{Key: "nested2", Count: 20},
			},
			NestedMap: map[string]MyInt{
				"map-key1": MyInt(100),
				"map-key2": MyInt(200),
			},
			StringPtr:    &strPtr,
			StringPtrPtr: &strPtrPtr,
		}

		testAllSerializationPaths(t, executor, serializerStructWorkflowD, input, "struct-values-wf")
	})

	// Test nil values with pointer type workflow
	t.Run("NilStructPointer", func(t *testing.T) {
		testAllSerializationPaths(t, executor, recoveryStructPtrWorkflowD, (*TestWorkflowData)(nil), "nil-pointer-wf")
	})

	t.Run("Int", func(t *testing.T) {
		testAllSerializationPaths(t, executor, recoveryIntWorkflowD, 0, "recovery-int-wf")
	})

	t.Run("EmptyString", func(t *testing.T) {
		testAllSerializationPaths(t, executor, recoveryStringWorkflowD, "", "recovery-empty-string-wf")
	})

	// Pointer variants (single level only, nested pointers not supported)
	t.Run("Pointers", func(t *testing.T) {
		t.Run("NonNil", func(t *testing.T) {
			v := 123
			input := &v
			testAllSerializationPaths(t, executor, recoveryIntPtrWorkflowD, input, "recovery-int-ptr-wf")

		})

		t.Run("Nil", func(t *testing.T) {
			var input *int = nil
			testAllSerializationPaths(t, executor, recoveryIntPtrWorkflowD, input, "recovery-int-ptr-nil-wf")
		})
	})

	t.Run("NestedPointers", func(t *testing.T) {
		t.Run("NonNil", func(t *testing.T) {
			v := 123
			ptr := &v
			ptrPtr := &ptr
			testAllSerializationPaths(t, executor, recoveryNestedIntPtrWorkflowD, ptrPtr, "recovery-nested-int-ptr-wf")

		})

		t.Run("Nil", func(t *testing.T) {
			var ptrPtr **int = nil
			testAllSerializationPaths(t, executor, recoveryNestedIntPtrWorkflowD, ptrPtr, "recovery-nested-int-ptr-nil-wf")
		})
	})

	t.Run("SlicesAndArrays", func(t *testing.T) {
		t.Run("NonEmptySlice", func(t *testing.T) {
			input := []int{1, 2, 3}
			testAllSerializationPaths(t, executor, recoveryIntSliceWorkflowD, input, "recovery-int-slice-wf")
		})

		t.Run("NilSlice", func(t *testing.T) {
			var input []int = nil
			testAllSerializationPaths(t, executor, recoveryIntSliceWorkflowD, input, "recovery-int-slice-nil-wf")
		})

		t.Run("Array", func(t *testing.T) {
			input := [3]int{1, 2, 3}
			testAllSerializationPaths(t, executor, recoveryIntArrayWorkflowD, input, "recovery-int-array-wf")
		})
	})

	t.Run("ByteSlices", func(t *testing.T) {
		t.Run("NonEmpty", func(t *testing.T) {
			input := []byte{1, 2, 3, 4, 5}
			testAllSerializationPaths(t, executor, recoveryByteSliceWorkflowD, input, "recovery-byte-slice-wf")
		})

		t.Run("Nil", func(t *testing.T) {
			var input []byte = nil
			testAllSerializationPaths(t, executor, recoveryByteSliceWorkflowD, input, "recovery-byte-slice-nil-wf")
		})
	})

	t.Run("Maps", func(t *testing.T) {
		t.Run("NonEmptyMap", func(t *testing.T) {
			input := map[string]int{"x": 1, "y": 2}
			testAllSerializationPaths(t, executor, recoveryStringIntMapWorkflowD, input, "recovery-string-int-map-wf")
		})

		t.Run("NilMap", func(t *testing.T) {
			var input map[string]int = nil
			testAllSerializationPaths(t, executor, recoveryStringIntMapWorkflowD, input, "recovery-string-int-map-nil-wf")
		})
	})

	t.Run("CustomTypes", func(t *testing.T) {
		t.Run("MyInt", func(t *testing.T) {
			input := MyInt(7)
			testAllSerializationPaths(t, executor, recoveryMyIntWorkflowD, input, "recovery-myint-wf")
		})

		t.Run("MyString", func(t *testing.T) {
			input := MyString("zeta")
			testAllSerializationPaths(t, executor, recoveryMyStringWorkflowD, input, "recovery-mystring-wf")
		})

		t.Run("MyStringSlice", func(t *testing.T) {
			input := []MyString{"a", "b"}
			testAllSerializationPaths(t, executor, recoveryMyStringSliceWorkflowD, input, "recovery-mystring-slice-wf")
		})

		t.Run("StringMyIntMap", func(t *testing.T) {
			input := map[string]MyInt{"k": 9}
			testAllSerializationPaths(t, executor, recoveryStringMyIntMapWorkflowD, input, "recovery-string-myint-map-wf")
		})
	})

	// Empty struct
	t.Run("EmptyStruct", func(t *testing.T) {
		input := struct{}{}
		testAllSerializationPaths(t, executor, recoveryEmptyStructWorkflowD, input, "recovery-empty-struct-wf")
	})

	// Nested collections
	t.Run("NestedCollections", func(t *testing.T) {
		t.Run("SliceOfSlices", func(t *testing.T) {
			input := IntSliceSlice{{1, 2}, {3, 4, 5}}
			testAllSerializationPaths(t, executor, recoveryIntSliceSliceWorkflowD, input, "recovery-int-slice-slice-wf")
		})

		t.Run("NestedMap", func(t *testing.T) {
			input := map[string]map[string]int{
				"outer1": {"inner1": 1, "inner2": 2},
				"outer2": {"inner3": 3},
			}
			testAllSerializationPaths(t, executor, recoveryNestedMapWorkflowD, input, "recovery-nested-map-wf")
		})
	})

	// Slices of pointers
	t.Run("SliceOfPointers", func(t *testing.T) {
		t.Run("NonNil", func(t *testing.T) {
			v1 := 10
			v2 := 20
			v3 := 30
			input := []*int{&v1, &v2, &v3}
			testAllSerializationPaths(t, executor, recoveryIntPtrSliceWorkflowD, input, "recovery-int-ptr-slice-wf")
		})

		t.Run("NilSlice", func(t *testing.T) {
			var input []*int = nil
			testAllSerializationPaths(t, executor, recoveryIntPtrSliceWorkflowD, input, "recovery-int-ptr-slice-nil-wf")
		})
	})

	// Test workflow with any signature using testAllSerializationPaths
	t.Run("Any", func(t *testing.T) {
		// Test with a string value (avoids JSON number type conversion issues)
		input := any("test-value")
		testAllSerializationPaths(t, executor, recoveryAnyWorkflowD, input, "recovery-any-string-wf")
	})

	// Test error values
	t.Run("ErrorValues", func(t *testing.T) {
		input := TestWorkflowData{
			ID:       "error-test-id",
			Message:  "error test",
			Value:    123,
			Active:   true,
			Data:     TestData{Message: "error data", Value: 456, Active: false},
			Metadata: map[string]string{"type": "error"},
			NestedSlice: []NestedTestData{
				{Key: "error-nested", Count: 99},
			},
			NestedMap: map[string]MyInt{
				"error-key": MyInt(999),
			},
			StringPtr:    nil,
			StringPtrPtr: nil,
		}

		handle, err := serializerErrorWorkflowD(executor, input)
		require.NoError(t, err, "Error workflow execution failed")

		// 1. Test with handle.GetResult()
		t.Run("HandleGetResult", func(t *testing.T) {
			_, err := handle.GetResult()
			require.Error(t, err, "Should get step error")
			assert.Contains(t, err.Error(), "step error", "Error message should be preserved")
		})

		// 2. Test with GetWorkflowSteps
		t.Run("GetWorkflowSteps", func(t *testing.T) {
			steps, err := GetWorkflowSteps(executor, handle.GetWorkflowID())
			require.NoError(t, err, "Failed to get workflow steps")
			require.Len(t, steps, 1, "Expected 1 step")

			step := steps[0]
			require.NotNil(t, step.Error, "Step should have error")
			assert.Contains(t, step.Error.Error(), "step error", "Step error should be preserved")
		})
	})

	// Test Send/Recv with non-basic types
	t.Run("SendRecv", func(t *testing.T) {
		strPtr := "sendrecv pointer"
		strPtrPtr := &strPtr
		input := TestWorkflowData{
			ID:       "sendrecv-test-id",
			Message:  "test message",
			Value:    99,
			Active:   true,
			Data:     TestData{Message: "nested", Value: 200, Active: true},
			Metadata: map[string]string{"comm": "sendrecv"},
			NestedSlice: []NestedTestData{
				{Key: "sendrecv-nested", Count: 50},
			},
			NestedMap: map[string]MyInt{
				"sendrecv-key": MyInt(500),
			},
			StringPtr:    &strPtr,
			StringPtrPtr: &strPtrPtr,
		}

		testSendRecv(t, executor, serializerSenderWorkflowD, serializerReceiverWorkflowD, input, "sender-wf")
	})

	// Test SetEvent/GetEvent with non-basic types
	t.Run("SetGetEvent", func(t *testing.T) {
		strPtr := "event pointer"
		strPtrPtr := &strPtr
		input := TestWorkflowData{
			ID:       "event-test-id",
			Message:  "event message",
			Value:    77,
			Active:   false,
			Data:     TestData{Message: "event nested", Value: 333, Active: true},
			Metadata: map[string]string{"type": "event"},
			NestedSlice: []NestedTestData{
				{Key: "event-nested1", Count: 30},
				{Key: "event-nested2", Count: 40},
			},
			NestedMap: map[string]MyInt{
				"event-key1": MyInt(300),
				"event-key2": MyInt(400),
			},
			StringPtr:    &strPtr,
			StringPtrPtr: &strPtrPtr,
		}

		testSetGetEvent(t, executor, serializerSetEventWorkflowD, serializerGetEventWorkflowD, input, "setevent-wf", "getevent-wf")
	})

	// Test typed Send/Recv and SetEvent/GetEvent with various types
	t.Run("TypedSendRecvAndSetGetEvent", func(t *testing.T) {
		// Test int (scalar type)
		t.Run("Int", func(t *testing.T) {
			input := 42
			testSendRecv(t, executor, serializerIntSenderWorkflowD, serializerIntReceiverWorkflowD, input, "typed-int-sender-wf")
			testSetGetEvent(t, executor, serializerIntSetEventWorkflowD, serializerIntGetEventWorkflowD, input, "typed-int-setevent-wf", "typed-int-getevent-wf")
		})

		// Test MyInt (user defined type)
		t.Run("MyInt", func(t *testing.T) {
			input := MyInt(73)
			testSendRecv(t, executor, serializerMyIntSenderWorkflowD, serializerMyIntReceiverWorkflowD, input, "typed-myint-sender-wf")
			testSetGetEvent(t, executor, serializerMyIntSetEventWorkflowD, serializerMyIntGetEventWorkflowD, input, "typed-myint-setevent-wf", "typed-myint-getevent-wf")
		})

		// Test *int (pointer type, set)
		t.Run("IntPtrSet", func(t *testing.T) {
			v := 99
			input := &v
			testSendRecv(t, executor, serializerIntPtrSenderWorkflowD, serializerIntPtrReceiverWorkflowD, input, "typed-intptr-set-sender-wf")
			testSetGetEvent(t, executor, serializerIntPtrSetEventWorkflowD, serializerIntPtrGetEventWorkflowD, input, "typed-intptr-set-setevent-wf", "typed-intptr-set-getevent-wf")
		})
	})

	// Test queued workflow with TestWorkflowData type
	t.Run("QueuedWorkflow", func(t *testing.T) {
		strPtr := "queued pointer"
		strPtrPtr := &strPtr
		input := TestWorkflowData{
			ID:       "queued-test-id",
			Message:  "queued test message",
			Value:    456,
			Active:   false,
			Data:     TestData{Message: "queued nested", Value: 789, Active: true},
			Metadata: map[string]string{"type": "queued"},
			NestedSlice: []NestedTestData{
				{Key: "queued-nested", Count: 222},
			},
			NestedMap: map[string]MyInt{
				"queued-key": MyInt(2222),
			},
			StringPtr:    &strPtr,
			StringPtrPtr: &strPtrPtr,
		}

		handle, err := queuedSerializerWorkflow(executor, input, WithWorkflowID("serializer-queued-wf"))
		require.NoError(t, err, "failed to start queued workflow")

		// Get result from the handle
		result, err := handle.GetResult()
		require.NoError(t, err, "queued workflow should complete successfully")
		assert.Equal(t, input, result, "queued workflow result should match input")
	})

	// Test WriteStream/ReadStream
	t.Run("WriteReadStream", func(t *testing.T) {
		input := TestWorkflowData{
			ID: "stream-test", Message: "stream data", Value: 111,
			Data:     TestData{Message: "streamed", Value: 222},
			Metadata: map[string]string{"stream": "json"},
		}
		handle, err := serializerStreamWorkflowD(executor, input, WithWorkflowID("json-stream-wf"))
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result)

		values, closed, err := ReadStream[TestWorkflowData](executor, "json-stream-wf", "test-stream")
		require.NoError(t, err)
		assert.True(t, closed)
		require.Len(t, values, 1)
		assert.Equal(t, input, values[0])
	})
}

// ===== Gob Serializer Tests =====

// GobOnlyType is a type that uses GobEncoder/GobDecoder for custom binary encoding.
// JSON cannot handle this because it has unexported fields and custom encoding logic.
type GobOnlyType struct {
	real float64
	imag float64
	tag  string
}

func (g GobOnlyType) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(g.real); err != nil {
		return nil, err
	}
	if err := enc.Encode(g.imag); err != nil {
		return nil, err
	}
	if err := enc.Encode(g.tag); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (g *GobOnlyType) GobDecode(data []byte) error {
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&g.real); err != nil {
		return err
	}
	if err := dec.Decode(&g.imag); err != nil {
		return err
	}
	return dec.Decode(&g.tag)
}

func init() {
	// Register types for gob encoding/decoding through any interface
	gob.Register(TestWorkflowData{})
	gob.Register(TestData{})
	gob.Register(NestedTestData{})
	gob.Register(map[string]string{})
	gob.Register([]NestedTestData{})
	gob.Register(map[string]MyInt{})
	gob.Register(MyInt(0))
	gob.Register(MyString(""))
	gob.Register([]MyString{})
	gob.Register(map[string]int{})
	gob.Register([]int{})
	gob.Register([3]int{})
	gob.Register([]byte{})
	gob.Register(GobOnlyType{})
}

var (
	gobRecoveryStructWorkflow   = makeRecoveryWorkflow[TestWorkflowData]()
	gobRecoveryIntWorkflow      = makeRecoveryWorkflow[int]()
	gobRecoveryStringWorkflow   = makeRecoveryWorkflow[string]()
	gobRecoveryIntSliceWorkflow = makeRecoveryWorkflow[[]int]()
	gobRecoveryMapWorkflow      = makeRecoveryWorkflow[map[string]int]()
	gobRecoveryMyIntWorkflow    = makeRecoveryWorkflow[MyInt]()
	gobRecoveryGobOnlyWorkflow  = makeRecoveryWorkflow[GobOnlyType]()
	gobSenderWorkflow           = makeSenderWorkflow[TestWorkflowData]()
	gobReceiverWorkflow         = makeReceiverWorkflow[TestWorkflowData]()
	gobSetEventWorkflow         = makeSetEventWorkflow[TestWorkflowData]()
	gobGetEventWorkflow         = makeGetEventWorkflow[TestWorkflowData]()
	gobGobOnlyWorkflow          = makeTestWorkflow[GobOnlyType]()
	gobGobOnlySenderWorkflow    = makeSenderWorkflow[GobOnlyType]()
	gobGobOnlyReceiverWorkflow  = makeReceiverWorkflow[GobOnlyType]()
	gobGobOnlySetEventWorkflow  = makeSetEventWorkflow[GobOnlyType]()
	gobGobOnlyGetEventWorkflow  = makeGetEventWorkflow[GobOnlyType]()
	gobStreamWorkflow           = makeStreamWorkflow[TestWorkflowData]()
	gobGobOnlyStreamWorkflow    = makeStreamWorkflow[GobOnlyType]()
	gobQueuedWorkflow           = makeTestWorkflow[TestWorkflowData]()
)

// TestGobSerializer tests the built-in gob serializer through all workflow paths.
func TestGobSerializer(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true, serializer: NewGobSerializer()})

	gobRecoveryStructWorkflowD := NewWorkflow(executor, gobRecoveryStructWorkflow)
	gobRecoveryIntWorkflowD := NewWorkflow(executor, gobRecoveryIntWorkflow)
	gobRecoveryStringWorkflowD := NewWorkflow(executor, gobRecoveryStringWorkflow)
	gobRecoveryIntSliceWorkflowD := NewWorkflow(executor, gobRecoveryIntSliceWorkflow)
	gobRecoveryMapWorkflowD := NewWorkflow(executor, gobRecoveryMapWorkflow)
	gobRecoveryMyIntWorkflowD := NewWorkflow(executor, gobRecoveryMyIntWorkflow)
	gobRecoveryGobOnlyWorkflowD := NewWorkflow(executor, gobRecoveryGobOnlyWorkflow)
	gobSenderWorkflowD := NewWorkflow(executor, gobSenderWorkflow)
	gobReceiverWorkflowD := NewWorkflow(executor, gobReceiverWorkflow)
	gobSetEventWorkflowD := NewWorkflow(executor, gobSetEventWorkflow)
	gobGetEventWorkflowD := NewWorkflow(executor, gobGetEventWorkflow)
	gobGobOnlyWorkflowD := NewWorkflow(executor, gobGobOnlyWorkflow)
	gobGobOnlySenderWorkflowD := NewWorkflow(executor, gobGobOnlySenderWorkflow)
	gobGobOnlyReceiverWorkflowD := NewWorkflow(executor, gobGobOnlyReceiverWorkflow)
	gobGobOnlySetEventWorkflowD := NewWorkflow(executor, gobGobOnlySetEventWorkflow)
	gobGobOnlyGetEventWorkflowD := NewWorkflow(executor, gobGobOnlyGetEventWorkflow)
	gobStreamWorkflowD := NewWorkflow(executor, gobStreamWorkflow)
	gobGobOnlyStreamWorkflowD := NewWorkflow(executor, gobGobOnlyStreamWorkflow)
	queuedGobWorkflow := NewWorkflow(executor, gobQueuedWorkflow)

	err := Launch(executor)
	require.NoError(t, err)
	defer Shutdown(executor, 10*time.Second)

	t.Run("Struct", func(t *testing.T) {
		input := TestWorkflowData{
			ID:       "gob-test",
			Message:  "gob message",
			Value:    42,
			Active:   true,
			Data:     TestData{Message: "embedded", Value: 123, Active: false},
			Metadata: map[string]string{"key": "value"},
			NestedSlice: []NestedTestData{
				{Key: "nested1", Count: 10},
			},
			NestedMap: map[string]MyInt{"k": MyInt(100)},
		}
		testAllSerializationPaths(t, executor, gobRecoveryStructWorkflowD, input, "gob-struct-wf")
	})

	t.Run("Int", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryIntWorkflowD, 42, "gob-int-wf")
	})

	t.Run("String", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryStringWorkflowD, "hello gob", "gob-string-wf")
	})

	t.Run("IntSlice", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryIntSliceWorkflowD, []int{1, 2, 3}, "gob-int-slice-wf")
	})

	t.Run("Map", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryMapWorkflowD, map[string]int{"x": 1, "y": 2}, "gob-map-wf")
	})

	t.Run("MyInt", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryMyIntWorkflowD, MyInt(7), "gob-myint-wf")
	})

	// Test gob-only type: uses GobEncoder/GobDecoder with unexported fields.
	// JSON cannot serialize this type. Uses a simple workflow (not recovery-based)
	// because recovery involves step output re-encoding which differs for GobOnly types.
	t.Run("GobOnlyType", func(t *testing.T) {
		input := GobOnlyType{real: 3.14, imag: 2.71, tag: "complex-value"}
		handle, err := gobGobOnlyWorkflowD(executor, input, WithWorkflowID("gob-only-type-wf"))
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result, "gob-only type should roundtrip correctly")

		// Verify RetrieveWorkflow also works (reads from DB, decodes with gob)
		h2, err := RetrieveWorkflow[GobOnlyType](executor, "gob-only-type-wf")
		require.NoError(t, err)
		result2, err := h2.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result2, "gob-only type should roundtrip via RetrieveWorkflow")
	})

	t.Run("SendRecv", func(t *testing.T) {
		input := TestWorkflowData{
			ID: "gob-sendrecv", Message: "gob msg", Value: 99,
			Data:     TestData{Message: "nested", Value: 200},
			Metadata: map[string]string{"comm": "gob"},
		}
		testSendRecv(t, executor, gobSenderWorkflowD, gobReceiverWorkflowD, input, "gob-sender-wf")
	})

	t.Run("SetGetEvent", func(t *testing.T) {
		input := TestWorkflowData{
			ID: "gob-event", Message: "gob event", Value: 77,
			Data:     TestData{Message: "event nested", Value: 333},
			Metadata: map[string]string{"type": "gob-event"},
		}
		testSetGetEvent(t, executor, gobSetEventWorkflowD, gobGetEventWorkflowD, input, "gob-setevent-wf", "gob-getevent-wf")
	})

	// Test gob-only type through Send/Recv
	t.Run("GobOnlySendRecv", func(t *testing.T) {
		input := GobOnlyType{real: 1.5, imag: 2.5, tag: "sendrecv"}
		testSendRecv(t, executor, gobGobOnlySenderWorkflowD, gobGobOnlyReceiverWorkflowD, input, "gob-only-sender-wf")
	})

	// Test gob-only type through SetEvent/GetEvent
	t.Run("GobOnlySetGetEvent", func(t *testing.T) {
		input := GobOnlyType{real: 9.8, imag: 6.7, tag: "event"}
		testSetGetEvent(t, executor, gobGobOnlySetEventWorkflowD, gobGobOnlyGetEventWorkflowD, input, "gob-only-setevent-wf", "gob-only-getevent-wf")
	})

	// Test WriteStream/ReadStream with struct
	t.Run("WriteReadStream", func(t *testing.T) {
		input := TestWorkflowData{
			ID: "gob-stream", Message: "stream data", Value: 55,
			Data:     TestData{Message: "streamed", Value: 555},
			Metadata: map[string]string{"stream": "gob"},
		}
		handle, err := gobStreamWorkflowD(executor, input, WithWorkflowID("gob-stream-wf"))
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result)

		values, closed, err := ReadStream[TestWorkflowData](executor, "gob-stream-wf", "test-stream")
		require.NoError(t, err)
		assert.True(t, closed)
		require.Len(t, values, 1)
		assert.Equal(t, input, values[0])
	})

	// Test WriteStream/ReadStream with gob-only type
	t.Run("GobOnlyWriteReadStream", func(t *testing.T) {
		input := GobOnlyType{real: 7.7, imag: 8.8, tag: "streamed"}
		handle, err := gobGobOnlyStreamWorkflowD(executor, input, WithWorkflowID("gob-only-stream-wf"))
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result)

		values, closed, err := ReadStream[GobOnlyType](executor, "gob-only-stream-wf", "test-stream")
		require.NoError(t, err)
		assert.True(t, closed)
		require.Len(t, values, 1)
		assert.Equal(t, input, values[0])
	})

	// Test queued workflow
	t.Run("QueuedWorkflow", func(t *testing.T) {
		input := TestWorkflowData{
			ID: "gob-queued", Message: "queued msg", Value: 88,
			Data:     TestData{Message: "queued", Value: 888},
			Metadata: map[string]string{"type": "gob-queued"},
		}
		handle, err := queuedGobWorkflow(executor, input, WithWorkflowID("gob-queued-wf"))
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result)
	})

	// Test recovery with gob-only type
	t.Run("GobOnlyRecovery", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryGobOnlyWorkflowD, GobOnlyType{real: 5.5, imag: 6.6, tag: "recovered"}, "gob-only-recovery-wf")
	})
}

func TestPortableInterop(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// InteropInput exercises the full portable JSON value space:
	// strings, integers, floats, booleans, nulls, arrays, nested objects, RFC3339 timestamps
	type NestedObj struct {
		Deep bool `json:"deep"`
	}
	type MapArg struct {
		Key1   string    `json:"key1"`
		Key2   int       `json:"key2"`
		Nested NestedObj `json:"nested"`
	}

	// The workflow accepts 7 positional args matching the golden JSON below.
	// Go workflows take a single input param, so we use a struct that mirrors the positional args.
	type InteropArgs struct {
		Str       string   `json:"str"`
		Num       int      `json:"num"`
		Timestamp string   `json:"timestamp"`
		Arr       []string `json:"arr"`
		Obj       MapArg   `json:"obj"`
		Flag      bool     `json:"flag"`
		Nullable  *string  `json:"nullable"`
	}

	expectedArgs := InteropArgs{
		Str:       "hello-interop",
		Num:       42,
		Timestamp: "2025-06-15T10:30:00.000Z",
		Arr:       []string{"alpha", "beta", "gamma"},
		Obj:       MapArg{Key1: "value1", Key2: 99, Nested: NestedObj{Deep: true}},
		Flag:      true,
		Nullable:  nil,
	}

	// Golden JSON matching what Python/TS would produce, including namedArgs (ignored by Go)
	goldenInputsJSON := `{"positionalArgs":[{"str":"hello-interop","num":42,"timestamp":"2025-06-15T10:30:00.000Z","arr":["alpha","beta","gamma"],"obj":{"key1":"value1","key2":99,"nested":{"deep":true}},"flag":true,"nullable":null}],"namedArgs":{"unused_kwarg":"should_be_ignored","another":123}}`

	// InteropResult captures intermediate results to prove each encode/decode path works.
	type InteropResult struct {
		Input        InteropArgs `json:"input"`
		StepOutput   InteropArgs `json:"stepOutput"`
		RecvOutput   InteropArgs `json:"recvOutput"`
		EventOutput  InteropArgs `json:"eventOutput"`
		StreamOutput InteropArgs `json:"streamOutput"`
	}

	// A single workflow that exercises all serialization paths:
	// step output, send/recv, set_event/get_event, and write_stream/read_stream.
	portableWf := func(ctx DBOSContext, input InteropArgs) (InteropResult, error) {
		// 1. Step: encode/decode step output
		stepOut, err := Run(ctx, func(_ context.Context) (InteropArgs, error) {
			return input, nil
		})
		if err != nil {
			return InteropResult{}, fmt.Errorf("step failed: %w", err)
		}

		// 2. Send to self, then Recv
		wfID, err := GetWorkflowID(ctx)
		if err != nil {
			return InteropResult{}, err
		}
		if err := Send(ctx, wfID, input, "test-topic"); err != nil {
			return InteropResult{}, fmt.Errorf("send failed: %w", err)
		}
		recvOut, err := Recv[InteropArgs](ctx, "test-topic", 10*time.Second)
		if err != nil {
			return InteropResult{}, fmt.Errorf("recv failed: %w", err)
		}

		// 3. SetEvent then GetEvent (from own workflow)
		if err := SetEvent(ctx, "test-key", input); err != nil {
			return InteropResult{}, fmt.Errorf("set event failed: %w", err)
		}
		eventOut, err := GetEvent[InteropArgs](ctx, wfID, "test-key", 10*time.Second)
		if err != nil {
			return InteropResult{}, fmt.Errorf("get event failed: %w", err)
		}

		// 4. Stream: write, close, then read back
		if err := WriteStream(ctx, "test-stream", input); err != nil {
			return InteropResult{}, fmt.Errorf("write stream failed: %w", err)
		}
		if err := CloseStream(ctx, "test-stream"); err != nil {
			return InteropResult{}, fmt.Errorf("close stream failed: %w", err)
		}
		streamValues, closed, err := ReadStream[InteropArgs](ctx, wfID, "test-stream")
		if err != nil {
			return InteropResult{}, fmt.Errorf("read stream failed: %w", err)
		}
		if !closed {
			return InteropResult{}, fmt.Errorf("expected stream to be closed")
		}
		if len(streamValues) != 1 {
			return InteropResult{}, fmt.Errorf("expected 1 stream value, got %d", len(streamValues))
		}

		return InteropResult{
			Input:        input,
			StepOutput:   stepOut,
			RecvOutput:   recvOut,
			EventOutput:  eventOut,
			StreamOutput: streamValues[0],
		}, nil
	}
	NewWorkflow(executor, portableWf, WithWorkflowName("interop_workflow"))

	require.NoError(t, Launch(executor))
	defer Shutdown(executor, 10*time.Second)

	// Helper to insert a portable workflow directly into the DB (simulating another language).
	insertPortableWorkflow := func(t *testing.T, workflowID, status string, queueName *string) {
		t.Helper()
		c := executor.(*dbosContext)
		Kernel := c.kernel
		insertQuery := Kernel.renderSQL(`INSERT INTO %sworkflow_status (
			workflow_uuid, status, name, inputs, serialization, queue_name,
			created_at, updated_at, recovery_attempts, executor_id, priority,
			application_version, application_id, authenticated_user, assumed_role, authenticated_roles
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
			"")
		now := time.Now().UnixMilli()
		_, err := Kernel.pool.Exec(context.Background(), insertQuery,
			workflowID, status, "interop_workflow", goldenInputsJSON, PortableSerializerName, queueName,
			now, now, 0, "local", 0, c.applicationVersion, "", "", "", "[]")
		require.NoError(t, err)
	}

	// verifyResult checks all intermediate results match expectedArgs.
	verifyResult := func(t *testing.T, result InteropResult) {
		t.Helper()
		assert.Equal(t, expectedArgs, result.Input, "workflow input")
		assert.Equal(t, expectedArgs, result.StepOutput, "step output")
		assert.Equal(t, expectedArgs, result.RecvOutput, "recv output")
		assert.Equal(t, expectedArgs, result.EventOutput, "event output")
		assert.Equal(t, expectedArgs, result.StreamOutput, "stream output")
	}

	// 1. Recovery path: direct DB insert with status=PENDING, then recover.
	t.Run("DirectDBInsertRecovery", func(t *testing.T) {
		workflowID := "interop-recovery-" + t.Name()
		insertPortableWorkflow(t, workflowID, string(WorkflowStatusPending), nil)

		c := executor.(*dbosContext)
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)

		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrievedHandle, err := RetrieveWorkflow[InteropResult](executor, workflowID)
		require.NoError(t, err)
		result, err := retrievedHandle.GetResult()
		require.NoError(t, err)
		verifyResult(t, result)

		// Verify ListWorkflows returns portable inputs/outputs correctly
		wfs, err := ListWorkflows(executor,
			WithWorkflowIDs([]string{workflowID}),
			WithLoadInput(true), WithLoadOutput(true))
		require.NoError(t, err)
		require.Len(t, wfs, 1)
		wf := wfs[0]
		assert.Equal(t, PortableSerializerName, wf.Serialization)

		require.NotNil(t, wf.Input)
		inputStr, ok := wf.Input.(string)
		require.True(t, ok, "expected string for portable input, got %T", wf.Input)
		assert.True(t, json.Valid([]byte(inputStr)), "input should be valid JSON")
		var envelope PortableWorkflowArgs
		require.NoError(t, json.Unmarshal([]byte(inputStr), &envelope))
		assert.Len(t, envelope.PositionalArgs, 1)

		require.NotNil(t, wf.Output)
		outputStr, ok := wf.Output.(string)
		require.True(t, ok, "expected string for portable output, got %T", wf.Output)
		assert.True(t, json.Valid([]byte(outputStr)), "output should be valid JSON")
		var outputMap map[string]any
		require.NoError(t, json.Unmarshal([]byte(outputStr), &outputMap))
		assert.Contains(t, outputMap, "input")
		assert.Contains(t, outputMap, "stepOutput")
		assert.Contains(t, outputMap, "recvOutput")
		assert.Contains(t, outputMap, "eventOutput")
	})

	// 2. Queue path: direct DB insert with status=ENQUEUED + queue_name, let the queue runner dequeue.
	t.Run("DirectDBInsertQueue", func(t *testing.T) {
		workflowID := "interop-queue-" + t.Name()
		queueName := "portable-interop-queue"
		insertPortableWorkflow(t, workflowID, string(WorkflowStatusEnqueued), &queueName)

		retrievedHandle, err := RetrieveWorkflow[InteropResult](executor, workflowID)
		require.NoError(t, err)
		result, err := retrievedHandle.GetResult()
		require.NoError(t, err)
		verifyResult(t, result)
	})

	// 3. DBOSAdmin enqueue path: Go dbosAdmin with PortableWorkflowArgs.
	t.Run("ClientEnqueuePortable", func(t *testing.T) {
		dbosAdmin, err := NewDBOSAdmin(context.Background(), DBOSAdminConfig{
			DatabaseURL: executor.(*dbosContext).config.DatabaseURL,
		})
		require.NoError(t, err)
		t.Cleanup(func() { dbosAdmin.Shutdown(5 * time.Second) })

		portableArgs := PortableWorkflowArgs{
			PositionalArgs: []any{expectedArgs, "extra-positional", 99},
			NamedArgs:      map[string]any{"lang": "python", "debug": true},
		}
		handle, err := Enqueue[PortableWorkflowArgs, InteropResult](dbosAdmin, "portable-interop-queue", "interop_workflow", portableArgs)
		require.NoError(t, err)
		require.NotEmpty(t, handle.GetWorkflowID())

		// Verify the DB has portable_json serialization and the correct envelope
		c := executor.(*dbosContext)
		Kernel := c.kernel
		var storedInputs, storedSerialization string
		selectQuery := Kernel.renderSQL(`SELECT inputs, serialization FROM %sworkflow_status WHERE workflow_uuid = $1`,
			"")
		err = Kernel.pool.QueryRow(context.Background(), selectQuery, handle.GetWorkflowID()).Scan(&storedInputs, &storedSerialization)
		require.NoError(t, err)
		assert.Equal(t, PortableSerializerName, storedSerialization)

		// Verify envelope format — extra positional/named args are preserved in the DB
		var envelope PortableWorkflowArgs
		require.NoError(t, json.Unmarshal([]byte(storedInputs), &envelope))
		assert.Len(t, envelope.PositionalArgs, 3)
		assert.Len(t, envelope.NamedArgs, 2)

		// Wait for execution and verify all paths
		retrievedHandle, err := RetrieveWorkflow[InteropResult](executor, handle.GetWorkflowID())
		require.NoError(t, err)
		result, err := retrievedHandle.GetResult()
		require.NoError(t, err)
		verifyResult(t, result)
	})

	// 4. Wrong-type input: enqueue with a string where InteropArgs is expected.
	// Go's type system catches this during deserialization; the workflow should fail.
	t.Run("WrongTypeInput", func(t *testing.T) {
		workflowID := "interop-wrongtype-" + t.Name()
		queueName := "portable-interop-queue"
		badInputsJSON := `{"positionalArgs":["not-an-object"],"namedArgs":{}}`

		c := executor.(*dbosContext)
		Kernel := c.kernel
		insertQuery := Kernel.renderSQL(`INSERT INTO %sworkflow_status (
			workflow_uuid, status, name, inputs, serialization, queue_name,
			created_at, updated_at, recovery_attempts, executor_id, priority,
			application_version, application_id, authenticated_user, assumed_role, authenticated_roles
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
			"")
		now := time.Now().UnixMilli()
		_, err := Kernel.pool.Exec(context.Background(), insertQuery,
			workflowID, string(WorkflowStatusEnqueued), "interop_workflow", badInputsJSON, PortableSerializerName, &queueName,
			now, now, 0, "local", 0, c.applicationVersion, "", "", "", "[]")
		require.NoError(t, err)

		retrievedHandle, err := RetrieveWorkflow[InteropResult](executor, workflowID)
		require.NoError(t, err)
		_, err = retrievedHandle.GetResult()
		require.Error(t, err)
		var pe *PortableWorkflowError
		require.ErrorAs(t, err, &pe)
		assert.Equal(t, "Portable Error", pe.Name)
		assert.Contains(t, err.Error(), "DBOS Error 10")
	})
}

// TestPortablePerOperationOptions verifies that WithPortableSend, WithPortableSetEvent, and
// WithPortableWriteStream force portable_json serialization for individual operations,
// even when the calling workflow uses the default serializer.
func TestPortablePerOperationOptions(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	type Payload struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}

	payload := Payload{Name: "portable-op", Count: 7}

	c := executor.(*dbosContext)
	Kernel := c.kernel

	// Helper: fetch the serialization recorded in operation_outputs for the Recv step of a workflow.
	// The Recv step stores the serialization of the message it consumed, which reflects what the sender used.
	recvStepSerialization := func(t *testing.T, workflowID string) string {
		t.Helper()
		var ser string
		q := Kernel.renderSQL(`SELECT serialization FROM %soperation_outputs WHERE workflow_uuid = $1 AND function_name = 'DBOS.recv' ORDER BY function_id ASC LIMIT 1`,
			"")
		require.NoError(t, Kernel.pool.QueryRow(context.Background(), q, workflowID).Scan(&ser))
		return ser
	}

	// Helper: fetch the serialization column for a workflow event.
	eventSerialization := func(t *testing.T, workflowID, key string) string {
		t.Helper()
		var ser string
		q := Kernel.renderSQL(`SELECT serialization FROM %sworkflow_events WHERE workflow_uuid = $1 AND key = $2`,
			"")
		require.NoError(t, Kernel.pool.QueryRow(context.Background(), q, workflowID, key).Scan(&ser))
		return ser
	}

	// Helper: fetch the serialization column for the first stream entry (non-sentinel).
	streamSerialization := func(t *testing.T, workflowID, key string) string {
		t.Helper()
		var ser string
		q := Kernel.renderSQL(`SELECT serialization FROM %sstreams WHERE workflow_uuid = $1 AND key = $2 AND value != $3 ORDER BY "offset" LIMIT 1`,
			"")
		require.NoError(t, Kernel.pool.QueryRow(context.Background(), q, workflowID, key, _DBOS_STREAM_CLOSED_SENTINEL).Scan(&ser))
		return ser
	}

	// Workflows must be registered before Launch.
	var (
		portableSendSenderWf   WorkflowFn[string, string]
		portableSendReceiverWf WorkflowFn[string, Payload]
		portableSetterWf       WorkflowFn[string, string]
		portableGetterWf       WorkflowFn[string, Payload]
		portableWriterWf       WorkflowFn[string, string]
	)

	portableSendSenderWf = func(ctx DBOSContext, receiverID string) (string, error) {
		return "", Send(ctx, receiverID, payload, "topic", WithPortableSend())
	}
	portableSendReceiverWf = func(ctx DBOSContext, _ string) (Payload, error) {
		return Recv[Payload](ctx, "topic", 10*time.Second)
	}
	portableSetterWf = func(ctx DBOSContext, _ string) (string, error) {
		return "", SetEvent(ctx, "evt-key", payload, WithPortableSetEvent())
	}
	portableGetterWf = func(ctx DBOSContext, targetID string) (Payload, error) {
		return GetEvent[Payload](ctx, targetID, "evt-key", 10*time.Second)
	}
	portableWriterWf = func(ctx DBOSContext, _ string) (string, error) {
		if err := WriteStream(ctx, "stream-key", payload, WithPortableWriteStream()); err != nil {
			return "", err
		}
		return "", CloseStream(ctx, "stream-key")
	}

	portableSendSenderWfD := NewWorkflow(executor, portableSendSenderWf, WithWorkflowName("portable-op-send-sender"))
	portableSendReceiverWfD := NewWorkflow(executor, portableSendReceiverWf, WithWorkflowName("portable-op-send-receiver"))
	portableSetterWfD := NewWorkflow(executor, portableSetterWf, WithWorkflowName("portable-op-setter"))
	portableGetterWfD := NewWorkflow(executor, portableGetterWf, WithWorkflowName("portable-op-getter"))
	portableWriterWfD := NewWorkflow(executor, portableWriterWf, WithWorkflowName("portable-op-writer"))

	require.NoError(t, Launch(executor))
	defer Shutdown(executor, 10*time.Second)

	// WithPortableSend: a standard workflow sends with portable serialization; a standard
	// workflow Recvs it and gets the correct value back. The serialization recorded in the
	// receiver's Recv step output reflects what the sender used.
	t.Run("WithPortableSend", func(t *testing.T) {
		receiverID := "portable-op-send-receiver-" + t.Name()
		senderID := "portable-op-send-sender-" + t.Name()

		receiverHandle, err := portableSendReceiverWfD(executor, "", WithWorkflowID(receiverID))
		require.NoError(t, err)

		_, err = portableSendSenderWfD(executor, receiverID, WithWorkflowID(senderID))
		require.NoError(t, err)

		result, err := receiverHandle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, payload, result)

		// The Recv step records the serialization of the consumed message in operation_outputs.
		assert.Equal(t, PortableSerializerName, recvStepSerialization(t, receiverID))
	})

	// WithPortableSetEvent: a standard workflow sets an event with portable serialization;
	// a standard GetEvent reads it back correctly.
	t.Run("WithPortableSetEvent", func(t *testing.T) {
		setterID := "portable-op-setter-" + t.Name()
		getterID := "portable-op-getter-" + t.Name()

		setHandle, err := portableSetterWfD(executor, "", WithWorkflowID(setterID))
		require.NoError(t, err)
		_, err = setHandle.GetResult()
		require.NoError(t, err)

		assert.Equal(t, PortableSerializerName, eventSerialization(t, setterID, "evt-key"))

		getHandle, err := portableGetterWfD(executor, setterID, WithWorkflowID(getterID))
		require.NoError(t, err)
		result, err := getHandle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, payload, result)
	})

	// WithPortableWriteStream: a standard workflow writes with portable serialization;
	// ReadStream reads it back correctly.
	t.Run("WithPortableWriteStream", func(t *testing.T) {
		wfID := "portable-op-writer-" + t.Name()

		handle, err := portableWriterWfD(executor, "", WithWorkflowID(wfID))
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.NoError(t, err)

		assert.Equal(t, PortableSerializerName, streamSerialization(t, wfID, "stream-key"))

		values, closed, err := ReadStream[Payload](executor, wfID, "stream-key")
		require.NoError(t, err)
		assert.True(t, closed)
		require.Len(t, values, 1)
		assert.Equal(t, payload, values[0])
	})
}

// TestDirectRunPortableWorkflow tests starting a workflow in portable mode via RunWorkflow,
// verifying the DB contains the correct portable JSON envelope, then recovering it.
func TestDirectRunPortableWorkflow(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	type InteropInput struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	expectedInput := InteropInput{Name: "direct-portable", Value: 99}

	// Simple workflow that returns its input through a step (exercises encode/decode).
	portableEchoWf := func(ctx DBOSContext, input InteropInput) (InteropInput, error) {
		stepOut, err := Run(ctx, func(_ context.Context) (InteropInput, error) {
			return input, nil
		})
		if err != nil {
			return InteropInput{}, err
		}
		return stepOut, nil
	}
	portableEchoWfD := NewWorkflow(executor, portableEchoWf, WithWorkflowName("portable_echo"))

	// Workflow that accepts the full PortableWorkflowArgs envelope directly.
	portableEnvelopeWf := func(ctx DBOSContext, input PortableWorkflowArgs) (PortableWorkflowArgs, error) {
		stepOut, err := Run(ctx, func(_ context.Context) (PortableWorkflowArgs, error) {
			return input, nil
		})
		if err != nil {
			return PortableWorkflowArgs{}, err
		}
		return stepOut, nil
	}
	portableEnvelopeWfD := NewWorkflow(executor, portableEnvelopeWf, WithWorkflowName("portable_envelope"))

	// Workflows for primitive input tests (int, string).
	portableIntEchoWf := func(ctx DBOSContext, input int) (int, error) {
		return Run(ctx, func(_ context.Context) (int, error) { return input, nil })
	}
	portableIntEchoWfD := NewWorkflow(executor, portableIntEchoWf, WithWorkflowName("portable_int_echo"))

	portableStringEchoWf := func(ctx DBOSContext, input string) (string, error) {
		return Run(ctx, func(_ context.Context) (string, error) { return input, nil })
	}
	portableStringEchoWfD := NewWorkflow(executor, portableStringEchoWf, WithWorkflowName("portable_string_echo"))

	// Multi-step workflow for partial recovery test (must register before Launch).
	type PartialRecoveryResult struct {
		StepOut  InteropInput `json:"stepOut"`
		RecvOut  InteropInput `json:"recvOut"`
		EventOut InteropInput `json:"eventOut"`
	}
	multiStepWf := func(ctx DBOSContext, input InteropInput) (PartialRecoveryResult, error) {
		stepOut, err := Run(ctx, func(_ context.Context) (InteropInput, error) {
			return input, nil
		})
		if err != nil {
			return PartialRecoveryResult{}, fmt.Errorf("step: %w", err)
		}

		wfID, err := GetWorkflowID(ctx)
		if err != nil {
			return PartialRecoveryResult{}, err
		}
		if err := Send(ctx, wfID, input, "partial-topic"); err != nil {
			return PartialRecoveryResult{}, fmt.Errorf("send: %w", err)
		}
		recvOut, err := Recv[InteropInput](ctx, "partial-topic", 10*time.Second)
		if err != nil {
			return PartialRecoveryResult{}, fmt.Errorf("recv: %w", err)
		}

		if err := SetEvent(ctx, "partial-key", input); err != nil {
			return PartialRecoveryResult{}, fmt.Errorf("setEvent: %w", err)
		}
		eventOut, err := GetEvent[InteropInput](ctx, wfID, "partial-key", 10*time.Second)
		if err != nil {
			return PartialRecoveryResult{}, fmt.Errorf("getEvent: %w", err)
		}

		return PartialRecoveryResult{StepOut: stepOut, RecvOut: recvOut, EventOut: eventOut}, nil
	}
	multiStepWfD := NewWorkflow(executor, multiStepWf, WithWorkflowName("partial_recovery_wf"))

	require.NoError(t, Launch(executor))
	defer Shutdown(executor, 10*time.Second)

	c := executor.(*dbosContext)
	Kernel := c.kernel

	// Helper: read the stored inputs and serialization from the DB.
	readStoredInputs := func(t *testing.T, workflowID string) (string, string) {
		t.Helper()
		var storedInputs, storedSerialization string
		q := Kernel.renderSQL(`SELECT inputs, serialization FROM %sworkflow_status WHERE workflow_uuid = $1`,
			"")
		err := Kernel.pool.QueryRow(context.Background(), q, workflowID).Scan(&storedInputs, &storedSerialization)
		require.NoError(t, err)
		return storedInputs, storedSerialization
	}

	// Helper: flip a completed workflow back to PENDING for recovery.
	resetToPending := func(t *testing.T, workflowID string) {
		t.Helper()
		schemaPrefix := ""
		q := Kernel.renderSQL(`UPDATE %sworkflow_status SET status = $1, output = NULL, error = NULL WHERE workflow_uuid = $2`, schemaPrefix)
		_, err := Kernel.pool.Exec(context.Background(), q, string(WorkflowStatusPending), workflowID)
		require.NoError(t, err)
		// Also clear operation outputs so the workflow re-executes its steps.
		dq := Kernel.renderSQL(`DELETE FROM %soperation_outputs WHERE workflow_uuid = $1`, schemaPrefix)
		_, err = Kernel.pool.Exec(context.Background(), dq, workflowID)
		require.NoError(t, err)
	}

	// 1. Normal struct input → WithPortableWorkflow → run, verify DB envelope, recover.
	t.Run("NormalInputPortableMode", func(t *testing.T) {
		workflowID := "direct-portable-normal-" + t.Name()
		handle, err := portableEchoWfD(executor, expectedInput,
			WithWorkflowID(workflowID), WithPortableWorkflow())
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, expectedInput, result)

		// Verify the DB has portable_json serialization with the correct envelope.
		storedInputs, storedSerialization := readStoredInputs(t, workflowID)
		assert.Equal(t, PortableSerializerName, storedSerialization)

		var envelope portableArgsRaw
		require.NoError(t, json.Unmarshal([]byte(storedInputs), &envelope))
		assert.Len(t, envelope.PositionalArgs, 1, "expected 1 positional arg")
		// The first positional arg should unmarshal to expectedInput.
		var decoded InteropInput
		require.NoError(t, json.Unmarshal(envelope.PositionalArgs[0], &decoded))
		assert.Equal(t, expectedInput, decoded)

		// Reset workflow to PENDING and recover — should re-execute and produce the same result.
		resetToPending(t, workflowID)
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrieved, err := RetrieveWorkflow[InteropInput](executor, workflowID)
		require.NoError(t, err)
		recoveredResult, err := retrieved.GetResult()
		require.NoError(t, err)
		assert.Equal(t, expectedInput, recoveredResult)
	})

	// 2. PortableWorkflowArgs envelope input → WithPortableWorkflow → run, verify, recover.
	t.Run("EnvelopeInputPortableMode", func(t *testing.T) {
		workflowID := "direct-portable-envelope-" + t.Name()
		envelopeInput := PortableWorkflowArgs{
			PositionalArgs: []any{expectedInput, "extra", 42},
			NamedArgs:      map[string]any{"lang": "go", "debug": false},
		}
		handle, err := portableEnvelopeWfD(executor, envelopeInput,
			WithWorkflowID(workflowID), WithPortableWorkflow())
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		// The workflow receives and returns the full envelope.
		assert.Len(t, result.PositionalArgs, 3)
		assert.Len(t, result.NamedArgs, 2)

		// Verify DB: the stored inputs should be the envelope itself (not double-wrapped).
		storedInputs, storedSerialization := readStoredInputs(t, workflowID)
		assert.Equal(t, PortableSerializerName, storedSerialization)

		var storedEnvelope PortableWorkflowArgs
		require.NoError(t, json.Unmarshal([]byte(storedInputs), &storedEnvelope))
		assert.Len(t, storedEnvelope.PositionalArgs, 3, "envelope should not be double-wrapped")
		assert.Len(t, storedEnvelope.NamedArgs, 2)

		// Reset and recover.
		resetToPending(t, workflowID)
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrieved, err := RetrieveWorkflow[PortableWorkflowArgs](executor, workflowID)
		require.NoError(t, err)
		recoveredResult, err := retrieved.GetResult()
		require.NoError(t, err)
		assert.Len(t, recoveredResult.PositionalArgs, 3)
		assert.Len(t, recoveredResult.NamedArgs, 2)
	})

	// 3. Primitive int input → WithPortableWorkflow → run, verify DB envelope, recover.
	t.Run("PrimitiveIntInputPortableMode", func(t *testing.T) {
		workflowID := "direct-portable-int-" + t.Name()
		handle, err := portableIntEchoWfD(executor, 42,
			WithWorkflowID(workflowID), WithPortableWorkflow())
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, 42, result)

		// Verify DB envelope wraps the primitive as a single positional arg.
		storedInputs, storedSerialization := readStoredInputs(t, workflowID)
		assert.Equal(t, PortableSerializerName, storedSerialization)

		var envelope portableArgsRaw
		require.NoError(t, json.Unmarshal([]byte(storedInputs), &envelope))
		assert.Len(t, envelope.PositionalArgs, 1, "expected 1 positional arg")
		var decoded int
		require.NoError(t, json.Unmarshal(envelope.PositionalArgs[0], &decoded))
		assert.Equal(t, 42, decoded)

		// Recover.
		resetToPending(t, workflowID)
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrieved, err := RetrieveWorkflow[int](executor, workflowID)
		require.NoError(t, err)
		recoveredResult, err := retrieved.GetResult()
		require.NoError(t, err)
		assert.Equal(t, 42, recoveredResult)
	})

	// 4. Primitive string input → WithPortableWorkflow → run, verify DB envelope, recover.
	t.Run("PrimitiveStringInputPortableMode", func(t *testing.T) {
		workflowID := "direct-portable-str-" + t.Name()
		handle, err := portableStringEchoWfD(executor, "hello-portable",
			WithWorkflowID(workflowID), WithPortableWorkflow())
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "hello-portable", result)

		// Verify DB envelope wraps the string as a single positional arg.
		storedInputs, storedSerialization := readStoredInputs(t, workflowID)
		assert.Equal(t, PortableSerializerName, storedSerialization)

		var envelope portableArgsRaw
		require.NoError(t, json.Unmarshal([]byte(storedInputs), &envelope))
		assert.Len(t, envelope.PositionalArgs, 1, "expected 1 positional arg")
		var decoded string
		require.NoError(t, json.Unmarshal(envelope.PositionalArgs[0], &decoded))
		assert.Equal(t, "hello-portable", decoded)

		// Recover.
		resetToPending(t, workflowID)
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrieved, err := RetrieveWorkflow[string](executor, workflowID)
		require.NoError(t, err)
		recoveredResult, err := retrieved.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "hello-portable", recoveredResult)
	})

	// 5. Partial recovery: run a multi-step portable workflow, keep operation_outputs,
	// reset to PENDING. On recovery every step is replayed from stored results using
	// the serialization column in operation_outputs — NOT re-executed.
	t.Run("PartialRecoveryFromStoredSteps", func(t *testing.T) {
		workflowID := "partial-recovery-" + t.Name()
		handle, err := multiStepWfD(executor, expectedInput,
			WithWorkflowID(workflowID), WithPortableWorkflow())
		require.NoError(t, err)
		firstResult, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, expectedInput, firstResult.StepOut)
		assert.Equal(t, expectedInput, firstResult.RecvOut)
		assert.Equal(t, expectedInput, firstResult.EventOut)

		// Verify operation_outputs exist for this workflow.
		var stepCount int
		schemaPrefix := ""
		countQ := Kernel.renderSQL(`SELECT count(*) FROM %soperation_outputs WHERE workflow_uuid = $1`, schemaPrefix)
		require.NoError(t, Kernel.pool.QueryRow(context.Background(), countQ, workflowID).Scan(&stepCount))
		require.Greater(t, stepCount, 0, "expected operation_outputs rows from first execution")

		// Reset to PENDING but KEEP operation_outputs — steps will be replayed from DB.
		resetQ := Kernel.renderSQL(`UPDATE %sworkflow_status SET status = $1, output = NULL, error = NULL WHERE workflow_uuid = $2`, schemaPrefix)
		_, err = Kernel.pool.Exec(context.Background(), resetQ, string(WorkflowStatusPending), workflowID)
		require.NoError(t, err)

		// Recover — each step hits checkOperationExecution and decodes from stored serialization.
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrieved, err := RetrieveWorkflow[PartialRecoveryResult](executor, workflowID)
		require.NoError(t, err)
		recoveredResult, err := retrieved.GetResult()
		require.NoError(t, err)
		assert.Equal(t, expectedInput, recoveredResult.StepOut, "step replayed from DB")
		assert.Equal(t, expectedInput, recoveredResult.RecvOut, "recv replayed from DB")
		assert.Equal(t, expectedInput, recoveredResult.EventOut, "getEvent replayed from DB")
	})
}

func TestPortableWorkflowError(t *testing.T) {
	executor := setupDBOS(t, setupDBOSOptions{dropDB: true, checkLeaks: true})

	// Workflow that runs a step then raises a PortableWorkflowError with all fields set.
	portableErrWf := func(ctx DBOSContext, input string) (string, error) {
		_, err := Run(ctx, func(_ context.Context) (string, error) {
			return input, nil
		})
		if err != nil {
			return "", err
		}
		return "", &PortableWorkflowError{
			Name:    "ValidationError",
			Message: "invalid input: " + input,
			Code:    400,
			Data:    map[string]any{"field": "input"},
		}
	}
	portableErrWfD := NewWorkflow(executor, portableErrWf, WithWorkflowName("portable_err_wf"))

	// Workflow that runs a step that itself fails with a PortableWorkflowError.
	portableStepErrWf := func(ctx DBOSContext, input string) (string, error) {
		return Run(ctx, func(_ context.Context) (string, error) {
			return "", &PortableWorkflowError{
				Name:    "StepError",
				Message: "step failed: " + input,
				Code:    500,
			}
		})
	}
	portableStepErrWfD := NewWorkflow(executor, portableStepErrWf, WithWorkflowName("portable_step_err_wf"))

	// Workflow that runs a step then raises a plain Go error (triggers best-effort conversion).
	plainErrWf := func(ctx DBOSContext, input string) (string, error) {
		_, err := Run(ctx, func(_ context.Context) (string, error) {
			return input, nil
		})
		if err != nil {
			return "", err
		}
		return "", fmt.Errorf("something went wrong: %s", input)
	}
	plainErrWfD := NewWorkflow(executor, plainErrWf, WithWorkflowName("plain_err_portable_wf"))

	require.NoError(t, Launch(executor))
	defer Shutdown(executor, 10*time.Second)

	c := executor.(*dbosContext)
	Kernel := c.kernel

	readStoredError := func(t *testing.T, workflowID string) string {
		t.Helper()
		var storedError *string
		q := Kernel.renderSQL(`SELECT error FROM %sworkflow_status WHERE workflow_uuid = $1`,
			"")
		require.NoError(t, Kernel.pool.QueryRow(context.Background(), q, workflowID).Scan(&storedError))
		require.NotNil(t, storedError)
		return *storedError
	}

	readStoredStepError := func(t *testing.T, workflowID string, stepID int) string {
		t.Helper()
		var storedError *string
		q := Kernel.renderSQL(`SELECT error FROM %soperation_outputs WHERE workflow_uuid = $1 AND function_id = $2`,
			"")
		require.NoError(t, Kernel.pool.QueryRow(context.Background(), q, workflowID, stepID).Scan(&storedError))
		require.NotNil(t, storedError)
		return *storedError
	}

	t.Run("PortableWorkflowErrorFields", func(t *testing.T) {
		wfID := "portable-err-fields"
		handle, err := portableErrWfD(executor, "test-value",
			WithWorkflowID(wfID), WithPortableWorkflow())
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.Error(t, err)

		// Direct handle returns the raw *PortableWorkflowError from the goroutine.
		var pe *PortableWorkflowError
		require.ErrorAs(t, err, &pe)
		assert.Equal(t, "ValidationError", pe.Name)
		assert.Equal(t, "invalid input: test-value", pe.Message)

		// Stored in DB as portable JSON.
		var errData map[string]any
		require.NoError(t, json.Unmarshal([]byte(readStoredError(t, wfID)), &errData))
		assert.Equal(t, "ValidationError", errData["name"])
		assert.Equal(t, "invalid input: test-value", errData["message"])
		assert.Equal(t, float64(400), errData["code"])

		// RetrieveWorkflow goes through DB deserialization — returns *PortableWorkflowError.
		retrieved, err := RetrieveWorkflow[string](executor, wfID)
		require.NoError(t, err)
		_, err = retrieved.GetResult()
		require.Error(t, err)
		var dbPe *PortableWorkflowError
		require.ErrorAs(t, err, &dbPe)
		assert.Equal(t, "ValidationError", dbPe.Name)
		assert.Equal(t, "invalid input: test-value", dbPe.Message)
		assert.Equal(t, float64(400), dbPe.Code) // JSON numbers unmarshal to float64
		dbData, ok := dbPe.Data.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "input", dbData["field"])
	})

	t.Run("PlainErrorBestEffortConversion", func(t *testing.T) {
		wfID := "portable-err-plain"
		handle, err := plainErrWfD(executor, "oops",
			WithWorkflowID(wfID), WithPortableWorkflow())
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.Error(t, err)

		// Stored in DB as portable JSON with best-effort name and message.
		var errData map[string]any
		require.NoError(t, json.Unmarshal([]byte(readStoredError(t, wfID)), &errData))
		assert.Equal(t, "something went wrong: oops", errData["message"])
		assert.Equal(t, "Portable Error", errData["name"])

		// The stored JSON deserializes directly into *PortableWorkflowError.
		var storedPe PortableWorkflowError
		require.NoError(t, json.Unmarshal([]byte(readStoredError(t, wfID)), &storedPe))
		assert.Equal(t, "something went wrong: oops", storedPe.Message)
		assert.Equal(t, "Portable Error", storedPe.Name)

		// RetrieveWorkflow deserializes the portable JSON back to *PortableWorkflowError.
		retrieved, err := RetrieveWorkflow[string](executor, wfID)
		require.NoError(t, err)
		_, err = retrieved.GetResult()
		require.Error(t, err)
		var pe *PortableWorkflowError
		require.ErrorAs(t, err, &pe)
		assert.Equal(t, "something went wrong: oops", pe.Message)
		assert.Equal(t, "Portable Error", pe.Name)
	})

	t.Run("StepPortableWorkflowError", func(t *testing.T) {
		wfID := "portable-step-err"
		handle, err := portableStepErrWfD(executor, "step-input",
			WithWorkflowID(wfID), WithPortableWorkflow())
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.Error(t, err)

		// Step error stored as portable JSON in operation_outputs (step 0).
		var stepErrData map[string]any
		require.NoError(t, json.Unmarshal([]byte(readStoredStepError(t, wfID, 0)), &stepErrData))
		assert.Equal(t, "StepError", stepErrData["name"])
		assert.Equal(t, "step failed: step-input", stepErrData["message"])
		assert.Equal(t, float64(500), stepErrData["code"])

		// GetWorkflowSteps deserializes the step error as *PortableWorkflowError.
		steps, err := GetWorkflowSteps(executor, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		var stepPe *PortableWorkflowError
		require.ErrorAs(t, steps[0].Error, &stepPe)
		assert.Equal(t, "StepError", stepPe.Name)
		assert.Equal(t, "step failed: step-input", stepPe.Message)
		assert.Equal(t, float64(500), stepPe.Code)
	})

	t.Run("ListWorkflowsAndGetWorkflowSteps", func(t *testing.T) {
		wfID := "portable-list-wf"
		handle, err := portableErrWfD(executor, "list-test",
			WithWorkflowID(wfID), WithPortableWorkflow())
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.Error(t, err)

		// ListWorkflows: error goes through errors.New → .Error() → deserializeWorkflowError.
		wfs, err := ListWorkflows(executor, WithWorkflowIDs([]string{wfID}))
		require.NoError(t, err)
		require.Len(t, wfs, 1)
		var listPe *PortableWorkflowError
		require.ErrorAs(t, wfs[0].Error, &listPe)
		assert.Equal(t, "ValidationError", listPe.Name)
		assert.Equal(t, "invalid input: list-test", listPe.Message)
		assert.Equal(t, float64(400), listPe.Code)

		// GetWorkflowSteps: first step succeeds (just echoes input), workflow error is separate.
		steps, err := GetWorkflowSteps(executor, wfID)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		assert.Nil(t, steps[0].Error) // step succeeded; error is on the workflow, not the step
		// Portable step output is returned as raw JSON string (not base64-decoded).
		require.NotNil(t, steps[0].Output)
		assert.Equal(t, `"list-test"`, steps[0].Output)
	})
}
