package dbos

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testAllSerializationPaths[T any](
	t *testing.T,
	executor DbosContext,
	recoveryWorkflow Workflow[T, T],
	input T,
) {
	t.Helper()

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

	startEvent := NewEvent()
	blockingEvent := NewEvent()

	handle, err := recoveryWorkflow(executor, input)
	require.NoError(t, err, "failed to start blocking workflow")
	workflowId := handle.GetWorkflowId()
	recoveryEventRegistry.Store(workflowId, recoveryEvents{startEvent, blockingEvent})
	defer recoveryEventRegistry.Delete(workflowId)

	startEvent.Wait()

	dbosCtx, ok := executor.(*dbosContext)
	require.True(t, ok, "expected dbosContext")
	recoveredHandles, err := recoverPendingWorkflows(dbosCtx, []string{"local"})
	require.NoError(t, err, "failed to recover pending workflows")

	var recoveredHandle *WorkflowHandle[any]
	for _, h := range recoveredHandles {
		if h.GetWorkflowId() == handle.GetWorkflowId() {
			recoveredHandle = h
			break
		}
	}
	require.NotNil(t, recoveredHandle, "expected to find recovered handle")

	blockingEvent.Set()

	expectedOutput := input

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
		h2, err := RetrieveWorkflow[T](executor, handle.GetWorkflowId())
		require.NoError(t, err)
		output, err := h2.GetResult()
		require.NoError(t, err)
		if isNilExpected {
			assert.Nil(t, output, "Retrieved workflow result should be nil")
		} else {
			assert.Equal(t, expectedOutput, output, "Retrieved workflow result should match expected output")
		}
	})

	customSer := getCustomSerializerFromCtx(executor)
	t.Run("GetWorkflowSteps", func(t *testing.T) {
		steps, err := GetWorkflowSteps(executor, handle.GetWorkflowId())
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(steps), 1, "Should have at least one step")
		if len(steps) > 0 {
			lastStep := steps[len(steps)-1]
			if isNilExpected {
				assert.Nil(t, lastStep.Output, "Step output should be nil")
			} else {
				require.NotNil(t, lastStep.Output)
				if customSer != nil {

					assert.Equal(t, expectedOutput, lastStep.Output, "Step output should match expected output")
				} else {

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

	t.Run("ListWorkflows", func(t *testing.T) {
		wfs, err := ListWorkflows(executor,
			WithWorkflowIds([]string{handle.GetWorkflowId()}),
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

				assert.Equal(t, input, wf.Input, "Workflow input should match input")
				assert.Equal(t, expectedOutput, wf.Output, "Workflow output should match expected output")
			} else {

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

	if isNilExpected {
		t.Run("DatabaseNilMarker", func(t *testing.T) {

			dbosCtx, ok := executor.(*dbosContext)
			require.True(t, ok, "expected dbosContext")
			Kernel := dbosCtx.kernel

			ctx := context.Background()
			schemaPrefix := ""
			query := Kernel.renderSql(`SELECT inputs, output FROM %sworkflow_status WHERE workflow_uuid = $1`, schemaPrefix)

			var inputString, outputString *string
			err := Kernel.pool.QueryRow(ctx, query, workflowId).Scan(&inputString, &outputString)
			require.NoError(t, err, "failed to query workflow status")

			require.NotNil(t, inputString, "input should not be NULL in database")
			assert.Equal(t, nilMarker, *inputString, "input should be the nil marker")

			require.NotNil(t, outputString, "output should not be NULL in database")
			assert.Equal(t, nilMarker, *outputString, "output should be the nil marker")

			stepQuery := Kernel.renderSql(`SELECT output FROM %soperation_outputs WHERE workflow_uuid = $1 ORDER BY function_id LIMIT 1`, schemaPrefix)
			var stepOutputString *string
			err = Kernel.pool.QueryRow(ctx, stepQuery, workflowId).Scan(&stepOutputString)
			require.NoError(t, err, "failed to query step output")
			require.NotNil(t, stepOutputString, "step output should not be NULL in database")
			assert.Equal(t, nilMarker, *stepOutputString, "step output should be the nil marker")
		})
	}
}

func testSendRecv[T any](
	t *testing.T,
	executor DbosContext,
	senderWorkflow Workflow[T, T],
	receiverWorkflow Workflow[T, T],
	input T,
) {
	t.Helper()

	receiverHandle, err := receiverWorkflow(executor, input)
	require.NoError(t, err, "Receiver workflow execution failed")

	senderHandle, err := senderWorkflow(executor, input)
	require.NoError(t, err, "Sender workflow execution failed")
	senderDestRegistry.Store(senderHandle.GetWorkflowId(), receiverHandle.GetWorkflowId())
	defer senderDestRegistry.Delete(senderHandle.GetWorkflowId())

	senderResult, err := senderHandle.GetResult()
	require.NoError(t, err, "Sender workflow should complete")

	receiverResult, err := receiverHandle.GetResult()
	require.NoError(t, err, "Receiver workflow should complete")

	assert.Equal(t, input, senderResult, "Sender result should match input")
	assert.Equal(t, input, receiverResult, "Received data should match sent data")
}

func testSetGetEvent[T any](
	t *testing.T,
	executor DbosContext,
	setEventWorkflow Workflow[T, T],
	getEventWorkflow Workflow[string, T],
	input T,
) {
	t.Helper()

	setEventHandle, err := setEventWorkflow(executor, input)
	require.NoError(t, err, "SetEvent workflow execution failed")

	setResult, err := setEventHandle.GetResult()
	require.NoError(t, err, "SetEvent workflow should complete")

	getEventHandle, err := getEventWorkflow(executor, setEventHandle.GetWorkflowId())
	require.NoError(t, err, "GetEvent workflow execution failed")

	getResult, err := getEventHandle.GetResult()
	require.NoError(t, err, "GetEvent workflow should complete")

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
	Id           string
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

	recoveryEmptyStructWorkflow   = makeRecoveryWorkflow[struct{}]()
	recoveryIntSliceSliceWorkflow = makeRecoveryWorkflow[IntSliceSlice]()
	recoveryNestedMapWorkflow     = makeRecoveryWorkflow[map[string]map[string]int]()
	recoveryIntPtrSliceWorkflow   = makeRecoveryWorkflow[[]*int]()
	recoveryAnyWorkflow           = makeRecoveryWorkflow[any]()
)

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

var serializerStreamWorkflow = makeStreamWorkflow[TestWorkflowData]()

func makeStreamWorkflow[T any]() WorkflowFn[T, T] {
	return func(ctx DbosContext, input T) (T, error) {
		if err := WriteStream(ctx, "test-stream", input); err != nil {
			return *new(T), fmt.Errorf("write stream failed: %w", err)
		}
		if err := CloseStream(ctx, "test-stream"); err != nil {
			return *new(T), fmt.Errorf("close stream failed: %w", err)
		}
		return input, nil
	}
}

var senderDestRegistry sync.Map

func makeSenderWorkflow[T any]() WorkflowFn[T, T] {
	return func(ctx DbosContext, input T) (T, error) {
		myId, err := GetWorkflowId(ctx)
		if err != nil {
			return *new(T), fmt.Errorf("failed to get workflow ID: %w", err)
		}
		var destId string
		for {
			if v, ok := senderDestRegistry.Load(myId); ok {
				destId = v.(string)
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		err = Send(ctx, destId, input, "test-topic")
		if err != nil {
			return *new(T), fmt.Errorf("send failed: %w", err)
		}
		return input, nil
	}
}

func makeReceiverWorkflow[T any]() WorkflowFn[T, T] {
	return func(ctx DbosContext, _ T) (T, error) {
		received, err := Recv[T](ctx, "test-topic", 10*time.Second)
		if err != nil {
			return *new(T), fmt.Errorf("recv failed: %w", err)
		}
		return received, nil
	}
}

func makeSetEventWorkflow[T any]() WorkflowFn[T, T] {
	return func(ctx DbosContext, input T) (T, error) {
		err := SetEvent(ctx, "test-key", input)
		if err != nil {
			return *new(T), fmt.Errorf("set event failed: %w", err)
		}
		return input, nil
	}
}

func makeGetEventWorkflow[T any]() WorkflowFn[string, T] {
	return func(ctx DbosContext, targetWorkflowId string) (T, error) {
		event, err := GetEvent[T](ctx, targetWorkflowId, "test-key", 10*time.Second)
		if err != nil {
			return *new(T), fmt.Errorf("get event failed: %w", err)
		}
		return event, nil
	}
}

func makeTestWorkflow[T any]() WorkflowFn[T, T] {
	return func(ctx DbosContext, input T) (T, error) {
		return Run(ctx, func(context context.Context) (T, error) {
			return input, nil
		})
	}
}

func serializerErrorStep(_ context.Context, _ TestWorkflowData) (TestWorkflowData, error) {
	return TestWorkflowData{}, fmt.Errorf("step error")
}

func serializerErrorWorkflow(ctx DbosContext, input TestWorkflowData) (TestWorkflowData, error) {
	return Run(ctx, func(context context.Context) (TestWorkflowData, error) {
		return serializerErrorStep(context, input)
	})
}

type recoveryEvents struct {
	startEvent    *Event
	blockingEvent *Event
}

var recoveryEventRegistry sync.Map

func makeRecoveryWorkflow[T any]() WorkflowFn[T, T] {
	return func(ctx DbosContext, input T) (T, error) {

		firstStepOutput, err := Run(ctx, func(context context.Context) (T, error) {
			return input, nil
		}, WithStepName("FirstStep"))
		if err != nil {
			fmt.Printf("makeRecoveryWorkflow: FirstStep error: %v\n", err)
			return *new(T), err
		}

		return Run(ctx, func(context context.Context) (T, error) {
			workflowId, err := GetWorkflowId(ctx)
			if err != nil {
				return *new(T), fmt.Errorf("failed to get workflow ID: %w", err)
			}
			var events recoveryEvents
			for {
				if v, ok := recoveryEventRegistry.Load(workflowId); ok {
					events = v.(recoveryEvents)
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			events.startEvent.Set()
			events.blockingEvent.Wait()

			return firstStepOutput, nil
		}, WithStepName("BlockingStep"))
	}
}

type TestDataProcessor interface {
	Process(data string) string
}

type TestStringProcessor struct {
	Prefix string
}

func (p *TestStringProcessor) Process(data string) string {
	return p.Prefix + data
}

func TestSerializer(t *testing.T) {
	executor := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	queuedSerializerWorkflow := NewWorkflow(executor, serializerWorkflow)
	recoveryStructPtrWorkflowD := NewWorkflow(executor, recoveryStructPtrWorkflow)
	serializerErrorWorkflowD := NewWorkflow(executor, serializerErrorWorkflow)
	serializerSenderWorkflowD := NewWorkflow(executor, serializerSenderWorkflow)
	serializerReceiverWorkflowD := NewWorkflow(executor, serializerReceiverWorkflow)
	serializerSetEventWorkflowD := NewWorkflow(executor, serializerSetEventWorkflow)
	serializerGetEventWorkflowD := NewWorkflow(executor, serializerGetEventWorkflow)
	serializerStructWorkflowD := NewWorkflow(executor, serializerStructWorkflow)

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

	recoveryEmptyStructWorkflowD := NewWorkflow(executor, recoveryEmptyStructWorkflow)
	recoveryIntSliceSliceWorkflowD := NewWorkflow(executor, recoveryIntSliceSliceWorkflow)
	recoveryNestedMapWorkflowD := NewWorkflow(executor, recoveryNestedMapWorkflow)
	recoveryIntPtrSliceWorkflowD := NewWorkflow(executor, recoveryIntPtrSliceWorkflow)
	recoveryAnyWorkflowD := NewWorkflow(executor, recoveryAnyWorkflow)

	serializerIntSenderWorkflowD := NewWorkflow(executor, serializerIntSenderWorkflow)
	serializerIntReceiverWorkflowD := NewWorkflow(executor, serializerIntReceiverWorkflow)
	serializerIntPtrSenderWorkflowD := NewWorkflow(executor, serializerIntPtrSenderWorkflow)
	serializerIntPtrReceiverWorkflowD := NewWorkflow(executor, serializerIntPtrReceiverWorkflow)
	serializerMyIntSenderWorkflowD := NewWorkflow(executor, serializerMyIntSenderWorkflow)
	serializerMyIntReceiverWorkflowD := NewWorkflow(executor, serializerMyIntReceiverWorkflow)

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

	t.Run("StructValues", func(t *testing.T) {
		strPtr := "pointer value"
		strPtrPtr := &strPtr
		input := TestWorkflowData{
			Id:       "test-id",
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

		testAllSerializationPaths(t, executor, serializerStructWorkflowD, input)
	})

	t.Run("NilStructPointer", func(t *testing.T) {
		testAllSerializationPaths(t, executor, recoveryStructPtrWorkflowD, (*TestWorkflowData)(nil))
	})

	t.Run("Int", func(t *testing.T) {
		testAllSerializationPaths(t, executor, recoveryIntWorkflowD, 0)
	})

	t.Run("EmptyString", func(t *testing.T) {
		testAllSerializationPaths(t, executor, recoveryStringWorkflowD, "")
	})

	t.Run("Pointers", func(t *testing.T) {
		t.Run("NonNil", func(t *testing.T) {
			v := 123
			input := &v
			testAllSerializationPaths(t, executor, recoveryIntPtrWorkflowD, input)

		})

		t.Run("Nil", func(t *testing.T) {
			var input *int = nil
			testAllSerializationPaths(t, executor, recoveryIntPtrWorkflowD, input)
		})
	})

	t.Run("NestedPointers", func(t *testing.T) {
		t.Run("NonNil", func(t *testing.T) {
			v := 123
			ptr := &v
			ptrPtr := &ptr
			testAllSerializationPaths(t, executor, recoveryNestedIntPtrWorkflowD, ptrPtr)

		})

		t.Run("Nil", func(t *testing.T) {
			var ptrPtr **int = nil
			testAllSerializationPaths(t, executor, recoveryNestedIntPtrWorkflowD, ptrPtr)
		})
	})

	t.Run("SlicesAndArrays", func(t *testing.T) {
		t.Run("NonEmptySlice", func(t *testing.T) {
			input := []int{1, 2, 3}
			testAllSerializationPaths(t, executor, recoveryIntSliceWorkflowD, input)
		})

		t.Run("NilSlice", func(t *testing.T) {
			var input []int = nil
			testAllSerializationPaths(t, executor, recoveryIntSliceWorkflowD, input)
		})

		t.Run("Array", func(t *testing.T) {
			input := [3]int{1, 2, 3}
			testAllSerializationPaths(t, executor, recoveryIntArrayWorkflowD, input)
		})
	})

	t.Run("ByteSlices", func(t *testing.T) {
		t.Run("NonEmpty", func(t *testing.T) {
			input := []byte{1, 2, 3, 4, 5}
			testAllSerializationPaths(t, executor, recoveryByteSliceWorkflowD, input)
		})

		t.Run("Nil", func(t *testing.T) {
			var input []byte = nil
			testAllSerializationPaths(t, executor, recoveryByteSliceWorkflowD, input)
		})
	})

	t.Run("Maps", func(t *testing.T) {
		t.Run("NonEmptyMap", func(t *testing.T) {
			input := map[string]int{"x": 1, "y": 2}
			testAllSerializationPaths(t, executor, recoveryStringIntMapWorkflowD, input)
		})

		t.Run("NilMap", func(t *testing.T) {
			var input map[string]int = nil
			testAllSerializationPaths(t, executor, recoveryStringIntMapWorkflowD, input)
		})
	})

	t.Run("CustomTypes", func(t *testing.T) {
		t.Run("MyInt", func(t *testing.T) {
			input := MyInt(7)
			testAllSerializationPaths(t, executor, recoveryMyIntWorkflowD, input)
		})

		t.Run("MyString", func(t *testing.T) {
			input := MyString("zeta")
			testAllSerializationPaths(t, executor, recoveryMyStringWorkflowD, input)
		})

		t.Run("MyStringSlice", func(t *testing.T) {
			input := []MyString{"a", "b"}
			testAllSerializationPaths(t, executor, recoveryMyStringSliceWorkflowD, input)
		})

		t.Run("StringMyIntMap", func(t *testing.T) {
			input := map[string]MyInt{"k": 9}
			testAllSerializationPaths(t, executor, recoveryStringMyIntMapWorkflowD, input)
		})
	})

	t.Run("EmptyStruct", func(t *testing.T) {
		input := struct{}{}
		testAllSerializationPaths(t, executor, recoveryEmptyStructWorkflowD, input)
	})

	t.Run("NestedCollections", func(t *testing.T) {
		t.Run("SliceOfSlices", func(t *testing.T) {
			input := IntSliceSlice{{1, 2}, {3, 4, 5}}
			testAllSerializationPaths(t, executor, recoveryIntSliceSliceWorkflowD, input)
		})

		t.Run("NestedMap", func(t *testing.T) {
			input := map[string]map[string]int{
				"outer1": {"inner1": 1, "inner2": 2},
				"outer2": {"inner3": 3},
			}
			testAllSerializationPaths(t, executor, recoveryNestedMapWorkflowD, input)
		})
	})

	t.Run("SliceOfPointers", func(t *testing.T) {
		t.Run("NonNil", func(t *testing.T) {
			v1 := 10
			v2 := 20
			v3 := 30
			input := []*int{&v1, &v2, &v3}
			testAllSerializationPaths(t, executor, recoveryIntPtrSliceWorkflowD, input)
		})

		t.Run("NilSlice", func(t *testing.T) {
			var input []*int = nil
			testAllSerializationPaths(t, executor, recoveryIntPtrSliceWorkflowD, input)
		})
	})

	t.Run("Any", func(t *testing.T) {

		input := any("test-value")
		testAllSerializationPaths(t, executor, recoveryAnyWorkflowD, input)
	})

	t.Run("ErrorValues", func(t *testing.T) {
		input := TestWorkflowData{
			Id:       "error-test-id",
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

		t.Run("HandleGetResult", func(t *testing.T) {
			_, err := handle.GetResult()
			require.Error(t, err, "Should get step error")
			assert.Contains(t, err.Error(), "step error", "Error message should be preserved")
		})

		t.Run("GetWorkflowSteps", func(t *testing.T) {
			steps, err := GetWorkflowSteps(executor, handle.GetWorkflowId())
			require.NoError(t, err, "Failed to get workflow steps")
			require.Len(t, steps, 1, "Expected 1 step")

			step := steps[0]
			require.NotNil(t, step.Error, "Step should have error")
			assert.Contains(t, step.Error.Error(), "step error", "Step error should be preserved")
		})
	})

	t.Run("SendRecv", func(t *testing.T) {
		strPtr := "sendrecv pointer"
		strPtrPtr := &strPtr
		input := TestWorkflowData{
			Id:       "sendrecv-test-id",
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

		testSendRecv(t, executor, serializerSenderWorkflowD, serializerReceiverWorkflowD, input)
	})

	t.Run("SetGetEvent", func(t *testing.T) {
		strPtr := "event pointer"
		strPtrPtr := &strPtr
		input := TestWorkflowData{
			Id:       "event-test-id",
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

		testSetGetEvent(t, executor, serializerSetEventWorkflowD, serializerGetEventWorkflowD, input)
	})

	t.Run("TypedSendRecvAndSetGetEvent", func(t *testing.T) {

		t.Run("Int", func(t *testing.T) {
			input := 42
			testSendRecv(t, executor, serializerIntSenderWorkflowD, serializerIntReceiverWorkflowD, input)
			testSetGetEvent(t, executor, serializerIntSetEventWorkflowD, serializerIntGetEventWorkflowD, input)
		})

		t.Run("MyInt", func(t *testing.T) {
			input := MyInt(73)
			testSendRecv(t, executor, serializerMyIntSenderWorkflowD, serializerMyIntReceiverWorkflowD, input)
			testSetGetEvent(t, executor, serializerMyIntSetEventWorkflowD, serializerMyIntGetEventWorkflowD, input)
		})

		t.Run("IntPtrSet", func(t *testing.T) {
			v := 99
			input := &v
			testSendRecv(t, executor, serializerIntPtrSenderWorkflowD, serializerIntPtrReceiverWorkflowD, input)
			testSetGetEvent(t, executor, serializerIntPtrSetEventWorkflowD, serializerIntPtrGetEventWorkflowD, input)
		})
	})

	t.Run("QueuedWorkflow", func(t *testing.T) {
		strPtr := "queued pointer"
		strPtrPtr := &strPtr
		input := TestWorkflowData{
			Id:       "queued-test-id",
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

		handle, err := queuedSerializerWorkflow(executor, input)
		require.NoError(t, err, "failed to start queued workflow")

		result, err := handle.GetResult()
		require.NoError(t, err, "queued workflow should complete successfully")
		assert.Equal(t, input, result, "queued workflow result should match input")
	})

	t.Run("WriteReadStream", func(t *testing.T) {
		input := TestWorkflowData{
			Id: "stream-test", Message: "stream data", Value: 111,
			Data:     TestData{Message: "streamed", Value: 222},
			Metadata: map[string]string{"stream": "json"},
		}
		handle, err := serializerStreamWorkflowD(executor, input)
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result)

		values, closed, err := ReadStream[TestWorkflowData](executor, handle.GetWorkflowId(), "test-stream")
		require.NoError(t, err)
		assert.True(t, closed)
		require.Len(t, values, 1)
		assert.Equal(t, input, values[0])
	})
}

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

func TestGobSerializer(t *testing.T) {
	executor := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true, serializer: NewGobSerializer()})

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
			Id:       "gob-test",
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
		testAllSerializationPaths(t, executor, gobRecoveryStructWorkflowD, input)
	})

	t.Run("Int", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryIntWorkflowD, 42)
	})

	t.Run("String", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryStringWorkflowD, "hello gob")
	})

	t.Run("IntSlice", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryIntSliceWorkflowD, []int{1, 2, 3})
	})

	t.Run("Map", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryMapWorkflowD, map[string]int{"x": 1, "y": 2})
	})

	t.Run("MyInt", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryMyIntWorkflowD, MyInt(7))
	})

	// JSON cannot serialize this type. Uses a simple workflow (not recovery-based)
	// because recovery involves step output re-encoding which differs for GobOnly types.
	t.Run("GobOnlyType", func(t *testing.T) {
		input := GobOnlyType{real: 3.14, imag: 2.71, tag: "complex-value"}
		handle, err := gobGobOnlyWorkflowD(executor, input)
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result, "gob-only type should roundtrip correctly")

		h2, err := RetrieveWorkflow[GobOnlyType](executor, handle.GetWorkflowId())
		require.NoError(t, err)
		result2, err := h2.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result2, "gob-only type should roundtrip via RetrieveWorkflow")
	})

	t.Run("SendRecv", func(t *testing.T) {
		input := TestWorkflowData{
			Id: "gob-sendrecv", Message: "gob msg", Value: 99,
			Data:     TestData{Message: "nested", Value: 200},
			Metadata: map[string]string{"comm": "gob"},
		}
		testSendRecv(t, executor, gobSenderWorkflowD, gobReceiverWorkflowD, input)
	})

	t.Run("SetGetEvent", func(t *testing.T) {
		input := TestWorkflowData{
			Id: "gob-event", Message: "gob event", Value: 77,
			Data:     TestData{Message: "event nested", Value: 333},
			Metadata: map[string]string{"type": "gob-event"},
		}
		testSetGetEvent(t, executor, gobSetEventWorkflowD, gobGetEventWorkflowD, input)
	})

	t.Run("GobOnlySendRecv", func(t *testing.T) {
		input := GobOnlyType{real: 1.5, imag: 2.5, tag: "sendrecv"}
		testSendRecv(t, executor, gobGobOnlySenderWorkflowD, gobGobOnlyReceiverWorkflowD, input)
	})

	t.Run("GobOnlySetGetEvent", func(t *testing.T) {
		input := GobOnlyType{real: 9.8, imag: 6.7, tag: "event"}
		testSetGetEvent(t, executor, gobGobOnlySetEventWorkflowD, gobGobOnlyGetEventWorkflowD, input)
	})

	t.Run("WriteReadStream", func(t *testing.T) {
		input := TestWorkflowData{
			Id: "gob-stream", Message: "stream data", Value: 55,
			Data:     TestData{Message: "streamed", Value: 555},
			Metadata: map[string]string{"stream": "gob"},
		}
		handle, err := gobStreamWorkflowD(executor, input)
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result)

		values, closed, err := ReadStream[TestWorkflowData](executor, handle.GetWorkflowId(), "test-stream")
		require.NoError(t, err)
		assert.True(t, closed)
		require.Len(t, values, 1)
		assert.Equal(t, input, values[0])
	})

	t.Run("GobOnlyWriteReadStream", func(t *testing.T) {
		input := GobOnlyType{real: 7.7, imag: 8.8, tag: "streamed"}
		handle, err := gobGobOnlyStreamWorkflowD(executor, input)
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result)

		values, closed, err := ReadStream[GobOnlyType](executor, handle.GetWorkflowId(), "test-stream")
		require.NoError(t, err)
		assert.True(t, closed)
		require.Len(t, values, 1)
		assert.Equal(t, input, values[0])
	})

	t.Run("QueuedWorkflow", func(t *testing.T) {
		input := TestWorkflowData{
			Id: "gob-queued", Message: "queued msg", Value: 88,
			Data:     TestData{Message: "queued", Value: 888},
			Metadata: map[string]string{"type": "gob-queued"},
		}
		handle, err := queuedGobWorkflow(executor, input)
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, input, result)
	})

	t.Run("GobOnlyRecovery", func(t *testing.T) {
		testAllSerializationPaths(t, executor, gobRecoveryGobOnlyWorkflowD, GobOnlyType{real: 5.5, imag: 6.6, tag: "recovered"})
	})
}

func TestPortableInterop(t *testing.T) {
	executor := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	type NestedObj struct {
		Deep bool `json:"deep"`
	}
	type MapArg struct {
		Key1   string    `json:"key1"`
		Key2   int       `json:"key2"`
		Nested NestedObj `json:"nested"`
	}

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

	goldenInputsJson := `{"positionalArgs":[{"str":"hello-interop","num":42,"timestamp":"2025-06-15T10:30:00.000Z","arr":["alpha","beta","gamma"],"obj":{"key1":"value1","key2":99,"nested":{"deep":true}},"flag":true,"nullable":null}],"namedArgs":{"unused_kwarg":"should_be_ignored","another":123}}`

	type InteropResult struct {
		Input        InteropArgs `json:"input"`
		StepOutput   InteropArgs `json:"stepOutput"`
		RecvOutput   InteropArgs `json:"recvOutput"`
		EventOutput  InteropArgs `json:"eventOutput"`
		StreamOutput InteropArgs `json:"streamOutput"`
	}

	portableWf := func(ctx DbosContext, input InteropArgs) (InteropResult, error) {

		stepOut, err := Run(ctx, func(_ context.Context) (InteropArgs, error) {
			return input, nil
		})
		if err != nil {
			return InteropResult{}, fmt.Errorf("step failed: %w", err)
		}

		wfId, err := GetWorkflowId(ctx)
		if err != nil {
			return InteropResult{}, err
		}
		if err := Send(ctx, wfId, input, "test-topic"); err != nil {
			return InteropResult{}, fmt.Errorf("send failed: %w", err)
		}
		recvOut, err := Recv[InteropArgs](ctx, "test-topic", 10*time.Second)
		if err != nil {
			return InteropResult{}, fmt.Errorf("recv failed: %w", err)
		}

		if err := SetEvent(ctx, "test-key", input); err != nil {
			return InteropResult{}, fmt.Errorf("set event failed: %w", err)
		}
		eventOut, err := GetEvent[InteropArgs](ctx, wfId, "test-key", 10*time.Second)
		if err != nil {
			return InteropResult{}, fmt.Errorf("get event failed: %w", err)
		}

		if err := WriteStream(ctx, "test-stream", input); err != nil {
			return InteropResult{}, fmt.Errorf("write stream failed: %w", err)
		}
		if err := CloseStream(ctx, "test-stream"); err != nil {
			return InteropResult{}, fmt.Errorf("close stream failed: %w", err)
		}
		streamValues, closed, err := ReadStream[InteropArgs](ctx, wfId, "test-stream")
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

	insertPortableWorkflow := func(t *testing.T, workflowId, status string, queueName *string) {
		t.Helper()
		c := executor.(*dbosContext)
		Kernel := c.kernel
		insertQuery := Kernel.renderSql(`INSERT INTO %sworkflow_status (
			workflow_uuid, status, name, inputs, serialization, queue_name,
			created_at, updated_at, recovery_attempts, executor_id, priority,
			application_version, application_id, authenticated_user, assumed_role, authenticated_roles
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
			"")
		now := time.Now().UnixMilli()
		_, err := Kernel.pool.Exec(context.Background(), insertQuery,
			workflowId, status, "interop_workflow", goldenInputsJson, PortableSerializerName, queueName,
			now, now, 0, "local", 0, c.applicationVersion, "", "", "", "[]")
		require.NoError(t, err)
	}

	verifyResult := func(t *testing.T, result InteropResult) {
		t.Helper()
		assert.Equal(t, expectedArgs, result.Input, "workflow input")
		assert.Equal(t, expectedArgs, result.StepOutput, "step output")
		assert.Equal(t, expectedArgs, result.RecvOutput, "recv output")
		assert.Equal(t, expectedArgs, result.EventOutput, "event output")
		assert.Equal(t, expectedArgs, result.StreamOutput, "stream output")
	}

	t.Run("DirectDBInsertRecovery", func(t *testing.T) {
		workflowId := "interop-recovery-" + t.Name()
		insertPortableWorkflow(t, workflowId, string(WorkflowStatusPending), nil)

		c := executor.(*dbosContext)
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)

		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrievedHandle, err := RetrieveWorkflow[InteropResult](executor, workflowId)
		require.NoError(t, err)
		result, err := retrievedHandle.GetResult()
		require.NoError(t, err)
		verifyResult(t, result)

		wfs, err := ListWorkflows(executor,
			WithWorkflowIds([]string{workflowId}),
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

	t.Run("DirectDBInsertQueue", func(t *testing.T) {
		workflowId := "interop-queue-" + t.Name()
		queueName := "portable-interop-queue"
		insertPortableWorkflow(t, workflowId, string(WorkflowStatusEnqueued), &queueName)

		retrievedHandle, err := RetrieveWorkflow[InteropResult](executor, workflowId)
		require.NoError(t, err)
		result, err := retrievedHandle.GetResult()
		require.NoError(t, err)
		verifyResult(t, result)
	})

	t.Run("ClientEnqueuePortable", func(t *testing.T) {
		dbosAdmin, err := NewDbosAdmin(context.Background(), DbosAdminConfig{
			DatabaseUrl: executor.(*dbosContext).config.DatabaseUrl,
		})
		require.NoError(t, err)
		t.Cleanup(func() { dbosAdmin.Shutdown(5 * time.Second) })

		portableArgs := PortableWorkflowArgs{
			PositionalArgs: []any{expectedArgs, "extra-positional", 99},
			NamedArgs:      map[string]any{"lang": "python", "debug": true},
		}
		handle, err := Enqueue[PortableWorkflowArgs, InteropResult](dbosAdmin, "portable-interop-queue", "interop_workflow", portableArgs)
		require.NoError(t, err)
		require.NotEmpty(t, handle.GetWorkflowId())

		c := executor.(*dbosContext)
		Kernel := c.kernel
		var storedInputs, storedSerialization string
		selectQuery := Kernel.renderSql(`SELECT inputs, serialization FROM %sworkflow_status WHERE workflow_uuid = $1`,
			"")
		err = Kernel.pool.QueryRow(context.Background(), selectQuery, handle.GetWorkflowId()).Scan(&storedInputs, &storedSerialization)
		require.NoError(t, err)
		assert.Equal(t, PortableSerializerName, storedSerialization)

		var envelope PortableWorkflowArgs
		require.NoError(t, json.Unmarshal([]byte(storedInputs), &envelope))
		assert.Len(t, envelope.PositionalArgs, 3)
		assert.Len(t, envelope.NamedArgs, 2)

		retrievedHandle, err := RetrieveWorkflow[InteropResult](executor, handle.GetWorkflowId())
		require.NoError(t, err)
		result, err := retrievedHandle.GetResult()
		require.NoError(t, err)
		verifyResult(t, result)
	})

	t.Run("WrongTypeInput", func(t *testing.T) {
		workflowId := "interop-wrongtype-" + t.Name()
		queueName := "portable-interop-queue"
		badInputsJson := `{"positionalArgs":["not-an-object"],"namedArgs":{}}`

		c := executor.(*dbosContext)
		Kernel := c.kernel
		insertQuery := Kernel.renderSql(`INSERT INTO %sworkflow_status (
			workflow_uuid, status, name, inputs, serialization, queue_name,
			created_at, updated_at, recovery_attempts, executor_id, priority,
			application_version, application_id, authenticated_user, assumed_role, authenticated_roles
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`,
			"")
		now := time.Now().UnixMilli()
		_, err := Kernel.pool.Exec(context.Background(), insertQuery,
			workflowId, string(WorkflowStatusEnqueued), "interop_workflow", badInputsJson, PortableSerializerName, &queueName,
			now, now, 0, "local", 0, c.applicationVersion, "", "", "", "[]")
		require.NoError(t, err)

		retrievedHandle, err := RetrieveWorkflow[InteropResult](executor, workflowId)
		require.NoError(t, err)
		_, err = retrievedHandle.GetResult()
		require.Error(t, err)
		var pe *PortableWorkflowError
		require.ErrorAs(t, err, &pe)
		assert.Equal(t, "Portable Error", pe.Name)
		assert.Contains(t, err.Error(), "DBOS Error 10")
	})
}

func TestPortablePerOperationOptions(t *testing.T) {
	executor := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	type Payload struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}

	payload := Payload{Name: "portable-op", Count: 7}

	c := executor.(*dbosContext)
	Kernel := c.kernel

	recvStepSerialization := func(t *testing.T, workflowId string) string {
		t.Helper()
		var ser string
		q := Kernel.renderSql(`SELECT serialization FROM %soperation_outputs WHERE workflow_uuid = $1 AND function_name = 'DBOS.recv' ORDER BY function_id ASC LIMIT 1`,
			"")
		require.NoError(t, Kernel.pool.QueryRow(context.Background(), q, workflowId).Scan(&ser))
		return ser
	}

	eventSerialization := func(t *testing.T, workflowId, key string) string {
		t.Helper()
		var ser string
		q := Kernel.renderSql(`SELECT serialization FROM %sworkflow_events WHERE workflow_uuid = $1 AND key = $2`,
			"")
		require.NoError(t, Kernel.pool.QueryRow(context.Background(), q, workflowId, key).Scan(&ser))
		return ser
	}

	streamSerialization := func(t *testing.T, workflowId, key string) string {
		t.Helper()
		var ser string
		q := Kernel.renderSql(`SELECT serialization FROM %sstreams WHERE workflow_uuid = $1 AND key = $2 AND value != $3 ORDER BY "offset" LIMIT 1`,
			"")
		require.NoError(t, Kernel.pool.QueryRow(context.Background(), q, workflowId, key, _dbosStreamClosedSentinel).Scan(&ser))
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

	portableSendSenderWf = func(ctx DbosContext, receiverId string) (string, error) {
		return "", Send(ctx, receiverId, payload, "topic", WithPortableSend())
	}
	portableSendReceiverWf = func(ctx DbosContext, _ string) (Payload, error) {
		return Recv[Payload](ctx, "topic", 10*time.Second)
	}
	portableSetterWf = func(ctx DbosContext, _ string) (string, error) {
		return "", SetEvent(ctx, "evt-key", payload, WithPortableSetEvent())
	}
	portableGetterWf = func(ctx DbosContext, targetId string) (Payload, error) {
		return GetEvent[Payload](ctx, targetId, "evt-key", 10*time.Second)
	}
	portableWriterWf = func(ctx DbosContext, _ string) (string, error) {
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

	t.Run("WithPortableSend", func(t *testing.T) {
		receiverHandle, err := portableSendReceiverWfD(executor, "")
		require.NoError(t, err)
		receiverId := receiverHandle.GetWorkflowId()

		_, err = portableSendSenderWfD(executor, receiverId)
		require.NoError(t, err)

		result, err := receiverHandle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, payload, result)

		assert.Equal(t, PortableSerializerName, recvStepSerialization(t, receiverId))
	})

	t.Run("WithPortableSetEvent", func(t *testing.T) {
		setHandle, err := portableSetterWfD(executor, "")
		require.NoError(t, err)
		setterId := setHandle.GetWorkflowId()
		_, err = setHandle.GetResult()
		require.NoError(t, err)

		assert.Equal(t, PortableSerializerName, eventSerialization(t, setterId, "evt-key"))

		getHandle, err := portableGetterWfD(executor, setterId)
		require.NoError(t, err)
		result, err := getHandle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, payload, result)
	})

	t.Run("WithPortableWriteStream", func(t *testing.T) {
		handle, err := portableWriterWfD(executor, "")
		require.NoError(t, err)
		wfId := handle.GetWorkflowId()
		_, err = handle.GetResult()
		require.NoError(t, err)

		assert.Equal(t, PortableSerializerName, streamSerialization(t, wfId, "stream-key"))

		values, closed, err := ReadStream[Payload](executor, wfId, "stream-key")
		require.NoError(t, err)
		assert.True(t, closed)
		require.Len(t, values, 1)
		assert.Equal(t, payload, values[0])
	})
}

func TestDirectRunPortableWorkflow(t *testing.T) {
	executor := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	type InteropInput struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	expectedInput := InteropInput{Name: "direct-portable", Value: 99}

	portableEchoWf := func(ctx DbosContext, input InteropInput) (InteropInput, error) {
		stepOut, err := Run(ctx, func(_ context.Context) (InteropInput, error) {
			return input, nil
		})
		if err != nil {
			return InteropInput{}, err
		}
		return stepOut, nil
	}
	portableEchoWfD := NewWorkflow(executor, portableEchoWf, WithWorkflowName("portable_echo"))

	portableEnvelopeWf := func(ctx DbosContext, input PortableWorkflowArgs) (PortableWorkflowArgs, error) {
		stepOut, err := Run(ctx, func(_ context.Context) (PortableWorkflowArgs, error) {
			return input, nil
		})
		if err != nil {
			return PortableWorkflowArgs{}, err
		}
		return stepOut, nil
	}
	portableEnvelopeWfD := NewWorkflow(executor, portableEnvelopeWf, WithWorkflowName("portable_envelope"))

	portableIntEchoWf := func(ctx DbosContext, input int) (int, error) {
		return Run(ctx, func(_ context.Context) (int, error) { return input, nil })
	}
	portableIntEchoWfD := NewWorkflow(executor, portableIntEchoWf, WithWorkflowName("portable_int_echo"))

	portableStringEchoWf := func(ctx DbosContext, input string) (string, error) {
		return Run(ctx, func(_ context.Context) (string, error) { return input, nil })
	}
	portableStringEchoWfD := NewWorkflow(executor, portableStringEchoWf, WithWorkflowName("portable_string_echo"))

	type PartialRecoveryResult struct {
		StepOut  InteropInput `json:"stepOut"`
		RecvOut  InteropInput `json:"recvOut"`
		EventOut InteropInput `json:"eventOut"`
	}
	multiStepWf := func(ctx DbosContext, input InteropInput) (PartialRecoveryResult, error) {
		stepOut, err := Run(ctx, func(_ context.Context) (InteropInput, error) {
			return input, nil
		})
		if err != nil {
			return PartialRecoveryResult{}, fmt.Errorf("step: %w", err)
		}

		wfId, err := GetWorkflowId(ctx)
		if err != nil {
			return PartialRecoveryResult{}, err
		}
		if err := Send(ctx, wfId, input, "partial-topic"); err != nil {
			return PartialRecoveryResult{}, fmt.Errorf("send: %w", err)
		}
		recvOut, err := Recv[InteropInput](ctx, "partial-topic", 10*time.Second)
		if err != nil {
			return PartialRecoveryResult{}, fmt.Errorf("recv: %w", err)
		}

		if err := SetEvent(ctx, "partial-key", input); err != nil {
			return PartialRecoveryResult{}, fmt.Errorf("setEvent: %w", err)
		}
		eventOut, err := GetEvent[InteropInput](ctx, wfId, "partial-key", 10*time.Second)
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

	readStoredInputs := func(t *testing.T, workflowId string) (string, string) {
		t.Helper()
		var storedInputs, storedSerialization string
		q := Kernel.renderSql(`SELECT inputs, serialization FROM %sworkflow_status WHERE workflow_uuid = $1`,
			"")
		err := Kernel.pool.QueryRow(context.Background(), q, workflowId).Scan(&storedInputs, &storedSerialization)
		require.NoError(t, err)
		return storedInputs, storedSerialization
	}

	resetToPending := func(t *testing.T, workflowId string) {
		t.Helper()
		schemaPrefix := ""
		q := Kernel.renderSql(`UPDATE %sworkflow_status SET status = $1, output = NULL, error = NULL WHERE workflow_uuid = $2`, schemaPrefix)
		_, err := Kernel.pool.Exec(context.Background(), q, string(WorkflowStatusPending), workflowId)
		require.NoError(t, err)

		dq := Kernel.renderSql(`DELETE FROM %soperation_outputs WHERE workflow_uuid = $1`, schemaPrefix)
		_, err = Kernel.pool.Exec(context.Background(), dq, workflowId)
		require.NoError(t, err)
	}

	t.Run("NormalInputPortableMode", func(t *testing.T) {
		handle, err := portableEchoWfD(executor, expectedInput,
			WithPortableWorkflow())
		require.NoError(t, err)
		workflowId := handle.GetWorkflowId()

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, expectedInput, result)

		storedInputs, storedSerialization := readStoredInputs(t, workflowId)
		assert.Equal(t, PortableSerializerName, storedSerialization)

		var envelope portableArgsRaw
		require.NoError(t, json.Unmarshal([]byte(storedInputs), &envelope))
		assert.Len(t, envelope.PositionalArgs, 1, "expected 1 positional arg")

		var decoded InteropInput
		require.NoError(t, json.Unmarshal(envelope.PositionalArgs[0], &decoded))
		assert.Equal(t, expectedInput, decoded)

		resetToPending(t, workflowId)
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrieved, err := RetrieveWorkflow[InteropInput](executor, workflowId)
		require.NoError(t, err)
		recoveredResult, err := retrieved.GetResult()
		require.NoError(t, err)
		assert.Equal(t, expectedInput, recoveredResult)
	})

	t.Run("EnvelopeInputPortableMode", func(t *testing.T) {
		envelopeInput := PortableWorkflowArgs{
			PositionalArgs: []any{expectedInput, "extra", 42},
			NamedArgs:      map[string]any{"lang": "go", "debug": false},
		}
		handle, err := portableEnvelopeWfD(executor, envelopeInput,
			WithPortableWorkflow())
		require.NoError(t, err)
		workflowId := handle.GetWorkflowId()

		result, err := handle.GetResult()
		require.NoError(t, err)

		assert.Len(t, result.PositionalArgs, 3)
		assert.Len(t, result.NamedArgs, 2)

		storedInputs, storedSerialization := readStoredInputs(t, workflowId)
		assert.Equal(t, PortableSerializerName, storedSerialization)

		var storedEnvelope PortableWorkflowArgs
		require.NoError(t, json.Unmarshal([]byte(storedInputs), &storedEnvelope))
		assert.Len(t, storedEnvelope.PositionalArgs, 3, "envelope should not be double-wrapped")
		assert.Len(t, storedEnvelope.NamedArgs, 2)

		resetToPending(t, workflowId)
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrieved, err := RetrieveWorkflow[PortableWorkflowArgs](executor, workflowId)
		require.NoError(t, err)
		recoveredResult, err := retrieved.GetResult()
		require.NoError(t, err)
		assert.Len(t, recoveredResult.PositionalArgs, 3)
		assert.Len(t, recoveredResult.NamedArgs, 2)
	})

	t.Run("PrimitiveIntInputPortableMode", func(t *testing.T) {
		handle, err := portableIntEchoWfD(executor, 42,
			WithPortableWorkflow())
		require.NoError(t, err)
		workflowId := handle.GetWorkflowId()

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, 42, result)

		storedInputs, storedSerialization := readStoredInputs(t, workflowId)
		assert.Equal(t, PortableSerializerName, storedSerialization)

		var envelope portableArgsRaw
		require.NoError(t, json.Unmarshal([]byte(storedInputs), &envelope))
		assert.Len(t, envelope.PositionalArgs, 1, "expected 1 positional arg")
		var decoded int
		require.NoError(t, json.Unmarshal(envelope.PositionalArgs[0], &decoded))
		assert.Equal(t, 42, decoded)

		resetToPending(t, workflowId)
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrieved, err := RetrieveWorkflow[int](executor, workflowId)
		require.NoError(t, err)
		recoveredResult, err := retrieved.GetResult()
		require.NoError(t, err)
		assert.Equal(t, 42, recoveredResult)
	})

	t.Run("PrimitiveStringInputPortableMode", func(t *testing.T) {
		handle, err := portableStringEchoWfD(executor, "hello-portable",
			WithPortableWorkflow())
		require.NoError(t, err)
		workflowId := handle.GetWorkflowId()

		result, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "hello-portable", result)

		storedInputs, storedSerialization := readStoredInputs(t, workflowId)
		assert.Equal(t, PortableSerializerName, storedSerialization)

		var envelope portableArgsRaw
		require.NoError(t, json.Unmarshal([]byte(storedInputs), &envelope))
		assert.Len(t, envelope.PositionalArgs, 1, "expected 1 positional arg")
		var decoded string
		require.NoError(t, json.Unmarshal(envelope.PositionalArgs[0], &decoded))
		assert.Equal(t, "hello-portable", decoded)

		resetToPending(t, workflowId)
		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrieved, err := RetrieveWorkflow[string](executor, workflowId)
		require.NoError(t, err)
		recoveredResult, err := retrieved.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "hello-portable", recoveredResult)
	})

	t.Run("PartialRecoveryFromStoredSteps", func(t *testing.T) {
		handle, err := multiStepWfD(executor, expectedInput,
			WithPortableWorkflow())
		require.NoError(t, err)
		workflowId := handle.GetWorkflowId()
		firstResult, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, expectedInput, firstResult.StepOut)
		assert.Equal(t, expectedInput, firstResult.RecvOut)
		assert.Equal(t, expectedInput, firstResult.EventOut)

		var stepCount int
		schemaPrefix := ""
		countQ := Kernel.renderSql(`SELECT count(*) FROM %soperation_outputs WHERE workflow_uuid = $1`, schemaPrefix)
		require.NoError(t, Kernel.pool.QueryRow(context.Background(), countQ, workflowId).Scan(&stepCount))
		require.Greater(t, stepCount, 0, "expected operation_outputs rows from first execution")

		resetQ := Kernel.renderSql(`UPDATE %sworkflow_status SET status = $1, output = NULL, error = NULL WHERE workflow_uuid = $2`, schemaPrefix)
		_, err = Kernel.pool.Exec(context.Background(), resetQ, string(WorkflowStatusPending), workflowId)
		require.NoError(t, err)

		handles, err := recoverPendingWorkflows(c, []string{"local"})
		require.NoError(t, err)
		require.Len(t, handles, 1)
		_, err = handles[0].GetResult()
		require.NoError(t, err)

		retrieved, err := RetrieveWorkflow[PartialRecoveryResult](executor, workflowId)
		require.NoError(t, err)
		recoveredResult, err := retrieved.GetResult()
		require.NoError(t, err)
		assert.Equal(t, expectedInput, recoveredResult.StepOut, "step replayed from DB")
		assert.Equal(t, expectedInput, recoveredResult.RecvOut, "recv replayed from DB")
		assert.Equal(t, expectedInput, recoveredResult.EventOut, "getEvent replayed from DB")
	})
}

func TestPortableWorkflowError(t *testing.T) {
	executor := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	portableErrWf := func(ctx DbosContext, input string) (string, error) {
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

	portableStepErrWf := func(ctx DbosContext, input string) (string, error) {
		return Run(ctx, func(_ context.Context) (string, error) {
			return "", &PortableWorkflowError{
				Name:    "StepError",
				Message: "step failed: " + input,
				Code:    500,
			}
		})
	}
	portableStepErrWfD := NewWorkflow(executor, portableStepErrWf, WithWorkflowName("portable_step_err_wf"))

	plainErrWf := func(ctx DbosContext, input string) (string, error) {
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

	readStoredError := func(t *testing.T, workflowId string) string {
		t.Helper()
		var storedError *string
		q := Kernel.renderSql(`SELECT error FROM %sworkflow_status WHERE workflow_uuid = $1`,
			"")
		require.NoError(t, Kernel.pool.QueryRow(context.Background(), q, workflowId).Scan(&storedError))
		require.NotNil(t, storedError)
		return *storedError
	}

	readStoredStepError := func(t *testing.T, workflowId string, stepId int) string {
		t.Helper()
		var storedError *string
		q := Kernel.renderSql(`SELECT error FROM %soperation_outputs WHERE workflow_uuid = $1 AND function_id = $2`,
			"")
		require.NoError(t, Kernel.pool.QueryRow(context.Background(), q, workflowId, stepId).Scan(&storedError))
		require.NotNil(t, storedError)
		return *storedError
	}

	t.Run("PortableWorkflowErrorFields", func(t *testing.T) {
		handle, err := portableErrWfD(executor, "test-value",
			WithPortableWorkflow())
		require.NoError(t, err)
		wfId := handle.GetWorkflowId()
		_, err = handle.GetResult()
		require.Error(t, err)

		var pe *PortableWorkflowError
		require.ErrorAs(t, err, &pe)
		assert.Equal(t, "ValidationError", pe.Name)
		assert.Equal(t, "invalid input: test-value", pe.Message)

		var errData map[string]any
		require.NoError(t, json.Unmarshal([]byte(readStoredError(t, wfId)), &errData))
		assert.Equal(t, "ValidationError", errData["name"])
		assert.Equal(t, "invalid input: test-value", errData["message"])
		assert.Equal(t, float64(400), errData["code"])

		retrieved, err := RetrieveWorkflow[string](executor, wfId)
		require.NoError(t, err)
		_, err = retrieved.GetResult()
		require.Error(t, err)
		var dbPe *PortableWorkflowError
		require.ErrorAs(t, err, &dbPe)
		assert.Equal(t, "ValidationError", dbPe.Name)
		assert.Equal(t, "invalid input: test-value", dbPe.Message)
		assert.Equal(t, float64(400), dbPe.Code)
		dbData, ok := dbPe.Data.(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "input", dbData["field"])
	})

	t.Run("PlainErrorBestEffortConversion", func(t *testing.T) {
		handle, err := plainErrWfD(executor, "oops",
			WithPortableWorkflow())
		require.NoError(t, err)
		wfId := handle.GetWorkflowId()
		_, err = handle.GetResult()
		require.Error(t, err)

		var errData map[string]any
		require.NoError(t, json.Unmarshal([]byte(readStoredError(t, wfId)), &errData))
		assert.Equal(t, "something went wrong: oops", errData["message"])
		assert.Equal(t, "Portable Error", errData["name"])

		var storedPe PortableWorkflowError
		require.NoError(t, json.Unmarshal([]byte(readStoredError(t, wfId)), &storedPe))
		assert.Equal(t, "something went wrong: oops", storedPe.Message)
		assert.Equal(t, "Portable Error", storedPe.Name)

		retrieved, err := RetrieveWorkflow[string](executor, wfId)
		require.NoError(t, err)
		_, err = retrieved.GetResult()
		require.Error(t, err)
		var pe *PortableWorkflowError
		require.ErrorAs(t, err, &pe)
		assert.Equal(t, "something went wrong: oops", pe.Message)
		assert.Equal(t, "Portable Error", pe.Name)
	})

	t.Run("StepPortableWorkflowError", func(t *testing.T) {
		handle, err := portableStepErrWfD(executor, "step-input",
			WithPortableWorkflow())
		require.NoError(t, err)
		wfId := handle.GetWorkflowId()
		_, err = handle.GetResult()
		require.Error(t, err)

		var stepErrData map[string]any
		require.NoError(t, json.Unmarshal([]byte(readStoredStepError(t, wfId, 0)), &stepErrData))
		assert.Equal(t, "StepError", stepErrData["name"])
		assert.Equal(t, "step failed: step-input", stepErrData["message"])
		assert.Equal(t, float64(500), stepErrData["code"])

		steps, err := GetWorkflowSteps(executor, wfId)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		var stepPe *PortableWorkflowError
		require.ErrorAs(t, steps[0].Error, &stepPe)
		assert.Equal(t, "StepError", stepPe.Name)
		assert.Equal(t, "step failed: step-input", stepPe.Message)
		assert.Equal(t, float64(500), stepPe.Code)
	})

	t.Run("ListWorkflowsAndGetWorkflowSteps", func(t *testing.T) {
		handle, err := portableErrWfD(executor, "list-test",
			WithPortableWorkflow())
		require.NoError(t, err)
		wfId := handle.GetWorkflowId()
		_, err = handle.GetResult()
		require.Error(t, err)

		wfs, err := ListWorkflows(executor, WithWorkflowIds([]string{wfId}))
		require.NoError(t, err)
		require.Len(t, wfs, 1)
		var listPe *PortableWorkflowError
		require.ErrorAs(t, wfs[0].Error, &listPe)
		assert.Equal(t, "ValidationError", listPe.Name)
		assert.Equal(t, "invalid input: list-test", listPe.Message)
		assert.Equal(t, float64(400), listPe.Code)

		steps, err := GetWorkflowSteps(executor, wfId)
		require.NoError(t, err)
		require.Len(t, steps, 1)
		assert.Nil(t, steps[0].Error)

		require.NotNil(t, steps[0].Output)
		assert.Equal(t, `"list-test"`, steps[0].Output)
	})
}
