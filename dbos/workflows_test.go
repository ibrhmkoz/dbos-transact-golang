package dbos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var idempotencyCounter int64

// without re-invoking. The deduplication key (a durable UUID) makes the start

// runtime-assigned workflow ID without waiting for the result.
func startChildWorkflow[P any, R any](ctx DbosContext, wf Workflow[P, R], input P, opts ...WorkflowOption) (string, error) {
	dedupKey, err := Uuid(ctx)
	if err != nil {
		return "", err
	}
	return Run(ctx, func(context.Context) (string, error) {
		handle, err := wf(ctx, input, append(opts, WithDeduplicationId(dedupKey))...)
		if err != nil {
			return "", err
		}
		return handle.GetWorkflowId(), nil
	}, WithStepName("startChildWorkflow"))
}

type childWorkflowOutcome[R any] struct {
	Id     string
	Result R
}

func callChildWorkflow[P any, R any](ctx DbosContext, wf Workflow[P, R], input P, opts ...WorkflowOption) (string, R, error) {
	var zero R
	dedupKey, err := Uuid(ctx)
	if err != nil {
		return "", zero, err
	}
	outcome, err := Run(ctx, func(context.Context) (childWorkflowOutcome[R], error) {
		handle, err := wf(ctx, input, append(opts, WithDeduplicationId(dedupKey))...)
		if err != nil {
			return childWorkflowOutcome[R]{}, err
		}
		result, err := handle.GetResult()
		if err != nil {
			return childWorkflowOutcome[R]{}, err
		}
		return childWorkflowOutcome[R]{Id: handle.GetWorkflowId(), Result: result}, nil
	}, WithStepName("callChildWorkflow"))
	if err != nil {
		return "", zero, err
	}
	return outcome.Id, outcome.Result, nil
}

func simpleWorkflow(dbosCtx DbosContext, input string) (string, error) {
	return input, nil
}

func simpleWorkflowError(dbosCtx DbosContext, input string) (int, error) {
	return 0, fmt.Errorf("failure")
}

func simpleWorkflowWithStep(dbosCtx DbosContext, input string) (string, error) {
	return Run(dbosCtx, func(ctx context.Context) (string, error) {
		return simpleStep(ctx)
	})
}

func slowWorkflow(dbosCtx DbosContext, sleepTime time.Duration) (string, error) {
	Sleep(dbosCtx, sleepTime)
	return "done", nil
}

func simpleStep(_ context.Context) (string, error) {
	return "from step", nil
}

func simpleStepError(_ context.Context) (string, error) {
	return "", fmt.Errorf("step failure")
}

func stepWithSleep(_ context.Context, duration time.Duration) (string, error) {
	time.Sleep(duration)
	return fmt.Sprintf("from step that slept for %s", duration), nil
}

func simpleWorkflowWithStepError(dbosCtx DbosContext, input string) (string, error) {
	return Run(dbosCtx, func(ctx context.Context) (string, error) {
		return simpleStepError(ctx)
	})
}

func simpleWorkflowWithSchedule(dbosCtx DbosContext, scheduledTime time.Time) (time.Time, error) {
	return scheduledTime, nil
}

func incrementCounter(_ context.Context, value int64) (int64, error) {
	idempotencyCounter += value
	return idempotencyCounter, nil
}

type workflowStruct struct{}

func (w *workflowStruct) simpleWorkflow(dbosCtx DbosContext, input string) (string, error) {
	return simpleWorkflow(dbosCtx, input)
}

func (w workflowStruct) simpleWorkflowValue(dbosCtx DbosContext, input string) (string, error) {
	return input + "-value", nil
}

type TestWorkflowInterface interface {
	Execute(dbosCtx DbosContext, input string) (string, error)
}

type workflowImplementation struct {
	field string
}

func (w *workflowImplementation) Execute(dbosCtx DbosContext, input string) (string, error) {
	return input + "-" + w.field + "-interface", nil
}

func Identity[T any](dbosCtx DbosContext, in T) (T, error) {
	return in, nil
}

func TestResolveWorkflowFunctionName(t *testing.T) {
	t.Run("non-generic workflow does not exercise generic branch", func(t *testing.T) {
		runtimeName := runtime.FuncForPC(reflect.ValueOf(simpleWorkflow).Pointer()).Name()

		require.NotContains(t, runtimeName, "[")
		assert.Equal(t, runtimeName, resolveWorkflowFunctionName(simpleWorkflow))
	})

	t.Run("generic workflow exercises generic branch", func(t *testing.T) {
		runtimeName := runtime.FuncForPC(reflect.ValueOf(Identity[int]).Pointer()).Name()
		baseName := strings.Split(runtimeName, "[")[0]

		require.Contains(t, runtimeName, "[")
		assert.Equal(t, baseName+"[int,int]", resolveWorkflowFunctionName(Identity[int]))
	})
}

func TestCallableWorkflowDefinition(t *testing.T) {
	parallelTest(t)
	producerCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})
	workerCtx := setupDbos(t, setupDbosOptions{dropDB: false, checkLeaks: true})

	globalConcurrency := 7
	retention := 12 * time.Hour
	producerWorkflow := NewWorkflow(producerCtx, simpleWorkflow, WithGlobalConcurrency(globalConcurrency), WithWorkflowRetention(retention), WithWorkflowName("definition-workflow"))
	NewWorkflow(workerCtx, simpleWorkflow, WithGlobalConcurrency(globalConcurrency), WithWorkflowRetention(retention), WithWorkflowName("definition-workflow"))
	NewWorkflow(producerCtx, simpleWorkflowError, WithWorkflowName("other-definition-workflow"))
	registeredWorkflows, err := ListRegisteredWorkflows(producerCtx)
	require.NoError(t, err)
	var registeredDefinition *WorkflowRegistryEntry
	for i := range registeredWorkflows {
		if registeredWorkflows[i].Name == "definition-workflow" {
			registeredDefinition = &registeredWorkflows[i]
			break
		}
	}
	require.NotNil(t, registeredDefinition)
	require.NotNil(t, registeredDefinition.GlobalConcurrency)
	require.Equal(t, globalConcurrency, *registeredDefinition.GlobalConcurrency)
	require.Equal(t, retention, registeredDefinition.Retention)
	require.PanicsWithValue(t, "workflow retention must be greater than 0", func() {
		NewWorkflow(producerCtx, Identity[int], WithWorkflowRetention(0))
	})
	require.Panics(t, func() {
		NewWorkflow(producerCtx, simpleWorkflow, WithWorkflowName("definition-workflow"))
	})

	handle, err := producerWorkflow(producerCtx, "input")
	require.NoError(t, err)
	status, err := handle.GetStatus()
	require.NoError(t, err)
	require.Equal(t, WorkflowStatusPending, status.Status)
	require.Empty(t, status.QueueName)

	require.NoError(t, Launch(workerCtx))
	result, err := handle.GetResult()
	require.NoError(t, err)
	require.Equal(t, "input", result)
}

func TestWorkflowsRegistration(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	simpleWorkflowD := NewWorkflow(dbosCtx, simpleWorkflow)
	simpleWorkflowErrorD := NewWorkflow(dbosCtx, simpleWorkflowError)
	simpleWorkflowWithStepD := NewWorkflow(dbosCtx, simpleWorkflowWithStep)
	simpleWorkflowWithStepErrorD := NewWorkflow(dbosCtx, simpleWorkflowWithStepError)

	s := workflowStruct{}
	sSimpleWorkflowD := NewWorkflow(dbosCtx, s.simpleWorkflow)
	sSimpleWorkflowValueD := NewWorkflow(dbosCtx, s.simpleWorkflowValue)

	workflowIface := TestWorkflowInterface(&workflowImplementation{
		field: "example",
	})
	workflowIfaceExecuteD := NewWorkflow(dbosCtx, workflowIface.Execute)

	identityIntD := NewWorkflow(dbosCtx, Identity[int])
	identityStringD := NewWorkflow(dbosCtx, Identity[string])

	prefix := "hello-"
	closureWorkflow := func(dbosCtx DbosContext, in string) (string, error) {
		return prefix + in, nil
	}
	closureWorkflowD := NewWorkflow(dbosCtx, closureWorkflow)

	anonymousWorkflow := func(dbosCtx DbosContext, in string) (string, error) {
		return "anonymous-" + in, nil
	}
	anonymousWorkflowD := NewWorkflow(dbosCtx, anonymousWorkflow)

	type testCase struct {
		name           string
		workflowFunc   func(DbosContext, string, ...WorkflowOption) (any, error)
		input          string
		expectedResult any
		expectError    bool
		expectedError  string
	}

	tests := []testCase{
		{
			name: "SimpleWorkflow",
			workflowFunc: func(dbosCtx DbosContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := simpleWorkflowD(dbosCtx, input, opts...)
				if err != nil {
					return nil, err
				}
				result, err := handle.GetResult()
				if err != nil {
					return nil, err
				}

				result2, err2 := handle.GetResult()
				if err2 != nil {
					return nil, fmt.Errorf("Second call to GetResult should not error: %w", err2)
				}
				if !reflect.DeepEqual(result, result2) {
					return nil, fmt.Errorf("Second call to GetResult returned different result: %v vs %v", result2, result)
				}
				return result, err
			},
			input:          "echo",
			expectedResult: "echo",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowError",
			workflowFunc: func(dbosCtx DbosContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := simpleWorkflowErrorD(dbosCtx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:         "echo",
			expectError:   true,
			expectedError: "failure",
		},
		{
			name: "SimpleWorkflowWithStep",
			workflowFunc: func(dbosCtx DbosContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := simpleWorkflowWithStepD(dbosCtx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "from step",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowStruct",
			workflowFunc: func(dbosCtx DbosContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := sSimpleWorkflowD(dbosCtx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo",
			expectError:    false,
		},
		{
			name: "ValueReceiverWorkflow",
			workflowFunc: func(dbosCtx DbosContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := sSimpleWorkflowValueD(dbosCtx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo-value",
			expectError:    false,
		},
		{
			name: "interfaceMethodWorkflow",
			workflowFunc: func(dbosCtx DbosContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := workflowIfaceExecuteD(dbosCtx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo-example-interface",
			expectError:    false,
		},
		{
			name: "GenericWorkflow",
			workflowFunc: func(dbosCtx DbosContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := identityIntD(dbosCtx, 42, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "42",
			expectedResult: 42,
			expectError:    false,
		},
		{
			name: "GenericWorkflowWithString",
			workflowFunc: func(dbosCtx DbosContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := identityStringD(dbosCtx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "test-generic",
			expectedResult: "test-generic",
			expectError:    false,
		},
		{
			name: "ClosureWithCapturedState",
			workflowFunc: func(dbosCtx DbosContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := closureWorkflowD(dbosCtx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "world",
			expectedResult: "hello-world",
			expectError:    false,
		},
		{
			name: "AnonymousClosure",
			workflowFunc: func(dbosCtx DbosContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := anonymousWorkflowD(dbosCtx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "test",
			expectedResult: "anonymous-test",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowWithStepError",
			workflowFunc: func(dbosCtx DbosContext, input string, opts ...WorkflowOption) (any, error) {
				handle, err := simpleWorkflowWithStepErrorD(dbosCtx, input, opts...)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:         "echo",
			expectError:   true,
			expectedError: "step failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.workflowFunc(dbosCtx, tc.input)

			if tc.expectError {
				require.Error(t, err, "expected error but got none")
				if tc.expectedError != "" {
					assert.Equal(t, tc.expectedError, err.Error())
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedResult, result)
			}
		})
	}

	t.Run("DoubleRegistrationWithoutName", func(t *testing.T) {

		freshCtx := setupDbos(t, setupDbosOptions{dropDB: false, checkLeaks: true})

		NewWorkflow(freshCtx, simpleWorkflow)

		defer func() {
			r := recover()
			require.NotNil(t, r, "expected panic from double registration but got none")
			dbosErr, ok := r.(*DbosError)
			require.True(t, ok, "expected panic to be *DbosError, got %T", r)
			assert.Equal(t, ConflictingRegistrationError, dbosErr.Code)
		}()
		NewWorkflow(freshCtx, simpleWorkflow)
	})

	t.Run("DoubleRegistrationWithCustomName", func(t *testing.T) {

		freshCtx := setupDbos(t, setupDbosOptions{dropDB: false, checkLeaks: true})

		NewWorkflow(freshCtx, simpleWorkflow, WithWorkflowName("custom-workflow"))

		defer func() {
			r := recover()
			require.NotNil(t, r, "expected panic from double registration with custom name but got none")
			dbosErr, ok := r.(*DbosError)
			require.True(t, ok, "expected panic to be *DbosError, got %T", r)
			assert.Equal(t, ConflictingRegistrationError, dbosErr.Code)
		}()
		NewWorkflow(freshCtx, simpleWorkflow, WithWorkflowName("custom-workflow"))
	})

	t.Run("DifferentWorkflowsSameCustomName", func(t *testing.T) {

		freshCtx := setupDbos(t, setupDbosOptions{dropDB: false, checkLeaks: true})

		NewWorkflow(freshCtx, simpleWorkflow, WithWorkflowName("same-name"))

		defer func() {
			r := recover()
			require.NotNil(t, r, "expected panic from registering different workflows with same custom name but got none")
			dbosErr, ok := r.(*DbosError)
			require.True(t, ok, "expected panic to be *DbosError, got %T", r)
			assert.Equal(t, ConflictingRegistrationError, dbosErr.Code)
		}()
		NewWorkflow(freshCtx, simpleWorkflowError, WithWorkflowName("same-name"))
	})

	t.Run("RegisterAfterLaunchPanics", func(t *testing.T) {

		freshCtx := setupDbos(t, setupDbosOptions{dropDB: false, checkLeaks: true})

		err := Launch(freshCtx)
		require.NoError(t, err)
		defer Shutdown(freshCtx, 10*time.Second)

		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected panic from registration after launch but got none")
			}
		}()
		NewWorkflow(freshCtx, simpleWorkflow)
	})
}

func stepWithinAStep(ctx context.Context) (string, error) {
	return simpleStep(ctx)
}

func stepWithinAStepWorkflow(dbosCtx DbosContext, input string) (string, error) {
	return Run(dbosCtx, func(ctx context.Context) (string, error) {
		return stepWithinAStep(ctx)
	})
}

var stepRetryAttemptCount int

func stepRetryAlwaysFailsStep(_ context.Context) (string, error) {
	stepRetryAttemptCount++
	return "", fmt.Errorf("always fails - attempt %d", stepRetryAttemptCount)
}

var stepIdempotencyCounter int

func stepIdempotencyTest(_ context.Context) (string, error) {
	stepIdempotencyCounter++
	return "", nil
}

func stepRetryWorkflow(dbosCtx DbosContext, input string) (string, error) {
	Run(dbosCtx, func(ctx context.Context) (string, error) {
		return stepIdempotencyTest(ctx)
	})

	return Run(dbosCtx, func(ctx context.Context) (string, error) {
		return stepRetryAlwaysFailsStep(ctx)
	}, WithStepMaxRetries(5), WithBaseInterval(1*time.Millisecond), WithMaxInterval(10*time.Millisecond))
}

func step1(_ context.Context) (string, error) {
	return "", nil
}

func testStepWf1(dbosCtx DbosContext, input string) (string, error) {
	return Run(dbosCtx, step1)
}

func step2(_ context.Context) (string, error) {
	return "", nil
}

func testStepWf2(dbosCtx DbosContext, input string) (string, error) {
	return Run(dbosCtx, step2)
}

func genericStep[T any](_ context.Context, value T) (T, error) {
	return value, nil
}

func genericStepWorkflow(dbosCtx DbosContext, input string) (string, error) {

	result1, err := Run(dbosCtx, func(ctx context.Context) (string, error) {
		return genericStep(ctx, input+"-processed")
	})
	if err != nil {
		return "", err
	}

	result2, err := Run(dbosCtx, func(ctx context.Context) (int, error) {
		return genericStep(ctx, 21)
	})
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s-%d", result1, result2*2), nil
}

func TestSteps(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	stepWithinAStepWorkflowD := NewWorkflow(dbosCtx, stepWithinAStepWorkflow)
	stepRetryWorkflowD := NewWorkflow(dbosCtx, stepRetryWorkflow)
	testStepWf1D := NewWorkflow(dbosCtx, testStepWf1)
	testStepWf2D := NewWorkflow(dbosCtx, testStepWf2)
	genericStepWorkflowD := NewWorkflow(dbosCtx, genericStepWorkflow)

	customNameWorkflow := func(dbosCtx DbosContext, input string) (string, error) {

		result1, err := Run(dbosCtx, func(ctx context.Context) (string, error) {
			return "custom-step-1-result", nil
		}, WithStepName("MyCustomStep1"))
		if err != nil {
			return "", err
		}

		result2, err := Run(dbosCtx, func(ctx context.Context) (string, error) {
			return "custom-step-2-result", nil
		}, WithStepName("MyCustomStep2"))
		if err != nil {
			return "", err
		}

		return result1 + "-" + result2, nil
	}

	customNameWorkflowD := NewWorkflow(dbosCtx, customNameWorkflow)

	type StepInput struct {
		Name      string            `json:"name"`
		Count     int               `json:"count"`
		Active    bool              `json:"active"`
		Metadata  map[string]string `json:"metadata"`
		CreatedAt time.Time         `json:"created_at"`
	}

	type StepOutput struct {
		ProcessedName string    `json:"processed_name"`
		TotalCount    int       `json:"total_count"`
		Success       bool      `json:"success"`
		ProcessedAt   time.Time `json:"processed_at"`
		Details       []string  `json:"details"`
	}

	processUserObjectStep := func(_ context.Context, input StepInput) (StepOutput, error) {

		output := StepOutput{
			ProcessedName: fmt.Sprintf("Processed_%s", input.Name),
			TotalCount:    input.Count * 2,
			Success:       input.Active,
			ProcessedAt:   time.Now(),
			Details:       []string{"step1", "step2", "step3"},
		}

		if input.Metadata == nil {
			return StepOutput{}, fmt.Errorf("metadata map was not properly deserialized")
		}

		return output, nil
	}

	userObjectWorkflow := func(dbosCtx DbosContext, workflowInput string) (string, error) {

		stepInput := StepInput{
			Name:   workflowInput,
			Count:  42,
			Active: true,
			Metadata: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			CreatedAt: time.Now(),
		}

		output, err := Run(dbosCtx, func(ctx context.Context) (StepOutput, error) {
			return processUserObjectStep(ctx, stepInput)
		})
		if err != nil {
			return "", fmt.Errorf("step failed: %w", err)
		}

		if output.ProcessedName == "" {
			return "", fmt.Errorf("output ProcessedName is empty")
		}
		if output.TotalCount != 84 {
			return "", fmt.Errorf("expected TotalCount to be 84, got %d", output.TotalCount)
		}
		if len(output.Details) != 3 {
			return "", fmt.Errorf("expected 3 details, got %d", len(output.Details))
		}

		return "", nil
	}

	userObjectWorkflowD := NewWorkflow(dbosCtx, userObjectWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	t.Run("StepsMustRunInsideWorkflows", func(t *testing.T) {

		_, err := Run(dbosCtx, func(ctx context.Context) (string, error) {
			return simpleStep(ctx)
		})
		require.Error(t, err, "expected error when running step outside of workflow context, but got none")

		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected error to be of type *DbosError, got %T", err)

		require.Equal(t, StepExecutionError, dbosErr.Code, "expected error code to be StepExecutionError, got %v", dbosErr.Code)

		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart, "expected error message to contain %q, but got %q", expectedMessagePart, err.Error())
	})

	t.Run("StepWithinAStepAreJustFunctions", func(t *testing.T) {
		handle, err := stepWithinAStepWorkflowD(dbosCtx, "test")
		require.NoError(t, err, "failed to run step within a step")
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from step within a step")
		assert.Equal(t, "from step", result)

		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowId())
		require.NoError(t, err, "failed to list steps")
		require.Len(t, steps, 1, "expected 1 step, got %d", len(steps))
	})

	t.Run("StepRetryWithExponentialBackoff", func(t *testing.T) {

		stepRetryAttemptCount = 0
		stepIdempotencyCounter = 0

		handle, err := stepRetryWorkflowD(dbosCtx, "test")
		require.NoError(t, err, "failed to start retry workflow")

		_, err = handle.GetResult()
		require.Error(t, err, "expected error from failing workflow but got none")

		assert.Equal(t, 6, stepRetryAttemptCount, "expected 6 attempts")

		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected error to be of type *DbosError, got %T", err)

		assert.Equal(t, MaxStepRetriesExceeded, dbosErr.Code, "expected error code to be MaxStepRetriesExceeded")

		expectedErrorMessage := "has exceeded its maximum of 5 retries"
		assert.Contains(t, dbosErr.Message, expectedErrorMessage, "expected error message to contain expected text")

		for i := 1; i <= 5; i++ {
			expectedMsg := fmt.Sprintf("always fails - attempt %d", i)
			assert.Contains(t, dbosErr.Error(), expectedMsg, "expected joined error to contain expected message")
		}

		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps")

		require.Len(t, steps, 2, "expected 2 recorded steps")

		step := steps[1]
		require.NotNil(t, step.Error, "expected error in recorded step, got none")

		assert.Equal(t, dbosErr.Error(), step.Error.Error(), "expected recorded step error to match joined error")

		assert.Equal(t, 1, stepIdempotencyCounter, "expected idempotency step to be executed only once")
	})

	t.Run("checkStepName", func(t *testing.T) {

		handle1, err := testStepWf1D(dbosCtx, "test-input-1")
		require.NoError(t, err, "failed to run testStepWf1")
		_, err = handle1.GetResult()
		require.NoError(t, err, "failed to get result from testStepWf1")

		handle2, err := testStepWf2D(dbosCtx, "test-input-2")
		require.NoError(t, err, "failed to run testStepWf2")
		_, err = handle2.GetResult()
		require.NoError(t, err, "failed to get result from testStepWf2")

		steps1, err := GetWorkflowSteps(dbosCtx, handle1.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps for testStepWf1")
		require.Len(t, steps1, 1, "expected 1 step in testStepWf1")
		s1 := steps1[0]
		expectedStepName1 := runtime.FuncForPC(reflect.ValueOf(step1).Pointer()).Name()
		assert.Equal(t, expectedStepName1, s1.StepName, "expected step name to match runtime function name")

		steps2, err := GetWorkflowSteps(dbosCtx, handle2.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps for testStepWf2")
		require.Len(t, steps2, 1, "expected 1 step in testStepWf2")
		s2 := steps2[0]
		expectedStepName2 := runtime.FuncForPC(reflect.ValueOf(step2).Pointer()).Name()
		assert.Equal(t, expectedStepName2, s2.StepName, "expected step name to match runtime function name")
	})

	t.Run("customStepNames", func(t *testing.T) {

		handle, err := customNameWorkflowD(dbosCtx, "test-input")
		require.NoError(t, err, "failed to run workflow with custom step names")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from workflow with custom step names")
		assert.Equal(t, "custom-step-1-result-custom-step-2-result", result)

		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 2, "expected 2 steps")

		assert.Equal(t, "MyCustomStep1", steps[0].StepName, "expected first step to have custom name")
		assert.Equal(t, 0, steps[0].StepId)

		assert.Equal(t, "MyCustomStep2", steps[1].StepName, "expected second step to have custom name")
		assert.Equal(t, 1, steps[1].StepId)
	})

	t.Run("stepsOutputEncoding", func(t *testing.T) {

		handle, err := userObjectWorkflowD(dbosCtx, "TestObject")
		require.NoError(t, err, "failed to run workflow with user-defined objects")

		_, err = handle.GetResult()
		require.NoError(t, err, "failed to get result from workflow")

		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 1, "expected 1 step")

		step := steps[0]
		require.NotNil(t, step.Output, "step output should not be nil")
		assert.Nil(t, step.Error)

		var storedOutput StepOutput
		err = json.Unmarshal([]byte(step.Output.(string)), &storedOutput)
		require.NoError(t, err, "failed to decode step output to StepOutput")

		assert.Equal(t, "Processed_TestObject", storedOutput.ProcessedName, "ProcessedName not correctly serialized")
		assert.Equal(t, 84, storedOutput.TotalCount, "TotalCount not correctly serialized")
		assert.True(t, storedOutput.Success, "Success flag not correctly serialized")
		assert.Len(t, storedOutput.Details, 3, "Details array length incorrect")
		assert.Equal(t, []string{"step1", "step2", "step3"}, storedOutput.Details, "Details array not correctly serialized")
		assert.False(t, storedOutput.ProcessedAt.IsZero(), "ProcessedAt timestamp should not be zero")
	})

	t.Run("genericStepFunction", func(t *testing.T) {

		handle, err := genericStepWorkflowD(dbosCtx, "test-input")
		require.NoError(t, err, "failed to run workflow with generic step function")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from workflow with generic step function")
		assert.Equal(t, "test-input-processed-42", result, "expected combined result from both generic steps")

		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 2, "expected 2 steps (one for string, one for int)")
		assert.NotEmpty(t, steps[0].StepName, "first step name should not be empty")
		assert.NotEmpty(t, steps[1].StepName, "second step name should not be empty")
	})
}

func stepReturningStepId(ctx context.Context) (int, error) {
	stepId, err := GetStepId(ctx.(DbosContext))
	if err != nil {
		return -1, err
	}
	return stepId, nil
}

func TestGoRunningStepsInsideGoRoutines(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	t.Run("Go must run steps inside a workflow", func(t *testing.T) {
		_, err := Go(dbosCtx, func(ctx context.Context) (string, error) {
			return stepWithSleep(ctx, 1*time.Second)
		})
		require.Error(t, err, "expected error when running step outside of workflow context, but got none")

		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected error to be of type *DbosError, got %T", err)
		require.Equal(t, StepExecutionError, dbosErr.Code)
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart, "expected error message to contain %q, but got %q", expectedMessagePart, err.Error())
	})

	t.Run("Go must return step error correctly", func(t *testing.T) {
		goWorkflow := func(dbosCtx DbosContext, input string) (string, error) {
			result, _ := Go(dbosCtx, func(ctx context.Context) (string, error) {
				return "", fmt.Errorf("step error")
			})

			resultChan := <-result
			return resultChan.Result, resultChan.Err
		}
		goWorkflowD := NewWorkflow(dbosCtx, goWorkflow)

		handle, err := goWorkflowD(dbosCtx, "test-input")
		require.NoError(t, err, "failed to run go workflow")
		_, err = handle.GetResult()
		require.Error(t, err, "expected error when running step, but got none")
		require.Equal(t, "step error", err.Error())
	})

	t.Run("Go must execute 100 steps simultaneously then return the stepIDs in the correct sequence", func(t *testing.T) {
		const numSteps = 100
		results := make(chan string, numSteps)
		defer close(results)
		resultChans := make([]<-chan StepOutcome[int], 0)

		goWorkflow := func(dbosCtx DbosContext, input string) (string, error) {
			for range numSteps {
				resultChan, err := Go(dbosCtx, func(ctx context.Context) (int, error) {
					return stepReturningStepId(ctx)
				})

				if err != nil {
					return "", err
				}
				resultChans = append(resultChans, resultChan)
			}

			return "", nil
		}
		goWorkflowD := NewWorkflow(dbosCtx, goWorkflow)

		handle, err := goWorkflowD(dbosCtx, "test-input")
		require.NoError(t, err, "failed to run go workflow")
		_, err = handle.GetResult()
		require.NoError(t, err, "failed to get result from go workflow")
		assert.Equal(t, len(resultChans), numSteps, "expected %d results, got %d", numSteps, len(resultChans))
		for i, resultChan := range resultChans {
			res := <-resultChan
			assert.Equal(t, i, res.Result, "expected step ID to be %d, got %d", i, res.Result)
			assert.NoError(t, res.Err, "expected no error, got %v", res.Err)

			res2, ok := <-resultChan
			assert.False(t, ok, "channel should be closed after receiving result")
			assert.Equal(t, StepOutcome[int]{}, res2, "closed channel should return zero value")
		}
	})

	t.Run("Go idempotency", func(t *testing.T) {
		goWorkflow := func(dbosCtx DbosContext, input string) (string, error) {
			channels := make([]chan StepOutcome[string], 0, 10)
			for range 10 {
				ch, err := Go(dbosCtx, func(ctx context.Context) (string, error) {
					return stepWithSleep(ctx, 1*time.Second)
				})
				if err != nil {
					return "", err
				}
				channels = append(channels, ch)
			}
			for _, ch := range channels {
				outcome := <-ch
				if outcome.Err != nil {
					return "", outcome.Err
				}
			}
			return "ok", nil
		}
		goWorkflowD := NewWorkflow(dbosCtx, goWorkflow)

		handle1, err := goWorkflowD(dbosCtx, "test-input")
		require.NoError(t, err, "failed to run go workflow")
		workflowId := handle1.GetWorkflowId()
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first run")

		setWorkflowStatusPending(t, dbosCtx, workflowId)

		handles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, handles, 1, "expected 1 recovered handle")
		require.Equal(t, workflowId, handles[0].GetWorkflowId(), "expected recovered handle to have the same ID as the original workflow")
		handle2 := handles[0]
		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from second run")
		require.Equal(t, result1, result2, "both runs should return the same result")
	})
}

func TestSelect(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	selectWorkflow := func(dbosCtx DbosContext, input string) (string, error) {
		return Select(dbosCtx, []<-chan StepOutcome[string]{})
	}
	selectWorkflowD := NewWorkflow(dbosCtx, selectWorkflow)

	selectBlockStartEvent := NewEvent()
	selectBlockEvent := NewEvent()
	selectCancelWorkflow := func(dbosCtx DbosContext, input string) (string, error) {
		ch1, err := Go(dbosCtx, func(ctx context.Context) (string, error) {
			selectBlockEvent.Wait()
			return "result", nil
		})
		if err != nil {
			return "", err
		}

		selectBlockStartEvent.Set()

		return Select(dbosCtx, []<-chan StepOutcome[string]{ch1})
	}
	selectCancelWorkflowD := NewWorkflow(dbosCtx, selectCancelWorkflow)

	selectIdempotencyWorkflow := func(dbosCtx DbosContext, input string) (string, error) {
		ch1, err := Go(dbosCtx, func(ctx context.Context) (string, error) {
			return "result1", nil
		})
		if err != nil {
			return "", err
		}
		ch2, err := Go(dbosCtx, func(ctx context.Context) (string, error) {
			return "result2", nil
		})
		if err != nil {
			return "", err
		}
		selectedResult, err := Select(dbosCtx, []<-chan StepOutcome[string]{ch1, ch2})
		if err != nil {
			return "", err
		}
		return selectedResult, nil
	}
	selectIdempotencyWorkflowD := NewWorkflow(dbosCtx, selectIdempotencyWorkflow)

	dbosCtx.Launch()

	t.Run("Select must run inside a workflow", func(t *testing.T) {
		ch1, _ := Go(dbosCtx, func(ctx context.Context) (string, error) {
			return "result1", nil
		})
		channels := []<-chan StepOutcome[string]{ch1}
		_, err := Select(dbosCtx, channels)
		require.Error(t, err, "expected error when running Select outside of workflow context, but got none")

		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected error to be of type *DbosError, got %T", err)
		require.Equal(t, StepExecutionError, dbosErr.Code)
		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart, "expected error message to contain %q, but got %q", expectedMessagePart, err.Error())
	})

	t.Run("Select with empty channels slice", func(t *testing.T) {
		handle, err := selectWorkflowD(dbosCtx, "test-input")
		require.NoError(t, err, "failed to run select workflow")
		result, err := handle.GetResult()
		require.NoError(t, err, "expected no error for empty channels")

		assert.Equal(t, "", result)

		steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 0, "expected no steps for empty channels slice")
	})

	t.Run("Select with context cancellation", func(t *testing.T) {

		cancelCtx, cancelFunc := WithCancelCause(dbosCtx)
		defer cancelFunc(nil)

		handle, err := selectCancelWorkflowD(cancelCtx, "test-input")
		require.NoError(t, err, "failed to run select workflow")

		selectBlockStartEvent.Wait()
		selectBlockStartEvent.Clear()

		cancelFunc(nil)

		result, err := handle.GetResult()
		require.Error(t, err, "expected error from cancelled workflow")
		assert.Equal(t, "", result, "expected zero value string when cancelled")

		assert.True(t, errors.Is(err, context.Canceled), "expected context.Canceled error, got: %v", err)

		selectBlockEvent.Set()

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusError, status.Status, "expected workflow status to be WorkflowStatusError")

		// There is a race condition here, so we need to wait for the steps to be recorded
		require.Eventually(t, func() bool {
			steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowId())
			if err != nil {
				return false
			}
			if len(steps) != 2 {
				return false
			}
			return steps[1].StepName == "DBOS.select" && steps[1].StepId == 1
		}, 5*time.Second, 100*time.Millisecond, "expected 2 steps (Go + Select) with DBOS.select at index 1")
	})

	t.Run("Select idempotency", func(t *testing.T) {
		handle1, err := selectIdempotencyWorkflowD(dbosCtx, "test-input")
		require.NoError(t, err, "failed to run select workflow")
		workflowId := handle1.GetWorkflowId()
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first run")

		for i := range 10 {
			setWorkflowStatusPending(t, dbosCtx, workflowId)
			handles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
			require.NoError(t, err, "failed to recover pending workflows (iteration %d)", i+1)
			require.Len(t, handles, 1, "expected 1 recovered handle (iteration %d)", i+1)
			handle2 := handles[0]
			require.Equal(t, workflowId, handle2.GetWorkflowId(), "expected recovered handle to have the same ID as the original workflow")
			result2, err := handle2.GetResult()
			require.NoError(t, err, "failed to get result from run (iteration %d)", i+1)
			require.Equal(t, result1, result2, "run (iteration %d) should return the same result", i+1)
		}

		steps, err := GetWorkflowSteps(dbosCtx, workflowId)
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 3, "expected 3 steps (2 Go + Select)")
		assert.Equal(t, 0, steps[0].StepId, "first step should have StepID 0")
		assert.Equal(t, 1, steps[1].StepId, "second step should have StepID 1")
		assert.Equal(t, "DBOS.select", steps[2].StepName, "third step should be DBOS.select")
		assert.Equal(t, 2, steps[2].StepId, "Select step should have StepID 2")
		var output0 string
		err = json.Unmarshal([]byte(steps[0].Output.(string)), &output0)
		require.NoError(t, err, "failed to decode step 0 output")
		assert.Equal(t, "result1", output0, "first Go step should have output 'result1'")
		var output1 string
		err = json.Unmarshal([]byte(steps[1].Output.(string)), &output1)
		require.NoError(t, err, "failed to decode step 1 output")
		assert.Equal(t, "result2", output1, "second Go step should have output 'result2'")
		var output2 string
		err = json.Unmarshal([]byte(steps[2].Output.(string)), &output2)
		require.NoError(t, err, "failed to decode step 2 output")
		assert.Equal(t, result1, output2, "Select step output should match workflow result")
	})
}

func TestChildWorkflow(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	simpleChildWf := func(dbosCtx DbosContext, input string) (string, error) {
		return Run(dbosCtx, func(ctx context.Context) (string, error) {
			return simpleStep(ctx)
		})
	}
	simpleChildWfD := NewWorkflow(dbosCtx, simpleChildWf)

	childWfForStepTest := func(dbosCtx DbosContext, input string) (string, error) {
		return "child-result", nil
	}
	childWfForStepTestD := NewWorkflow(dbosCtx, childWfForStepTest)

	parentWfForStepTest := func(ctx DbosContext, input string) (string, error) {
		return startChildWorkflow(ctx, childWfForStepTestD, input)
	}
	parentWfForStepTestD := NewWorkflow(dbosCtx, parentWfForStepTest)

	simpleParentWf := func(ctx DbosContext, _ string) (string, error) {
		childId, result, err := callChildWorkflow(ctx, simpleChildWfD, "test-child-input")
		if err != nil {
			return "", fmt.Errorf("failed to call child workflow: %w", err)
		}
		if result != "from step" {
			return "", fmt.Errorf("unexpected child result: %q", result)
		}

		return childId, nil
	}

	simpleParentWfD := NewWorkflow(dbosCtx, simpleParentWf)

	deleteBlockEvent := NewEvent()
	deleteBlockingWf := func(ctx DbosContext, _ string) (string, error) {
		deleteBlockEvent.Wait()
		return "done", nil
	}
	deleteBlockingWfD := NewWorkflow(dbosCtx, deleteBlockingWf)

	deleteLeafWf := func(ctx DbosContext, input string) (string, error) {
		return "leaf:" + input, nil
	}
	deleteLeafWfD := NewWorkflow(dbosCtx, deleteLeafWf)

	deleteMidWf := func(ctx DbosContext, input string) ([]string, error) {
		var ids []string
		for range 2 {
			id, _, err := callChildWorkflow(ctx, deleteLeafWfD, input)
			if err != nil {
				return nil, err
			}
			ids = append(ids, id)
		}
		return ids, nil
	}
	deleteMidWfD := NewWorkflow(dbosCtx, deleteMidWf)

	deleteRootWf := func(ctx DbosContext, input string) ([]string, error) {
		var ids []string
		for range 2 {
			id, leafIds, err := callChildWorkflow(ctx, deleteMidWfD, input)
			if err != nil {
				return nil, err
			}
			ids = append(ids, id)
			ids = append(ids, leafIds...)
		}
		return ids, nil
	}
	deleteRootWfD := NewWorkflow(dbosCtx, deleteRootWf)

	deleteCascadeWf := func(ctx DbosContext, _ string) (string, error) {
		if err := SetEvent(ctx, "cascade-key", "cascade-value"); err != nil {
			return "", err
		}
		if err := WriteStream(ctx, "cascade-stream", "stream-data"); err != nil {
			return "", err
		}
		if err := CloseStream(ctx, "cascade-stream"); err != nil {
			return "", err
		}

		_, err := Recv[string](ctx, "test-topic", 10*time.Second)
		if err != nil {
			return "", err
		}
		return "done", nil
	}
	deleteCascadeWfD := NewWorkflow(dbosCtx, deleteCascadeWf)

	t.Cleanup(func() { deleteBlockEvent.Set() })

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	t.Run("ChildWorkflowCalledWithinStep", func(t *testing.T) {
		parentHandle, err := simpleParentWfD(dbosCtx, "")
		require.NoError(t, err, "failed to start parent workflow")

		childId, err := parentHandle.GetResult()
		require.NoError(t, err, "failed to get result from parent workflow")
		require.NotEmpty(t, childId)

		steps, err := GetWorkflowSteps(dbosCtx, parentHandle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 2)
		require.Equal(t, "DBOS.uuid", steps[0].StepName)
		require.Equal(t, "callChildWorkflow", steps[1].StepName)

		parentStatus, err := parentHandle.GetStatus()
		require.NoError(t, err, "failed to get parent workflow status")
		require.Empty(t, parentStatus.ParentWorkflowId, "top-level parent workflow should have no ParentWorkflowID")

		childHandle, err := RetrieveWorkflow[string](dbosCtx, childId)
		require.NoError(t, err, "failed to retrieve child workflow")
		childResult, err := childHandle.GetResult()
		require.NoError(t, err, "failed to get child workflow result")
		require.Equal(t, "from step", childResult)
		childStatus, err := childHandle.GetStatus()
		require.NoError(t, err, "failed to get child workflow status")
		require.Equal(t, parentHandle.GetWorkflowId(), childStatus.ParentWorkflowId, "child workflow ParentWorkflowID should be parent's workflow ID")
	})

	t.Run("WorkflowCanBeStartedFromStep", func(t *testing.T) {
		handle, err := parentWfForStepTestD(dbosCtx, "test-input")
		require.NoError(t, err, "failed to start parent workflow")

		startedWorkflowId, err := handle.GetResult()
		require.NoError(t, err)
		startedHandle, err := RetrieveWorkflow[string](dbosCtx, startedWorkflowId)
		require.NoError(t, err)
		startedStatus, err := startedHandle.GetStatus()
		require.NoError(t, err)
		require.Equal(t, handle.GetWorkflowId(), startedStatus.ParentWorkflowId)
	})

	t.Run("DeleteCompletedWorkflow", func(t *testing.T) {
		handle, err := simpleChildWfD(dbosCtx, "test-delete")
		require.NoError(t, err)

		result, err := handle.GetResult()
		require.NoError(t, err)
		require.Equal(t, "from step", result)

		err = DeleteWorkflows(dbosCtx, []string{handle.GetWorkflowId()})
		require.NoError(t, err)

		_, err = RetrieveWorkflow[string](dbosCtx, handle.GetWorkflowId())
		require.Error(t, err)
		var dbosErr *DbosError
		require.ErrorAs(t, err, &dbosErr)
		require.Equal(t, NonExistentWorkflowError, dbosErr.Code)
	})

	t.Run("DeletePendingWorkflow", func(t *testing.T) {
		deleteBlockEvent.Clear()
		handle, err := deleteBlockingWfD(dbosCtx, "pending")
		require.NoError(t, err)

		err = DeleteWorkflows(dbosCtx, []string{handle.GetWorkflowId()})
		require.NoError(t, err)

		_, err = RetrieveWorkflow[string](dbosCtx, handle.GetWorkflowId())
		require.Error(t, err)
		var dbosErr *DbosError
		require.ErrorAs(t, err, &dbosErr)
		require.Equal(t, NonExistentWorkflowError, dbosErr.Code)
	})

	t.Run("DeleteNonExistentWorkflowIsNoOp", func(t *testing.T) {
		err := DeleteWorkflows(dbosCtx, []string{"non-existent-delete-wf-id"})
		require.NoError(t, err, "expected no error when deleting non-existent workflow")
	})

	t.Run("DeleteCascadesRelatedData", func(t *testing.T) {
		handle, err := deleteCascadeWfD(dbosCtx, "input")
		require.NoError(t, err)
		wfId := handle.GetWorkflowId()

		err = Send(dbosCtx, wfId, "test-notification", "test-topic")
		require.NoError(t, err)

		_, err = handle.GetResult()
		require.NoError(t, err)

		err = Send(dbosCtx, wfId, "extra-notification", "test-topic")
		require.NoError(t, err)

		Kernel := dbosCtx.(*dbosContext).kernel
		schemaPrefix := ""

		var eventCount, streamCount, notifCount, stepCount int
		err = Kernel.pool.QueryRow(dbosCtx,
			Kernel.renderSql(`SELECT COUNT(*) FROM %sworkflow_events WHERE workflow_uuid = $1`, schemaPrefix),
			wfId).Scan(&eventCount)
		require.NoError(t, err)
		require.Greater(t, eventCount, 0, "expected events to exist before deletion")

		err = Kernel.pool.QueryRow(dbosCtx,
			Kernel.renderSql(`SELECT COUNT(*) FROM %sstreams WHERE workflow_uuid = $1`, schemaPrefix),
			wfId).Scan(&streamCount)
		require.NoError(t, err)
		require.Greater(t, streamCount, 0, "expected stream entries to exist before deletion")

		err = Kernel.pool.QueryRow(dbosCtx,
			Kernel.renderSql(`SELECT COUNT(*) FROM %snotifications WHERE destination_uuid = $1`, schemaPrefix),
			wfId).Scan(&notifCount)
		require.NoError(t, err)
		require.Greater(t, notifCount, 0, "expected notifications to exist before deletion")

		steps, err := GetWorkflowSteps(dbosCtx, wfId)
		require.NoError(t, err)

		// so we just check the floor.
		require.GreaterOrEqual(t, len(steps), 4, "expected at least 4 steps: SetEvent, WriteStream, CloseStream, Recv")

		err = Kernel.pool.QueryRow(dbosCtx,
			Kernel.renderSql(`SELECT COUNT(*) FROM %soperation_outputs WHERE workflow_uuid = $1`, schemaPrefix),
			wfId).Scan(&stepCount)
		require.NoError(t, err)
		require.Greater(t, stepCount, 0, "expected operation_outputs to exist before deletion")

		err = DeleteWorkflows(dbosCtx, []string{wfId})
		require.NoError(t, err)

		err = Kernel.pool.QueryRow(dbosCtx,
			Kernel.renderSql(`SELECT COUNT(*) FROM %sworkflow_events WHERE workflow_uuid = $1`, schemaPrefix),
			wfId).Scan(&eventCount)
		require.NoError(t, err)
		require.Equal(t, 0, eventCount, "expected events to be cascade-deleted")

		err = Kernel.pool.QueryRow(dbosCtx,
			Kernel.renderSql(`SELECT COUNT(*) FROM %sstreams WHERE workflow_uuid = $1`, schemaPrefix),
			wfId).Scan(&streamCount)
		require.NoError(t, err)
		require.Equal(t, 0, streamCount, "expected stream entries to be cascade-deleted")

		err = Kernel.pool.QueryRow(dbosCtx,
			Kernel.renderSql(`SELECT COUNT(*) FROM %snotifications WHERE destination_uuid = $1`, schemaPrefix),
			wfId).Scan(&notifCount)
		require.NoError(t, err)
		require.Equal(t, 0, notifCount, "expected notifications to be cascade-deleted")

		err = Kernel.pool.QueryRow(dbosCtx,
			Kernel.renderSql(`SELECT COUNT(*) FROM %soperation_outputs WHERE workflow_uuid = $1`, schemaPrefix),
			wfId).Scan(&stepCount)
		require.NoError(t, err)
		require.Equal(t, 0, stepCount, "expected operation_outputs to be cascade-deleted")
	})

	t.Run("DeleteWithChildrenThreeLayers", func(t *testing.T) {

		handle, err := deleteRootWfD(dbosCtx, "tree")
		require.NoError(t, err)

		descendantIds, err := handle.GetResult()
		require.NoError(t, err)
		require.Len(t, descendantIds, 6, "expected 2 mid + 4 leaf descendants")

		rootId := handle.GetWorkflowId()
		allIds := append([]string{rootId}, descendantIds...)

		for _, id := range allIds {
			_, err := RetrieveWorkflow[string](dbosCtx, id)
			require.NoError(t, err, "expected workflow %s to exist", id)
		}

		err = DeleteWorkflows(dbosCtx, []string{rootId}, WithDeleteChildren())
		require.NoError(t, err)

		for _, id := range allIds {
			_, err := RetrieveWorkflow[string](dbosCtx, id)
			require.Error(t, err, "expected workflow %s to be deleted", id)
			var dbosErr *DbosError
			require.ErrorAs(t, err, &dbosErr)
			require.Equal(t, NonExistentWorkflowError, dbosErr.Code)
		}
	})

	t.Run("DeleteMultipleWorkflowsWithChildren", func(t *testing.T) {

		h1, err := deleteRootWfD(dbosCtx, "multi-1")
		require.NoError(t, err)
		h2, err := deleteRootWfD(dbosCtx, "multi-2")
		require.NoError(t, err)
		desc1, err := h1.GetResult()
		require.NoError(t, err)
		desc2, err := h2.GetResult()
		require.NoError(t, err)

		root1 := h1.GetWorkflowId()
		root2 := h2.GetWorkflowId()

		allIds := []string{root1, root2}
		allIds = append(allIds, desc1...)
		allIds = append(allIds, desc2...)
		require.Len(t, allIds, 14)

		for _, id := range allIds {
			_, err := RetrieveWorkflow[string](dbosCtx, id)
			require.NoError(t, err, "expected workflow %s to exist", id)
		}

		err = DeleteWorkflows(dbosCtx, []string{root1, root2}, WithDeleteChildren())
		require.NoError(t, err)

		for _, id := range allIds {
			_, err := RetrieveWorkflow[string](dbosCtx, id)
			require.Error(t, err, "expected workflow %s to be deleted", id)
			var dbosErr *DbosError
			require.ErrorAs(t, err, &dbosErr)
			require.Equal(t, NonExistentWorkflowError, dbosErr.Code)
		}
	})
}

func idempotencyWorkflow(dbosCtx DbosContext, input string) (string, error) {
	Run(dbosCtx, func(ctx context.Context) (int64, error) {
		return incrementCounter(ctx, int64(1))
	})
	return input, nil
}

func TestWorkflowIdempotency(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})
	idempotencyWorkflowD := NewWorkflow(dbosCtx, idempotencyWorkflow)

	t.Run("WorkflowExecutedOnlyOnce", func(t *testing.T) {
		idempotencyCounter = 0

		dedupKey := uuid.NewString()
		input := "idempotency-test"

		handle1, err := idempotencyWorkflowD(dbosCtx, input, WithDeduplicationId(dedupKey))
		require.NoError(t, err, "failed to execute workflow first time")
		result1, err := handle1.GetResult()
		require.NoError(t, err, "failed to get result from first execution")

		handle2, err := idempotencyWorkflowD(dbosCtx, input, WithDeduplicationId(dedupKey))
		require.NoError(t, err, "failed to execute workflow second time")
		result2, err := handle2.GetResult()
		require.NoError(t, err, "failed to get result from second execution")

		require.Equal(t, handle1.GetWorkflowId(), handle2.GetWorkflowId())

		require.Equal(t, result1, result2)

		require.Equal(t, int64(1), idempotencyCounter, "expected counter to be 1 (workflow executed only once)")
	})
}

func TestUuid(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	uuidWorkflow := NewWorkflow(dbosCtx, func(ctx DbosContext, _ string) (string, error) {
		return Uuid(ctx)
	}, WithWorkflowName("uuid-workflow"))
	require.NoError(t, Launch(dbosCtx))

	handle, err := uuidWorkflow(dbosCtx, "")
	require.NoError(t, err)
	result, err := handle.GetResult()
	require.NoError(t, err)

	id, err := uuid.Parse(result)
	require.NoError(t, err)
	require.Equal(t, uuid.Version(7), id.Version())

	steps, err := GetWorkflowSteps(dbosCtx, handle.GetWorkflowId())
	require.NoError(t, err)
	require.Len(t, steps, 1)
	require.Equal(t, "DBOS.uuid", steps[0].StepName)

	setWorkflowStatusPending(t, dbosCtx, handle.GetWorkflowId())
	handles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
	require.NoError(t, err)
	require.Len(t, handles, 1)
	replayed, err := handles[0].GetResult()
	require.NoError(t, err)
	require.Equal(t, result, replayed)
}

func TestNoConcurrentWorkflowSameId(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	startedEvent := NewEvent()
	unblockEvent := NewEvent()
	var runCount int64

	blockingWorkflow := func(dbosCtx DbosContext, input string) (string, error) {
		_, err := Run(dbosCtx, func(ctx context.Context) (int64, error) {
			n := atomic.AddInt64(&runCount, 1)
			startedEvent.Set()
			return n, nil
		})
		if err != nil {
			return "", err
		}
		unblockEvent.Wait()
		return "done", nil
	}
	blockingWorkflowD := NewWorkflow(dbosCtx, blockingWorkflow)

	dedupKey := uuid.NewString()

	handle1, err := blockingWorkflowD(dbosCtx, "input", WithDeduplicationId(dedupKey))
	require.NoError(t, err, "failed to start first workflow")

	startedEvent.Wait()

	handle2, err := blockingWorkflowD(dbosCtx, "input", WithDeduplicationId(dedupKey))
	require.NoError(t, err, "failed to run second workflow call")
	require.Equal(t, handle1.GetWorkflowId(), handle2.GetWorkflowId(), "both handles should refer to the same workflow ID")

	unblockEvent.Set()

	result1, err := handle1.GetResult()
	require.NoError(t, err, "failed to get result from first handle")
	result2, err := handle2.GetResult()
	require.NoError(t, err, "failed to get result from second handle")
	require.Equal(t, result1, result2, "both handles should observe the same result")
	require.Equal(t, "done", result1)

	require.Equal(t, int64(1), atomic.LoadInt64(&runCount), "workflow body should run only once")

	status, err := handle1.GetStatus()
	require.NoError(t, err, "failed to get status from first handle")
	require.Equal(t, 1, status.Attempts, "expected number of attempts to be 1")
}

func TestWorkflowRecovery(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	var recoveryCounters []int64

	recoveryWorkflow := func(dbosCtx DbosContext, index int) (int64, error) {

		_, err := Run(dbosCtx, func(ctx context.Context) (int64, error) {
			recoveryCounters[index]++
			return recoveryCounters[index], nil
		}, WithStepName("step-one"))
		if err != nil {
			return 0, err
		}

		_, err = Run(dbosCtx, func(ctx context.Context) (string, error) {
			return fmt.Sprintf("completed-%d", index), nil
		}, WithStepName("step-two"))
		if err != nil {
			return 0, err
		}

		return recoveryCounters[index], nil
	}

	recoveryWorkflowD := NewWorkflow(dbosCtx, recoveryWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	t.Run("WorkflowRecovery", func(t *testing.T) {
		const numWorkflows = 5

		recoveryCounters = make([]int64, numWorkflows)

		handles := make([]*WorkflowHandle[int64], numWorkflows)
		for i := range numWorkflows {
			handle, err := recoveryWorkflowD(dbosCtx, i)
			require.NoError(t, err, "failed to start workflow %d", i)
			handles[i] = handle
		}
		for i := range numWorkflows {
			_, err := handles[i].GetResult()
			require.NoError(t, err, "failed to get result from workflow %d", i)
		}

		for i := range numWorkflows {
			setWorkflowStatusPending(t, dbosCtx, handles[i].GetWorkflowId())
		}
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recoveredHandles, numWorkflows, "expected %d recovered handles, got %d", numWorkflows, len(recoveredHandles))

		recoveredMap := make(map[string]*WorkflowHandle[any])
		for _, h := range recoveredHandles {
			recoveredMap[h.GetWorkflowId()] = h
		}

		for i := range numWorkflows {
			recoveredHandle := recoveredMap[handles[i].GetWorkflowId()]
			require.NotNil(t, recoveredHandle, "workflow %d not found in recovered handles", i)
			result, err := recoveredHandle.GetResult()
			require.NoError(t, err, "failed to get result from recovered workflow %d", i)
			require.Equal(t, float64(1), result.(float64), "workflow %d result should be 1", i)
		}

		for i := range numWorkflows {
			steps, err := GetWorkflowSteps(dbosCtx, handles[i].GetWorkflowId())
			require.NoError(t, err, "failed to get steps for workflow %d", i)
			require.Len(t, steps, 2, "expected 2 steps for workflow %d", i)
			assert.Equal(t, "step-one", steps[0].StepName, "workflow %d first step name", i)
			assert.Equal(t, 0, steps[0].StepId, "workflow %d first step ID", i)
			assert.NotNil(t, steps[0].Output, "workflow %d first step should have output", i)
			assert.Nil(t, steps[0].Error, "workflow %d first step should not have error", i)
			assert.Equal(t, "step-two", steps[1].StepName, "workflow %d second step name", i)
			assert.Equal(t, 1, steps[1].StepId, "workflow %d second step ID", i)
			assert.NotNil(t, steps[1].Output, "workflow %d second step should have output", i)
			assert.Nil(t, steps[1].Error, "workflow %d second step should not have error", i)
		}

		workflowIds := make([]string, numWorkflows)
		for i := range numWorkflows {
			workflowIds[i] = handles[i].GetWorkflowId()
		}
		workflows, err := dbosCtx.(*dbosContext).kernel.listWorkflows(dbosCtx, listWorkflowsDBInput{
			workflowIds: workflowIds,
		})
		require.NoError(t, err, "failed to list workflows")
		require.Len(t, workflows, numWorkflows, "expected %d workflow entries", numWorkflows)
		workflowsById := make(map[string]struct{ Attempts int }, numWorkflows)
		for _, wf := range workflows {
			workflowsById[wf.Id] = struct{ Attempts int }{Attempts: wf.Attempts}
		}
		for i := range numWorkflows {
			wf, ok := workflowsById[handles[i].GetWorkflowId()]
			require.True(t, ok, "workflow %d not found in list result", i)
			require.Equal(t, 2, wf.Attempts, "workflow %d should have 2 attempts after recovery", i)
		}
	})
}

var (
	maxRecoveryAttempts = 20
	recoveryCount       int64
)

func deadLetterQueueWorkflow(ctx DbosContext, input string) (int, error) {
	recoveryCount++
	wfid, err := GetWorkflowId(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get workflow ID: %v", err)
	}
	fmt.Printf("Dead letter queue workflow %s started, recovery count: %d\n", wfid, recoveryCount)
	return 0, nil
}

func infiniteDeadLetterQueueWorkflow(ctx DbosContext, input string) (int, error) {
	return 0, nil
}
func TestWorkflowDeadLetterQueue(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})
	deadLetterQueueWorkflowD := NewWorkflow(dbosCtx, deadLetterQueueWorkflow, WithMaxRetries(maxRecoveryAttempts))
	infiniteDeadLetterQueueWorkflowD := NewWorkflow(dbosCtx, infiniteDeadLetterQueueWorkflow, WithMaxRetries(-1))
	dbosCtx.Launch()

	t.Run("DeadLetterQueueBehavior", func(t *testing.T) {
		recoveryCount = 0

		handle, err := deadLetterQueueWorkflowD(dbosCtx, "test")
		require.NoError(t, err, "failed to start dead letter queue workflow")
		wfId := handle.GetWorkflowId()
		result1, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from initial run")
		require.Equal(t, int64(1), recoveryCount, "expected recovery count 1 after initial run")

		setWorkflowStatusPending(t, dbosCtx, wfId)
		for i := range maxRecoveryAttempts {
			recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
			require.NoError(t, err, "failed to recover pending workflows on attempt %d", i+1)
			require.Len(t, recoveredHandles, 1, "expected 1 recovered handle on attempt %d", i+1)
			require.Equal(t, wfId, recoveredHandles[0].GetWorkflowId(), "expected recovered handle to have the same ID as the original workflow")
			_, err = recoveredHandles[0].GetResult()
			require.NoError(t, err, "failed to get result from recovered handle on attempt %d", i+1)
			expectedCount := int64(i + 2)
			require.Equal(t, expectedCount, recoveryCount, "expected recovery count to be %d, got %d", expectedCount, recoveryCount)
			status, err := recoveredHandles[0].GetStatus()
			require.NoError(t, err, "failed to get status from recovered handle")
			require.Equal(t, int(expectedCount), status.Attempts, "expected number of attempts to be %d, got %d", expectedCount, status.Attempts)
			setWorkflowStatusPending(t, dbosCtx, wfId)
		}

		_, err = recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.Error(t, err, "expected dead letter queue error but got none")
		require.True(t, errors.Is(err, &DbosError{Code: DeadLetterQueueError}), "expected error to be DeadLetterQueueError, got %T", err)

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		require.Equal(t, WorkflowStatusMaxRecoveryAttemptsExceeded, status.Status)

		retrievedHandle, err := RetrieveWorkflow[int](dbosCtx, wfId)
		require.NoError(t, err, "failed to retrieve workflow")
		_, err = retrievedHandle.GetResult()
		require.Error(t, err, "expected dead letter queue error but got none")
		expectedDLQMsg := fmt.Sprintf("Workflow %s has been moved to the dead-letter queue after exceeding the maximum of %d retries", wfId, maxRecoveryAttempts)
		require.Contains(t, err.Error(), expectedDLQMsg, "expected error to mention dead-letter queue, got: %v", err)

		resumedHandle, err := ResumeWorkflow[int](dbosCtx, wfId)
		require.NoError(t, err, "failed to resume workflow")

		result2, err := resumedHandle.GetResult()
		require.NoError(t, err, "failed to get result from resumed handle")
		require.Equal(t, result1, result2)
		setWorkflowStatusPending(t, dbosCtx, wfId)

		// Recover pending workflows again - should work without error
		handles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.Len(t, handles, 1, "expected 1 recovered handle after resume")
		require.Equal(t, resumedHandle.GetWorkflowId(), handles[0].GetWorkflowId(), "expected recovered handle to have the same ID as the resumed handle")
		require.NoError(t, err, "failed to recover pending workflows after resume")

		result3, err := handles[0].GetResult()
		require.NoError(t, err, "failed to get result from resumed handle")

		require.Equal(t, result1, int(result3.(float64)))

		status, err = handle.GetStatus()
		require.NoError(t, err, "failed to get final workflow status")
		require.Equal(t, WorkflowStatusSuccess, status.Status)

	})

	t.Run("InfiniteRetriesWorkflow", func(t *testing.T) {
		// Verify that a workflow with MaxRetries=-1 (infinite retries) can be recovered many times without hitting DLQ
		handle, err := infiniteDeadLetterQueueWorkflowD(dbosCtx, "test")
		require.NoError(t, err, "failed to start infinite dead letter queue workflow")
		wfId := handle.GetWorkflowId()
		result1, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from initial run")
		require.Equal(t, 0, result1)

		const infiniteRetryIterations = 10
		for i := range infiniteRetryIterations {
			setWorkflowStatusPending(t, dbosCtx, wfId)
			recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
			require.NoError(t, err, "failed to recover pending workflows on attempt %d", i+1)
			require.Len(t, recoveredHandles, 1, "expected 1 recovered handle on attempt %d", i+1)
			resultAny, err := recoveredHandles[0].GetResult()
			require.NoError(t, err, "failed to get result from recovered handle on attempt %d", i+1)
			jsonBytes, err := json.Marshal(resultAny)
			require.NoError(t, err, "failed to marshal result to JSON")
			var result int
			err = json.Unmarshal(jsonBytes, &result)
			require.NoError(t, err, "failed to decode result to int")
			require.Equal(t, 0, result, "expected result 0 on attempt %d", i+1)
		}
	})
}

func TestCancelWorkflows(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	blockEvent := NewEvent()
	blockingWorkflow := func(ctx DbosContext, input string) (string, error) {
		blockEvent.Wait()
		return input, nil
	}
	blockingWorkflowD := NewWorkflow(dbosCtx, blockingWorkflow)

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS instance")

	startBlockedWorkflows := func(t *testing.T, n int, prefix string) []string {
		t.Helper()
		ids := make([]string, n)
		for i := range ids {
			h, err := blockingWorkflowD(dbosCtx, fmt.Sprintf("%s-%d", prefix, i))
			require.NoError(t, err, "failed to start workflow %d", i)
			ids[i] = h.GetWorkflowId()
		}
		return ids
	}

	t.Run("CancelWorkflowsBatch", func(t *testing.T) {
		blockEvent.Clear()
		defer blockEvent.Set()
		ids := startBlockedWorkflows(t, 3, "cancel-batch")

		require.NoError(t, CancelWorkflows(dbosCtx, ids), "failed to cancel workflows batch")

		for _, id := range ids {
			handle, err := RetrieveWorkflow[string](dbosCtx, id)
			require.NoError(t, err, "failed to retrieve workflow %s", id)
			status, err := handle.GetStatus()
			require.NoError(t, err, "failed to get status for workflow %s", id)
			assert.Equal(t, WorkflowStatusCancelled, status.Status, "workflow %s should be CANCELLED", id)
			assert.Empty(t, status.QueueName, "workflow %s queue should be cleared", id)
		}
	})

	t.Run("CancelWorkflowsSkipsMissingIDs", func(t *testing.T) {
		blockEvent.Clear()
		defer blockEvent.Set()
		ids := startBlockedWorkflows(t, 1, "cancel-mixed")

		missingId := "missing-" + uuid.NewString()
		require.NoError(t, CancelWorkflows(dbosCtx, []string{missingId, ids[0]}),
			"CancelWorkflows should not error on missing IDs")

		handle, err := RetrieveWorkflow[string](dbosCtx, ids[0])
		require.NoError(t, err, "failed to retrieve workflow")
		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status)
	})

	t.Run("CancelWorkflowsLeavesTerminalUntouched", func(t *testing.T) {
		blockEvent.Set()
		h, err := blockingWorkflowD(dbosCtx, "cancel-terminal")
		require.NoError(t, err, "failed to start workflow")
		_, err = h.GetResult()
		require.NoError(t, err, "workflow should complete successfully")

		require.NoError(t, CancelWorkflows(dbosCtx, []string{h.GetWorkflowId()}),
			"CancelWorkflows should succeed on already-completed workflow")

		status, err := h.GetStatus()
		require.NoError(t, err, "failed to get status")
		assert.Equal(t, WorkflowStatusSuccess, status.Status, "completed workflow should remain SUCCESS")
	})

	t.Run("CancelWorkflowsEmpty", func(t *testing.T) {
		require.NoError(t, CancelWorkflows(dbosCtx, nil),
			"CancelWorkflows with nil slice should be a no-op")
		require.NoError(t, CancelWorkflows(dbosCtx, []string{}),
			"CancelWorkflows with empty slice should be a no-op")
	})

	t.Run("CancelWorkflowsNilContext", func(t *testing.T) {
		require.Error(t, CancelWorkflows(nil, []string{"id"}),
			"CancelWorkflows should error on nil context")
	})
}

var (
	receiveIdempotencyStartEvent = NewEvent()
	sendRecvSyncEvent            = NewEvent()
	numConcurrentRecvWfs         = 5
	concurrentRecvReadyEvents    = make([]*Event, numConcurrentRecvWfs)
	concurrentRecvStartEvent     = NewEvent()
)

type sendWorkflowInput struct {
	DestinationId string
	Topic         string
}

func sendWorkflow(ctx DbosContext, input sendWorkflowInput) (string, error) {
	err := Send(ctx, input.DestinationId, "message1", input.Topic)
	if err != nil {
		return "", err
	}
	err = Send(ctx, input.DestinationId, "message2", input.Topic)
	if err != nil {
		return "", err
	}
	err = Send(ctx, input.DestinationId, "message3", input.Topic)
	if err != nil {
		return "", err
	}
	return "", nil
}

func receiveWorkflow(ctx DbosContext, input struct {
	Topic   string
	Timeout time.Duration
}) (string, error) {
	logger := ctx.(*dbosContext).logger

	sendRecvSyncEvent.Wait()

	msg1, err := Recv[string](ctx, input.Topic, input.Timeout)
	if err != nil {
		logger.Error("failed to receive first message", "error", err)
		return "", err
	}
	msg2, err := Recv[string](ctx, input.Topic, input.Timeout)
	if err != nil {
		logger.Error("failed to receive second message", "error", err, "msg1", msg1)
		return "", err
	}
	msg3, err := Recv[string](ctx, input.Topic, input.Timeout)
	if err != nil {
		logger.Error("failed to receive third message", "error", err, "msg1", msg1, "msg2", msg2)
		return "", err
	}
	return msg1 + "-" + msg2 + "-" + msg3, nil
}

func receiveWorkflowCoordinated(ctx DbosContext, input struct {
	Topic string
	i     int
}) (string, error) {

	concurrentRecvReadyEvents[input.i].Set()

	concurrentRecvStartEvent.Wait()

	msg, err := Recv[string](ctx, input.Topic, 3*time.Second)
	if err != nil {
		return "", err
	}
	return msg, nil
}

func sendStructWorkflow(ctx DbosContext, input sendWorkflowInput) (string, error) {
	testStruct := sendRecvType{Value: "test-struct-value"}
	err := Send(ctx, input.DestinationId, testStruct, input.Topic)
	return "", err
}

func receiveStructWorkflow(ctx DbosContext, topic string) (sendRecvType, error) {

	sendRecvSyncEvent.Wait()
	return Recv[sendRecvType](ctx, topic, 3*time.Second)
}

func sendIdempotencyWorkflow(ctx DbosContext, input sendWorkflowInput) (string, error) {
	err := Send(ctx, input.DestinationId, "m1", input.Topic)
	if err != nil {
		return "", err
	}
	return "idempotent-send-completed", nil
}

func receiveIdempotencyWorkflow(ctx DbosContext, topic string) (string, error) {

	sendRecvSyncEvent.Wait()
	msg, err := Recv[string](ctx, topic, 60*time.Minute)
	if err != nil {

		receiveIdempotencyStartEvent.Set()
		return "", err
	}
	return msg, nil
}

func durableRecvSleepWorkflow(ctx DbosContext, topic string) (string, error) {

	msg1, err := Recv[string](ctx, topic, 2*time.Second)
	if err != nil && !strings.Contains(err.Error(), fmt.Sprintf("DBOS Error %d", TimeoutError)) {
		return "", fmt.Errorf("unexpected error in first recv: %w", err)
	}

	msg2, err := Recv[string](ctx, topic, 2*time.Second)
	if err != nil && !strings.Contains(err.Error(), fmt.Sprintf("DBOS Error %d", TimeoutError)) {
		return "", fmt.Errorf("unexpected error in second recv: %w", err)
	}

	return msg1 + msg2, nil
}

func stepThatCallsSend(ctx context.Context, input sendWorkflowInput) (string, error) {
	err := Send(ctx.(DbosContext), input.DestinationId, "message-from-step", input.Topic)
	if err != nil {
		return "", err
	}
	return "send-completed", nil
}

func workflowThatCallsSendInStep(ctx DbosContext, input sendWorkflowInput) (string, error) {
	return Run(ctx, func(context context.Context) (string, error) {
		return stepThatCallsSend(context, input)
	})
}

type sendRecvType struct {
	Value string
}

func recvContextCancelWorkflow(ctx DbosContext, topic string) (string, error) {

	msg, err := Recv[string](ctx, topic, 5*time.Second)
	if err != nil {
		return "", err
	}
	return msg, nil
}

func TestSendRecv(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	sendWorkflowD := NewWorkflow(dbosCtx, sendWorkflow)
	receiveWorkflowD := NewWorkflow(dbosCtx, receiveWorkflow)
	NewWorkflow(dbosCtx, receiveWorkflowCoordinated)
	sendStructWorkflowD := NewWorkflow(dbosCtx, sendStructWorkflow)
	receiveStructWorkflowD := NewWorkflow(dbosCtx, receiveStructWorkflow)
	sendIdempotencyWorkflowD := NewWorkflow(dbosCtx, sendIdempotencyWorkflow)
	receiveIdempotencyWorkflowD := NewWorkflow(dbosCtx, receiveIdempotencyWorkflow)
	NewWorkflow(dbosCtx, durableRecvSleepWorkflow)
	workflowThatCallsSendInStepD := NewWorkflow(dbosCtx, workflowThatCallsSendInStep)
	recvContextCancelWorkflowD := NewWorkflow(dbosCtx, recvContextCancelWorkflow)

	Launch(dbosCtx)

	t.Run("SendRecvSuccess", func(t *testing.T) {

		sendRecvSyncEvent.Clear()

		receiveHandle, err := receiveWorkflowD(dbosCtx, struct {
			Topic   string
			Timeout time.Duration
		}{
			Topic:   "test-topic",
			Timeout: 30 * time.Second,
		})
		require.NoError(t, err, "failed to start receive workflow")

		sendHandle, err := sendWorkflowD(dbosCtx, sendWorkflowInput{
			DestinationId: receiveHandle.GetWorkflowId(),
			Topic:         "test-topic",
		})
		require.NoError(t, err, "failed to send message")

		_, err = sendHandle.GetResult()
		require.NoError(t, err, "failed to get result from send workflow")

		sendRecvSyncEvent.Set()

		result, err := receiveHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive workflow")
		require.Equal(t, "message1-message2-message3", result)

		sendSteps, err := GetWorkflowSteps(dbosCtx, sendHandle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps for send workflow")
		require.Len(t, sendSteps, 3, "expected 3 steps in send workflow (3 Send calls), got %d", len(sendSteps))
		for i, step := range sendSteps {
			require.Equal(t, i, step.StepId, "expected step %d to have correct StepID", i)
			require.Equal(t, "DBOS.send", step.StepName, "expected step %d to have StepName 'DBOS.send'", i)
			require.False(t, step.StartedAt.IsZero(), "expected step %d to have StartedAt set", i)
			require.False(t, step.CompletedAt.IsZero(), "expected step %d to have CompletedAt set", i)
			require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
				"expected step %d CompletedAt to be after or equal to StartedAt", i)
		}

		receiveSteps, err := GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps for receive workflow")
		require.Len(t, receiveSteps, 3, "expected 3 steps in receive workflow (3 Recv calls), got %d", len(receiveSteps))
		require.Equal(t, "DBOS.recv", receiveSteps[0].StepName, "expected step 0 to have StepName 'DBOS.recv'")
		require.Equal(t, "DBOS.recv", receiveSteps[1].StepName, "expected step 1 to have StepName 'DBOS.recv'")
		require.Equal(t, "DBOS.recv", receiveSteps[2].StepName, "expected step 2 to have StepName 'DBOS.recv'")
		for i, step := range receiveSteps {
			require.False(t, step.StartedAt.IsZero(), "expected recv step %d to have StartedAt set", i)
			require.False(t, step.CompletedAt.IsZero(), "expected recv step %d to have CompletedAt set", i)
			require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
				"expected recv step %d CompletedAt to be after or equal to StartedAt", i)
		}
	})

	t.Run("SendRecvCustomStruct", func(t *testing.T) {

		sendRecvSyncEvent.Clear()

		receiveHandle, err := receiveStructWorkflowD(dbosCtx, "struct-topic")
		require.NoError(t, err, "failed to start receive workflow")

		sendHandle, err := sendStructWorkflowD(dbosCtx, sendWorkflowInput{
			DestinationId: receiveHandle.GetWorkflowId(),
			Topic:         "struct-topic",
		})
		require.NoError(t, err, "failed to send struct")

		_, err = sendHandle.GetResult()
		require.NoError(t, err, "failed to get result from send workflow")

		sendRecvSyncEvent.Set()

		result, err := receiveHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive workflow")

		require.Equal(t, "test-struct-value", result.Value)

		sendSteps, err := GetWorkflowSteps(dbosCtx, sendHandle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps for send struct workflow")
		require.Len(t, sendSteps, 1, "expected 1 step in send struct workflow (1 Send call), got %d", len(sendSteps))
		require.Equal(t, 0, sendSteps[0].StepId)
		require.Equal(t, "DBOS.send", sendSteps[0].StepName)
		require.False(t, sendSteps[0].StartedAt.IsZero(), "expected send step to have StartedAt set")
		require.False(t, sendSteps[0].CompletedAt.IsZero(), "expected send step to have CompletedAt set")
		require.True(t, sendSteps[0].CompletedAt.After(sendSteps[0].StartedAt) || sendSteps[0].CompletedAt.Equal(sendSteps[0].StartedAt),
			"expected send step CompletedAt to be after or equal to StartedAt")

		receiveSteps, err := GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps for receive struct workflow")
		require.Len(t, receiveSteps, 1, "expected 1 step in receive struct workflow (1 Recv call), got %d", len(receiveSteps))
		require.Equal(t, 0, receiveSteps[0].StepId)
		require.Equal(t, "DBOS.recv", receiveSteps[0].StepName)
		require.False(t, receiveSteps[0].StartedAt.IsZero(), "expected recv step to have StartedAt set")
		require.False(t, receiveSteps[0].CompletedAt.IsZero(), "expected recv step to have CompletedAt set")
		require.True(t, receiveSteps[0].CompletedAt.After(receiveSteps[0].StartedAt) || receiveSteps[0].CompletedAt.Equal(receiveSteps[0].StartedAt),
			"expected recv step CompletedAt to be after or equal to StartedAt")
	})

	t.Run("SendToNonExistentUUID", func(t *testing.T) {

		destUuid := uuid.NewString()

		handle, err := sendWorkflowD(dbosCtx, sendWorkflowInput{
			DestinationId: destUuid,
			Topic:         "testtopic",
		})
		require.NoError(t, err, "failed to start send workflow")

		_, err = handle.GetResult()
		require.Error(t, err, "expected error when sending to non-existent UUID but got none")
		require.True(t, errors.Is(err, &DbosError{Code: NonExistentWorkflowError}), "expected error to be NonExistentWorkflowError, got %T", err)

		expectedErrorMsg := fmt.Sprintf("workflow %s does not exist", destUuid)
		require.Contains(t, err.Error(), expectedErrorMsg)
	})

	t.Run("RecvTimeout", func(t *testing.T) {

		sendRecvSyncEvent.Set()

		receiveHandle, err := receiveWorkflowD(dbosCtx, struct {
			Topic   string
			Timeout time.Duration
		}{
			Topic:   "timeout-test-topic",
			Timeout: 2 * time.Second,
		})
		require.NoError(t, err, "failed to start receive workflow")
		_, err = receiveHandle.GetResult()
		require.Error(t, err, "expected timeout error")

		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected error to be of type *DbosError, got %T", err)
		require.Equal(t, TimeoutError, dbosErr.Code, "expected TimeoutError code")
		require.Contains(t, err.Error(), "DBOS.recv timed out", "error message should contain 'Operation timed out'")

		steps, err := GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 2, "expected 2 steps in receive workflow (recv that timed out and sleep that timed out), got %d", len(steps))

		require.Equal(t, "DBOS.recv", steps[0].StepName, "expected step 0 to have StepName 'DBOS.recv'")
		require.NotNil(t, steps[0].Error, "expected step 0 to have an error")
		require.Contains(t, steps[0].Error.Error(), "DBOS.recv timed out", "expected step 0 to contain 'DBOS.recv timed out' in error message")

		require.Equal(t, "DBOS.sleep", steps[1].StepName, "expected step 1 to have StepName 'DBOS.sleep'")
	})

	t.Run("RecvMustRunInsideWorkflows", func(t *testing.T) {

		_, err := Recv[string](dbosCtx, "test-topic", 1*time.Second)
		require.Error(t, err, "expected error when running Recv outside of workflow context, but got none")

		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected error to be of type *DbosError, got %T", err)
		require.Equal(t, StepExecutionError, dbosErr.Code)

		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart)
	})

	t.Run("SendOutsideWorkflow", func(t *testing.T) {

		sendRecvSyncEvent.Clear()

		receiveHandle, err := receiveWorkflowD(dbosCtx, struct {
			Topic   string
			Timeout time.Duration
		}{
			Topic:   "outside-workflow-topic",
			Timeout: 30 * time.Second,
		})
		require.NoError(t, err, "failed to start receive workflow")

		for i := range 3 {
			err = Send(dbosCtx, receiveHandle.GetWorkflowId(), fmt.Sprintf("message%d", i+1), "outside-workflow-topic")
			require.NoError(t, err, "failed to send message%d from outside workflow", i+1)
		}

		sendRecvSyncEvent.Set()

		result, err := receiveHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive workflow")
		assert.Equal(t, "message1-message2-message3", result, "expected correct result from receive workflow")

		receiveSteps, err := GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps for receive workflow")
		require.Len(t, receiveSteps, 3, "expected 3 steps in receive workflow (3 Recv calls), got %d", len(receiveSteps))
		for i, step := range receiveSteps {

			require.Equal(t, i*2, step.StepId, "expected step %d to have correct StepID", i)
			require.Equal(t, "DBOS.recv", step.StepName, "expected step %d to have StepName 'DBOS.recv'", i)
			require.False(t, step.StartedAt.IsZero(), "expected recv step %d to have StartedAt set", i)
			require.False(t, step.CompletedAt.IsZero(), "expected recv step %d to have CompletedAt set", i)
			require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
				"expected recv step %d CompletedAt to be after or equal to StartedAt", i)
		}
	})

	t.Run("SendRecvIdempotency", func(t *testing.T) {

		sendRecvSyncEvent.Clear()

		receiveHandle, err := receiveIdempotencyWorkflowD(dbosCtx, "idempotency-topic")
		require.NoError(t, err, "failed to start receive idempotency workflow")

		sendHandle, err := sendIdempotencyWorkflowD(dbosCtx, sendWorkflowInput{
			DestinationId: receiveHandle.GetWorkflowId(),
			Topic:         "idempotency-topic",
		})
		require.NoError(t, err, "failed to send idempotency message")

		require.Eventually(t, func() bool {
			steps, err := GetWorkflowSteps(dbosCtx, sendHandle.GetWorkflowId())
			return err == nil && len(steps) > 0 && !steps[0].CompletedAt.IsZero()
		}, 5*time.Second, 10*time.Millisecond, "send step should complete")

		sendRecvSyncEvent.Set()

		result, err := receiveHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive workflow")
		require.Equal(t, "m1", result, "expected result to be 'm1'")

		result2, err := sendHandle.GetResult()
		require.NoError(t, err, "failed to get result from send idempotency workflow")
		assert.Equal(t, "idempotent-send-completed", result2, "expected result to be 'idempotent-send-completed'")

		setWorkflowStatusPending(t, dbosCtx, sendHandle.GetWorkflowId())
		setWorkflowStatusPending(t, dbosCtx, receiveHandle.GetWorkflowId())

		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recoveredHandles, 2, "expected 2 recovered handles, got %d", len(recoveredHandles))

		var sendRecoveredHandle *WorkflowHandle[any]
		var receiveRecoveredHandle *WorkflowHandle[any]
		for _, handle := range recoveredHandles {
			if handle.GetWorkflowId() == sendHandle.GetWorkflowId() {
				sendRecoveredHandle = handle
			}
			if handle.GetWorkflowId() == receiveHandle.GetWorkflowId() {
				receiveRecoveredHandle = handle
			}
		}
		require.NotNil(t, sendRecoveredHandle, "failed to find recovered handle for send workflow")
		require.NotNil(t, receiveRecoveredHandle, "failed to find recovered handle for receive workflow")

		steps, err := GetWorkflowSteps(dbosCtx, sendHandle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 1, "expected 1 step in send idempotency workflow, got %d", len(steps))
		assert.Equal(t, 0, steps[0].StepId, "expected send idempotency step to have StepID 0")
		assert.Equal(t, "DBOS.send", steps[0].StepName, "expected send idempotency step to have StepName 'DBOS.send'")
		require.False(t, steps[0].StartedAt.IsZero(), "expected send step to have StartedAt set")
		require.False(t, steps[0].CompletedAt.IsZero(), "expected send step to have CompletedAt set")
		require.True(t, steps[0].CompletedAt.After(steps[0].StartedAt) || steps[0].CompletedAt.Equal(steps[0].StartedAt),
			"expected send step CompletedAt to be after or equal to StartedAt")

		steps, err = GetWorkflowSteps(dbosCtx, receiveHandle.GetWorkflowId())
		require.NoError(t, err, "failed to get steps for receive idempotency workflow")
		require.Len(t, steps, 1, "expected 1 step in receive idempotency workflow (1 Recv call), got %d", len(steps))
		assert.Equal(t, 0, steps[0].StepId, "expected receive idempotency step to have StepID 0")
		assert.Equal(t, "DBOS.recv", steps[0].StepName, "expected receive idempotency step to have StepName 'DBOS.recv'")
		require.False(t, steps[0].StartedAt.IsZero(), "expected recv step to have StartedAt set")
		require.False(t, steps[0].CompletedAt.IsZero(), "expected recv step to have CompletedAt set")
		require.True(t, steps[0].CompletedAt.After(steps[0].StartedAt) || steps[0].CompletedAt.Equal(steps[0].StartedAt),
			"expected recv step CompletedAt to be after or equal to StartedAt")

		result3, err := receiveRecoveredHandle.GetResult()
		require.NoError(t, err, "failed to get result from receive idempotency workflow")
		assert.Equal(t, "m1", result3, "expected result to be 'm1'")

		result4, err := sendRecoveredHandle.GetResult()
		require.NoError(t, err, "failed to get result from send idempotency workflow")
		assert.Equal(t, "idempotent-send-completed", result4, "expected result to be 'idempotent-send-completed'")
	})

	t.Run("SendCannotBeCalledWithinStep", func(t *testing.T) {

		sendRecvSyncEvent.Set()

		receiveHandle, err := receiveWorkflowD(dbosCtx, struct {
			Topic   string
			Timeout time.Duration
		}{
			Topic:   "send-within-step-topic",
			Timeout: 500 * time.Millisecond,
		})
		require.NoError(t, err, "failed to start receive workflow")

		handle, err := workflowThatCallsSendInStepD(dbosCtx, sendWorkflowInput{
			DestinationId: receiveHandle.GetWorkflowId(),
			Topic:         "send-within-step-topic",
		})
		require.NoError(t, err, "failed to start workflow")

		_, err = handle.GetResult()
		require.Error(t, err, "expected error when calling Send within a step, but got none")

		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected error to be of type *DbosError, got %T", err)
		require.Equal(t, StepExecutionError, dbosErr.Code)

		expectedMessagePart := "cannot call Send within a step"
		require.Contains(t, err.Error(), expectedMessagePart, "expected error message to contain expected text")

		_, err = receiveHandle.GetResult()
		require.Error(t, err, "expected timout error when getting result from receive workflow, but got none")
		require.Contains(t, err.Error(), "DBOS.recv timed out", "expected error message to contain 'DBOS.recv timed out'")
	})

	t.Run("RecvContextCancellation", func(t *testing.T) {

		timeoutCtx, cancel := WithTimeout(dbosCtx, 1*time.Second)
		defer cancel()

		handle, err := recvContextCancelWorkflowD(timeoutCtx, "context-cancel-topic")
		require.NoError(t, err, "failed to start recv context cancel workflow")

		result, err := handle.GetResult()
		require.Error(t, err, "expected error from context cancellation")
		require.True(t, errors.Is(err, context.DeadlineExceeded), "expected context.DeadlineExceeded error, got: %v", err)
		require.Equal(t, "", result, "expected empty result when context cancelled")

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		require.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	})
}

var (
	setEventStart                 = NewEvent()
	setSecondEventSignal          = NewEvent()
	setThirdEventSignal           = NewEvent()
	getEventWorkflowStartedSignal = NewEvent()
	firstEventSetSignal           = NewEvent()
	secondEventSetSignal          = NewEvent()
	thirdEventSetSignal           = NewEvent()
)

type setEventWorkflowInput struct {
	Key     string
	Message string
}

func setEventWorkflow(ctx DbosContext, input setEventWorkflowInput) (string, error) {
	err := SetEvent(ctx, input.Key, input.Message)
	if err != nil {
		return "", err
	}
	setEventStart.Set()
	return "event-set", nil
}

type getEventWorkflowInput struct {
	TargetWorkflowId string
	Key              string
}

func getEventWorkflow(ctx DbosContext, input getEventWorkflowInput) (string, error) {
	getEventWorkflowStartedSignal.Set()
	result, err := GetEvent[string](ctx, input.TargetWorkflowId, input.Key, 3*time.Second)
	if err != nil {
		return "", err
	}
	return result, nil
}

func setTwoEventsWorkflow(ctx DbosContext, input setEventWorkflowInput) (string, error) {

	err := SetEvent(ctx, "event", "first-event-message")
	if err != nil {
		return "", err
	}
	firstEventSetSignal.Set()

	setSecondEventSignal.Wait()

	err = SetEvent(ctx, "event", "second-event-message")
	if err != nil {
		return "", err
	}
	secondEventSetSignal.Set()

	setThirdEventSignal.Wait()

	err = SetEvent(ctx, "anotherevent", "third-event-message")
	if err != nil {
		return "", err
	}
	thirdEventSetSignal.Set()

	return "two-events-set", nil
}

func setEventIdempotencyWorkflow(ctx DbosContext, input setEventWorkflowInput) (string, error) {
	err := SetEvent(ctx, input.Key, input.Message)
	if err != nil {
		return "", err
	}
	return "idempotent-set-completed", nil
}

func getEventIdempotencyWorkflow(ctx DbosContext, input setEventWorkflowInput) (string, error) {
	result, err := GetEvent[string](ctx, input.Key, input.Message, 3*time.Second)
	if err != nil {
		return "", err
	}
	return result, nil
}

func durableGetEventSleepWorkflow(ctx DbosContext, targetWorkflowId string) (string, error) {

	val1, err := GetEvent[string](ctx, targetWorkflowId, "key1", 2*time.Second)
	if err != nil && !strings.Contains(err.Error(), "timed out") {
		return "", fmt.Errorf("unexpected error in first getEvent: %w", err)
	}

	val2, err := GetEvent[string](ctx, targetWorkflowId, "key2", 2*time.Second)
	if err != nil && !strings.Contains(err.Error(), "timed out") {
		return "", fmt.Errorf("unexpected error in second getEvent: %w", err)
	}

	return val1 + val2, nil
}

func TestSetGetEvent(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	setEventWorkflowD := NewWorkflow(dbosCtx, setEventWorkflow)
	getEventWorkflowD := NewWorkflow(dbosCtx, getEventWorkflow)
	setTwoEventsWorkflowD := NewWorkflow(dbosCtx, setTwoEventsWorkflow)
	setEventIdempotencyWorkflowD := NewWorkflow(dbosCtx, setEventIdempotencyWorkflow)
	getEventIdempotencyWorkflowD := NewWorkflow(dbosCtx, getEventIdempotencyWorkflow)
	NewWorkflow(dbosCtx, durableGetEventSleepWorkflow)

	Launch(dbosCtx)

	t.Run("SetGetEventFromWorkflow", func(t *testing.T) {

		setSecondEventSignal.Clear()
		setThirdEventSignal.Clear()
		firstEventSetSignal.Clear()
		secondEventSetSignal.Clear()
		thirdEventSetSignal.Clear()

		setHandle, err := setTwoEventsWorkflowD(dbosCtx, setEventWorkflowInput{
			Key:     "unused",
			Message: "unused",
		})
		require.NoError(t, err, "failed to start set two events workflow")
		setWorkflowId := setHandle.GetWorkflowId()

		testCases := []struct {
			name           string
			key            string
			expectedValue  string
			setEventSignal *Event
			eventSetSignal *Event
		}{
			{
				name:           "first",
				key:            "event",
				expectedValue:  "first-event-message",
				setEventSignal: nil,
				eventSetSignal: firstEventSetSignal,
			},
			{
				name:           "second",
				key:            "event",
				expectedValue:  "second-event-message",
				setEventSignal: setSecondEventSignal,
				eventSetSignal: secondEventSetSignal,
			},
			{
				name:           "third",
				key:            "anotherevent",
				expectedValue:  "third-event-message",
				setEventSignal: setThirdEventSignal,
				eventSetSignal: thirdEventSetSignal,
			},
		}

		var getEventHandles []*WorkflowHandle[string]

		for _, tc := range testCases {
			// If this event requires a signal to be set, signal the set workflow
			if tc.setEventSignal != nil {
				tc.setEventSignal.Set()
			}

			tc.eventSetSignal.Wait()

			getEventHandle, err := getEventWorkflowD(dbosCtx, getEventWorkflowInput{
				TargetWorkflowId: setWorkflowId,
				Key:              tc.key,
			})
			require.NoError(t, err, "failed to start get %s event workflow", tc.name)
			getEventHandles = append(getEventHandles, getEventHandle)

			message, err := getEventHandle.GetResult()
			require.NoError(t, err, "failed to get result from %s event workflow", tc.name)
			assert.Equal(t, tc.expectedValue, message, "expected %s message to be '%s'", tc.name, tc.expectedValue)
		}

		result, err := setHandle.GetResult()
		require.NoError(t, err, "failed to get result from set two events workflow")
		assert.Equal(t, "two-events-set", result, "expected result to be 'two-events-set'")

		setSteps, err := GetWorkflowSteps(dbosCtx, setHandle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps for set two events workflow")
		require.Len(t, setSteps, 3, "expected 3 steps in set two events workflow (3 SetEvent calls), got %d", len(setSteps))
		for i, step := range setSteps {
			assert.Equal(t, i, step.StepId, "expected step %d to have StepID %d", i, i)
			assert.Equal(t, "DBOS.setEvent", step.StepName, "expected step %d to have StepName 'DBOS.setEvent'", i)
			require.False(t, step.StartedAt.IsZero(), "expected setEvent step %d to have StartedAt set", i)
			require.False(t, step.CompletedAt.IsZero(), "expected setEvent step %d to have CompletedAt set", i)
			require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
				"expected setEvent step %d CompletedAt to be after or equal to StartedAt", i)
		}

		for i, getEventHandle := range getEventHandles {
			steps, err := GetWorkflowSteps(dbosCtx, getEventHandle.GetWorkflowId())
			require.NoError(t, err, "failed to get workflow steps for get event workflow %d", i)
			require.Len(t, steps, 1, "expected 1 step in get event workflow %d (getEvent only, no sleep), got %d", i, len(steps))
			assert.Equal(t, 0, steps[0].StepId, "expected step to have StepID 0")
			assert.Equal(t, "DBOS.getEvent", steps[0].StepName, "expected step to have StepName 'DBOS.getEvent'")
			require.False(t, steps[0].StartedAt.IsZero(), "expected getEvent step to have StartedAt set")
			require.False(t, steps[0].CompletedAt.IsZero(), "expected getEvent step to have CompletedAt set")
			require.True(t, steps[0].CompletedAt.After(steps[0].StartedAt) || steps[0].CompletedAt.Equal(steps[0].StartedAt),
				"expected getEvent step CompletedAt to be after or equal to StartedAt")
		}
	})

	t.Run("GetEventFromOutsideWorkflow", func(t *testing.T) {

		setHandle, err := setEventWorkflowD(dbosCtx, setEventWorkflowInput{
			Key:     "test-key",
			Message: "test-message",
		})
		if err != nil {
			t.Fatalf("failed to start set event workflow: %v", err)
		}

		_, err = setHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from set event workflow: %v", err)
		}

		message, err := GetEvent[string](dbosCtx, setHandle.GetWorkflowId(), "test-key", 3*time.Second)
		if err != nil {
			t.Fatalf("failed to get event from outside workflow: %v", err)
		}
		if message != "test-message" {
			t.Fatalf("expected received message to be 'test-message', got '%s'", message)
		}

		setSteps, err := GetWorkflowSteps(dbosCtx, setHandle.GetWorkflowId())
		if err != nil {
			t.Fatalf("failed to get workflow steps for set event workflow: %v", err)
		}
		require.Len(t, setSteps, 1, "expected 1 step in set event workflow (1 SetEvent call), got %d", len(setSteps))
		if setSteps[0].StepId != 0 {
			t.Fatalf("expected step to have StepID 0, got %d", setSteps[0].StepId)
		}
		if setSteps[0].StepName != "DBOS.setEvent" {
			t.Fatalf("expected step to have StepName 'DBOS.setEvent', got '%s'", setSteps[0].StepName)
		}
		require.False(t, setSteps[0].StartedAt.IsZero(), "expected setEvent step to have StartedAt set")
		require.False(t, setSteps[0].CompletedAt.IsZero(), "expected setEvent step to have CompletedAt set")
		require.True(t, setSteps[0].CompletedAt.After(setSteps[0].StartedAt) || setSteps[0].CompletedAt.Equal(setSteps[0].StartedAt),
			"expected setEvent step CompletedAt to be after or equal to StartedAt")
	})

	t.Run("GetEventTimeout", func(t *testing.T) {

		nonExistentId := uuid.NewString()
		_, err := GetEvent[string](dbosCtx, nonExistentId, "test-key", 3*time.Second)
		require.Error(t, err, "expected timeout error when getting event from non-existent workflow, but got none")

		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected error to be of type *DbosError, got %T", err)
		require.Equal(t, TimeoutError, dbosErr.Code, "expected TimeoutError code")
		require.Contains(t, err.Error(), "no event found for key 'test-key' within 3s", "expected error message to contain 'no event found for key 'test-key' within 3s'")

		// Try to get an event from an existing workflow but with a key that doesn't exist
		setHandle, err := setEventWorkflowD(dbosCtx, setEventWorkflowInput{
			Key:     "test-key",
			Message: "test-message",
		})
		require.NoError(t, err, "failed to set event")
		_, err = setHandle.GetResult()
		require.NoError(t, err, "failed to get result from set event workflow")
		_, err = GetEvent[string](dbosCtx, setHandle.GetWorkflowId(), "non-existent-key", 3*time.Second)
		require.Error(t, err, "expected timeout error when getting event with non-existent key, but got none")
		require.Contains(t, err.Error(), "no event found for key 'non-existent-key' within 3s", "expected error message to contain 'no event found for key 'non-existent-key' within 3s'")

		dbosErr, ok = err.(*DbosError)
		require.True(t, ok, "expected error to be of type *DbosError, got %T", err)
		require.Equal(t, TimeoutError, dbosErr.Code, "expected TimeoutError code")
		require.Contains(t, err.Error(), "no event found for key 'non-existent-key' within 3s", "expected error message to contain 'no event found for key 'non-existent-key' within 3s'")
	})

	t.Run("SetGetEventMustRunInsideWorkflows", func(t *testing.T) {

		err := SetEvent(dbosCtx, "test-key", "test-message")
		require.Error(t, err, "expected error when running SetEvent outside of workflow context, but got none")

		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected error to be of type *DbosError, got %T", err)
		require.Equal(t, StepExecutionError, dbosErr.Code)

		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart)
	})

	t.Run("SetGetEventIdempotency", func(t *testing.T) {

		setHandle, err := setEventIdempotencyWorkflowD(dbosCtx, setEventWorkflowInput{
			Key:     "idempotency-key",
			Message: "idempotency-message",
		})
		if err != nil {
			t.Fatalf("failed to start set event idempotency workflow: %v", err)
		}
		setResult, err := setHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from set event idempotency workflow: %v", err)
		}
		require.Equal(t, "idempotent-set-completed", setResult, "set workflow result")

		getHandle, err := getEventIdempotencyWorkflowD(dbosCtx, setEventWorkflowInput{
			Key:     setHandle.GetWorkflowId(),
			Message: "idempotency-key",
		})
		if err != nil {
			t.Fatalf("failed to start get event idempotency workflow: %v", err)
		}
		getResult, err := getHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from get event idempotency workflow: %v", err)
		}
		require.Equal(t, "idempotency-message", getResult, "get workflow result (event content)")

		setWorkflowStatusPending(t, dbosCtx, setHandle.GetWorkflowId())
		setWorkflowStatusPending(t, dbosCtx, getHandle.GetWorkflowId())

		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recoveredHandles, 2, "expected 2 recovered handles, got %d", len(recoveredHandles))

		setSteps, err := GetWorkflowSteps(dbosCtx, setHandle.GetWorkflowId())
		require.NoError(t, err, "get steps for set event idempotency workflow")
		require.Len(t, setSteps, 1, "expected 1 step in set event idempotency workflow")
		require.Equal(t, 0, setSteps[0].StepId, "set step StepID")
		require.Equal(t, "DBOS.setEvent", setSteps[0].StepName, "set step StepName")
		require.False(t, setSteps[0].StartedAt.IsZero(), "setEvent step StartedAt set")
		require.False(t, setSteps[0].CompletedAt.IsZero(), "setEvent step CompletedAt set")

		getSteps, err := GetWorkflowSteps(dbosCtx, getHandle.GetWorkflowId())
		require.NoError(t, err, "get steps for get event idempotency workflow")
		require.Len(t, getSteps, 1, "expected 1 step in get event idempotency workflow")
		require.Equal(t, 0, getSteps[0].StepId, "get step StepID")
		require.Equal(t, "DBOS.getEvent", getSteps[0].StepName, "get step StepName")
		require.False(t, getSteps[0].StartedAt.IsZero(), "getEvent step StartedAt set")
		require.False(t, getSteps[0].CompletedAt.IsZero(), "getEvent step CompletedAt set")

		// Recovered handles must return the same results
		for _, recoveredHandle := range recoveredHandles {
			if recoveredHandle.GetWorkflowId() == setHandle.GetWorkflowId() {
				recoveredSetResult, err := recoveredHandle.GetResult()
				require.NoError(t, err, "recovered set workflow GetResult")
				require.Equal(t, "idempotent-set-completed", recoveredSetResult, "recovered set result")
			}
			if recoveredHandle.GetWorkflowId() == getHandle.GetWorkflowId() {
				recoveredGetResult, err := recoveredHandle.GetResult()
				require.NoError(t, err, "recovered get workflow GetResult")
				require.Equal(t, "idempotency-message", recoveredGetResult, "recovered get result (event content)")
			}
		}
	})

	t.Run("ConcurrentGetEvent", func(t *testing.T) {

		setHandle, err := setEventWorkflowD(dbosCtx, setEventWorkflowInput{
			Key:     "concurrent-event-key",
			Message: "concurrent-event-message",
		})
		if err != nil {
			t.Fatalf("failed to start set event workflow: %v", err)
		}

		_, err = setHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from set event workflow: %v", err)
		}

		numGoroutines := 5
		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines)
		wg.Add(numGoroutines)
		for range numGoroutines {
			go func() {
				defer wg.Done()
				res, err := GetEvent[string](dbosCtx, setHandle.GetWorkflowId(), "concurrent-event-key", 10*time.Second)
				if err != nil {
					errors <- fmt.Errorf("failed to get event in goroutine: %v", err)
					return
				}
				if res != "concurrent-event-message" {
					errors <- fmt.Errorf("expected result in goroutine to be 'concurrent-event-message', got '%s'", res)
					return
				}
			}()
		}
		wg.Wait()
		close(errors)

		for err := range errors {
			require.FailNow(t, "goroutine error: %v", err)
		}
	})
}

func conflictWorkflowA(dbosCtx DbosContext, input string) (string, error) {
	return Run(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepA(ctx)
	})
}

func conflictWorkflowB(dbosCtx DbosContext, input string) (string, error) {
	return Run(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepB(ctx)
	})
}

func conflictStepA(_ context.Context) (string, error) {
	return "step-a-result", nil
}

func conflictStepB(_ context.Context) (string, error) {
	return "step-b-result", nil
}

func workflowWithMultipleSteps(dbosCtx DbosContext, input string) (string, error) {

	result1, err := Run(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepA(ctx)
	})
	if err != nil {
		return "", err
	}

	result2, err := Run(dbosCtx, func(ctx context.Context) (string, error) {
		return conflictStepB(ctx)
	})
	if err != nil {
		return "", err
	}

	return result1 + "-" + result2, nil
}

func TestWorkflowExecutionMismatch(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	conflictWorkflowAD := NewWorkflow(dbosCtx, conflictWorkflowA)
	conflictWorkflowBD := NewWorkflow(dbosCtx, conflictWorkflowB)
	workflowWithMultipleStepsD := NewWorkflow(dbosCtx, workflowWithMultipleSteps)

	_ = conflictWorkflowAD
	_ = conflictWorkflowBD

	t.Run("StepNameConflict", func(t *testing.T) {
		handle, err := workflowWithMultipleStepsD(dbosCtx, "test-input")
		require.NoError(t, err, "failed to start workflow")
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from workflow")
		require.Equal(t, "step-a-result-step-b-result", result)

		workflowId := handle.GetWorkflowId()

		wrongStepName := "wrong-step-name"
		_, err = dbosCtx.(*dbosContext).kernel.checkOperationExecution(dbosCtx, checkOperationExecutionDBInput{
			workflowId: workflowId,
			stepId:     0,
			stepName:   wrongStepName,
		})

		require.Error(t, err, "expected UnexpectedStep error when checking operation with wrong step name, but got none")

		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected error to be of type *DbosError, got %T", err)
		require.Equal(t, UnexpectedStep, dbosErr.Code)

		require.Contains(t, err.Error(), "Check that your workflow is deterministic")
		require.Contains(t, err.Error(), wrongStepName)
	})
}

func sleepRecoveryWorkflow(dbosCtx DbosContext, duration time.Duration) (time.Duration, error) {
	return Sleep(dbosCtx, duration)
}

func TestSleep(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})
	sleepRecoveryWorkflowD := NewWorkflow(dbosCtx, sleepRecoveryWorkflow)

	t.Run("SleepDurableRecovery", func(t *testing.T) {
		sleepDuration := 2 * time.Second

		handle1, err := sleepRecoveryWorkflowD(dbosCtx, sleepDuration)
		require.NoError(t, err, "failed to start sleep recovery workflow")
		workflowId := handle1.GetWorkflowId()
		_, err = handle1.GetResult()
		require.NoError(t, err, "failed to get result from first run")

		setWorkflowStatusPending(t, dbosCtx, workflowId)

		startTime := time.Now()
		handles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to start second sleep recovery workflow")
		require.Len(t, handles, 1, "expected 1 recovered handle")
		handle2 := handles[0]
		_, err = handle2.GetResult()
		require.NoError(t, err, "failed to get result from second run")
		elapsed := time.Since(startTime)
		assert.Less(t, elapsed, sleepDuration, "expected elapsed time to be less than sleep duration")

		steps, err := GetWorkflowSteps(dbosCtx, workflowId)
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 1, "expected 1 step (the sleep), got %d", len(steps))
		step := steps[0]
		assert.Equal(t, 0, step.StepId, "expected step to have StepID 0")
		assert.Equal(t, "DBOS.sleep", step.StepName, "expected step name to be 'DBOS.sleep'")
		assert.Nil(t, step.Error, "expected step to have no error")
		require.False(t, step.StartedAt.IsZero(), "expected sleep step to have StartedAt set")
		require.False(t, step.CompletedAt.IsZero(), "expected sleep step to have CompletedAt set")
		require.True(t, step.CompletedAt.After(step.StartedAt) || step.CompletedAt.Equal(step.StartedAt),
			"expected sleep step CompletedAt to be after or equal to StartedAt")
	})

	t.Run("SleepCannotBeCalledOutsideWorkflow", func(t *testing.T) {

		_, err := Sleep(dbosCtx, 1*time.Second)
		require.Error(t, err, "expected error when calling Sleep outside of workflow context, but got none")

		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected error to be of type *DbosError, got %T", err)
		require.Equal(t, StepExecutionError, dbosErr.Code)

		expectedMessagePart := "workflow state not found in context: are you running this step within a workflow?"
		require.Contains(t, err.Error(), expectedMessagePart)
	})
}

func TestWorkflowTimeout(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	waitForCancelWorkflow := func(ctx DbosContext, _ string) (string, error) {

		<-ctx.Done()
		assert.True(t, errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded),
			"workflow was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		return "", ctx.Err()
	}
	waitForCancelWorkflowD := NewWorkflow(dbosCtx, waitForCancelWorkflow)

	t.Run("WorkflowTimeout", func(t *testing.T) {

		// So we are almost guaranteed that the workflow will be cancelled before returning, hence GetStatus will show it as cancelled

		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc()
		handle, err := waitForCancelWorkflowD(cancelCtx, "wait-for-cancel")
		require.NoError(t, err, "failed to start wait for cancel workflow")

		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, &DbosError{Code: AwaitedWorkflowCancelled}), "expected AwaitedWorkflowCancelled error, got: %v", err)
		assert.Equal(t, "", result, "expected result to be an empty string")

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	})

	wfcStart := NewEvent()
	wfcStop := NewEvent()
	waitForCancelWorkflowManual := func(ctx DbosContext, _ string) (string, error) {

		<-ctx.Done()
		assert.True(t, errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded),
			"workflow was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		wfcStart.Set()
		wfcStop.Wait()
		return "", ctx.Err()
	}
	waitForCancelWorkflowManualD := NewWorkflow(dbosCtx, waitForCancelWorkflowManual)

	t.Run("ManuallyCancelWorkflow", func(t *testing.T) {
		// This test requires an event to prevent the workflow for returning before we GetStatus
		// This is because direct cancellation through the cancel function can happen faster than the timeout context AfterFunc

		// Thus the workflow status will be "Error" instead of "Cancelled" and the test fail
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 5*time.Hour)
		defer cancelFunc()
		handle, err := waitForCancelWorkflowManualD(cancelCtx, "manual-cancel")
		require.NoError(t, err, "failed to start manual cancel workflow")

		cancelFunc()
		wfcStart.Wait()

		require.Eventually(t, func() bool {
			status, err := handle.GetStatus()
			require.NoError(t, err, "failed to get workflow status")
			return status.Status == WorkflowStatusCancelled
		}, 5*time.Second, 100*time.Millisecond, "workflow did not reach cancelled status in time")

		wfcStop.Set()

		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, &DbosError{Code: AwaitedWorkflowCancelled}), "expected AwaitedWorkflowCancelled error, got: %v", err)
		assert.Equal(t, "", result, "expected result to be an empty string")
	})

	waitForCancelStep := func(ctx context.Context) (string, error) {

		<-ctx.Done()
		if !errors.Is(ctx.Err(), context.Canceled) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("step was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		}
		return "", ctx.Err()
	}

	waitForCancelWorkflowWithStep := func(ctx DbosContext, _ string) (string, error) {
		return Run(ctx, func(context context.Context) (string, error) {
			return waitForCancelStep(context)
		})
	}
	waitForCancelWorkflowWithStepD := NewWorkflow(dbosCtx, waitForCancelWorkflowWithStep)

	t.Run("WorkflowWithStepTimeout", func(t *testing.T) {

		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 100*time.Millisecond)
		defer cancelFunc()
		handle, err := waitForCancelWorkflowWithStepD(cancelCtx, "wf-with-step-timeout")
		require.NoError(t, err, "failed to start workflow with step timeout")

		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, &DbosError{Code: AwaitedWorkflowCancelled}), "expected AwaitedWorkflowCancelled error, got: %v", err)
		assert.Equal(t, "", result, "expected result to be an empty string")

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	})

	waitForCancelWorkflowWithStepAfterCancel := func(ctx DbosContext, _ string) (string, error) {
		uncancellableCtx := WithoutCancel(ctx)

		<-ctx.Done()

		if !errors.Is(ctx.Err(), context.Canceled) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", fmt.Errorf("workflow was cancelled, but context error is not context.Canceled nor context.DeadlineExceeded: %v", ctx.Err())
		}

		wfid, err := GetWorkflowId(uncancellableCtx)
		if err != nil {
			return "", fmt.Errorf("failed to get workflow ID: %w", err)
		}
		dbosCtxInternal, ok := uncancellableCtx.(*dbosContext)
		if !ok {
			return "", fmt.Errorf("failed to cast DbosContext to dbosContext")
		}
		Kernel := dbosCtxInternal.kernel
		query := Kernel.renderSql(`SELECT status FROM %sworkflow_status WHERE workflow_uuid = $1`, "")
		require.Eventually(t, func() bool {
			var status WorkflowStatusType
			err := Kernel.pool.QueryRow(uncancellableCtx, query, wfid).Scan(&status)
			if err != nil {
				return false
			}
			return status == WorkflowStatusCancelled
		}, 5*time.Second, 50*time.Millisecond, "workflow did not transition to cancelled status in time")

		return Run(ctx, simpleStep)
	}
	waitForCancelWorkflowWithStepAfterCancelD := NewWorkflow(dbosCtx, waitForCancelWorkflowWithStepAfterCancel)

	t.Run("WorkflowWithStepAfterTimeout", func(t *testing.T) {

		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc()
		handle, err := waitForCancelWorkflowWithStepAfterCancelD(cancelCtx, "wf-with-step-after-timeout")
		require.NoError(t, err, "failed to start workflow with step after timeout")

		result, err := handle.GetResult()

		require.Error(t, err, "expected error from workflow")

		targetErr := &DbosError{Code: AwaitedWorkflowCancelled}
		assert.True(t, errors.Is(err, targetErr), "expected AwaitedWorkflowCancelled error, got: %v", err)
		assert.Equal(t, "", result, "expected result to be an empty string")

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	})

	shorterStepTimeoutWorkflow := func(ctx DbosContext, _ string) (string, error) {

		stepCtx, stepCancelFunc := WithTimeout(ctx, 1*time.Millisecond)
		defer stepCancelFunc()
		_, err := Run(stepCtx, func(context context.Context) (string, error) {
			return waitForCancelStep(context)
		})
		assert.True(t, errors.Is(err, context.DeadlineExceeded), "expected step to timeout, got: %v", err)
		return "step-timed-out", nil
	}
	shorterStepTimeoutWorkflowD := NewWorkflow(dbosCtx, shorterStepTimeoutWorkflow)

	t.Run("ShorterStepTimeout", func(t *testing.T) {

		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 5*time.Second)
		defer cancelFunc()
		handle, err := shorterStepTimeoutWorkflowD(cancelCtx, "shorter-step-timeout")
		require.NoError(t, err, "failed to start shorter step timeout workflow")

		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result from shorter step timeout workflow")
		assert.Equal(t, "step-timed-out", result, "expected result to be 'step-timed-out'")

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusSuccess, status.Status, "expected workflow status to be WorkflowStatusSuccess")
	})

	detachedStep := func(ctx context.Context, timeout time.Duration) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(timeout):
		}
		return "detached-step-completed", nil
	}

	detachedStepWorkflow := func(ctx DbosContext, timeout time.Duration) (string, error) {

		stepCtx := WithoutCancel(ctx)
		res, err := Run(stepCtx, func(context context.Context) (string, error) {
			return detachedStep(context, timeout*2)
		})
		// The step itself cannot be cancelled, but by the time it completes the

		if err != nil {
			assert.True(t, errors.Is(err, &DbosError{Code: WorkflowCancelled}),
				"unexpected detached step error: %v", err)
			return "", err
		}
		assert.Equal(t, "detached-step-completed", res, "expected detached step result to be 'detached-step-completed'")
		return res, ctx.Err()
	}
	detachedStepWorkflowD := NewWorkflow(dbosCtx, detachedStepWorkflow)

	t.Run("DetachedStepWorkflow", func(t *testing.T) {

		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 1*time.Millisecond)
		defer cancelFunc()

		handle, err := detachedStepWorkflowD(cancelCtx, 1*time.Second)
		require.NoError(t, err, "failed to start detached step workflow")

		_, err = handle.GetResult()
		assert.True(t, errors.Is(err, &DbosError{Code: AwaitedWorkflowCancelled}), "expected AwaitedWorkflowCancelled error, got: %v", err)

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	})

	var childIdRegistry sync.Map

	waitForCancelParent := func(ctx DbosContext, registryKey string) (string, error) {

		childId, err := startChildWorkflow(ctx, waitForCancelWorkflowD, "child-wait-for-cancel")
		if err != nil {
			return "", err
		}
		childIdRegistry.Store(registryKey, childId)

		childHandle, err := RetrieveWorkflow[string](ctx, childId)
		if err != nil {
			return "", err
		}

		result, err := childHandle.GetResult()
		assert.True(t,
			errors.Is(err, &DbosError{Code: AwaitedWorkflowCancelled}) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled),
			"expected child workflow to be cancelled, got: %v", err)
		return result, ctx.Err()
	}
	waitForCancelParentD := NewWorkflow(dbosCtx, waitForCancelParent)

	t.Run("ChildWorkflowTimesout", func(t *testing.T) {

		cancelCtx, cancelFunc := WithTimeout(dbosCtx, 250*time.Millisecond)
		defer cancelFunc()

		registryKey := "child-wait-for-cancel-" + uuid.NewString()
		handle, err := waitForCancelParentD(cancelCtx, registryKey)
		require.NoError(t, err, "failed to start parent workflow")
		var childWorkflowId string
		require.Eventually(t, func() bool {
			v, ok := childIdRegistry.Load(registryKey)
			if ok {
				childWorkflowId = v.(string)
			}
			return ok
		}, 5*time.Second, 10*time.Millisecond, "child workflow ID was not announced")

		result, err := handle.GetResult()
		assert.True(t, errors.Is(err, &DbosError{Code: AwaitedWorkflowCancelled}), "expected AwaitedWorkflowCancelled error, got: %v", err)
		assert.Equal(t, "", result, "expected result to be an empty string")

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")

		childHandle, err := RetrieveWorkflow[string](dbosCtx, childWorkflowId)
		require.NoError(t, err, "failed to get child workflow handle")
		require.Eventually(t, func() bool {
			s, err := childHandle.GetStatus()
			return err == nil && s.Status == WorkflowStatusCancelled
		}, 5*time.Second, 50*time.Millisecond, "expected child workflow status to be WorkflowStatusCancelled")
	})

	detachedChild := func(ctx DbosContext, timeout time.Duration) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(timeout):
		}
		return "detached-step-completed", nil
	}
	detachedChildD := NewWorkflow(dbosCtx, detachedChild)

	detachedChildWorkflowParent := func(ctx DbosContext, timeout time.Duration) (string, error) {

		childCtx := WithoutCancel(ctx)
		myId, err := GetWorkflowId(ctx)
		require.NoError(t, err, "failed to get parent workflow ID")
		childId, result, err := callChildWorkflow(childCtx, detachedChildD, timeout*2)
		require.NoError(t, err, "failed to call child workflow")
		childIdRegistry.Store(myId+"-detached-child", childId)

		return result, ctx.Err()
	}
	detachedChildWorkflowParentD := NewWorkflow(dbosCtx, detachedChildWorkflowParent)

	t.Run("ChildWorkflowDetached", func(t *testing.T) {
		timeout := 500 * time.Millisecond
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, timeout)
		defer cancelFunc()
		handle, err := detachedChildWorkflowParentD(cancelCtx, timeout)
		require.NoError(t, err, "failed to start parent workflow with detached child")

		_, err = handle.GetResult()
		assert.True(t, errors.Is(err, &DbosError{Code: AwaitedWorkflowCancelled}), "expected AwaitedWorkflowCancelled error, got: %v", err)

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")

		var detachedChildId string
		require.Eventually(t, func() bool {
			v, ok := childIdRegistry.Load(handle.GetWorkflowId() + "-detached-child")
			if ok {
				detachedChildId = v.(string)
			}
			return ok
		}, 5*time.Second, 50*time.Millisecond, "child workflow ID was not announced")
		childHandle, err := RetrieveWorkflow[string](dbosCtx, detachedChildId)
		require.NoError(t, err, "failed to get child workflow handle")
		require.Eventually(t, func() bool {
			s, err := childHandle.GetStatus()
			return err == nil && s.Status == WorkflowStatusSuccess
		}, 5*time.Second, 50*time.Millisecond, "expected child workflow status to be WorkflowStatusSuccess")
	})

	t.Run("RecoverWaitForCancelWorkflow", func(t *testing.T) {
		start := time.Now()
		timeout := 1 * time.Second
		cancelCtx, cancelFunc := WithTimeout(dbosCtx, timeout)
		defer cancelFunc()
		handle, err := waitForCancelWorkflowD(cancelCtx, "recover-wait-for-cancel")
		require.NoError(t, err, "failed to start wait for cancel workflow")

		_, err = handle.GetResult()
		require.True(t, errors.Is(err, &DbosError{Code: AwaitedWorkflowCancelled}), "expected AwaitedWorkflowCancelled, got: %v", err)

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")

		setWorkflowStatusPending(t, dbosCtx, handle.GetWorkflowId())

		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")
		require.Len(t, recoveredHandles, 1, "expected 1 recovered handle, got %d", len(recoveredHandles))
		recoveredHandle := recoveredHandles[0]
		assert.Equal(t, handle.GetWorkflowId(), recoveredHandle.GetWorkflowId(), "expected recovered handle to have same ID")

		_, err = recoveredHandle.GetResult()

		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected error to be of type *DbosError, got %T", err)
		require.Equal(t, AwaitedWorkflowCancelled, dbosErr.Code)

		recoveredStatus, err := recoveredHandle.GetStatus()
		require.NoError(t, err, "failed to get recovered workflow status")
		assert.Equal(t, WorkflowStatusCancelled, recoveredStatus.Status, "expected recovered workflow status to be WorkflowStatusCancelled")

		expectedDeadline := start.Add(timeout * 10 / 100)
		assert.True(t, status.Deadline.After(expectedDeadline) && status.Deadline.Before(start.Add(timeout)),
			"expected workflow deadline to be within %v and %v, got %v", expectedDeadline, start.Add(timeout), status.Deadline)
	})
}

var pairTargetRegistry sync.Map

func announcePairTarget(role string, pairId int, workflowId string) {
	pairTargetRegistry.Store(fmt.Sprintf("%s-%d", role, pairId), workflowId)
}

func lookupPairTarget(role string, pairId int) string {
	for {
		if v, ok := pairTargetRegistry.Load(fmt.Sprintf("%s-%d", role, pairId)); ok {
			return v.(string)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func notificationWaiterWorkflow(ctx DbosContext, pairId int) (string, error) {
	setterId := lookupPairTarget("notification-setter", pairId)
	result, err := GetEvent[string](ctx, setterId, "event-key", 10*time.Second)
	if err != nil {
		return "", err
	}
	return result, nil
}

func notificationSetterWorkflow(ctx DbosContext, pairId int) (string, error) {
	err := SetEvent(ctx, "event-key", fmt.Sprintf("notification-message-%d", pairId))
	if err != nil {
		return "", err
	}
	return "event-set", nil
}

func sendRecvReceiverWorkflow(ctx DbosContext, pairId int) (string, error) {
	result, err := Recv[string](ctx, "send-recv-topic", 10*time.Second)
	if err != nil {
		return "", err
	}
	return result, nil
}

func sendRecvSenderWorkflow(ctx DbosContext, pairId int) (string, error) {
	receiverId := lookupPairTarget("send-recv-receiver", pairId)
	err := Send(ctx, receiverId, fmt.Sprintf("send-recv-message-%d", pairId), "send-recv-topic")
	if err != nil {
		return "", err
	}
	return "message-sent", nil
}

func concurrentSimpleWorkflow(dbosCtx DbosContext, input int) (int, error) {
	return Run(dbosCtx, func(ctx context.Context) (int, error) {
		return input * 2, nil
	})
}

func TestConcurrentWorkflows(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})
	concurrentSimpleWorkflowD := NewWorkflow(dbosCtx, concurrentSimpleWorkflow)
	notificationWaiterWorkflowD := NewWorkflow(dbosCtx, notificationWaiterWorkflow)
	notificationSetterWorkflowD := NewWorkflow(dbosCtx, notificationSetterWorkflow)
	sendRecvReceiverWorkflowD := NewWorkflow(dbosCtx, sendRecvReceiverWorkflow)
	sendRecvSenderWorkflowD := NewWorkflow(dbosCtx, sendRecvSenderWorkflow)

	t.Run("SimpleWorkflow", func(t *testing.T) {
		const numGoroutines = 500
		var wg sync.WaitGroup
		results := make(chan int, numGoroutines)
		errors := make(chan error, numGoroutines)

		wg.Add(numGoroutines)
		for i := range numGoroutines {
			go func(input int) {
				defer wg.Done()
				handle, err := concurrentSimpleWorkflowD(dbosCtx, input)
				if err != nil {
					errors <- fmt.Errorf("failed to start workflow %d: %w", input, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for workflow %d: %w", input, err)
					return
				}
				expectedResult := input * 2
				if result != expectedResult {
					errors <- fmt.Errorf("workflow %d: expected result %d, got %d", input, expectedResult, result)
					return
				}
				results <- result
			}(i)
		}

		wg.Wait()
		close(results)
		close(errors)

		if len(errors) > 0 {
			for err := range errors {
				t.Errorf("Error from send/recv workflows: %v", err)
			}
		}

		resultCount := 0
		receivedResults := make(map[int]bool)
		for result := range results {
			resultCount++
			if result < 0 || result >= numGoroutines*2 || result%2 != 0 {
				t.Errorf("Unexpected result %d", result)
			} else {
				receivedResults[result] = true
			}
		}

		assert.Equal(t, numGoroutines, resultCount, "Expected correct number of results")
	})

	t.Run("NotificationWorkflows", func(t *testing.T) {
		const numPairs = 500
		var wg sync.WaitGroup
		waiterResults := make(chan string, numPairs)
		setterResults := make(chan string, numPairs)
		errors := make(chan error, numPairs*2)

		wg.Add(numPairs * 2)

		for i := range numPairs {
			go func(pairId int) {
				defer wg.Done()
				handle, err := notificationSetterWorkflowD(dbosCtx, pairId)
				if err != nil {
					errors <- fmt.Errorf("failed to start setter workflow %d: %w", pairId, err)
					return
				}
				announcePairTarget("notification-setter", pairId, handle.GetWorkflowId())
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for setter workflow %d: %w", pairId, err)
					return
				}
				setterResults <- result
			}(i)

			go func(pairId int) {
				defer wg.Done()
				handle, err := notificationWaiterWorkflowD(dbosCtx, pairId)
				if err != nil {
					errors <- fmt.Errorf("failed to start waiter workflow %d: %w", pairId, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for waiter workflow %d: %w", pairId, err)
					return
				}
				expectedMessage := fmt.Sprintf("notification-message-%d", pairId)
				if result != expectedMessage {
					errors <- fmt.Errorf("waiter workflow %d: expected message '%s', got '%s'", pairId, expectedMessage, result)
					return
				}
				waiterResults <- result
			}(i)
		}

		wg.Wait()
		close(waiterResults)
		close(setterResults)
		close(errors)

		if len(errors) > 0 {
			for err := range errors {
				t.Errorf("Error from send/recv workflows: %v", err)
			}
		}

		waiterCount := 0
		receivedWaiterResults := make(map[string]bool)
		for result := range waiterResults {
			waiterCount++
			receivedWaiterResults[result] = true
		}

		setterCount := 0
		for result := range setterResults {
			setterCount++
			assert.Equal(t, "event-set", result, "Expected setter result to be 'event-set'")
		}

		assert.Equal(t, numPairs, waiterCount, "Expected correct number of waiter results")
		assert.Equal(t, numPairs, setterCount, "Expected correct number of setter results")

		for i := range numPairs {
			expectedWaiterResult := fmt.Sprintf("notification-message-%d", i)
			assert.True(t, receivedWaiterResults[expectedWaiterResult], "Expected waiter result '%s' not found", expectedWaiterResult)
		}
	})

	t.Run("SendRecvWorkflows", func(t *testing.T) {
		numPairs := 500
		var wg sync.WaitGroup
		receiverResults := make(chan string, numPairs)
		senderResults := make(chan string, numPairs)
		errors := make(chan error, numPairs*2)

		receiverHandles := make([]*WorkflowHandle[string], numPairs)
		regErrs := make([]error, numPairs)
		var regWg sync.WaitGroup
		regWg.Add(numPairs)
		for i := range numPairs {
			go func(pairId int) {
				defer regWg.Done()
				h, err := sendRecvReceiverWorkflowD(dbosCtx, pairId)
				if err != nil {
					regErrs[pairId] = err
					return
				}
				announcePairTarget("send-recv-receiver", pairId, h.GetWorkflowId())
				receiverHandles[pairId] = h
			}(i)
		}
		regWg.Wait()
		for i, err := range regErrs {
			if err != nil {
				t.Fatalf("failed to start receiver workflow %d: %v", i, err)
			}
		}

		wg.Add(numPairs * 2)

		for i := range numPairs {
			go func(pairId int) {
				defer wg.Done()
				handle := receiverHandles[pairId]
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for receiver workflow %d: %w", pairId, err)
					return
				}
				expectedMessage := fmt.Sprintf("send-recv-message-%d", pairId)
				if result != expectedMessage {
					errors <- fmt.Errorf("receiver workflow %d: expected message '%s', got '%s'", pairId, expectedMessage, result)
					return
				}
				receiverResults <- result
			}(i)

			go func(pairId int) {
				defer wg.Done()
				handle, err := sendRecvSenderWorkflowD(dbosCtx, pairId)
				if err != nil {
					errors <- fmt.Errorf("failed to start sender workflow %d: %w", pairId, err)
					return
				}
				result, err := handle.GetResult()
				if err != nil {
					errors <- fmt.Errorf("failed to get result for sender workflow %d: %w", pairId, err)
					return
				}
				senderResults <- result
			}(i)
		}

		wg.Wait()
		close(receiverResults)
		close(senderResults)
		close(errors)

		if len(errors) > 0 {
			for err := range errors {
				t.Errorf("Error from send/recv workflows: %v", err)
			}
		}

		receiverCount := 0
		receivedReceiverResults := make(map[string]bool)
		for result := range receiverResults {
			receiverCount++
			receivedReceiverResults[result] = true
		}

		senderCount := 0
		for result := range senderResults {
			senderCount++
			assert.Equal(t, "message-sent", result, "Expected sender result to be 'message-sent'")
		}

		assert.Equal(t, numPairs, receiverCount, "Expected correct number of receiver results")
		assert.Equal(t, numPairs, senderCount, "Expected correct number of sender results")

		for i := range numPairs {
			expectedReceiverResult := fmt.Sprintf("send-recv-message-%d", i)
			assert.True(t, receivedReceiverResults[expectedReceiverResult], "Expected receiver result '%s' not found", expectedReceiverResult)
		}
	})
}

func TestWorkflowAtVersion(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	simpleWorkflowD := NewWorkflow(dbosCtx, simpleWorkflow)

	version := "test-app-version-12345"
	handle, err := simpleWorkflowD(dbosCtx, "input", WithApplicationVersion(version))
	require.NoError(t, err, "failed to start workflow")

	_, err = handle.GetResult()
	require.NoError(t, err, "failed to get workflow result")

	retrieved, err := RetrieveWorkflow[string](dbosCtx, handle.GetWorkflowId())
	require.NoError(t, err, "failed to retrieve workflow")

	status, err := retrieved.GetStatus()
	require.NoError(t, err, "failed to get workflow status")
	assert.Equal(t, version, status.ApplicationVersion, "expected correct application version")
}

func TestWorkflowCancel(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	blockingEvent := NewEvent()

	blockingWorkflow := func(ctx DbosContext, topic string) (string, error) {

		blockingEvent.Wait()

		msg, err := Recv[string](ctx, topic, 5*time.Second)
		if err != nil {
			return "", err
		}
		return msg, nil
	}
	blockingWorkflowD := NewWorkflow(dbosCtx, blockingWorkflow)

	t.Run("TestWorkflowCancelWithRecvError", func(t *testing.T) {
		topic := "cancel-test-topic"

		handle, err := blockingWorkflowD(dbosCtx, topic)
		require.NoError(t, err, "failed to start blocking workflow")

		err = CancelWorkflow(dbosCtx, handle.GetWorkflowId())
		require.NoError(t, err, "failed to cancel workflow")

		blockingEvent.Set()

		result, err := handle.GetResult()
		require.Error(t, err, "expected error from cancelled workflow")
		assert.Equal(t, "", result, "expected empty result from cancelled workflow")

		var dbosErr *DbosError
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *DbosError, got %T", err)
		assert.Equal(t, AwaitedWorkflowCancelled, dbosErr.Code, "expected AwaitedWorkflowCancelled error code, got: %v", dbosErr.Code)

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to be WorkflowStatusCancelled")
	})

	t.Run("TestWorkflowCancelWithSuccess", func(t *testing.T) {
		blockingEventNoError := NewEvent()

		// Workflow that waits for an event, then calls Recv(). Does NOT return error when Recv times out
		blockingWorkflowNoError := func(ctx DbosContext, topic string) (string, error) {

			blockingEventNoError.Wait()
			Recv[string](ctx, topic, 5*time.Second)

			return "", nil
		}
		blockingWorkflowNoErrorD := NewWorkflow(dbosCtx, blockingWorkflowNoError)

		topic := "cancel-no-error-test-topic"

		handle, err := blockingWorkflowNoErrorD(dbosCtx, topic)
		require.NoError(t, err, "failed to start blocking workflow")

		err = CancelWorkflow(dbosCtx, handle.GetWorkflowId())
		require.NoError(t, err, "failed to cancel workflow")

		blockingEventNoError.Set()

		result, err := handle.GetResult()
		require.Error(t, err, "expected error from cancelled workflow")
		assert.True(t, errors.Is(err, &DbosError{Code: AwaitedWorkflowCancelled}), "expected AwaitedWorkflowCancelled error, got: %v", err)
		assert.Equal(t, "", result, "expected empty result from cancelled workflow")

		pollingHandle, err := RetrieveWorkflow[string](dbosCtx, handle.GetWorkflowId())
		require.NoError(t, err, "failed to retrieve workflow with polling handle")

		result, err = pollingHandle.GetResult()
		require.Error(t, err, "expected error from cancelled workflow even when workflow returns success")
		assert.Equal(t, "", result, "expected empty result from cancelled workflow")

		var dbosErr *DbosError
		require.ErrorAs(t, err, &dbosErr, "expected error to be of type *DbosError, got %T", err)
		assert.Equal(t, AwaitedWorkflowCancelled, dbosErr.Code, "expected AwaitedWorkflowCancelled error code, got: %v", dbosErr.Code)

		status, err := handle.GetStatus()
		require.NoError(t, err, "failed to get workflow status")
		assert.Equal(t, WorkflowStatusCancelled, status.Status, "expected workflow status to remain WorkflowStatusCancelled due to gate")
	})
}

var cancelAllBeforeBlockEvent = NewEvent()

func cancelAllBeforeBlockingWorkflow(ctx DbosContext, input string) (string, error) {
	cancelAllBeforeBlockEvent.Wait()
	return input, nil
}

func gcTestStep(_ context.Context, x int) (int, error) {
	return x, nil
}

func gcTestWorkflow(dbosCtx DbosContext, x int) (int, error) {
	result, err := Run(dbosCtx, func(ctx context.Context) (int, error) {
		return gcTestStep(ctx, x)
	})
	if err != nil {
		return 0, err
	}
	return result, nil
}

func gcBlockedWorkflow(dbosCtx DbosContext, event *Event) (string, error) {
	event.Wait()
	workflowId, err := GetWorkflowId(dbosCtx)
	if err != nil {
		return "", err
	}
	return workflowId, nil
}

func TestGarbageCollect(t *testing.T) {
	parallelTest(t)
	t.Run("GarbageCollectByWorkflowDefinitionRetention", func(t *testing.T) {
		databaseUrl := backendDatabaseUrl(t)
		resetTestDatabase(t, databaseUrl)
		dbosCtx := setupDbos(t, setupDbosOptions{dropDB: false, checkLeaks: true})

		retention := time.Hour
		workflow := NewWorkflow(dbosCtx, gcTestWorkflow, WithWorkflowRetention(retention))
		require.NoError(t, Launch(dbosCtx))
		handle, err := workflow(dbosCtx, 42)
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.NoError(t, err)

		err = dbosCtx.(*dbosContext).kernel.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{})
		require.NoError(t, err)
		workflows, err := ListWorkflows(dbosCtx)
		require.NoError(t, err)
		require.Len(t, workflows, 1, "workflow inside its retention period must remain")

		expiredAt := time.Now().Add(-2 * retention).UnixMilli()
		Kernel := dbosCtx.(*dbosContext).kernel
		query := Kernel.renderSql(`UPDATE %sworkflow_status SET completed_at = $1 WHERE workflow_uuid = $2`, "")
		_, err = Kernel.pool.Exec(dbosCtx, query, expiredAt, handle.GetWorkflowId())
		require.NoError(t, err)

		err = dbosCtx.(*dbosContext).kernel.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{})
		require.NoError(t, err)
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err)
		require.Empty(t, workflows, "workflow past its definition retention must be deleted")
	})

	t.Run("GarbageCollectWithOffset", func(t *testing.T) {

		databaseUrl := backendDatabaseUrl(t)
		resetTestDatabase(t, databaseUrl)
		dbosCtx := setupDbos(t, setupDbosOptions{dropDB: false, checkLeaks: true})
		gcTestEvent := NewEvent()

		t.Cleanup(func() {
			gcTestEvent.Set()
		})

		gcTestWorkflowD := NewWorkflow(dbosCtx, gcTestWorkflow)
		gcBlockedWorkflowD := NewWorkflow(dbosCtx, gcBlockedWorkflow)

		gcTestEvent.Clear()
		numWorkflows := 10

		blockedHandle, err := gcBlockedWorkflowD(dbosCtx, gcTestEvent)
		require.NoError(t, err, "failed to start blocked workflow")
		time.Sleep(2 * time.Millisecond)

		var completedHandles []*WorkflowHandle[int]
		for i := range numWorkflows {
			handle, err := gcTestWorkflowD(dbosCtx, i)
			require.NoError(t, err, "failed to start test workflow %d", i)
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from test workflow %d", i)
			require.Equal(t, i, result, "expected result %d, got %d", i, result)
			completedHandles = append(completedHandles, handle)
			if i < numWorkflows-1 {
				time.Sleep(2 * time.Millisecond)
			}
		}

		workflows, err := ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows")
		require.Equal(t, numWorkflows+1, len(workflows), "expected exactly %d workflows before GC", numWorkflows+1)

		// The blocked workflow won't be deleted because it's pending
		threshold := 5
		err = dbosCtx.(*dbosContext).kernel.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			rowsThreshold: &threshold,
		})
		require.NoError(t, err, "failed to garbage collect workflows")

		// - 1 blocked workflow (preserved because it's pending)
		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after GC")
		require.Equal(t, 6, len(workflows), "expected exactly 6 workflows after GC (5 from threshold + 1 pending)")

		remainingIds := make(map[string]bool)
		for _, wf := range workflows {
			remainingIds[wf.Id] = true
		}

		require.True(t, remainingIds[blockedHandle.GetWorkflowId()], "blocked workflow should still exist after GC")

		for _, wf := range workflows {
			if wf.Id == blockedHandle.GetWorkflowId() {
				require.Equal(t, WorkflowStatusPending, wf.Status, "blocked workflow should still be pending")
				break
			}
		}

		for i := range numWorkflows {
			wfId := completedHandles[i].GetWorkflowId()
			if i < numWorkflows-threshold {

				require.False(t, remainingIds[wfId], "older workflow at index %d (ID: %s) should have been deleted", i, wfId)
			} else {

				require.True(t, remainingIds[wfId], "newer workflow at index %d (ID: %s) should have been preserved", i, wfId)
			}
		}

		gcTestEvent.Set()
		result, err := blockedHandle.GetResult()
		require.NoError(t, err, "failed to get result from blocked workflow")
		require.Equal(t, blockedHandle.GetWorkflowId(), result, "expected blocked workflow to return its ID")
	})

	t.Run("GarbageCollectWithCutoffTime", func(t *testing.T) {

		databaseUrl := backendDatabaseUrl(t)
		resetTestDatabase(t, databaseUrl)
		dbosCtx := setupDbos(t, setupDbosOptions{dropDB: false, checkLeaks: true})
		gcTestEvent := NewEvent()

		t.Cleanup(func() {
			gcTestEvent.Set()
		})

		gcTestWorkflowD := NewWorkflow(dbosCtx, gcTestWorkflow)
		gcBlockedWorkflowD := NewWorkflow(dbosCtx, gcBlockedWorkflow)

		gcTestEvent.Clear()
		numWorkflows := 10

		blockedHandle, err := gcBlockedWorkflowD(dbosCtx, gcTestEvent)
		require.NoError(t, err, "failed to start blocked workflow")

		var beforeCutoffHandles []*WorkflowHandle[int]
		for i := range numWorkflows {
			handle, err := gcTestWorkflowD(dbosCtx, i)
			require.NoError(t, err, "failed to start test workflow %d", i)
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from test workflow %d", i)
			require.Equal(t, i, result, "expected result %d, got %d", i, result)
			beforeCutoffHandles = append(beforeCutoffHandles, handle)
		}

		// Wait to ensure clear time separation between batches
		time.Sleep(500 * time.Millisecond)
		cutoffTime := time.Now()
		// Additional small delay to ensure cutoff is after all first batch workflows
		time.Sleep(100 * time.Millisecond)

		var afterCutoffHandles []*WorkflowHandle[int]
		for i := numWorkflows; i < numWorkflows*2; i++ {
			handle, err := gcTestWorkflowD(dbosCtx, i)
			require.NoError(t, err, "failed to start test workflow %d", i)
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from test workflow %d", i)
			require.Equal(t, i, result, "expected result %d, got %d", i, result)
			afterCutoffHandles = append(afterCutoffHandles, handle)
		}

		workflows, err := ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows")
		require.Equal(t, 21, len(workflows), "expected exactly 21 workflows before GC (1 blocked + 10 old + 10 new)")

		cutoffTimestamp := cutoffTime.UnixMilli()
		err = dbosCtx.(*dbosContext).kernel.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			cutoffEpochTimestampMs: &cutoffTimestamp,
		})
		require.NoError(t, err, "failed to garbage collect workflows by time")

		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after time-based GC")
		require.Equal(t, 11, len(workflows), "expected exactly 11 workflows after time-based GC (1 blocked + 10 new)")

		remainingIds := make(map[string]bool)
		for _, wf := range workflows {
			remainingIds[wf.Id] = true
		}

		require.True(t, remainingIds[blockedHandle.GetWorkflowId()], "blocked workflow should still exist after GC")

		for _, handle := range beforeCutoffHandles {
			wfId := handle.GetWorkflowId()
			require.False(t, remainingIds[wfId], "workflow created before cutoff (ID: %s) should have been deleted", wfId)
		}

		for _, handle := range afterCutoffHandles {
			wfId := handle.GetWorkflowId()
			require.True(t, remainingIds[wfId], "workflow created after cutoff (ID: %s) should have been preserved", wfId)
		}

		gcTestEvent.Set()
		result, err := blockedHandle.GetResult()
		require.NoError(t, err, "failed to get result from blocked workflow")
		require.Equal(t, blockedHandle.GetWorkflowId(), result, "expected blocked workflow to return its ID")

		// Wait a moment to ensure the completed workflow timestamp is after creation
		time.Sleep(100 * time.Millisecond)

		futureTimestamp := time.Now().Add(1 * time.Hour).UnixMilli()
		err = dbosCtx.(*dbosContext).kernel.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			cutoffEpochTimestampMs: &futureTimestamp,
		})
		require.NoError(t, err, "failed to garbage collect all completed workflows")

		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after final GC")
		require.Equal(t, 0, len(workflows), "expected exactly 0 workflows after final GC")
	})

	t.Run("GarbageCollectEmptyDatabase", func(t *testing.T) {

		databaseUrl := backendDatabaseUrl(t)
		resetTestDatabase(t, databaseUrl)
		dbosCtx := setupDbos(t, setupDbosOptions{dropDB: false, checkLeaks: true})

		NewWorkflow(dbosCtx, gcTestWorkflow)
		NewWorkflow(dbosCtx, gcBlockedWorkflow)

		workflows, err := ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows")
		require.Equal(t, 0, len(workflows), "expected exactly 0 workflows in empty database")

		// Verify GC runs without errors on a blank table
		threshold := 1
		err = dbosCtx.(*dbosContext).kernel.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			rowsThreshold: &threshold,
		})
		require.NoError(t, err, "garbage collect should work on empty database")

		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after row-based GC")
		require.Equal(t, 0, len(workflows), "expected exactly 0 workflows after row-based GC on empty database")

		currentTimestamp := time.Now().UnixMilli()
		err = dbosCtx.(*dbosContext).kernel.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			cutoffEpochTimestampMs: &currentTimestamp,
		})
		require.NoError(t, err, "time-based garbage collect should work on empty database")

		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after time-based GC")
		require.Equal(t, 0, len(workflows), "expected exactly 0 workflows after time-based GC on empty database")
	})

	t.Run("GarbageCollectOnlyCompletedWorkflows", func(t *testing.T) {

		databaseUrl := backendDatabaseUrl(t)
		resetTestDatabase(t, databaseUrl)
		dbosCtx := setupDbos(t, setupDbosOptions{dropDB: false, checkLeaks: true})
		gcTestEvent := NewEvent()

		t.Cleanup(func() {
			gcTestEvent.Set()
		})

		gcTestWorkflowD := NewWorkflow(dbosCtx, gcTestWorkflow)
		gcBlockedWorkflowD := NewWorkflow(dbosCtx, gcBlockedWorkflow)

		gcTestEvent.Clear()
		numWorkflows := 5

		blockedHandle, err := gcBlockedWorkflowD(dbosCtx, gcTestEvent)
		require.NoError(t, err, "failed to start blocked workflow")
		time.Sleep(2 * time.Millisecond)

		for i := range numWorkflows {
			handle, err := gcTestWorkflowD(dbosCtx, i)
			require.NoError(t, err, "failed to start test workflow %d", i)
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from test workflow %d", i)
			require.Equal(t, i, result, "expected result %d, got %d", i, result)
			if i < numWorkflows-1 {
				time.Sleep(2 * time.Millisecond)
			}
		}

		workflows, err := ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows")
		require.Equal(t, numWorkflows+1, len(workflows), "expected exactly %d workflows", numWorkflows+1)

		pendingCount := 0
		completedCount := 0
		for _, wf := range workflows {
			switch wf.Status {
			case WorkflowStatusPending:
				pendingCount++
			case WorkflowStatusSuccess:
				completedCount++
			}
		}
		require.Equal(t, 1, pendingCount, "expected exactly 1 pending workflow")
		require.Equal(t, numWorkflows, completedCount, "expected exactly %d completed workflows", numWorkflows)

		// The blocked workflow is the oldest but won't be deleted because it's pending
		// So we should have 2 workflows: 1 newest completed + 1 pending
		threshold := 1
		err = dbosCtx.(*dbosContext).kernel.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			rowsThreshold: &threshold,
		})
		require.NoError(t, err, "failed to garbage collect workflows")

		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after GC")
		require.Equal(t, 2, len(workflows), "expected exactly 2 workflows after GC (1 newest + 1 pending)")

		found := false
		pendingCount = 0
		completedCount = 0
		for _, wf := range workflows {
			if wf.Id == blockedHandle.GetWorkflowId() {
				found = true
				require.Equal(t, WorkflowStatusPending, wf.Status, "blocked workflow should still be pending")
			}
			switch wf.Status {
			case WorkflowStatusPending:
				pendingCount++
			case WorkflowStatusSuccess:
				completedCount++
			}
		}
		require.True(t, found, "pending workflow should remain")
		require.Equal(t, 1, pendingCount, "expected exactly 1 pending workflow after GC")
		require.Equal(t, 1, completedCount, "expected exactly 1 completed workflow after GC")

		gcTestEvent.Set()
		result, err := blockedHandle.GetResult()
		require.NoError(t, err, "failed to get result from blocked workflow")
		require.Equal(t, blockedHandle.GetWorkflowId(), result, "expected blocked workflow to return its ID")

		// Wait a moment to ensure the completed workflow timestamp is after creation
		time.Sleep(100 * time.Millisecond)

		futureTimestamp := time.Now().Add(1 * time.Hour).UnixMilli()
		err = dbosCtx.(*dbosContext).kernel.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			cutoffEpochTimestampMs: &futureTimestamp,
		})
		require.NoError(t, err, "failed to garbage collect all workflows")

		workflows, err = ListWorkflows(dbosCtx)
		require.NoError(t, err, "failed to list workflows after final GC")
		require.Equal(t, 0, len(workflows), "expected exactly 0 workflows after final GC")
	})

	t.Run("ThresholdAndCutoffTimestampInteraction", func(t *testing.T) {

		databaseUrl := backendDatabaseUrl(t)
		resetTestDatabase(t, databaseUrl)
		dbosCtx := setupDbos(t, setupDbosOptions{dropDB: false, checkLeaks: true})

		gcTestWorkflowD := NewWorkflow(dbosCtx, gcTestWorkflow)

		numWorkflows := 10
		handles := make([]*WorkflowHandle[int], numWorkflows)

		for i := range numWorkflows {
			handle, err := gcTestWorkflowD(dbosCtx, i)
			require.NoError(t, err, "failed to start workflow %d", i)
			handles[i] = handle

			// Add small delay to ensure distinct timestamps
			time.Sleep(10 * time.Millisecond)
		}

		for i, handle := range handles {
			result, err := handle.GetResult()
			require.NoError(t, err, "failed to get result from workflow %d", i)
			require.Equal(t, i, result)
		}

		workflows, err := ListWorkflows(dbosCtx, WithSortDesc())
		require.NoError(t, err, "failed to list workflows")
		require.Equal(t, numWorkflows, len(workflows))

		var cutoff1 int64
		var cutoff2 int64

		cutoff1 = workflows[7].CreatedAt.UnixMilli()
		cutoff2 = workflows[1].CreatedAt.UnixMilli()

		threshold := 6
		err = dbosCtx.(*dbosContext).kernel.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			rowsThreshold:          &threshold,
			cutoffEpochTimestampMs: &cutoff1,
		})
		require.NoError(t, err, "failed to garbage collect with threshold 6 and 7th newest timestamp")

		workflows, err = ListWorkflows(dbosCtx, WithSortDesc())
		require.NoError(t, err, "failed to list workflows after first GC")
		require.Equal(t, threshold, len(workflows), "expected 6 workflows when threshold has more recent cutoff than timestamp")

		for i := 0; i < len(workflows)-threshold; i++ {
			require.Equal(t, workflows[i].Id, handles[i].GetWorkflowId(), "expected workflow %d to remain", i)
		}

		threshold = 3
		err = dbosCtx.(*dbosContext).kernel.garbageCollectWorkflows(dbosCtx, garbageCollectWorkflowsInput{
			rowsThreshold:          &threshold,
			cutoffEpochTimestampMs: &cutoff2,
		})
		require.NoError(t, err, "failed to garbage collect with threshold 3 and 2nd newest timestamp")

		workflows, err = ListWorkflows(dbosCtx, WithSortDesc())
		require.NoError(t, err, "failed to list workflows after second GC")
		require.Equal(t, 2, len(workflows), "expected 2 workflows after second GC")
		require.Equal(t, workflows[0].Id, handles[numWorkflows-1].GetWorkflowId(), "expected newest workflow to remain")
		require.Equal(t, workflows[1].Id, handles[numWorkflows-2].GetWorkflowId(), "expected 2nd newest workflow to remain")
	})
}

func TestDeduplicationCollapsesIntoExistingWorkflow(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})
	workflow := NewWorkflow(dbosCtx, simpleWorkflow, WithWorkflowName("deduplicated-workflow"))
	require.NoError(t, Launch(dbosCtx))

	deduplicationId := uuid.NewString()
	first, err := workflow(dbosCtx, "first", WithDeduplicationId(deduplicationId))
	require.NoError(t, err)
	firstResult, err := first.GetResult()
	require.NoError(t, err)
	require.Equal(t, "first", firstResult)

	second, err := workflow(dbosCtx, "second", WithDeduplicationId(deduplicationId))
	require.NoError(t, err)
	require.Equal(t, first.GetWorkflowId(), second.GetWorkflowId())
	secondResult, err := second.GetResult()
	require.NoError(t, err)
	require.Equal(t, firstResult, secondResult)
}

func TestSpecialSteps(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	childEvent := NewEvent()

	childWorkflow := func(dbosCtx DbosContext, input string) (string, error) {

		childEvent.Wait()
		return fmt.Sprintf("auxiliary-result-%s", input), nil
	}
	childWorkflowD := NewWorkflow(dbosCtx, childWorkflow, WithWorkflowName("child-workflow"))

	specialStepsWorkflow := func(dbosCtx DbosContext, input string) (string, error) {
		currentWorkflowId, err := GetWorkflowId(dbosCtx)
		if err != nil {
			return "", fmt.Errorf("failed to get current workflow ID: %w", err)
		}

		// rerun replays the recorded child workflow ID instead of starting a new one.
		childId, err := startChildWorkflow(dbosCtx, childWorkflowD, "test")
		if err != nil {
			return "", fmt.Errorf("failed to start child workflow: %w", err)
		}

		err = CancelWorkflow(dbosCtx, childId)
		if err != nil {
			return "", fmt.Errorf("CancelWorkflow failed: %w", err)
		}

		retrievedHandle, err := RetrieveWorkflow[string](dbosCtx, childId)
		if err != nil {
			return "", fmt.Errorf("RetrieveWorkflow failed: %w", err)
		}
		if retrievedHandle.GetWorkflowId() != childId {
			return "", fmt.Errorf("RetrieveWorkflow returned wrong workflow ID")
		}

		status, err := retrievedHandle.GetStatus()
		if err != nil {
			return "", fmt.Errorf("failed to get status of retrieved workflow: %w", err)
		}
		if status.Status != WorkflowStatusCancelled {
			return "", fmt.Errorf("expected cancelled workflow status, got %v", status.Status)
		}

		resumeHandle, err := ResumeWorkflow[string](dbosCtx, childId)
		if err != nil {
			return "", fmt.Errorf("ResumeWorkflow failed: %w", err)
		}
		if resumeHandle.GetWorkflowId() != childId {
			return "", fmt.Errorf("ResumeWorkflow returned wrong workflow ID")
		}

		forkHandle, err := ForkWorkflow[string](dbosCtx, ForkWorkflowInput{
			OriginalWorkflowId: currentWorkflowId,
			StartStep:          0,
		})
		if err != nil {
			return "", fmt.Errorf("ForkWorkflow failed: %w", err)
		}
		if forkHandle.GetWorkflowId() == "" {
			return "", fmt.Errorf("ForkWorkflow returned empty workflow ID")
		}

		steps, err := GetWorkflowSteps(dbosCtx, currentWorkflowId)
		if err != nil {
			return "", fmt.Errorf("GetWorkflowSteps failed: %w", err)
		}
		if len(steps) != 7 {
			t.Logf("Expected 7 steps so far, got %d", len(steps))
			for step := range steps {
				t.Logf("Step %d: %s (Error: %v)\n", steps[step].StepId, steps[step].StepName, steps[step].Error)
			}
			return "", fmt.Errorf("Expected 7 steps so far, got %d", len(steps))
		}

		workflows, err := ListWorkflows(dbosCtx, WithLimit(100))
		if err != nil {
			return "", fmt.Errorf("ListWorkflows failed: %w", err)
		}

		foundMain := false
		foundChild := false
		foundForked := false
		for _, wf := range workflows {
			if wf.Id == currentWorkflowId {
				foundMain = true
			}
			if wf.Id == childId {
				foundChild = true
			}
			if wf.Id == forkHandle.GetWorkflowId() {
				foundForked = true
			}
		}
		if !foundMain || !foundChild || !foundForked {
			return "", fmt.Errorf("ListWorkflows did not return expected workflows. Found main: %v, child: %v, forked: %v", foundMain, foundChild, foundForked)
		}

		childEvent.Set()

		return "success", nil
	}

	specialStepsWorkflowD := NewWorkflow(dbosCtx, specialStepsWorkflow)

	t.Run("SpecialStepsExecution", func(t *testing.T) {
		handle, err := specialStepsWorkflowD(dbosCtx, "test-input")
		require.NoError(t, err, "failed to start special steps workflow")
		workflowId := handle.GetWorkflowId()

		result, err := handle.GetResult()
		require.NoError(t, err, "workflow should complete successfully")
		require.Equal(t, "success", result, "workflow should return success")

		setWorkflowStatusPending(t, dbosCtx, handle.GetWorkflowId())
		recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
		require.NoError(t, err, "failed to recover pending workflows")

		var recoveredHandle *WorkflowHandle[any]
		for _, h := range recoveredHandles {
			if h.GetWorkflowId() == workflowId {
				recoveredHandle = h
				break
			}
		}
		require.NotNil(t, recoveredHandle, "workflow should be recovered")

		recoveredResult, err := recoveredHandle.GetResult()
		require.NoError(t, err, "recovered workflow should complete successfully")
		require.Equal(t, "success", recoveredResult, "recovered workflow should return same result")

		steps, err := GetWorkflowSteps(dbosCtx, workflowId)
		require.NoError(t, err, "failed to get workflow steps")
		require.Len(t, steps, 9, "expected 9 steps")
		expectedSteps := []string{
			"DBOS.uuid",
			"startChildWorkflow",
			"DBOS.cancelWorkflow",
			"DBOS.retrieveWorkflow",
			"DBOS.getStatus",
			"DBOS.resumeWorkflow",
			"DBOS.forkWorkflow",
			"DBOS.getWorkflowSteps",
			"DBOS.listWorkflows",
		}
		for i, name := range expectedSteps {
			require.Equal(t, name, steps[i].StepName, "unexpected step at index %d", i)
		}
	})
}

func TestRegisteredWorkflowListing(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	NewWorkflow(dbosCtx, simpleWorkflow)
	NewWorkflow(dbosCtx, simpleWorkflowError, WithMaxRetries(5))
	NewWorkflow(dbosCtx, simpleWorkflowWithStep, WithWorkflowName("CustomStepWorkflow"))
	NewWorkflow(dbosCtx, simpleWorkflowWithSchedule, WithWorkflowName("ScheduledWorkflow"), WithSchedule("0 0 * * * *"))

	err := Launch(dbosCtx)
	require.NoError(t, err, "failed to launch DBOS")

	t.Run("ListRegisteredWorkflows", func(t *testing.T) {
		workflows, err := ListRegisteredWorkflows(dbosCtx)
		require.NoError(t, err, "ListRegisteredWorkflows should not return an error")

		require.GreaterOrEqual(t, len(workflows), 4, "Should have 4 registered workflows")

		workflowMap := make(map[string]WorkflowRegistryEntry)
		for _, wf := range workflows {
			workflowMap[wf.FQN] = wf
		}

		simpleWorkflowFQN := runtime.FuncForPC(reflect.ValueOf(simpleWorkflow).Pointer()).Name()
		simpleWf, exists := workflowMap[simpleWorkflowFQN]
		require.True(t, exists, "simpleWorkflow should be registered")
		require.Equal(t, _defaultMaxRecoveryAttempts, simpleWf.MaxRetries, "simpleWorkflow should have default max retries")
		require.Empty(t, simpleWf.CronSchedule, "simpleWorkflow should not have cron schedule")

		simpleWorkflowErrorFQN := runtime.FuncForPC(reflect.ValueOf(simpleWorkflowError).Pointer()).Name()
		errorWf, exists := workflowMap[simpleWorkflowErrorFQN]
		require.True(t, exists, "simpleWorkflowError should be registered")
		require.Equal(t, 5, errorWf.MaxRetries, "simpleWorkflowError should have custom max retries")
		require.Empty(t, errorWf.CronSchedule, "simpleWorkflowError should not have cron schedule")

		customStepWorkflowFQN := runtime.FuncForPC(reflect.ValueOf(simpleWorkflowWithStep).Pointer()).Name()
		customWf, exists := workflowMap[customStepWorkflowFQN]
		require.True(t, exists, "CustomStepWorkflow should be found")
		require.Equal(t, "CustomStepWorkflow", customWf.Name, "CustomStepWorkflow should have the correct name")
		require.Empty(t, customWf.CronSchedule, "CustomStepWorkflow should not have cron schedule")

		scheduledWorkflowFQN := runtime.FuncForPC(reflect.ValueOf(simpleWorkflowWithSchedule).Pointer()).Name()
		scheduledWf, exists := workflowMap[scheduledWorkflowFQN]
		require.True(t, exists, "ScheduledWorkflow should be found")
		require.Equal(t, "ScheduledWorkflow", scheduledWf.Name, "ScheduledWorkflow should have the correct name")
		require.Equal(t, "0 0 * * * *", scheduledWf.CronSchedule, "ScheduledWorkflow should have the correct cron schedule")
	})

	t.Run("ListRegisteredWorkflowsWithScheduledOnly", func(t *testing.T) {
		scheduledWorkflows, err := ListRegisteredWorkflows(dbosCtx, WithScheduledOnly())
		require.NoError(t, err, "ListRegisteredWorkflows with WithScheduledOnly should not return an error")
		require.Equal(t, 1, len(scheduledWorkflows), "Should have exactly 1 scheduled workflow")

		entry := scheduledWorkflows[0]
		scheduledWorkflowFQN := runtime.FuncForPC(reflect.ValueOf(simpleWorkflowWithSchedule).Pointer()).Name()
		require.Equal(t, scheduledWorkflowFQN, entry.FQN, "ScheduledWorkflow should have the correct FQN")
		require.Equal(t, "0 0 * * * *", entry.CronSchedule, "ScheduledWorkflow should have the correct cron schedule")
	})
}

func TestWorkflowIdentity(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})
	simpleWorkflowD := NewWorkflow(dbosCtx, simpleWorkflow)
	require.NoError(t, Launch(dbosCtx))
	handle, err := simpleWorkflowD(
		dbosCtx,
		"test",
		WithAuthenticatedUser("user123"),
		WithAssumedRole("admin"),
		WithAuthenticatedRoles([]string{"reader", "writer"}))
	require.NoError(t, err, "failed to start workflow")

	status, err := handle.GetStatus()
	require.NoError(t, err)

	t.Run("CheckAuthenticatedUser", func(t *testing.T) {
		assert.Equal(t, "user123", status.AuthenticatedUser)
	})

	t.Run("CheckAssumedRole", func(t *testing.T) {
		assert.Equal(t, "admin", status.AssumedRole)
	})

	t.Run("CheckAuthenticatedRoles", func(t *testing.T) {
		assert.Equal(t, []string{"reader", "writer"}, status.AuthenticatedRoles)
	})
}

type authSnapshot struct {
	User  string
	Role  string
	Roles []string
}

func captureAuthFromDB(ctx DbosContext) (authSnapshot, error) {
	wfId, err := GetWorkflowId(ctx)
	if err != nil {
		return authSnapshot{}, err
	}
	rows, err := ctx.(*dbosContext).kernel.listWorkflows(ctx, listWorkflowsDBInput{
		workflowIds: []string{wfId},
	})
	if err != nil || len(rows) == 0 {
		return authSnapshot{}, err
	}
	return authSnapshot{
		User:  rows[0].AuthenticatedUser,
		Role:  rows[0].AssumedRole,
		Roles: rows[0].AuthenticatedRoles,
	}, nil
}

var (
	authChildWorkflowD  Workflow[string, authSnapshot]
	authParentWorkflowD Workflow[string, authSnapshot]
)

func authChildWorkflow(ctx DbosContext, _ string) (authSnapshot, error) {
	return captureAuthFromDB(ctx)
}

// authParentWorkflow calls authChildWorkflow within a step without passing any auth options.
func authParentWorkflow(ctx DbosContext, _ string) (authSnapshot, error) {
	_, result, err := callChildWorkflow(ctx, authChildWorkflowD, "")
	return result, err
}

func authGrandparentWorkflow(ctx DbosContext, _ string) (authSnapshot, error) {
	_, result, err := callChildWorkflow(ctx, authParentWorkflowD, "")
	return result, err
}

func authParentWithOverrideWorkflow(ctx DbosContext, _ string) (authSnapshot, error) {
	_, result, err := callChildWorkflow(ctx, authChildWorkflowD, "",
		WithAuthenticatedUser("service-account"),
		WithAssumedRole("service"),
		WithAuthenticatedRoles([]string{"internal"}),
	)
	return result, err
}

func TestWorkflowAuthIndependence(t *testing.T) {
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})
	authChildWorkflowD = NewWorkflow(dbosCtx, authChildWorkflow)
	authParentWorkflowD = NewWorkflow(dbosCtx, authParentWorkflow)
	authGrandparentWorkflowD := NewWorkflow(dbosCtx, authGrandparentWorkflow)
	authParentWithOverrideWorkflowD := NewWorkflow(dbosCtx, authParentWithOverrideWorkflow)
	require.NoError(t, Launch(dbosCtx))

	t.Run("ParentAuthDoesNotPropagate", func(t *testing.T) {
		handle, err := authParentWorkflowD(dbosCtx, "",
			WithAuthenticatedUser("alice@example.com"),
			WithAssumedRole("customer"),
			WithAuthenticatedRoles([]string{"read", "write"}),
		)
		require.NoError(t, err)
		childAuth, err := handle.GetResult()
		require.NoError(t, err)
		assert.Empty(t, childAuth.User)
		assert.Empty(t, childAuth.Role)
		assert.Empty(t, childAuth.Roles)
	})

	t.Run("ChildExplicitOverridesParent", func(t *testing.T) {
		handle, err := authParentWithOverrideWorkflowD(dbosCtx, "",
			WithAuthenticatedUser("alice@example.com"),
			WithAssumedRole("customer"),
			WithAuthenticatedRoles([]string{"read", "write"}),
		)
		require.NoError(t, err)
		childAuth, err := handle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "service-account", childAuth.User)
		assert.Equal(t, "service", childAuth.Role)
		assert.Equal(t, []string{"internal"}, childAuth.Roles)
	})

	t.Run("EmptyParentLeavesChildEmpty", func(t *testing.T) {
		handle, err := authParentWorkflowD(dbosCtx, "")
		require.NoError(t, err)
		childAuth, err := handle.GetResult()
		require.NoError(t, err)
		assert.Empty(t, childAuth.User)
		assert.Empty(t, childAuth.Role)
		assert.Empty(t, childAuth.Roles)
	})

	t.Run("AuthDoesNotPropagateMultipleLevels", func(t *testing.T) {
		handle, err := authGrandparentWorkflowD(dbosCtx, "",
			WithAuthenticatedUser("alice@example.com"),
			WithAssumedRole("customer"),
			WithAuthenticatedRoles([]string{"read", "write"}),
		)
		require.NoError(t, err)
		grandchildAuth, err := handle.GetResult()
		require.NoError(t, err)
		assert.Empty(t, grandchildAuth.User)
		assert.Empty(t, grandchildAuth.Role)
		assert.Empty(t, grandchildAuth.Roles)
	})
}

func TestWorkflowHandles(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})
	slowWorkflowD := NewWorkflow(dbosCtx, slowWorkflow)

	workflowSleep := 1 * time.Second

	t.Run("WorkflowHandleTimeout", func(t *testing.T) {
		handle, err := slowWorkflowD(dbosCtx, workflowSleep)
		require.NoError(t, err, "failed to start workflow")

		start := time.Now()
		_, err = handle.GetResult(WithHandleTimeout(10*time.Millisecond), WithHandlePollingInterval(1*time.Millisecond))
		duration := time.Since(start)

		require.Error(t, err, "expected timeout error")
		assert.Contains(t, err.Error(), "workflow result timeout")
		assert.True(t, duration < 100*time.Millisecond, "timeout should occur quickly")
		assert.True(t, errors.Is(err, context.DeadlineExceeded),
			"expected error to be detectable as context.DeadlineExceeded, got: %v", err)
	})

	t.Run("WorkflowPollingHandleTimeout", func(t *testing.T) {

		originalHandle, err := slowWorkflowD(dbosCtx, workflowSleep)
		require.NoError(t, err, "failed to start workflow")

		pollingHandle, err := RetrieveWorkflow[string](dbosCtx, originalHandle.GetWorkflowId())
		require.NoError(t, err, "failed to retrieve workflow")

		start := time.Now()
		_, err = pollingHandle.GetResult(WithHandleTimeout(10*time.Millisecond), WithHandlePollingInterval(1*time.Millisecond))
		duration := time.Since(start)

		assert.True(t, duration < 100*time.Millisecond, "timeout should occur quickly")
		require.Error(t, err, "expected timeout error")
		assert.True(t, errors.Is(err, context.DeadlineExceeded),
			"expected error to be detectable as context.DeadlineExceeded, got: %v", err)
	})
}

func TestWorkflowHandleContextCancel(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})
	getEventWorkflowD := NewWorkflow(dbosCtx, getEventWorkflow)

	t.Run("WorkflowHandleContextCancel", func(t *testing.T) {
		getEventWorkflowStartedSignal.Clear()
		handle, err := getEventWorkflowD(dbosCtx, getEventWorkflowInput{
			TargetWorkflowId: "test-workflow-id",
			Key:              "test-key",
		})
		require.NoError(t, err, "failed to start workflow")

		resultChan := make(chan error)
		go func() {
			_, err := handle.GetResult()
			resultChan <- err
		}()

		getEventWorkflowStartedSignal.Wait()
		getEventWorkflowStartedSignal.Clear()

		dbosCtx.Shutdown(1 * time.Second)

		err = <-resultChan
		require.Error(t, err, "expected error from cancelled context")
	})
}

func TestPatching(t *testing.T) {
	t.Run("PatchingEnabled", func(t *testing.T) {

		databaseUrl := backendDatabaseUrl(t)
		resetTestDatabase(t, databaseUrl)
		dbosCtx, err := NewDbosContext(context.Background(), Config{
			DatabaseUrl:        databaseUrl,
			AppName:            "test-app-patching-enabled",
			EnablePatching:     true,
			ApplicationVersion: "PATCHING_ENABLED",
		})
		require.NoError(t, err, "failed to create DBOS context with patching enabled")
		require.Equal(t, "PATCHING_ENABLED", dbosCtx.GetApplicationVersion(), "expected application version to be PATCHING_ENABLED")

		t.Cleanup(func() {
			if dbosCtx != nil {
				Shutdown(dbosCtx, 30*time.Second)
			}
		})

		step := func(input int) (int, error) {
			return input + 1, nil
		}

		stepPatched := func(input int) (int, error) {
			return input + 2, nil
		}

		wf := func(ctx DbosContext, input int) (int, error) {

			Run(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("firstStep"))

			res, err := Run(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("patch-step"))
			if err != nil {
				return 0, err
			}

			Run(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("lastStep"))
			return res, nil
		}

		wfD := NewWorkflow(dbosCtx, wf, WithWorkflowName("wf"))
		require.NoError(t, Launch(dbosCtx))

		handle, err := wfD(dbosCtx, 1)
		require.NoError(t, err, "failed to start workflow")
		result, err := handle.GetResult()
		require.NoError(t, err, "failed to get result")
		require.Equal(t, 2, result, "expected result to be 2")

		wfPatched := func(ctx DbosContext, input int) (int, error) {

			Run(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("firstStep"))

			patched, err := Patch(ctx, "my-patch")
			if err != nil {
				return 0, err
			}
			var res int
			if patched {
				res, err = Run(ctx, func(ctx context.Context) (int, error) {
					return stepPatched(input)
				}, WithStepName("patched-step"))
				if err != nil {
					return 0, err
				}
			} else {
				res, err = Run(ctx, func(ctx context.Context) (int, error) {
					return step(input)
				}, WithStepName("patch-step"))
				if err != nil {
					return 0, err
				}
			}

			Run(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("lastStep"))

			return res, nil
		}

		dbosCtx.(*dbosContext).launched.Store(false)
		ClearRegistries(dbosCtx)
		wfPatchedD := NewWorkflow(dbosCtx, wfPatched, WithWorkflowName("wf"))
		dbosCtx.(*dbosContext).launched.Store(true)

		patchedHandle, err := wfPatchedD(dbosCtx, 1)
		require.NoError(t, err, "failed to start workflow")
		result, err = patchedHandle.GetResult()
		require.NoError(t, err, "failed to get result")
		require.Equal(t, 3, result, "expected result to be 3")
		steps, err := GetWorkflowSteps(dbosCtx, patchedHandle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps")
		require.Equal(t, 4, len(steps), "expected 4 steps")
		require.Equal(t, "DBOS.patch-my-patch", steps[1].StepName, "expected step name to be DBOS.patch-my-patch")

		for startStep := 0; startStep <= 2; startStep++ {
			forkHandle, err := ForkWorkflow[int](dbosCtx, ForkWorkflowInput{
				OriginalWorkflowId: handle.GetWorkflowId(),
				StartStep:          uint(startStep),
			})
			require.NoError(t, err, "failed to fork workflow at step %d", startStep)
			result, err := forkHandle.GetResult()
			require.NoError(t, err, "failed to get result for fork at step %d", startStep)
			steps, err := GetWorkflowSteps(dbosCtx, forkHandle.GetWorkflowId())
			require.NoError(t, err, "failed to get workflow steps for fork at step %d", startStep)

			if startStep < 2 {

				require.Equal(t, 3, result, "expected result to be 3 when forking at step %d", startStep)
				require.Equal(t, 4, len(steps), "expected 4 steps when forking at step %d", startStep)
				require.Equal(t, "DBOS.patch-my-patch", steps[1].StepName, "expected step name to be DBOS.patch-my-patch when forking at step %d", startStep)
			} else {

				require.Equal(t, 2, result, "expected result to be 2 when forking at step %d", startStep)
				require.Equal(t, 3, len(steps), "expected 3 steps when forking at step %d", startStep)
			}
		}

		wfDeprecatePatch := func(ctx DbosContext, input int) (int, error) {
			Run(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("firstStep"))
			DeprecatePatch(ctx, "my-patch")
			res, err := Run(ctx, func(ctx context.Context) (int, error) {
				return stepPatched(input)
			}, WithStepName("patched-step"))
			if err != nil {
				return 0, err
			}
			Run(ctx, func(ctx context.Context) (int, error) {
				return step(input)
			}, WithStepName("lastStep"))
			return res, nil
		}

		dbosCtx.(*dbosContext).launched.Store(false)
		ClearRegistries(dbosCtx)
		wfDeprecatePatchD := NewWorkflow(dbosCtx, wfDeprecatePatch, WithWorkflowName("wf"))
		dbosCtx.(*dbosContext).launched.Store(true)

		deprecatedHandle, err := wfDeprecatePatchD(dbosCtx, 1)
		require.NoError(t, err, "failed to start workflow")
		result, err = deprecatedHandle.GetResult()
		require.NoError(t, err, "failed to get result")
		require.Equal(t, 3, result, "expected result to be 3")
		steps, err = GetWorkflowSteps(dbosCtx, deprecatedHandle.GetWorkflowId())
		require.NoError(t, err, "failed to get workflow steps")
		require.Equal(t, 3, len(steps), "expected 3 steps")

		// Forking an old workflow (post-patch), at or after the patch step, on the new code should work without non-determinism errors
		// Because step 1 (the patch) is matched by DeprecatePatch in the new code
		for _, startStep := range []uint{2, 3} {
			forkHandle, err := ForkWorkflow[int](dbosCtx, ForkWorkflowInput{
				OriginalWorkflowId: patchedHandle.GetWorkflowId(),
				StartStep:          uint(startStep),
			})
			require.NoError(t, err, "failed to fork workflow")
			result, err = forkHandle.GetResult()
			require.NoError(t, err, "failed to get result")
			require.Equal(t, 3, result, "expected result to be 3")
			steps, err = GetWorkflowSteps(dbosCtx, forkHandle.GetWorkflowId())
			require.NoError(t, err, "failed to get workflow steps")
			require.Equal(t, 4, len(steps), "expected 4 steps")
			require.Equal(t, "DBOS.patch-my-patch", steps[1].StepName, "expected step name to be DBOS.patch-my-patch")
		}

		// Forking an old workflow (pre-patch), after the patch step, on the new code will result in a non-determinism error, because the 2nd step name changed
		// Because the patch step now has a new name
		forkHandle, err := ForkWorkflow[int](dbosCtx, ForkWorkflowInput{
			OriginalWorkflowId: handle.GetWorkflowId(),
			StartStep:          2,
		})
		require.NoError(t, err, "failed to fork workflow")
		_, err = forkHandle.GetResult()
		require.Error(t, err, "expected error when forking old workflow onto new workflow")
		require.Contains(t, err.Error(), fmt.Sprintf("DBOS Error %d", UnexpectedStep))
	})

	t.Run("PatchingNotEnabledError", func(t *testing.T) {
		// Create a DBOS context without enabling patching
		databaseUrl := backendDatabaseUrl(t)
		dbosCtxNoPatching, err := NewDbosContext(context.Background(), Config{
			DatabaseUrl:    databaseUrl,
			AppName:        "test-app-no-patching",
			EnablePatching: false,
		})
		require.NoError(t, err, "failed to create DBOS context without patching")
		require.False(t, dbosCtxNoPatching.GetApplicationVersion() == "PATCHING_ENABLED", "expected application version to not be PATCHING_ENABLED")

		wfWithPatch := func(ctx DbosContext, input int) (int, error) {
			patched, err := Patch(ctx, "test-patch")
			if err != nil {
				return 0, err
			}
			if patched {
				return input + 10, nil
			}
			return input, nil
		}
		wfWithPatchD := NewWorkflow(dbosCtxNoPatching, wfWithPatch)

		wfWithDeprecatePatch := func(ctx DbosContext, input int) (int, error) {
			err := DeprecatePatch(ctx, "test-patch")
			if err != nil {
				return 0, err
			}
			return input + 10, nil
		}
		wfWithDeprecatePatchD := NewWorkflow(dbosCtxNoPatching, wfWithDeprecatePatch)

		err = Launch(dbosCtxNoPatching)
		require.NoError(t, err, "failed to launch DBOS context")
		defer Shutdown(dbosCtxNoPatching, 10*time.Second)

		handle, err := wfWithPatchD(dbosCtxNoPatching, 1)
		require.NoError(t, err, "failed to start workflow")
		_, err = handle.GetResult()
		require.Error(t, err, "expected error when calling Patch without EnablePatching")

		dbosErr, ok := err.(*DbosError)
		require.True(t, ok, "expected error to be of type *DbosError, got %T", err)
		require.Equal(t, PatchingNotEnabled, dbosErr.Code, "expected error code to be PatchingNotEnabled")
		require.Contains(t, dbosErr.Message, "Patching system is not enabled", "expected error message to mention patching is not enabled")
		require.Contains(t, dbosErr.Message, "EnablePatching", "expected error message to mention EnablePatching")

		handle2, err := wfWithDeprecatePatchD(dbosCtxNoPatching, 1)
		require.NoError(t, err, "failed to start workflow with DeprecatePatch")
		_, err = handle2.GetResult()
		require.Error(t, err, "expected error when calling DeprecatePatch without EnablePatching")

		dbosErr2, ok := err.(*DbosError)
		require.True(t, ok, "expected error to be of type *DbosError, got %T", err)
		require.Equal(t, PatchingNotEnabled, dbosErr2.Code, "expected error code to be PatchingNotEnabled")
		require.Contains(t, dbosErr2.Message, "Patching system is not enabled", "expected error message to mention patching is not enabled")
		require.Contains(t, dbosErr2.Message, "EnablePatching", "expected error message to mention EnablePatching")
	})

	t.Run("PatchingEnabledWithVersioning", func(t *testing.T) {
		t.Run("PreservesApplicationVersionWhenSetInConfig", func(t *testing.T) {
			// Clear env vars to ensure we're testing config values
			t.Setenv("DBOS__APPVERSION", "")
			databaseUrl := backendDatabaseUrl(t)
			resetTestDatabase(t, databaseUrl)

			dbosCtx, err := NewDbosContext(context.Background(), Config{
				DatabaseUrl:        databaseUrl,
				AppName:            "test-app-patching-with-version",
				EnablePatching:     true,
				ApplicationVersion: "custom-version-1.2.3",
			})
			require.NoError(t, err, "failed to create DBOS context with patching enabled and custom version")
			require.Equal(t, "custom-version-1.2.3", dbosCtx.GetApplicationVersion(), "expected application version to be preserved from config")

			t.Cleanup(func() {
				if dbosCtx != nil {
					Shutdown(dbosCtx, 30*time.Second)
				}
			})
		})

		t.Run("EnvironmentVariableOverridesPatchingEnabled", func(t *testing.T) {

			t.Setenv("DBOS__APPVERSION", "env-override-version-2.0.0")
			databaseUrl := backendDatabaseUrl(t)
			resetTestDatabase(t, databaseUrl)

			dbosCtx, err := NewDbosContext(context.Background(), Config{
				DatabaseUrl:    databaseUrl,
				AppName:        "test-app-patching-env-override",
				EnablePatching: true,
			})
			require.NoError(t, err, "failed to create DBOS context with patching enabled")
			require.Equal(t, "env-override-version-2.0.0", dbosCtx.GetApplicationVersion(), "expected environment variable to override PATCHING_ENABLED")

			t.Cleanup(func() {
				if dbosCtx != nil {
					Shutdown(dbosCtx, 30*time.Second)
				}
			})
		})
	})
}

var (
	streamBlockEvent   *Event
	streamStartedEvent *Event
)

func writeStreamWorkflow(ctx DbosContext, input struct {
	StreamKey string
	Values    []string
	Close     bool
}) (string, error) {

	for _, value := range input.Values {
		if err := WriteStream(ctx, input.StreamKey, value); err != nil {
			return "", err
		}
	}

	if streamStartedEvent != nil {
		streamStartedEvent.Set()
	}

	if streamBlockEvent != nil {
		streamBlockEvent.Wait()
	}

	_, err := Run(ctx, func(stepCtx context.Context) (string, error) {
		return "", WriteStream(stepCtx.(DbosContext), input.StreamKey, "step-value")
	}, WithStepName("not-just-write"))
	if err != nil {
		return "", err
	}

	if input.Close {
		if err := CloseStream(ctx, input.StreamKey); err != nil {
			return "", err
		}

		return "", WriteStream(ctx, input.StreamKey, "should-fail")
	}
	return "done", nil
}

type readStreamFunc func(ctx DbosContext, workflowId string, key string) ([]string, bool, error)

func syncReadStream(ctx DbosContext, workflowId string, key string) ([]string, bool, error) {
	return ReadStream[string](ctx, workflowId, key)
}

func asyncReadStream(ctx DbosContext, workflowId string, key string) ([]string, bool, error) {
	ch, err := ReadStreamAsync[string](ctx, workflowId, key)
	if err != nil {
		return nil, false, err
	}
	return collectStreamValues(ch)
}

func TestStreams(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	writeStreamWorkflowD := NewWorkflow(dbosCtx, writeStreamWorkflow)

	Launch(dbosCtx)

	readFuncs := map[string]readStreamFunc{
		"Sync":  syncReadStream,
		"Async": asyncReadStream,
	}

	for name, readFunc := range readFuncs {
		t.Run(name, func(t *testing.T) {
			t.Run("SimpleReadWrite", func(t *testing.T) {
				streamBlockEvent = NewEvent()
				streamBlockEvent.Set()
				streamStartedEvent = nil

				streamKey := "test-stream"
				writerHandle, err := writeStreamWorkflowD(dbosCtx, struct {
					StreamKey string
					Values    []string
					Close     bool
				}{
					StreamKey: streamKey,
					Values:    []string{"value1", "value2", "value3"},
					Close:     true,
				})
				require.NoError(t, err, "failed to start writer workflow")

				_, err = writerHandle.GetResult()
				require.Error(t, err, "expected error when writing to closed stream")
				require.Contains(t, err.Error(), "stream 'test-stream' is already closed")

				values, closed, err := ReadStream[string](dbosCtx, writerHandle.GetWorkflowId(), streamKey)
				require.NoError(t, err, "failed to read stream")

				require.Equal(t, []string{"value1", "value2", "value3", "step-value"}, values, "expected 4 values")
				require.True(t, closed, "expected stream to be closed")

				steps, err := GetWorkflowSteps(dbosCtx, writerHandle.GetWorkflowId())
				require.NoError(t, err, "failed to get workflow steps")

				require.Len(t, steps, 6, "expected 6 steps (3 workflow writes + 1 RunAsStep with step write + 1 close + 1 failed writeStream step)")
				require.Equal(t, "DBOS.writeStream", steps[0].StepName, "expected first step to be DBOS.writeStream")
				require.Equal(t, "DBOS.writeStream", steps[1].StepName, "expected second step to be DBOS.writeStream")
				require.Equal(t, "DBOS.writeStream", steps[2].StepName, "expected third step to be DBOS.writeStream")
				require.Equal(t, "not-just-write", steps[3].StepName, "expected fourth step to be 'not-just-write' (RunAsStep with step-level write)")
				require.Equal(t, "DBOS.closeStream", steps[4].StepName, "expected last step to be DBOS.closeStream")
				require.Equal(t, "DBOS.writeStream", steps[5].StepName, "expected fifth step to be DBOS.writeStream")
			})

			t.Run("ReadWorkflowTermination", func(t *testing.T) {

				streamBlockEvent = NewEvent()
				streamBlockEvent.Set()

				streamKey := "test-stream-termination"
				writerHandle, err := writeStreamWorkflowD(dbosCtx, struct {
					StreamKey string
					Values    []string
					Close     bool
				}{
					StreamKey: streamKey,
					Values:    []string{"value1", "value2", "value3"},
					Close:     false,
				})
				require.NoError(t, err, "failed to start writer workflow")

				_, err = writerHandle.GetResult()
				require.NoError(t, err, "failed to get result from writer workflow")

				values, closed, err := readFunc(dbosCtx, writerHandle.GetWorkflowId(), streamKey)
				require.NoError(t, err, "failed to read stream")

				require.Equal(t, []string{"value1", "value2", "value3", "step-value"}, values, "expected 4 values")
				require.True(t, closed, "expected stream to be closed when workflow terminates")

				steps, err := GetWorkflowSteps(dbosCtx, writerHandle.GetWorkflowId())
				require.NoError(t, err, "failed to get workflow steps")

				require.Len(t, steps, 4, "expected 4 steps (3 workflow writes + 1 RunAsStep with step write, no close)")
				require.Equal(t, "DBOS.writeStream", steps[0].StepName, "expected first step to be DBOS.writeStream")
				require.Equal(t, "DBOS.writeStream", steps[1].StepName, "expected second step to be DBOS.writeStream")
				require.Equal(t, "DBOS.writeStream", steps[2].StepName, "expected third step to be DBOS.writeStream")
				require.Equal(t, "not-just-write", steps[3].StepName, "expected fourth step to be 'not-just-write' (RunAsStep with step-level write)")
			})

			t.Run("StreamWorkflowRecovery", func(t *testing.T) {
				streamBlockEvent = NewEvent()
				streamBlockEvent.Set()
				streamStartedEvent = nil

				streamKey := "test-stream-recovery"
				writerHandle, err := writeStreamWorkflowD(dbosCtx, struct {
					StreamKey string
					Values    []string
					Close     bool
				}{
					StreamKey: streamKey,
					Values:    []string{"value1", "value2", "value3"},
					Close:     false,
				})
				require.NoError(t, err, "failed to start writer workflow")
				workflowId := writerHandle.GetWorkflowId()

				_, err = writerHandle.GetResult()
				require.NoError(t, err, "failed to get result from writer workflow")

				setWorkflowStatusPending(t, dbosCtx, workflowId)
				recoveredHandles, err := recoverPendingWorkflows(dbosCtx.(*dbosContext), []string{"local"})
				require.NoError(t, err, "failed to recover pending workflows")
				require.Len(t, recoveredHandles, 1, "expected 1 recovered workflow")
				require.Equal(t, workflowId, recoveredHandles[0].GetWorkflowId(), "expected recovered workflow to have same ID")

				_, err = recoveredHandles[0].GetResult()
				require.NoError(t, err, "failed to get result from recovered workflow")

				values, closed, err := readFunc(dbosCtx, workflowId, streamKey)
				require.NoError(t, err, "failed to read stream")
				require.True(t, closed, "expected stream to be closed when workflow terminates")
				require.Equal(t, []string{"value1", "value2", "value3", "step-value"}, values, "expected value1, value2, value3 and step-value once each")
				steps, err := GetWorkflowSteps(dbosCtx, workflowId)
				require.NoError(t, err, "failed to get workflow steps")
				require.Len(t, steps, 4, "expected less than or equal to 5 steps (3 workflow writes + 1 RunAsStep with step write that can concurrently write)")
				require.Equal(t, "DBOS.writeStream", steps[0].StepName, "expected first step to be DBOS.writeStream")
				require.Equal(t, "DBOS.writeStream", steps[1].StepName, "expected second step to be DBOS.writeStream")
				require.Equal(t, "DBOS.writeStream", steps[2].StepName, "expected third step to be DBOS.writeStream")
				require.Equal(t, "not-just-write", steps[3].StepName, "expected fourth step to be 'not-just-write' (RunAsStep with step-level write)")
			})

			t.Run("ForkStreams", func(t *testing.T) {
				streamBlockEvent = NewEvent()
				streamStartedEvent = NewEvent()

				streamKey := "test-stream-fork"
				originalHandle, err := writeStreamWorkflowD(dbosCtx, struct {
					StreamKey string
					Values    []string
					Close     bool
				}{
					StreamKey: streamKey,
					Values:    []string{"value1", "value2"},
					Close:     false,
				})
				require.NoError(t, err, "failed to start original workflow")

				streamStartedEvent.Wait()

				forkHandle, err := ForkWorkflow[string](dbosCtx, ForkWorkflowInput{
					OriginalWorkflowId: originalHandle.GetWorkflowId(),
					StartStep:          2,
				})
				require.NoError(t, err, "failed to fork workflow")

				// Query database directly to avoid blocking (ReadStream would block)
				dbosCtxInternal, ok := dbosCtx.(*dbosContext)
				require.True(t, ok, "expected dbosContext")
				Kernel := dbosCtxInternal.kernel

				entries, closed, err := Kernel.readStream(context.Background(), readStreamDBInput{
					WorkflowId: forkHandle.GetWorkflowId(),
					Key:        streamKey,
					FromOffset: 0,
				})
				require.NoError(t, err, "failed to read stream from database")
				require.False(t, closed, "expected stream not to be closed")
				require.Len(t, entries, 2, "expected 2 stream entries in forked workflow")

				serializer := newJsonSerializer[string]()
				decodedValue1, err := serializer.Decode(&entries[0].Value)
				require.NoError(t, err, "failed to decode first stream entry")
				require.Equal(t, "value1", decodedValue1, "expected first entry to be value1")

				decodedValue2, err := serializer.Decode(&entries[1].Value)
				require.NoError(t, err, "failed to decode second stream entry")
				require.Equal(t, "value2", decodedValue2, "expected second entry to be value2")

				streamBlockEvent.Set()
				_, err = originalHandle.GetResult()
				require.NoError(t, err, "failed to get result from original workflow")
				_, err = forkHandle.GetResult()
				require.NoError(t, err, "failed to get result from forked workflow")
			})

			t.Run("WriteReadToClosedStream", func(t *testing.T) {
				streamBlockEvent = NewEvent()
				streamBlockEvent.Set()
				streamStartedEvent = nil

				streamKey := "test-stream-closed"
				writerHandle, err := writeStreamWorkflowD(dbosCtx, struct {
					StreamKey string
					Values    []string
					Close     bool
				}{
					StreamKey: streamKey,
					Values:    []string{"value1"},
					Close:     true,
				})
				require.NoError(t, err, "failed to start writer workflow")

				_, err = writerHandle.GetResult()
				require.Error(t, err, "expected error when writing to closed stream")
				require.Contains(t, err.Error(), "stream 'test-stream-closed' is already closed")

				_, closed, err := ReadStream[string](dbosCtx, writerHandle.GetWorkflowId(), streamKey)
				require.NoError(t, err, "failed to read stream")
				require.True(t, closed, "expected stream to be closed")
			})

			t.Run("StreamWithStruct", func(t *testing.T) {
				streamBlockEvent = NewEvent()
				streamBlockEvent.Set()

				streamKey := "test-stream-struct"

				testData := []string{"value1", "value2", "value3"}
				writerHandle, err := writeStreamWorkflowD(dbosCtx, struct {
					StreamKey string
					Values    []string
					Close     bool
				}{
					StreamKey: streamKey,
					Values:    testData,
					Close:     false,
				})
				require.NoError(t, err, "failed to start writer workflow")

				_, err = writerHandle.GetResult()
				require.NoError(t, err, "failed to get result from writer workflow")

				values, closed, err := readFunc(dbosCtx, writerHandle.GetWorkflowId(), streamKey)
				require.NoError(t, err, "failed to read stream")

				require.Equal(t, []string{"value1", "value2", "value3", "step-value"}, values, "expected all 4 values")
				require.True(t, closed, "expected stream to be closed")
			})
		})
	}

	t.Run("ForkStreams", func(t *testing.T) {
		streamBlockEvent = NewEvent()
		streamStartedEvent = NewEvent()

		streamKey := "test-stream-fork"
		originalHandle, err := writeStreamWorkflowD(dbosCtx, struct {
			StreamKey string
			Values    []string
			Close     bool
		}{
			StreamKey: streamKey,
			Values:    []string{"value1", "value2"},
			Close:     false,
		})
		require.NoError(t, err, "failed to start original workflow")

		streamStartedEvent.Wait()

		forkHandle, err := ForkWorkflow[string](dbosCtx, ForkWorkflowInput{
			OriginalWorkflowId: originalHandle.GetWorkflowId(),
			StartStep:          2,
		})
		require.NoError(t, err, "failed to fork workflow")

		// Query database directly to avoid blocking (ReadStream would block)
		dbosCtxInternal, ok := dbosCtx.(*dbosContext)
		require.True(t, ok, "expected dbosContext")
		Kernel := dbosCtxInternal.kernel

		entries, closed, err := Kernel.readStream(context.Background(), readStreamDBInput{
			WorkflowId: forkHandle.GetWorkflowId(),
			Key:        streamKey,
			FromOffset: 0,
		})
		require.NoError(t, err, "failed to read stream from database")
		require.False(t, closed, "expected stream not to be closed")
		require.Len(t, entries, 2, "expected 2 stream entries in forked workflow")

		serializer := newJsonSerializer[string]()
		decodedValue1, err := serializer.Decode(&entries[0].Value)
		require.NoError(t, err, "failed to decode first stream entry")
		require.Equal(t, "value1", decodedValue1, "expected first entry to be value1")

		decodedValue2, err := serializer.Decode(&entries[1].Value)
		require.NoError(t, err, "failed to decode second stream entry")
		require.Equal(t, "value2", decodedValue2, "expected second entry to be value2")

		streamBlockEvent.Set()
		_, err = originalHandle.GetResult()
		require.NoError(t, err, "failed to get result from original workflow")
		_, err = forkHandle.GetResult()
		require.NoError(t, err, "failed to get result from forked workflow")
	})

	t.Run("GoroutineLeakOnContextCancel", func(t *testing.T) {

		streamBlockEvent = NewEvent()
		streamStartedEvent = NewEvent()

		streamKey := "test-stream-leak"
		writerHandle, err := writeStreamWorkflowD(dbosCtx, struct {
			StreamKey string
			Values    []string
			Close     bool
		}{
			StreamKey: streamKey,
			Values:    []string{"value1", "value2", "value3"},
			Close:     false,
		})
		require.NoError(t, err)

		streamStartedEvent.Wait()

		cancelCtx, cancel := WithCancelCause(dbosCtx)
		defer cancel(nil)

		ch, err := ReadStreamAsync[string](cancelCtx, writerHandle.GetWorkflowId(), streamKey)
		require.NoError(t, err)

		streamValue := <-ch
		require.NoError(t, streamValue.Err)
		require.Equal(t, "value1", streamValue.Value)

		// Cancel the context and abandon the channel — the goroutine must exit on its own
		cancel(nil)

		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("readStream goroutine did not exit after context cancellation")
		}

		streamBlockEvent.Set()
		_, err = writerHandle.GetResult()
		require.NoError(t, err)
	})

	t.Run("Snapshot", func(t *testing.T) {

		streamBlockEvent = NewEvent()
		streamStartedEvent = NewEvent()

		streamKey := "test-stream-snapshot"
		writerHandle, err := writeStreamWorkflowD(dbosCtx, struct {
			StreamKey string
			Values    []string
			Close     bool
		}{
			StreamKey: streamKey,
			Values:    []string{"value1", "value2", "value3"},
			Close:     false,
		})
		require.NoError(t, err)

		streamStartedEvent.Wait()

		values, closed, err := ReadStream[string](dbosCtx, writerHandle.GetWorkflowId(), streamKey, WithReadStreamSnapshot(0))
		require.NoError(t, err)
		require.False(t, closed, "snapshot of an active workflow should report not closed")
		require.Equal(t, []string{"value1", "value2", "value3"}, values)

		values, closed, err = ReadStream[string](dbosCtx, writerHandle.GetWorkflowId(), streamKey, WithReadStreamSnapshot(2))
		require.NoError(t, err)
		require.False(t, closed)
		require.Equal(t, []string{"value3"}, values)

		streamBlockEvent.Set()
		_, err = writerHandle.GetResult()
		require.NoError(t, err)
	})

	t.Run("AsyncErrorHandling", func(t *testing.T) {

		nonExistentWorkflowId := uuid.NewString()
		ch, err := ReadStreamAsync[string](dbosCtx, nonExistentWorkflowId, "non-existent-stream")
		require.NoError(t, err, "failed to start async stream read")

		var receivedError error
		for streamValue := range ch {
			if streamValue.Err != nil {
				receivedError = streamValue.Err
				break
			}
		}

		require.Error(t, receivedError, "expected error for non-existent workflow")
		require.Contains(t, receivedError.Error(), "workflow", "error should mention workflow")

		_, ok := <-ch
		require.False(t, ok, "channel should be closed after error")
	})
}

func collectStreamValues[R any](ch <-chan StreamValue[R]) ([]R, bool, error) {
	var values []R
	var closed bool
	var err error

	for streamValue := range ch {
		if streamValue.Err != nil {
			return nil, false, streamValue.Err
		}
		if streamValue.Closed {
			closed = true
			break
		}
		values = append(values, streamValue.Value)
	}

	return values, closed, err
}

type exportTestAddress struct {
	Street  string `json:"street"`
	City    string `json:"city"`
	ZipCode int    `json:"zip_code"`
}

type exportTestPerson struct {
	Name      string              `json:"name"`
	Age       int                 `json:"age"`
	Addresses []exportTestAddress `json:"addresses"`
	Tags      map[string]string   `json:"tags"`
	Scores    []float64           `json:"scores"`
}

func TestExportImportWorkflow(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	eventKey := "export-event-key"
	streamKey := "export-stream-key"

	stepCounter := 0
	exportStep := func(_ context.Context) (string, error) {
		stepCounter++
		return fmt.Sprintf("step-result-%d", stepCounter), nil
	}

	grandchildWf := func(ctx DbosContext, input string) (string, error) {
		return input + "-grandchild", nil
	}
	grandchildWfD := NewWorkflow(dbosCtx, grandchildWf)

	childWf := func(ctx DbosContext, input exportTestPerson) (exportTestPerson, error) {

		gcId, gcResult, err := callChildWorkflow(ctx, grandchildWfD, input.Name)
		if err != nil {
			return exportTestPerson{}, err
		}
		input.Tags["grandchild_result"] = gcResult
		input.Tags["grandchild_id"] = gcId
		return input, nil
	}
	childWfD := NewWorkflow(dbosCtx, childWf)

	parentWf := func(ctx DbosContext, input exportTestPerson) (exportTestPerson, error) {

		childId, childResult, err := callChildWorkflow(ctx, childWfD, input)
		if err != nil {
			return exportTestPerson{}, err
		}
		childResult.Tags["child_id"] = childId

		for i := 0; i < 5; i++ {
			_, err := Run(ctx, func(sctx context.Context) (string, error) {
				return exportStep(sctx)
			})
			if err != nil {
				return exportTestPerson{}, err
			}
		}

		eventValue := map[string]any{
			"status":  "completed",
			"details": map[string]any{"count": float64(42), "flag": true},
		}
		if err := SetEvent(ctx, eventKey, eventValue); err != nil {
			return exportTestPerson{}, err
		}

		streamValue := map[string]any{
			"batch": float64(1),
			"items": []any{"alpha", "beta", "gamma"},
		}
		if err := WriteStream(ctx, streamKey, streamValue); err != nil {
			return exportTestPerson{}, err
		}

		childResult.Scores = append(childResult.Scores, 100.0)
		return childResult, nil
	}

	parentWfD := NewWorkflow(dbosCtx, parentWf)

	Launch(dbosCtx)

	input := exportTestPerson{
		Name: "Alice",
		Age:  30,
		Addresses: []exportTestAddress{
			{Street: "123 Main St", City: "Springfield", ZipCode: 62701},
			{Street: "456 Oak Ave", City: "Shelbyville", ZipCode: 62702},
		},
		Tags:   map[string]string{"role": "admin", "department": "engineering"},
		Scores: []float64{95.5, 87.3, 92.1},
	}

	handle, err := parentWfD(dbosCtx, input)
	require.NoError(t, err)

	result, err := handle.GetResult()
	require.NoError(t, err)
	assert.Equal(t, "Alice", result.Name)
	assert.Equal(t, "Alice-grandchild", result.Tags["grandchild_result"])
	assert.Equal(t, float64(100.0), result.Scores[len(result.Scores)-1])

	parentId := handle.GetWorkflowId()
	childId := result.Tags["child_id"]
	grandchildId := result.Tags["grandchild_id"]
	require.NotEmpty(t, childId)
	require.NotEmpty(t, grandchildId)

	originalParentSteps, err := GetWorkflowSteps(dbosCtx, parentId)
	require.NoError(t, err)
	require.Len(t, originalParentSteps, 9, "parent should have 9 steps")

	originalChildSteps, err := GetWorkflowSteps(dbosCtx, childId)
	require.NoError(t, err)
	require.Len(t, originalChildSteps, 2, "child should have 2 steps")

	originalGrandchildSteps, err := GetWorkflowSteps(dbosCtx, grandchildId)
	require.NoError(t, err)
	require.Len(t, originalGrandchildSteps, 0, "grandchild should have 0 steps")

	sdb := dbosCtx.(*dbosContext).kernel

	t.Run("ExportWithChildren", func(t *testing.T) {
		exported, err := sdb.exportWorkflow(dbosCtx, parentId, true)
		require.NoError(t, err)
		require.Len(t, exported, 3, "expected 3 exported workflows (parent + child + grandchild)")

		parentExport := exported[0]
		assert.Equal(t, parentId, parentExport.WorkflowStatus["workflow_uuid"].(string))
		assert.NotEmpty(t, parentExport.OperationOutputs, "expected operation outputs for parent")
		assert.NotEmpty(t, parentExport.WorkflowEvents, "expected workflow events for parent")
		assert.NotEmpty(t, parentExport.WorkflowEventsHistory, "expected workflow events history for parent")
		assert.NotEmpty(t, parentExport.Streams, "expected streams for parent")
	})

	t.Run("ExportWithoutChildren", func(t *testing.T) {
		exported, err := sdb.exportWorkflow(dbosCtx, parentId, false)
		require.NoError(t, err)
		require.Len(t, exported, 1, "expected only 1 exported workflow without children")
	})

	t.Run("ExportNonExistentWorkflow", func(t *testing.T) {
		_, err := sdb.exportWorkflow(dbosCtx, "non-existent-wf-id", false)
		require.Error(t, err)
		var dbosErr *DbosError
		require.ErrorAs(t, err, &dbosErr)
		assert.Equal(t, NonExistentWorkflowError, dbosErr.Code)
	})

	t.Run("ImportConflict", func(t *testing.T) {
		exported, err := sdb.exportWorkflow(dbosCtx, parentId, true)
		require.NoError(t, err)

		err = sdb.importWorkflow(dbosCtx, exported)
		require.Error(t, err, "expected error when importing duplicate workflow")
	})

	t.Run("ImportIntoCleanDB", func(t *testing.T) {
		exported, err := sdb.exportWorkflow(dbosCtx, parentId, true)
		require.NoError(t, err)
		require.Len(t, exported, 3)

		// Delete all workflows so we can re-import
		err = sdb.deleteWorkflows(dbosCtx, deleteWorkflowsDBInput{
			workflowIds:    []string{parentId},
			deleteChildren: true,
		})
		require.NoError(t, err)

		wfs, err := sdb.listWorkflows(dbosCtx, listWorkflowsDBInput{
			workflowIds: []string{parentId, childId, grandchildId},
		})
		require.NoError(t, err)
		require.Empty(t, wfs, "expected no workflows after deletion")

		err = sdb.importWorkflow(dbosCtx, exported)
		require.NoError(t, err)

		wfs, err = sdb.listWorkflows(dbosCtx, listWorkflowsDBInput{
			workflowIds: []string{parentId, childId, grandchildId},
			loadInput:   true,
			loadOutput:  true,
		})
		require.NoError(t, err)
		require.Len(t, wfs, 3, "expected 3 workflows after import")

		wfById := make(map[string]WorkflowStatus)
		for _, wf := range wfs {
			wfById[wf.Id] = wf
		}

		parentWF := wfById[parentId]
		assert.Equal(t, WorkflowStatusSuccess, parentWF.Status)
		require.NotNil(t, parentWF.Output)
		require.NotNil(t, parentWF.Input)

		ser := newJsonSerializer[exportTestPerson]()
		outputPtr, ok := parentWF.Output.(*string)
		require.True(t, ok)
		parentOutput, err := ser.Decode(outputPtr)
		require.NoError(t, err)
		assert.Equal(t, "Alice", parentOutput.Name)
		assert.Equal(t, "Alice-grandchild", parentOutput.Tags["grandchild_result"])
		assert.Equal(t, float64(100.0), parentOutput.Scores[len(parentOutput.Scores)-1])

		inputPtr, ok := parentWF.Input.(*string)
		require.True(t, ok)
		parentInput, err := ser.Decode(inputPtr)
		require.NoError(t, err)
		assert.Equal(t, "Alice", parentInput.Name)
		assert.Equal(t, 30, parentInput.Age)
		assert.Len(t, parentInput.Addresses, 2)
		assert.Equal(t, "123 Main St", parentInput.Addresses[0].Street)

		childWF := wfById[childId]
		assert.Equal(t, WorkflowStatusSuccess, childWF.Status)
		assert.Equal(t, parentId, childWF.ParentWorkflowId)

		grandchildWF := wfById[grandchildId]
		assert.Equal(t, WorkflowStatusSuccess, grandchildWF.Status)
		assert.Equal(t, childId, grandchildWF.ParentWorkflowId)

		importedParentSteps, err := GetWorkflowSteps(dbosCtx, parentId)
		require.NoError(t, err)
		require.Len(t, importedParentSteps, 9, "imported parent should have 9 steps")
		for i, imported := range importedParentSteps {
			assert.Equal(t, originalParentSteps[i].StepId, imported.StepId, "parent step ID mismatch at index %d", i)
			assert.Equal(t, originalParentSteps[i].StepName, imported.StepName, "parent step name mismatch at index %d", i)
		}

		importedChildSteps, err := GetWorkflowSteps(dbosCtx, childId)
		require.NoError(t, err)
		require.Len(t, importedChildSteps, 2, "imported child should have 2 steps")

		importedGrandchildSteps, err := GetWorkflowSteps(dbosCtx, grandchildId)
		require.NoError(t, err)
		require.Len(t, importedGrandchildSteps, 0, "imported grandchild should have 0 steps")

		schemaPrefix := ""

		var eventCount int
		err = sdb.pool.QueryRow(dbosCtx,
			sdb.renderSql(`SELECT COUNT(*) FROM %sworkflow_events WHERE workflow_uuid = $1`, schemaPrefix),
			parentId).Scan(&eventCount)
		require.NoError(t, err)
		assert.Greater(t, eventCount, 0, "expected events to be imported")

		var streamCount int
		err = sdb.pool.QueryRow(dbosCtx,
			sdb.renderSql(`SELECT COUNT(*) FROM %sstreams WHERE workflow_uuid = $1`, schemaPrefix),
			parentId).Scan(&streamCount)
		require.NoError(t, err)
		assert.Greater(t, streamCount, 0, "expected streams to be imported")

		var historyCount int
		err = sdb.pool.QueryRow(dbosCtx,
			sdb.renderSql(`SELECT COUNT(*) FROM %sworkflow_events_history WHERE workflow_uuid = $1`, schemaPrefix),
			parentId).Scan(&historyCount)
		require.NoError(t, err)
		assert.Greater(t, historyCount, 0, "expected workflow events history to be imported")

		forkHandle, err := ForkWorkflow[exportTestPerson](dbosCtx, ForkWorkflowInput{
			OriginalWorkflowId: parentId,
			StartStep:          uint(len(importedParentSteps)),
		})
		require.NoError(t, err)

		forkResult, err := forkHandle.GetResult()
		require.NoError(t, err)
		assert.Equal(t, "Alice", forkResult.Name)
		assert.Equal(t, "Alice-grandchild", forkResult.Tags["grandchild_result"])
	})
}

func aggregatesWorkflowSuccess(_ DbosContext, _ string) (string, error) {
	return "ok", nil
}

func aggregatesWorkflowFail(_ DbosContext, _ string) (string, error) {
	return "", fmt.Errorf("aggregate-fail")
}

func TestGetWorkflowAggregates(t *testing.T) {
	parallelTest(t)
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	aggregatesWorkflowSuccessD := NewWorkflow(dbosCtx, aggregatesWorkflowSuccess)
	aggregatesWorkflowFailD := NewWorkflow(dbosCtx, aggregatesWorkflowFail)

	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS instance")

	successFQN := runtime.FuncForPC(reflect.ValueOf(aggregatesWorkflowSuccess).Pointer()).Name()
	failFQN := runtime.FuncForPC(reflect.ValueOf(aggregatesWorkflowFail).Pointer()).Name()

	for i := 0; i < 3; i++ {
		handle, err := aggregatesWorkflowSuccessD(dbosCtx, fmt.Sprintf("ok-%d", i))
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.NoError(t, err)
	}
	for i := 0; i < 2; i++ {
		handle, err := aggregatesWorkflowFailD(dbosCtx, fmt.Sprintf("fail-%d", i))
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.Error(t, err)
	}

	t.Run("GroupByStatus", func(t *testing.T) {
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{GroupByStatus: true})
		require.NoError(t, err)
		statusCounts := map[string]int64{}
		for _, r := range rows {
			require.NotNil(t, r.Group["status"], "status grouping key should be non-nil")
			statusCounts[*r.Group["status"]] = r.Count
		}
		assert.Equal(t, int64(3), statusCounts[string(WorkflowStatusSuccess)])
		assert.Equal(t, int64(2), statusCounts[string(WorkflowStatusError)])
	})

	t.Run("GroupByName", func(t *testing.T) {
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{GroupByName: true})
		require.NoError(t, err)
		nameCounts := map[string]int64{}
		for _, r := range rows {
			require.NotNil(t, r.Group["name"])
			nameCounts[*r.Group["name"]] = r.Count
		}
		assert.Equal(t, int64(3), nameCounts[successFQN])
		assert.Equal(t, int64(2), nameCounts[failFQN])
	})

	t.Run("GroupByStatusAndName", func(t *testing.T) {
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByStatus: true,
			GroupByName:   true,
		})
		require.NoError(t, err)
		type key struct {
			status string
			name   string
		}
		combo := map[key]int64{}
		for _, r := range rows {
			require.NotNil(t, r.Group["status"])
			require.NotNil(t, r.Group["name"])
			combo[key{status: *r.Group["status"], name: *r.Group["name"]}] = r.Count
		}
		assert.Equal(t, int64(3), combo[key{status: string(WorkflowStatusSuccess), name: successFQN}])
		assert.Equal(t, int64(2), combo[key{status: string(WorkflowStatusError), name: failFQN}])
	})

	t.Run("FilterByStatus", func(t *testing.T) {
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByName: true,
			Status:      []WorkflowStatusType{WorkflowStatusSuccess},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.NotNil(t, rows[0].Group["name"])
		assert.Equal(t, successFQN, *rows[0].Group["name"])
		assert.Equal(t, int64(3), rows[0].Count)
	})

	t.Run("FilterByName", func(t *testing.T) {
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByStatus: true,
			Name:          []string{failFQN},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.NotNil(t, rows[0].Group["status"])
		assert.Equal(t, string(WorkflowStatusError), *rows[0].Group["status"])
		assert.Equal(t, int64(2), rows[0].Count)
	})

	t.Run("FilterByWorkflowIDPrefix", func(t *testing.T) {

		var prefixId string
		for i := 0; i < 2; i++ {
			handle, err := aggregatesWorkflowSuccessD(dbosCtx, fmt.Sprintf("prefix-%d", i))
			require.NoError(t, err)
			_, err = handle.GetResult()
			require.NoError(t, err)
			if i == 0 {
				prefixId = handle.GetWorkflowId()
			}
		}

		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByName:      true,
			WorkflowIdPrefix: []string{prefixId},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.NotNil(t, rows[0].Group["name"])
		assert.Equal(t, successFQN, *rows[0].Group["name"])
		assert.Equal(t, int64(1), rows[0].Count)

		rows, err = GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByStatus:    true,
			WorkflowIdPrefix: []string{"nonexistent-prefix-"},
		})
		require.NoError(t, err)
		assert.Empty(t, rows)
	})

	t.Run("NoGroupByErrors", func(t *testing.T) {
		_, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least one group_by")
	})

	t.Run("NegativeTimeBucketErrors", func(t *testing.T) {
		_, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByStatus:  true,
			TimeBucketSize: -time.Minute,
		})
		require.Error(t, err)
	})

	t.Run("TimeBucketAlone", func(t *testing.T) {
		oneHour := time.Hour
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			TimeBucketSize: oneHour,
		})
		require.NoError(t, err)
		require.NotEmpty(t, rows)
		var total int64
		for _, r := range rows {
			require.NotNil(t, r.Group["time_bucket"])
			tb := *r.Group["time_bucket"]
			// Each bucket value must be a multiple of the bucket size in ms
			var bucketMs int64
			_, scanErr := fmt.Sscanf(tb, "%d", &bucketMs)
			require.NoError(t, scanErr, "expected numeric time_bucket value, got %q", tb)
			assert.Equal(t, int64(0), bucketMs%oneHour.Milliseconds(),
				"bucket %d must be a multiple of %d", bucketMs, oneHour.Milliseconds())
			total += r.Count
		}

		assert.GreaterOrEqual(t, total, int64(7))
	})

	t.Run("TimeBucketWithGroupByStatus", func(t *testing.T) {
		oneHour := time.Hour
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			GroupByStatus:  true,
			TimeBucketSize: oneHour,
		})
		require.NoError(t, err)
		var successTotal, errorTotal int64
		for _, r := range rows {
			require.NotNil(t, r.Group["status"])
			require.NotNil(t, r.Group["time_bucket"])
			switch *r.Group["status"] {
			case string(WorkflowStatusSuccess):
				successTotal += r.Count
			case string(WorkflowStatusError):
				errorTotal += r.Count
			}
		}
		assert.GreaterOrEqual(t, successTotal, int64(5))
		assert.Equal(t, int64(2), errorTotal)
	})

	t.Run("TimeBucketWithStatusFilter", func(t *testing.T) {
		oneMinute := time.Minute
		rows, err := GetWorkflowAggregates(dbosCtx, GetWorkflowAggregatesInput{
			TimeBucketSize: oneMinute,
			Status:         []WorkflowStatusType{WorkflowStatusError},
		})
		require.NoError(t, err)
		require.NotEmpty(t, rows)
		var total int64
		for _, r := range rows {
			require.NotNil(t, r.Group["time_bucket"])
			tb := *r.Group["time_bucket"]
			var bucketMs int64
			_, scanErr := fmt.Sscanf(tb, "%d", &bucketMs)
			require.NoError(t, scanErr)
			assert.Equal(t, int64(0), bucketMs%oneMinute.Milliseconds())
			total += r.Count
		}
		assert.Equal(t, int64(2), total)
	})
}

func stepAggOK(_ context.Context) (string, error) { return "ok", nil }

func stepAggBad(_ context.Context) (string, error) { return "", errors.New("boom") }

func stepAggregatesWorkflow(ctx DbosContext, _ string) (string, error) {
	if _, err := Run(ctx, stepAggOK, WithStepName("aggStepOK")); err != nil {
		return "", err
	}
	if _, err := Run(ctx, stepAggOK, WithStepName("aggStepOK")); err != nil {
		return "", err
	}
	_, _ = Run(ctx, stepAggBad, WithStepName("aggStepBad"))
	return "done", nil
}

func TestGetStepAggregates(t *testing.T) {
	dbosCtx := setupDbos(t, setupDbosOptions{dropDB: true, checkLeaks: true})

	stepAggregatesWorkflowD := NewWorkflow(dbosCtx, stepAggregatesWorkflow)
	require.NoError(t, Launch(dbosCtx), "failed to launch DBOS instance")

	for i := 0; i < 3; i++ {
		handle, err := stepAggregatesWorkflowD(dbosCtx, fmt.Sprintf("in-%d", i))
		require.NoError(t, err)
		_, err = handle.GetResult()
		require.NoError(t, err)
	}

	t.Run("GroupByFunctionName", func(t *testing.T) {
		rows, err := GetStepAggregates(dbosCtx, GetStepAggregatesInput{
			GroupByFunctionName: true,
			SelectCount:         true,
		})
		require.NoError(t, err)
		counts := map[string]int64{}
		for _, r := range rows {
			require.NotNil(t, r.Group["function_name"])
			require.NotNil(t, r.Count)
			counts[*r.Group["function_name"]] = *r.Count
		}
		assert.Equal(t, int64(6), counts["aggStepOK"])
		assert.Equal(t, int64(3), counts["aggStepBad"])
	})

	t.Run("GroupByStatus", func(t *testing.T) {
		rows, err := GetStepAggregates(dbosCtx, GetStepAggregatesInput{
			GroupByStatus: true,
			SelectCount:   true,
		})
		require.NoError(t, err)
		counts := map[string]int64{}
		for _, r := range rows {
			require.NotNil(t, r.Group["status"])
			require.NotNil(t, r.Count)
			counts[*r.Group["status"]] = *r.Count
		}
		assert.Equal(t, int64(6), counts["SUCCESS"])
		assert.Equal(t, int64(3), counts["ERROR"])
	})

	t.Run("FilterByFunctionName", func(t *testing.T) {
		rows, err := GetStepAggregates(dbosCtx, GetStepAggregatesInput{
			GroupByStatus: true,
			SelectCount:   true,
			FunctionName:  []string{"aggStepBad"},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.NotNil(t, rows[0].Group["status"])
		assert.Equal(t, "ERROR", *rows[0].Group["status"])
		require.NotNil(t, rows[0].Count)
		assert.Equal(t, int64(3), *rows[0].Count)
	})

	t.Run("FilterByStatus", func(t *testing.T) {
		rows, err := GetStepAggregates(dbosCtx, GetStepAggregatesInput{
			GroupByFunctionName: true,
			SelectCount:         true,
			Status:              []string{"SUCCESS"},
		})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.NotNil(t, rows[0].Group["function_name"])
		assert.Equal(t, "aggStepOK", *rows[0].Group["function_name"])
		require.NotNil(t, rows[0].Count)
		assert.Equal(t, int64(6), *rows[0].Count)
	})

	t.Run("SelectMaxDuration", func(t *testing.T) {
		rows, err := GetStepAggregates(dbosCtx, GetStepAggregatesInput{
			GroupByFunctionName: true,
			SelectMaxDurationMs: true,
		})
		require.NoError(t, err)
		require.NotEmpty(t, rows)
		for _, r := range rows {
			assert.Nil(t, r.Count, "count should not be selected")
			require.NotNil(t, r.MaxDurationMs, "max_duration_ms should be selected")
			assert.GreaterOrEqual(t, *r.MaxDurationMs, int64(0))
		}
	})

	t.Run("NoGroupByReturnsError", func(t *testing.T) {
		_, err := GetStepAggregates(dbosCtx, GetStepAggregatesInput{SelectCount: true})
		require.Error(t, err)
	})

	t.Run("NoSelectReturnsError", func(t *testing.T) {
		_, err := GetStepAggregates(dbosCtx, GetStepAggregatesInput{GroupByFunctionName: true})
		require.Error(t, err)
	})
}
