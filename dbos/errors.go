package dbos

import "fmt"

type DbosErrorCode int

const (
	ConflictingIdError DbosErrorCode = iota + 1
	InitializationError
	NonExistentWorkflowError // Referenced workflow does not exist
	ConflictingWorkflowError
	WorkflowCancelled
	UnexpectedStep
	AwaitedWorkflowCancelled
	ConflictingRegistrationError
	WorkflowUnexpectedTypeError
	WorkflowExecutionError
	StepExecutionError
	DeadLetterQueueError
	MaxStepRetriesExceeded
	queueDeduplicatedRemoved
	PatchingNotEnabled
	TimeoutError
	NoApplicationVersions
)

type DbosError struct {
	Message string
	Code    DbosErrorCode

	WorkflowId      string
	DestinationId   string
	StepName        string
	QueueName       string
	DeduplicationId string
	StepId          int
	ExpectedName    string
	RecordedName    string
	MaxRetries      int

	wrappedErr error
}

func (e *DbosError) Error() string {
	return fmt.Sprintf("DBOS Error %d: %s", int(e.Code), e.Message)
}

func (e *DbosError) Unwrap() error {
	return e.wrappedErr
}

func (e *DbosError) Is(target error) bool {
	t, ok := target.(*DbosError)
	if !ok {
		return false
	}

	return t.Code != 0 && e.Code == t.Code
}

func newConflictingWorkflowError(workflowId, message string) *DbosError {
	msg := fmt.Sprintf("Conflicting workflow invocation with the same ID (%s)", workflowId)
	if message != "" {
		msg += ": " + message
	}
	return &DbosError{
		Message:    msg,
		Code:       ConflictingWorkflowError,
		WorkflowId: workflowId,
	}
}

func newInitializationError(message string) *DbosError {
	return &DbosError{
		Message: fmt.Sprintf("Error initializing DBOS Transact: %s", message),
		Code:    InitializationError,
	}
}

func newNonExistentWorkflowError(workflowId string) *DbosError {
	return &DbosError{
		Message:       fmt.Sprintf("workflow %s does not exist", workflowId),
		Code:          NonExistentWorkflowError,
		DestinationId: workflowId,
	}
}

func newConflictingRegistrationError(name string) *DbosError {
	return &DbosError{
		Message: fmt.Sprintf("%s is already registered", name),
		Code:    ConflictingRegistrationError,
	}
}

func newUnexpectedStepError(workflowId string, stepId int, expectedName, recordedName string) *DbosError {
	return &DbosError{
		Message:      fmt.Sprintf("During execution of workflow %s step %d, function %s was recorded when %s was expected. Check that your workflow is deterministic.", workflowId, stepId, recordedName, expectedName),
		Code:         UnexpectedStep,
		WorkflowId:   workflowId,
		StepId:       stepId,
		ExpectedName: expectedName,
		RecordedName: recordedName,
	}
}

func newAwaitedWorkflowCancelledError(workflowId string) *DbosError {
	return &DbosError{
		Message:    fmt.Sprintf("Awaited workflow %s was cancelled", workflowId),
		Code:       AwaitedWorkflowCancelled,
		WorkflowId: workflowId,
	}
}

func newWorkflowCancelledError(workflowId string) *DbosError {
	return &DbosError{
		Message: fmt.Sprintf("Workflow %s was cancelled", workflowId),
		Code:    WorkflowCancelled,
	}
}

func newWorkflowConflictIdError(workflowId string) *DbosError {
	return &DbosError{
		Message:    fmt.Sprintf("Conflicting workflow ID %s", workflowId),
		Code:       ConflictingIdError,
		WorkflowId: workflowId,
	}
}

func newWorkflowUnexpectedResultType(workflowId, expectedType, actualType string) *DbosError {
	return &DbosError{
		Message:    fmt.Sprintf("Workflow %s returned unexpected result type: expected %s, got %s", workflowId, expectedType, actualType),
		Code:       WorkflowUnexpectedTypeError,
		WorkflowId: workflowId,
	}
}

func newWorkflowUnexpectedInputType(workflowName, expectedType, actualType string) *DbosError {
	return &DbosError{
		Message: fmt.Sprintf("Workflow %s received unexpected input type: expected %s, got %s", workflowName, expectedType, actualType),
		Code:    WorkflowUnexpectedTypeError,
	}
}

func newWorkflowExecutionError(workflowId string, err error) *DbosError {
	return &DbosError{
		Message:    fmt.Sprintf("Workflow %s execution error: %s", workflowId, err.Error()),
		Code:       WorkflowExecutionError,
		WorkflowId: workflowId,
		wrappedErr: err,
	}
}

func newStepExecutionError(workflowId, stepName string, err error) *DbosError {
	return &DbosError{
		Message:    fmt.Sprintf("Step %s in workflow %s execution error: %v", stepName, workflowId, err),
		Code:       StepExecutionError,
		WorkflowId: workflowId,
		StepName:   stepName,
		wrappedErr: err,
	}
}

func newDeadLetterQueueError(workflowId string, maxRetries int) *DbosError {
	return &DbosError{
		Message:    fmt.Sprintf("Workflow %s has been moved to the dead-letter queue after exceeding the maximum of %d retries", workflowId, maxRetries),
		Code:       DeadLetterQueueError,
		WorkflowId: workflowId,
		MaxRetries: maxRetries,
	}
}

func newMaxStepRetriesExceededError(workflowId, stepName string, maxRetries int, err error) *DbosError {
	return &DbosError{
		Message:    fmt.Sprintf("Step %s has exceeded its maximum of %d retries: %v", stepName, maxRetries, err),
		Code:       MaxStepRetriesExceeded,
		WorkflowId: workflowId,
		StepName:   stepName,
		MaxRetries: maxRetries,
		wrappedErr: err,
	}
}

func newPatchingNotEnabledError() *DbosError {
	return &DbosError{
		Message: "Patching system is not enabled. Set EnablePatching to true in the DBOS context configuration to use Patch and DeprecatePatch",
		Code:    PatchingNotEnabled,
	}
}

func newNoApplicationVersionsError() *DbosError {
	return &DbosError{
		Message: "No application versions are registered",
		Code:    NoApplicationVersions,
	}
}

func newTimeoutError(workflowId, stepName, message string) *DbosError {
	msg := "Operation timed out"
	if stepName != "" {
		msg = fmt.Sprintf("Step %s timed out", stepName)
	}
	if workflowId != "" {
		msg += fmt.Sprintf(" in workflow %s", workflowId)
	}
	if message != "" {
		msg += ": " + message
	}
	return &DbosError{
		Message:    msg,
		Code:       TimeoutError,
		WorkflowId: workflowId,
		StepName:   stepName,
	}
}
