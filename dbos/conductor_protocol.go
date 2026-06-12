package dbos

import (
	"encoding/json"
	"strconv"
	"time"
)

type stringOrList []string

func (s *stringOrList) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*s = nil
		return nil
	}
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*s = stringOrList{single}
		return nil
	}
	var list []string
	if err := json.Unmarshal(data, &list); err != nil {
		return err
	}
	*s = stringOrList(list)
	return nil
}

func (s stringOrList) toSlice() []string {
	return []string(s)
}

type messageType string

const (
	executorInfo                 messageType = "executor_info"
	recoveryMessage              messageType = "recovery"
	cancelWorkflowMessage        messageType = "cancel"
	resumeWorkflowMessage        messageType = "resume"
	listWorkflowsMessage         messageType = "list_workflows"
	listStepsMessage             messageType = "list_steps"
	getWorkflowMessage           messageType = "get_workflow"
	forkWorkflowMessage          messageType = "fork_workflow"
	existPendingWorkflowsMessage messageType = "exist_pending_workflows"
	retentionMessage             messageType = "retention"
	getMetricsMessage            messageType = "get_metrics"
	exportWorkflowMessage        messageType = "export_workflow"
	importWorkflowMessage        messageType = "import_workflow"
	deleteWorkflowMessage        messageType = "delete"
	alertMessage                 messageType = "alert"
	listSchedulesMessage         messageType = "list_schedules"
	getScheduleMessage           messageType = "get_schedule"
	pauseScheduleMessage         messageType = "pause_schedule"
	resumeScheduleMessage        messageType = "resume_schedule"
	backfillScheduleMessage      messageType = "backfill_schedule"
	triggerScheduleMessage       messageType = "trigger_schedule"
	getWorkflowEventsMessage     messageType = "get_workflow_events"
	getWorkflowNotificationsMsg  messageType = "get_workflow_notifications"
	getWorkflowStreamsMessage    messageType = "get_workflow_streams"
	getWorkflowAggregatesMessage messageType = "get_workflow_aggregates"
	getStepAggregatesMessage     messageType = "get_step_aggregates"
	listAppVersionsMessage       messageType = "list_application_versions"
	setLatestAppVersionMessage   messageType = "set_latest_application_version"
)

type baseMessage struct {
	Type      messageType `json:"type"`
	RequestId string      `json:"request_id"`
}

type baseResponse struct {
	baseMessage
	ErrorMessage *string `json:"error_message,omitempty"`
}

type executorInfoRequest struct {
	baseMessage
}

type executorInfoResponse struct {
	baseResponse
	ExecutorId         string         `json:"executor_id"`
	ApplicationVersion string         `json:"application_version"`
	Hostname           *string        `json:"hostname,omitempty"`
	DbosVersion        string         `json:"dbos_version"`
	Language           string         `json:"language"`
	ExecutorMetadata   map[string]any `json:"executor_metadata,omitempty"`
}

type listWorkflowsConductorRequestBody struct {
	WorkflowUuids      []string     `json:"workflow_uuids,omitempty"`
	WorkflowName       stringOrList `json:"workflow_name,omitempty"`
	AuthenticatedUser  stringOrList `json:"authenticated_user,omitempty"`
	StartTime          *time.Time   `json:"start_time,omitempty"`
	EndTime            *time.Time   `json:"end_time,omitempty"`
	CompletedAfter     *time.Time   `json:"completed_after,omitempty"`
	CompletedBefore    *time.Time   `json:"completed_before,omitempty"`
	DequeuedAfter      *time.Time   `json:"dequeued_after,omitempty"`
	DequeuedBefore     *time.Time   `json:"dequeued_before,omitempty"`
	Status             stringOrList `json:"status,omitempty"`
	ApplicationVersion stringOrList `json:"application_version,omitempty"`
	ForkedFrom         stringOrList `json:"forked_from,omitempty"`
	ParentWorkflowId   stringOrList `json:"parent_workflow_id,omitempty"`
	WasForkedFrom      *bool        `json:"was_forked_from,omitempty"`
	HasParent          *bool        `json:"has_parent,omitempty"`
	Limit              *int         `json:"limit,omitempty"`
	Offset             *int         `json:"offset,omitempty"`
	SortDesc           bool         `json:"sort_desc"`
	WorkflowIdPrefix   stringOrList `json:"workflow_id_prefix,omitempty"`
	LoadInput          bool         `json:"load_input"`
	LoadOutput         bool         `json:"load_output"`
	ExecutorId         stringOrList `json:"executor_id,omitempty"`
}

type listWorkflowsConductorRequest struct {
	baseMessage
	Body listWorkflowsConductorRequestBody `json:"body"`
}

type listWorkflowsConductorResponseBody struct {
	WorkflowUuid            string  `json:"WorkflowUUID"`
	Status                  *string `json:"Status,omitempty"`
	WorkflowName            *string `json:"WorkflowName,omitempty"`
	WorkflowClassName       *string `json:"WorkflowClassName,omitempty"`
	WorkflowConfigName      *string `json:"WorkflowConfigName,omitempty"`
	AuthenticatedUser       *string `json:"AuthenticatedUser,omitempty"`
	AssumedRole             *string `json:"AssumedRole,omitempty"`
	AuthenticatedRoles      *string `json:"AuthenticatedRoles,omitempty"`
	Input                   *string `json:"Input,omitempty"`
	Output                  *string `json:"Output,omitempty"`
	Error                   *string `json:"Error,omitempty"`
	CreatedAt               *string `json:"CreatedAt,omitempty"`
	UpdatedAt               *string `json:"UpdatedAt,omitempty"`
	QueueName               *string `json:"QueueName,omitempty"`
	ApplicationVersion      *string `json:"ApplicationVersion,omitempty"`
	ExecutorId              *string `json:"ExecutorID,omitempty"`
	WorkflowTimeoutMS       *string `json:"WorkflowTimeoutMS,omitempty"`
	WorkflowDeadlineEpochMS *string `json:"WorkflowDeadlineEpochMS,omitempty"`
	DeduplicationId         *string `json:"DeduplicationID,omitempty"`
	Priority                *string `json:"Priority,omitempty"`
	QueuePartitionKey       *string `json:"QueuePartitionKey,omitempty"`
	ForkedFrom              *string `json:"ForkedFrom,omitempty"`
	WasForkedFrom           *bool   `json:"WasForkedFrom,omitempty"`
	ParentWorkflowId        *string `json:"ParentWorkflowID,omitempty"`
	DequeuedAt              *string `json:"DequeuedAt,omitempty"`
	DelayUntilEpochMS       *string `json:"DelayUntilEpochMS,omitempty"`
	CompletedAt             *string `json:"CompletedAt,omitempty"`
}

type listWorkflowsConductorResponse struct {
	baseResponse
	Output []listWorkflowsConductorResponseBody `json:"output"`
}

func formatListWorkflowsResponseBody(wf WorkflowStatus) listWorkflowsConductorResponseBody {
	output := listWorkflowsConductorResponseBody{
		WorkflowUuid: wf.Id,
	}

	if wf.Status != "" {
		status := string(wf.Status)
		output.Status = &status
	}

	if wf.Name != "" {
		output.WorkflowName = &wf.Name
	}

	if wf.AuthenticatedUser != "" {
		output.AuthenticatedUser = &wf.AuthenticatedUser
	}
	if wf.AssumedRole != "" {
		output.AssumedRole = &wf.AssumedRole
	}

	if len(wf.AuthenticatedRoles) > 0 {
		rolesJson, err := json.Marshal(wf.AuthenticatedRoles)
		if err == nil {
			rolesStr := string(rolesJson)
			output.AuthenticatedRoles = &rolesStr
		}
	}

	if wf.Input != nil {
		inputStr, ok := wf.Input.(string)
		if ok {
			output.Input = &inputStr
		}
	}
	if wf.Output != nil {
		outputStr, ok := wf.Output.(string)
		if ok {
			output.Output = &outputStr
		}
	}

	if wf.Error != nil {
		errorStr := wf.Error.Error()
		output.Error = &errorStr
	}

	if !wf.CreatedAt.IsZero() {
		createdStr := strconv.FormatInt(wf.CreatedAt.UnixMilli(), 10)
		output.CreatedAt = &createdStr
	}
	if !wf.UpdatedAt.IsZero() {
		updatedStr := strconv.FormatInt(wf.UpdatedAt.UnixMilli(), 10)
		output.UpdatedAt = &updatedStr
	}

	if wf.QueueName != "" {
		output.QueueName = &wf.QueueName
	}

	if wf.QueuePartitionKey != "" {
		output.QueuePartitionKey = &wf.QueuePartitionKey
	}

	if wf.DeduplicationId != "" {
		output.DeduplicationId = &wf.DeduplicationId
	}

	priorityStr := strconv.Itoa(wf.Priority)
	output.Priority = &priorityStr

	if wf.ApplicationVersion != "" {
		output.ApplicationVersion = &wf.ApplicationVersion
	}

	if wf.ExecutorId != "" {
		output.ExecutorId = &wf.ExecutorId
	}

	if wf.Timeout > 0 {
		timeoutStr := strconv.FormatInt(wf.Timeout.Milliseconds(), 10)
		output.WorkflowTimeoutMS = &timeoutStr
	}

	if !wf.Deadline.IsZero() {
		deadlineStr := strconv.FormatInt(wf.Deadline.UnixMilli(), 10)
		output.WorkflowDeadlineEpochMS = &deadlineStr
	}

	if wf.ForkedFrom != "" {
		output.ForkedFrom = &wf.ForkedFrom
	}

	wasForkedFrom := wf.WasForkedFrom
	output.WasForkedFrom = &wasForkedFrom

	if wf.ParentWorkflowId != "" {
		output.ParentWorkflowId = &wf.ParentWorkflowId
	}

	if (wf.Status == WorkflowStatusPending) && !wf.StartedAt.IsZero() {
		dequeuedStr := strconv.FormatInt(wf.StartedAt.UnixMilli(), 10)
		output.DequeuedAt = &dequeuedStr
	}

	if !wf.DelayUntil.IsZero() {
		delayStr := strconv.FormatInt(wf.DelayUntil.UnixMilli(), 10)
		output.DelayUntilEpochMS = &delayStr
	}

	if !wf.CompletedAt.IsZero() {
		completedStr := strconv.FormatInt(wf.CompletedAt.UnixMilli(), 10)
		output.CompletedAt = &completedStr
	}

	return output
}

type listStepsConductorRequest struct {
	baseMessage
	WorkflowId string `json:"workflow_id"`
	LoadOutput bool   `json:"load_output"`
}

type workflowStepsConductorResponseBody struct {
	FunctionId         int     `json:"function_id"`
	FunctionName       string  `json:"function_name"`
	Output             *string `json:"output,omitempty"`
	Error              *string `json:"error,omitempty"`
	StartedAtEpochMs   *string `json:"started_at_epoch_ms,omitempty"`
	CompletedAtEpochMs *string `json:"completed_at_epoch_ms,omitempty"`
}

type listStepsConductorResponse struct {
	baseResponse
	Output *[]workflowStepsConductorResponseBody `json:"output,omitempty"`
}

func formatWorkflowStepsResponseBody(step StepInfo) workflowStepsConductorResponseBody {
	output := workflowStepsConductorResponseBody{
		FunctionId:   step.StepId,
		FunctionName: step.StepName,
	}

	if step.Output != nil {
		outputStr, ok := step.Output.(string)
		if ok {
			output.Output = &outputStr
		}
	}

	if step.Error != nil {
		errorStr := step.Error.Error()
		output.Error = &errorStr
	}

	if !step.StartedAt.IsZero() {
		startedAtStr := strconv.FormatInt(step.StartedAt.UnixMilli(), 10)
		output.StartedAtEpochMs = &startedAtStr
	}
	if !step.CompletedAt.IsZero() {
		completedAtStr := strconv.FormatInt(step.CompletedAt.UnixMilli(), 10)
		output.CompletedAtEpochMs = &completedAtStr
	}

	return output
}

type getWorkflowConductorRequest struct {
	baseMessage
	WorkflowId string `json:"workflow_id"`
	LoadInput  bool   `json:"load_input"`
	LoadOutput bool   `json:"load_output"`
}

type getWorkflowConductorResponse struct {
	baseResponse
	Output *listWorkflowsConductorResponseBody `json:"output,omitempty"`
}

type forkWorkflowConductorRequestBody struct {
	WorkflowId         string  `json:"workflow_id"`
	StartStep          int     `json:"start_step"`
	ApplicationVersion *string `json:"application_version,omitempty"`
	NewWorkflowId      *string `json:"new_workflow_id,omitempty"`
	QueueName          *string `json:"queue_name,omitempty"`
	QueuePartitionKey  *string `json:"queue_partition_key,omitempty"`
}

type forkWorkflowConductorRequest struct {
	baseMessage
	Body forkWorkflowConductorRequestBody `json:"body"`
}

type forkWorkflowConductorResponse struct {
	baseResponse
	NewWorkflowId *string `json:"new_workflow_id,omitempty"`
}

type cancelWorkflowConductorRequest struct {
	baseMessage
	WorkflowId  string   `json:"workflow_id"`
	WorkflowIds []string `json:"workflow_ids"`
}

type cancelWorkflowConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

type recoveryConductorRequest struct {
	baseMessage
	ExecutorIds []string `json:"executor_ids"`
}

type recoveryConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

type existPendingWorkflowsConductorRequest struct {
	baseMessage
	ExecutorId         string `json:"executor_id"`
	ApplicationVersion string `json:"application_version"`
}

type existPendingWorkflowsConductorResponse struct {
	baseResponse
	Exist bool `json:"exist"`
}

type resumeWorkflowConductorRequest struct {
	baseMessage
	WorkflowId  string   `json:"workflow_id"`
	WorkflowIds []string `json:"workflow_ids"`
	QueueName   *string  `json:"queue_name,omitempty"`
}

type resumeWorkflowConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

type retentionConductorRequestBody struct {
	GCCutoffEpochMs      *int `json:"gc_cutoff_epoch_ms,omitempty"`
	GCRowsThreshold      *int `json:"gc_rows_threshold,omitempty"`
	TimeoutCutoffEpochMs *int `json:"timeout_cutoff_epoch_ms,omitempty"`
}

type retentionConductorRequest struct {
	baseMessage
	Body retentionConductorRequestBody `json:"body"`
}

type retentionConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

type getMetricsConductorRequest struct {
	baseMessage
	StartTime   string `json:"start_time"`
	EndTime     string `json:"end_time"`
	MetricClass string `json:"metric_class"`
}

type getMetricsConductorResponse struct {
	baseResponse
	Metrics []metricData `json:"metrics"`
}

type exportWorkflowConductorRequest struct {
	baseMessage
	WorkflowId     string `json:"workflow_id"`
	ExportChildren bool   `json:"export_children"`
}

type exportWorkflowConductorResponse struct {
	baseResponse
	SerializedWorkflow *string `json:"serialized_workflow,omitempty"`
}

type importWorkflowConductorRequest struct {
	baseMessage
	SerializedWorkflow string `json:"serialized_workflow"`
}

type importWorkflowConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

type deleteWorkflowConductorRequest struct {
	baseMessage
	WorkflowId     string   `json:"workflow_id"`
	WorkflowIds    []string `json:"workflow_ids"`
	DeleteChildren bool     `json:"delete_children"`
}

type deleteWorkflowConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

type alertRequest struct {
	baseMessage
	Name     string            `json:"name"`
	Message  string            `json:"message"`
	Metadata map[string]string `json:"metadata"`
}

type alertConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

// Context is rendered when load_context is true on the request, otherwise omitted.
type scheduleConductorOutput struct {
	ScheduleId        string  `json:"schedule_id"`
	ScheduleName      string  `json:"schedule_name"`
	WorkflowName      string  `json:"workflow_name"`
	WorkflowClassName *string `json:"workflow_class_name"`
	Schedule          string  `json:"schedule"`
	Status            string  `json:"status"`
	Context           *string `json:"context"`
	LastFiredAt       *string `json:"last_fired_at"`
	AutomaticBackfill bool    `json:"automatic_backfill"`
	CronTimezone      *string `json:"cron_timezone"`
	QueueName         *string `json:"queue_name"`
}

type listSchedulesConductorRequestBody struct {
	Status             stringOrList `json:"status,omitempty"`
	WorkflowName       stringOrList `json:"workflow_name,omitempty"`
	ScheduleNamePrefix stringOrList `json:"schedule_name_prefix,omitempty"`
	LoadContext        *bool        `json:"load_context,omitempty"`
}

type listSchedulesConductorRequest struct {
	baseMessage
	Body listSchedulesConductorRequestBody `json:"body"`
}

type listSchedulesConductorResponse struct {
	baseResponse
	Output []scheduleConductorOutput `json:"output"`
}

type getScheduleConductorRequest struct {
	baseMessage
	ScheduleName string `json:"schedule_name"`
	LoadContext  *bool  `json:"load_context,omitempty"`
}

type getScheduleConductorResponse struct {
	baseResponse
	Output *scheduleConductorOutput `json:"output"`
}

type pauseScheduleConductorRequest struct {
	baseMessage
	ScheduleName string `json:"schedule_name"`
}

type pauseScheduleConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

type resumeScheduleConductorRequest struct {
	baseMessage
	ScheduleName string `json:"schedule_name"`
}

type resumeScheduleConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}

type backfillScheduleConductorRequest struct {
	baseMessage
	ScheduleName string `json:"schedule_name"`
	Start        string `json:"start"`
	End          string `json:"end"`
}

type backfillScheduleConductorResponse struct {
	baseResponse
	WorkflowIds []string `json:"workflow_ids"`
}

type triggerScheduleConductorRequest struct {
	baseMessage
	ScheduleName string `json:"schedule_name"`
}

type triggerScheduleConductorResponse struct {
	baseResponse
	WorkflowId *string `json:"workflow_id"`
}

type eventOutput struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Topic is nil when the notification was sent without a topic.

type notificationOutput struct {
	Topic            *string `json:"topic"`
	Message          string  `json:"message"`
	CreatedAtEpochMs int64   `json:"created_at_epoch_ms"`
	Consumed         bool    `json:"consumed"`
}

type streamEntryOutput struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

type getWorkflowEventsConductorRequest struct {
	baseMessage
	WorkflowId string `json:"workflow_id"`
}

type getWorkflowEventsConductorResponse struct {
	baseResponse
	Events []eventOutput `json:"events"`
}

type getWorkflowNotificationsConductorRequest struct {
	baseMessage
	WorkflowId string `json:"workflow_id"`
}

type getWorkflowNotificationsConductorResponse struct {
	baseResponse
	Notifications []notificationOutput `json:"notifications"`
}

type getWorkflowStreamsConductorRequest struct {
	baseMessage
	WorkflowId string `json:"workflow_id"`
}

type getWorkflowStreamsConductorResponse struct {
	baseResponse
	Streams []streamEntryOutput `json:"streams"`
}

type getWorkflowAggregatesConductorRequestBody struct {
	GroupByStatus             bool         `json:"group_by_status"`
	GroupByName               bool         `json:"group_by_name"`
	GroupByQueueName          bool         `json:"group_by_queue_name"`
	GroupByExecutorId         bool         `json:"group_by_executor_id"`
	GroupByApplicationVersion bool         `json:"group_by_application_version"`
	TimeBucketSizeMs          *int64       `json:"time_bucket_size_ms,omitempty"`
	Status                    stringOrList `json:"status,omitempty"`
	StartTime                 *time.Time   `json:"start_time,omitempty"`
	EndTime                   *time.Time   `json:"end_time,omitempty"`
	Name                      stringOrList `json:"name,omitempty"`
	AppVersion                stringOrList `json:"app_version,omitempty"`
	ExecutorId                stringOrList `json:"executor_id,omitempty"`
	QueueName                 stringOrList `json:"queue_name,omitempty"`
	WorkflowIdPrefix          stringOrList `json:"workflow_id_prefix,omitempty"`
}

type getWorkflowAggregatesConductorRequest struct {
	baseMessage
	Body getWorkflowAggregatesConductorRequestBody `json:"body"`
}

type getWorkflowAggregatesConductorResponse struct {
	baseResponse
	Output []WorkflowAggregateRow `json:"output"`
}

type getStepAggregatesConductorRequestBody struct {
	GroupByFunctionName bool         `json:"group_by_function_name"`
	GroupByStatus       bool         `json:"group_by_status"`
	SelectCount         bool         `json:"select_count"`
	SelectMaxDurationMs bool         `json:"select_max_duration_ms"`
	TimeBucketSizeMs    *int64       `json:"time_bucket_size_ms,omitempty"`
	Status              stringOrList `json:"status,omitempty"`
	FunctionName        stringOrList `json:"function_name,omitempty"`
	WorkflowIdPrefix    stringOrList `json:"workflow_id_prefix,omitempty"`
	CompletedAfter      *time.Time   `json:"completed_after,omitempty"`
	CompletedBefore     *time.Time   `json:"completed_before,omitempty"`
}

type getStepAggregatesConductorRequest struct {
	baseMessage
	Body getStepAggregatesConductorRequestBody `json:"body"`
}

type getStepAggregatesConductorResponse struct {
	baseResponse
	Output []StepAggregateRow `json:"output"`
}

type applicationVersionOutput struct {
	Id        string `json:"version_id"`
	Name      string `json:"version_name"`
	Timestamp int64  `json:"version_timestamp"`
	CreatedAt int64  `json:"created_at"`
}

func formatApplicationVersionOutput(v VersionInfo) applicationVersionOutput {
	return applicationVersionOutput{
		Id:        v.Id,
		Name:      v.Name,
		Timestamp: v.Timestamp,
		CreatedAt: v.CreatedAt,
	}
}

type listApplicationVersionsConductorRequest struct {
	baseMessage
}

type listApplicationVersionsConductorResponse struct {
	baseResponse
	Output []applicationVersionOutput `json:"output"`
}

type setLatestApplicationVersionConductorRequest struct {
	baseMessage
	VersionName string `json:"version_name"`
}

type setLatestApplicationVersionConductorResponse struct {
	baseResponse
	Success bool `json:"success"`
}
