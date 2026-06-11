-- name: ExportWorkflowStatus :one
SELECT workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
       output, error, error_encoded, executor_id, created_at, updated_at, application_version, application_id,
       class_name, config_name, recovery_attempts, queue_name, workflow_timeout_ms,
       workflow_deadline_epoch_ms, started_at_epoch_ms, deduplication_id, inputs, priority,
       queue_partition_key, forked_from, parent_workflow_id, delay_until_epoch_ms, serialization
FROM workflow_status WHERE workflow_uuid = $1;

-- name: ExportOperationOutputs :many
SELECT workflow_uuid, function_id, function_name, output, error, error_encoded,
       started_at_epoch_ms, completed_at_epoch_ms
FROM operation_outputs WHERE workflow_uuid = $1;

-- name: ExportWorkflowEvents :many
SELECT workflow_uuid, key, value FROM workflow_events WHERE workflow_uuid = $1;

-- name: ExportWorkflowEventsHistory :many
SELECT workflow_uuid, function_id, key, value FROM workflow_events_history WHERE workflow_uuid = $1;

-- name: ExportStreams :many
SELECT workflow_uuid, key, value, "offset", function_id FROM streams WHERE workflow_uuid = $1;

-- name: ImportWorkflowStatus :exec
INSERT INTO workflow_status (
    workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
    output, error, error_encoded, executor_id, created_at, updated_at, application_version, application_id,
    class_name, config_name, recovery_attempts, queue_name, workflow_timeout_ms,
    workflow_deadline_epoch_ms, started_at_epoch_ms, deduplication_id, inputs, priority,
    queue_partition_key, forked_from, parent_workflow_id, delay_until_epoch_ms, serialization
) VALUES (
    @workflow_uuid, @status, @name, @authenticated_user, @assumed_role, @authenticated_roles,
    @output, @error, @error_encoded, @executor_id, @created_at::bigint, @updated_at::bigint, @application_version, @application_id,
    @class_name, @config_name, @recovery_attempts, @queue_name, @workflow_timeout_ms,
    @workflow_deadline_epoch_ms, @started_at_epoch_ms, @deduplication_id, @inputs, @priority::int,
    @queue_partition_key, @forked_from, @parent_workflow_id, @delay_until_epoch_ms, @serialization
);

-- name: ImportOperationOutput :exec
INSERT INTO operation_outputs (
    workflow_uuid, function_id, function_name, output, error, error_encoded,
    started_at_epoch_ms, completed_at_epoch_ms
) VALUES (@workflow_uuid, @function_id::int, @function_name, @output, @error, @error_encoded, @started_at_epoch_ms, @completed_at_epoch_ms);

-- name: ImportWorkflowEvent :exec
INSERT INTO workflow_events (workflow_uuid, key, value)
VALUES (@workflow_uuid, @key, @value::text);

-- name: ImportWorkflowEventHistory :exec
INSERT INTO workflow_events_history (workflow_uuid, function_id, key, value)
VALUES (@workflow_uuid, @function_id::int, @key, @value::text);

-- name: ImportStream :exec
INSERT INTO streams (workflow_uuid, key, value, "offset", function_id)
VALUES (@workflow_uuid, @key, @value::text, @stream_offset::int, @function_id::int);
