-- name: ForkInsertWorkflowStatus :exec
INSERT INTO workflow_status (
    workflow_uuid, status, name, authenticated_user, assumed_role,
    authenticated_roles, application_version, application_id, queue_name,
    queue_partition_key, inputs, created_at, updated_at, recovery_attempts,
    forked_from, serialization
) VALUES (
    @workflow_uuid, @status::text, @name::text, @authenticated_user::text, @assumed_role::text,
    @authenticated_roles::text, @application_version::text, @application_id::text, @queue_name::text,
    @queue_partition_key, @inputs, @created_at::bigint, @updated_at::bigint, @recovery_attempts::bigint,
    @forked_from::text, @serialization::text
);

-- name: MarkWorkflowForked :exec
UPDATE workflow_status SET was_forked_from = TRUE WHERE workflow_uuid = $1;

-- name: ForkCopyOperationOutputs :exec
INSERT INTO operation_outputs
    (workflow_uuid, function_id, output, error, function_name, started_at_epoch_ms, completed_at_epoch_ms)
SELECT @forked_id, oo.function_id, oo.output, oo.error, oo.function_name, oo.started_at_epoch_ms, oo.completed_at_epoch_ms
FROM operation_outputs oo
WHERE oo.workflow_uuid = @original_id AND oo.function_id < @start_step::int;

-- name: ForkCopyEventsHistory :exec
INSERT INTO workflow_events_history
    (workflow_uuid, function_id, key, value)
SELECT @forked_id, eh.function_id, eh.key, eh.value
FROM workflow_events_history eh
WHERE eh.workflow_uuid = @original_id AND eh.function_id < @start_step::int;

-- name: ForkCopyLatestEvents :exec
INSERT INTO workflow_events (workflow_uuid, key, value)
SELECT @forked_id, h.key, h.value
FROM workflow_events_history h
INNER JOIN (
    SELECT eh.key, MAX(eh.function_id) AS max_fid
    FROM workflow_events_history eh
    WHERE eh.workflow_uuid = @original_id AND eh.function_id < @start_step::int
    GROUP BY eh.key
) latest ON h.key = latest.key AND h.function_id = latest.max_fid
WHERE h.workflow_uuid = @original_id AND h.function_id < @start_step::int;

-- name: ForkCopyStreams :exec
INSERT INTO streams
    (workflow_uuid, key, value, "offset", function_id)
SELECT @forked_id, st.key, st.value, st."offset", st.function_id
FROM streams st
WHERE st.workflow_uuid = @original_id AND st.function_id < @start_step::int;
