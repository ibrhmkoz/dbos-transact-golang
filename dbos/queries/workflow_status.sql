-- name: CancelWorkflows :many
-- Sets matching non-terminal workflows to CANCELLED; returns the subset of
-- input IDs that exist (including terminal ones, which are not updated).
WITH existing AS (
    SELECT workflow_uuid FROM workflow_status WHERE workflow_uuid = ANY(@workflow_ids::text[])
), updated AS (
    UPDATE workflow_status
    SET status = @cancelled_status::text, updated_at = @now_ms::bigint, completed_at = @now_ms::bigint,
        started_at_epoch_ms = NULL, queue_name = NULL
    WHERE workflow_uuid = ANY(@workflow_ids::text[])
      AND status NOT IN (@success_status::text, @error_status::text, @cancelled_status::text)
    RETURNING workflow_uuid
)
SELECT workflow_uuid FROM existing;

-- name: ResumeWorkflows :many
-- Re-enqueues matching non-terminal workflows; returns the subset of input IDs
-- that exist (including terminal ones, which are not updated).
WITH existing AS (
    SELECT workflow_uuid FROM workflow_status WHERE workflow_uuid = ANY(@workflow_ids::text[])
), updated AS (
    UPDATE workflow_status
    SET status = @enqueued_status::text, queue_name = @queue_name::text, recovery_attempts = 0,
        workflow_deadline_epoch_ms = NULL, started_at_epoch_ms = NULL,
        updated_at = @now_ms::bigint, completed_at = NULL
    WHERE workflow_uuid = ANY(@workflow_ids::text[])
      AND status NOT IN (@success_status::text, @error_status::text)
    RETURNING workflow_uuid
)
SELECT workflow_uuid FROM existing;

-- name: UpdateWorkflowOutcome :exec
UPDATE workflow_status
SET status = @status::text, output = @output, error = @error::text, error_encoded = @error_encoded,
    updated_at = @now_ms::bigint, completed_at = @now_ms::bigint
WHERE workflow_uuid = @workflow_uuid
  AND NOT (status = @cancelled_status::text AND @status::text IN (@success_status::text, @error_status::text));

-- name: DeleteWorkflows :exec
DELETE FROM workflow_status WHERE workflow_uuid = ANY(@workflow_ids::text[]);

-- name: GetNthNewestCreatedAt :one
SELECT created_at FROM workflow_status ORDER BY created_at DESC LIMIT 1 OFFSET $1;

-- name: GarbageCollectByRetention :execrows
DELETE FROM workflow_status AS ws
WHERE ws.completed_at IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM workflow_current AS wc
    JOIN workflow_definitions AS wd
      ON wd.workflow_name = wc.workflow_name AND wd.digest = wc.digest
    WHERE wd.workflow_name = ws.name
      AND ws.completed_at + wd.workflow_retention_ms < @now_ms::bigint
  );

-- name: GarbageCollectByCutoff :execrows
DELETE FROM workflow_status
WHERE created_at < @cutoff::bigint
  AND status NOT IN (@pending_status::text, @enqueued_status::text, @delayed_status::text);

-- name: GetWorkflowOutcome :one
SELECT status, output, error, error_encoded, recovery_attempts, serialization
FROM workflow_status
WHERE workflow_uuid = $1;

-- name: GetWorkflowStatusOnly :one
SELECT status FROM workflow_status WHERE workflow_uuid = $1;

-- name: GetDeduplicatedWorkflow :one
SELECT workflow_uuid
FROM workflow_status
WHERE name = $1 AND deduplication_id = $2;

-- name: SetWorkflowDelay :exec
UPDATE workflow_status
SET delay_until_epoch_ms = @delay_until::bigint, updated_at = @updated_at::bigint
WHERE workflow_uuid = @workflow_uuid AND status = @status::text;

-- name: TransitionDelayedWorkflows :exec
UPDATE workflow_status
SET status = @new_status::text
WHERE status = @old_status::text AND delay_until_epoch_ms <= @now_ms::bigint;

-- name: InsertWorkflowStatus :one
INSERT INTO workflow_status (
    workflow_uuid, status, name, queue_name, authenticated_user, assumed_role,
    authenticated_roles, executor_id, application_version, application_id,
    created_at, recovery_attempts, updated_at, workflow_timeout_ms,
    workflow_deadline_epoch_ms, inputs, deduplication_id, priority,
    queue_partition_key, owner_xid, parent_workflow_id, class_name, config_name,
    serialization, delay_until_epoch_ms, definition_digest
) VALUES (
    @workflow_uuid, @status::text, @name::text, @queue_name::text, @authenticated_user::text, @assumed_role::text,
    @authenticated_roles::text, @executor_id::text, @application_version, @application_id::text,
    @created_at::bigint, @recovery_attempts::bigint, @updated_at::bigint, @workflow_timeout_ms,
    @workflow_deadline_epoch_ms, @inputs, @deduplication_id, @priority::int,
    @queue_partition_key, @owner_xid, @parent_workflow_id, @class_name, @config_name,
    @serialization::text, @delay_until_epoch_ms, @definition_digest
)
ON CONFLICT (workflow_uuid) DO UPDATE SET
    recovery_attempts = CASE
        WHEN EXCLUDED.status NOT IN (@enqueued_status::text, @delayed_status::text)
        THEN workflow_status.recovery_attempts + @recovery_increment::bigint
        ELSE workflow_status.recovery_attempts
    END,
    updated_at = EXCLUDED.updated_at,
    executor_id = CASE
        WHEN EXCLUDED.status IN (@enqueued_status::text, @delayed_status::text)
        THEN workflow_status.executor_id
        ELSE EXCLUDED.executor_id
    END
RETURNING recovery_attempts, status, name, queue_name, queue_partition_key,
          workflow_timeout_ms, workflow_deadline_epoch_ms, owner_xid;

-- name: MarkWorkflowMaxRecoveryExceeded :exec
UPDATE workflow_status
SET status = @new_status::text, started_at_epoch_ms = NULL, queue_name = NULL
WHERE workflow_uuid = @workflow_uuid AND status = @pending_status::text;

-- name: ClearQueueAssignment :execrows
UPDATE workflow_status
SET status = @enqueued_status::text, started_at_epoch_ms = NULL
WHERE workflow_uuid = @workflow_uuid AND queue_name IS NOT NULL AND status = @pending_status::text;
