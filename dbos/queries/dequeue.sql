-- name: CountRateLimitedWorkflows :one
SELECT COUNT(*) FROM workflow_status
WHERE name = @name::text
  AND rate_limited = TRUE
  AND status NOT IN (@enqueued_status::text, @delayed_status::text)
  AND started_at_epoch_ms > @cutoff_ms::bigint;

-- name: CountPendingWorkflows :one
SELECT COUNT(*) FROM workflow_status
WHERE name = @name::text AND status = @pending_status::text;

-- name: DequeueCandidatesSkipLocked :many
SELECT workflow_uuid FROM workflow_status
WHERE name = @name::text AND status = @enqueued_status::text
  AND (application_version = @app_version::text OR application_version IS NULL)
ORDER BY priority ASC, created_at ASC
FOR UPDATE SKIP LOCKED
LIMIT @max_tasks::int;

-- name: DequeueCandidatesNoWait :many
SELECT workflow_uuid FROM workflow_status
WHERE name = @name::text AND status = @enqueued_status::text
  AND (application_version = @app_version::text OR application_version IS NULL)
ORDER BY priority ASC, created_at ASC
FOR UPDATE NOWAIT
LIMIT @max_tasks::int;

-- name: DequeueClaimWorkflow :one
UPDATE workflow_status
SET status = @pending_status::text,
    application_version = @app_version::text,
    executor_id = @executor_id::text,
    started_at_epoch_ms = @started_at::bigint,
    rate_limited = @rate_limited::boolean,
    workflow_deadline_epoch_ms = CASE
        WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
        THEN workflow_timeout_ms + @started_at::bigint
        ELSE workflow_deadline_epoch_ms
    END
WHERE workflow_uuid = @workflow_uuid AND status = @enqueued_status::text
RETURNING name, inputs, serialization;
