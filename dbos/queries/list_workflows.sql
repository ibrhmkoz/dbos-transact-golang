-- name: ListWorkflows :many
SELECT
    workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
    executor_id, created_at, updated_at, application_version, application_id,
    recovery_attempts, queue_name, workflow_timeout_ms, workflow_deadline_epoch_ms, started_at_epoch_ms,
    deduplication_id, priority, queue_partition_key, forked_from, parent_workflow_id,
    serialization, delay_until_epoch_ms, was_forked_from, completed_at,
    output, error, inputs
FROM workflow_status
WHERE (NOT @filter_name::boolean OR name = ANY(@names::text[]))
  AND (NOT @filter_queue::boolean OR queue_name = ANY(@queue_names::text[]))
  AND (NOT @queues_only::boolean OR queue_name IS NOT NULL)
  AND (NOT @filter_id_prefix::boolean OR workflow_uuid LIKE ANY(@id_prefixes::text[]))
  AND (NOT @filter_ids::boolean OR workflow_uuid = ANY(@ids::text[]))
  AND (NOT @filter_auth_user::boolean OR authenticated_user = ANY(@auth_users::text[]))
  AND (NOT @filter_start::boolean OR created_at >= @start_ms::bigint)
  AND (NOT @filter_end::boolean OR created_at <= @end_ms::bigint)
  AND (NOT @filter_status::boolean OR status = ANY(@statuses::text[]))
  AND (NOT @filter_app_version::boolean OR application_version = ANY(@app_versions::text[]))
  AND (NOT @filter_executor::boolean OR executor_id = ANY(@executor_ids::text[]))
  AND (NOT @filter_forked::boolean OR forked_from = ANY(@forked_froms::text[]))
  AND (NOT @filter_parent::boolean OR parent_workflow_id = ANY(@parent_ids::text[]))
  AND (NOT @filter_dedup::boolean OR deduplication_id = ANY(@dedup_ids::text[]))
  AND (NOT @filter_completed_after::boolean OR completed_at >= @completed_after::bigint)
  AND (NOT @filter_completed_before::boolean OR completed_at <= @completed_before::bigint)
  AND (NOT @filter_dequeued_after::boolean OR started_at_epoch_ms >= @dequeued_after::bigint)
  AND (NOT @filter_dequeued_before::boolean OR started_at_epoch_ms <= @dequeued_before::bigint)
  AND (NOT @filter_was_forked::boolean OR was_forked_from = @was_forked::boolean)
  AND (NOT @filter_has_parent::boolean OR (
        (@has_parent::boolean AND parent_workflow_id IS NOT NULL) OR
        (NOT @has_parent::boolean AND parent_workflow_id IS NULL)))
ORDER BY
    CASE WHEN @sort_desc::boolean THEN created_at END DESC,
    created_at ASC
LIMIT NULLIF(@lim::bigint, -1)
OFFSET @off::bigint;
