-- name: GetWorkflowAggregates :many
-- Each group dimension is gated by a flag: when off, its column is NULL for all
-- rows so GROUP BY collapses that dimension. Group columns are scanned as
-- interface{} (string/int64/nil) and normalized to *string by the caller.
SELECT
    CASE WHEN @group_status::boolean THEN status ELSE NULL END AS g_status,
    CASE WHEN @group_name::boolean THEN name ELSE NULL END AS g_name,
    CASE WHEN @group_queue::boolean THEN queue_name ELSE NULL END AS g_queue_name,
    CASE WHEN @group_executor::boolean THEN executor_id ELSE NULL END AS g_executor_id,
    CASE WHEN @group_app_version::boolean THEN application_version ELSE NULL END AS g_app_version,
    CASE WHEN @group_time_bucket::boolean
        THEN CAST(FLOOR(created_at::numeric / @bucket_size::bigint) AS BIGINT) * @bucket_size::bigint
        ELSE NULL END AS g_time_bucket,
    COUNT(*) AS cnt
FROM workflow_status
WHERE (NOT @filter_status::boolean OR status = ANY(@statuses::text[]))
  AND (NOT @filter_start::boolean OR created_at >= @start_ms::bigint)
  AND (NOT @filter_end::boolean OR created_at <= @end_ms::bigint)
  AND (NOT @filter_name::boolean OR name = ANY(@names::text[]))
  AND (NOT @filter_app_version::boolean OR application_version = ANY(@app_versions::text[]))
  AND (NOT @filter_executor::boolean OR executor_id = ANY(@executor_ids::text[]))
  AND (NOT @filter_queue::boolean OR queue_name = ANY(@queue_names::text[]))
  AND (NOT @filter_id_prefix::boolean OR workflow_uuid LIKE ANY(@id_prefixes::text[]))
GROUP BY 1, 2, 3, 4, 5, 6
LIMIT @lim::bigint;

-- name: GetStepAggregates :many
-- Step status is derived from operation_outputs.error (NULL => SUCCESS).
-- Both aggregates are always computed; the caller surfaces only the ones requested.
SELECT
    CASE WHEN @group_function_name::boolean THEN function_name ELSE NULL END AS g_function_name,
    CASE WHEN @group_status::boolean
        THEN (CASE WHEN error IS NULL THEN 'SUCCESS' ELSE 'ERROR' END)
        ELSE NULL END AS g_status,
    CASE WHEN @group_time_bucket::boolean
        THEN CAST(FLOOR(completed_at_epoch_ms::numeric / @bucket_size::bigint) AS BIGINT) * @bucket_size::bigint
        ELSE NULL END AS g_time_bucket,
    COUNT(*) AS cnt,
    MAX(completed_at_epoch_ms - started_at_epoch_ms) AS max_duration_ms
FROM operation_outputs
WHERE (NOT @filter_status::boolean OR (CASE WHEN error IS NULL THEN 'SUCCESS' ELSE 'ERROR' END) = ANY(@statuses::text[]))
  AND (NOT @filter_function_name::boolean OR function_name = ANY(@function_names::text[]))
  AND (NOT @filter_id_prefix::boolean OR workflow_uuid LIKE ANY(@id_prefixes::text[]))
  AND (NOT @filter_completed_after::boolean OR completed_at_epoch_ms >= @completed_after::bigint)
  AND (NOT @filter_completed_before::boolean OR completed_at_epoch_ms <= @completed_before::bigint)
GROUP BY 1, 2, 3
LIMIT @lim::bigint;
