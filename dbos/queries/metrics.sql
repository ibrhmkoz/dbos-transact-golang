-- name: GetMetricWorkflowCount :many
SELECT name, COUNT(workflow_uuid) AS count
FROM workflow_status
WHERE created_at >= @start_ms::bigint AND created_at < @end_ms::bigint
GROUP BY name;

-- name: GetMetricStepCount :many
SELECT function_name, COUNT(*) AS count
FROM operation_outputs
WHERE completed_at_epoch_ms >= @start_ms::bigint AND completed_at_epoch_ms < @end_ms::bigint
GROUP BY function_name;
