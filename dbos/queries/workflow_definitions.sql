-- name: UpsertWorkflowDefinition :exec
INSERT INTO workflow_definitions (
    workflow_name, global_concurrency, rate_limit, rate_period_ms, workflow_retention_ms
) VALUES (@workflow_name, @global_concurrency, @rate_limit, @rate_period_ms, @workflow_retention_ms::bigint)
ON CONFLICT (workflow_name) DO UPDATE SET
    global_concurrency = EXCLUDED.global_concurrency,
    rate_limit = EXCLUDED.rate_limit,
    rate_period_ms = EXCLUDED.rate_period_ms,
    workflow_retention_ms = EXCLUDED.workflow_retention_ms;

-- name: GetWorkflowDefinition :one
SELECT global_concurrency, rate_limit, rate_period_ms
FROM workflow_definitions
WHERE workflow_name = $1;
