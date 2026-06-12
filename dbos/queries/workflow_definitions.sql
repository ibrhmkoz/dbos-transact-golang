-- Definitions are immutable and content-addressed: one row per (workflow_name, digest),
-- written once. Deploys move the workflow_current pointer; operators write only
-- workflow_overrides. Effective policy = COALESCE(override, declared).

-- name: InsertWorkflowDefinition :exec
INSERT INTO workflow_definitions (
    workflow_name, digest, input_schema, output_schema,
    debounce_delay_ms, debounce_timeout_ms, max_recovery_attempts,
    global_concurrency, rate_limit, rate_period_ms, workflow_retention_ms, cron_schedule
) VALUES (
    @workflow_name, @digest, @input_schema, @output_schema,
    @debounce_delay_ms, @debounce_timeout_ms, @max_recovery_attempts,
    @global_concurrency, @rate_limit, @rate_period_ms, @workflow_retention_ms::bigint, @cron_schedule
)
ON CONFLICT (workflow_name, digest) DO NOTHING;

-- name: SetCurrentWorkflowDefinition :exec
INSERT INTO workflow_current (workflow_name, digest)
VALUES (@workflow_name, @digest)
ON CONFLICT (workflow_name) DO UPDATE SET
    digest = EXCLUDED.digest,
    since = (EXTRACT(epoch FROM now())::numeric * 1000)::bigint
WHERE workflow_current.digest IS DISTINCT FROM EXCLUDED.digest;

-- name: GetEffectiveWorkflowDefinition :one
SELECT
    COALESCE(o.global_concurrency, d.global_concurrency) AS global_concurrency,
    COALESCE(o.rate_limit, d.rate_limit) AS rate_limit,
    COALESCE(o.rate_period_ms, d.rate_period_ms) AS rate_period_ms
FROM workflow_current c
JOIN workflow_definitions d ON d.workflow_name = c.workflow_name AND d.digest = c.digest
LEFT JOIN workflow_overrides o ON o.workflow_name = c.workflow_name
WHERE c.workflow_name = $1;
