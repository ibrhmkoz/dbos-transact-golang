CREATE TABLE IF NOT EXISTS workflow_definitions (
    workflow_name TEXT PRIMARY KEY,
    global_concurrency INTEGER,
    rate_limit INTEGER,
    rate_period_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_workflow_status_claim
    ON workflow_status (name, status, priority, created_at)
    WHERE status IN ('ENQUEUED', 'PENDING');

CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_status_name_dedup_id
    ON workflow_status (name, deduplication_id)
    WHERE deduplication_id IS NOT NULL;
