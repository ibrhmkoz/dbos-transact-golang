CREATE TABLE IF NOT EXISTS %s.workflow_definitions (
    workflow_name TEXT PRIMARY KEY,
    global_concurrency INTEGER,
    rate_limit INTEGER,
    rate_period_ms BIGINT
);

CREATE INDEX %s IF NOT EXISTS "idx_workflow_status_claim"
    ON %s.workflow_status ("name", "status", "priority", "created_at")
    WHERE "status" IN ('ENQUEUED', 'PENDING');

CREATE UNIQUE INDEX %s IF NOT EXISTS "uq_workflow_status_name_dedup_id"
    ON %s.workflow_status ("name", "deduplication_id")
    WHERE "deduplication_id" IS NOT NULL;
