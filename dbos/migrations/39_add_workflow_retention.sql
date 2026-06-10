ALTER TABLE %s.workflow_definitions
ADD COLUMN workflow_retention_ms BIGINT NOT NULL DEFAULT 86400000
CHECK (workflow_retention_ms > 0);
