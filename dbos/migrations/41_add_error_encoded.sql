-- The `error` column keeps the human-readable message for auditing and
-- cross-language tooling. `error_encoded` stores a cockroachdb/errors encoded
-- representation (base64 protobuf) so Go callers recover typed errors
-- (errors.Is/errors.As) when results are read back from the database.
ALTER TABLE %s.workflow_status ADD COLUMN IF NOT EXISTS error_encoded TEXT;
ALTER TABLE %s.operation_outputs ADD COLUMN IF NOT EXISTS error_encoded TEXT;
