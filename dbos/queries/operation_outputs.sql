-- name: RecordOperationResult :exec
INSERT INTO operation_outputs (
    workflow_uuid, function_id, output, error, function_name,
    started_at_epoch_ms, completed_at_epoch_ms, serialization
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

-- name: GetOperationOutput :one
SELECT output, error, function_name, serialization
FROM operation_outputs
WHERE workflow_uuid = $1 AND function_id = $2;

-- name: DoesPatchExist :one
SELECT function_name FROM operation_outputs
WHERE workflow_uuid = $1 AND function_id = $2;

-- name: InsertPatchMarker :exec
INSERT INTO operation_outputs (workflow_uuid, function_id, function_name)
VALUES ($1, $2, $3);

-- name: GetWorkflowSteps :many
SELECT function_id, function_name, output, error,
       started_at_epoch_ms, completed_at_epoch_ms, serialization
FROM operation_outputs
WHERE workflow_uuid = $1
ORDER BY function_id ASC;
