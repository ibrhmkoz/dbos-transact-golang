-- name: UpsertWorkflowEvent :exec
INSERT INTO workflow_events (workflow_uuid, key, value, serialization)
VALUES (@workflow_uuid, @key, @value::text, @serialization::text)
ON CONFLICT (workflow_uuid, key)
DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization;

-- name: InsertWorkflowEventHistory :exec
INSERT INTO workflow_events_history (workflow_uuid, function_id, key, value, serialization)
VALUES (@workflow_uuid, @function_id, @key, @value::text, @serialization::text)
ON CONFLICT (workflow_uuid, function_id, key)
DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization;

-- name: GetWorkflowEvent :one
SELECT value, serialization FROM workflow_events
WHERE workflow_uuid = $1 AND key = $2;

-- name: GetAllEvents :many
SELECT key, value, serialization FROM workflow_events
WHERE workflow_uuid = $1;
