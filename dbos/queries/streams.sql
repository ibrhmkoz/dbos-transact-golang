-- name: CheckStreamClosed :one
SELECT 1 FROM streams
WHERE workflow_uuid = $1 AND key = $2 AND value = $3
LIMIT 1;

-- name: InsertStreamEntry :exec
INSERT INTO streams (workflow_uuid, key, value, "offset", function_id, serialization)
SELECT @workflow_uuid, @key, @value::text, COALESCE(
    (SELECT MAX("offset") FROM streams WHERE workflow_uuid = @workflow_uuid AND key = @key), -1
) + 1, @function_id, @serialization::text;

-- name: ReadStream :many
SELECT value, "offset", serialization FROM streams
WHERE workflow_uuid = $1 AND key = $2 AND "offset" >= $3
ORDER BY "offset" ASC;

-- name: GetAllStreamEntries :many
SELECT key, value, serialization FROM streams
WHERE workflow_uuid = $1
ORDER BY key, "offset";
