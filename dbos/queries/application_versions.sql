-- name: CreateApplicationVersion :exec
INSERT INTO application_versions (version_id, version_name, version_timestamp, created_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (version_name) DO NOTHING;

-- name: UpdateApplicationVersionTimestamp :exec
UPDATE application_versions
SET version_timestamp = $1
WHERE version_name = $2;

-- name: ListApplicationVersions :many
SELECT version_id, version_name, version_timestamp, created_at
FROM application_versions
ORDER BY version_timestamp DESC;

-- name: GetLatestApplicationVersion :one
SELECT version_id, version_name, version_timestamp, created_at
FROM application_versions
ORDER BY version_timestamp DESC
LIMIT 1;
