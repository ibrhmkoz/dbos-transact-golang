-- name: CreateSchedule :exec
INSERT INTO workflow_schedules (
    schedule_id, schedule_name, workflow_name, workflow_class_name,
    schedule, context, status, automatic_backfill, cron_timezone, queue_name
) VALUES (
    @schedule_id, @schedule_name, @workflow_name, @workflow_class_name,
    @schedule, @context, @status::text, @automatic_backfill, @cron_timezone::text, @queue_name
);

-- name: ListSchedules :many
SELECT schedule_id, schedule_name, workflow_name, workflow_class_name,
       schedule, status, context, last_fired_at, automatic_backfill,
       cron_timezone, queue_name
FROM workflow_schedules
WHERE (NOT @filter_statuses::boolean OR status = ANY(@statuses::text[]))
  AND (NOT @filter_workflow_names::boolean OR workflow_name = ANY(@workflow_names::text[]))
  AND (NOT @filter_schedule_prefixes::boolean OR schedule_name LIKE ANY(@schedule_patterns::text[]));

-- name: UpdateSchedule :exec
UPDATE workflow_schedules
SET status = @status::text, last_fired_at = @last_fired_at
WHERE schedule_name = @schedule_name;

-- name: UpdateScheduleLastFiredAt :exec
UPDATE workflow_schedules
SET last_fired_at = @last_fired_at::text
WHERE schedule_name = @schedule_name;

-- name: DeleteSchedule :exec
DELETE FROM workflow_schedules WHERE schedule_name = $1;

-- name: WorkflowExists :one
SELECT 1 FROM workflow_status WHERE workflow_uuid = $1 LIMIT 1;
