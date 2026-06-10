-- name: InsertNotification :exec
INSERT INTO notifications (destination_uuid, topic, message, serialization, message_uuid, created_at_epoch_ms)
VALUES (@destination_uuid, @topic::text, @message::text, @serialization::text, @message_uuid, @created_at_epoch_ms::bigint);

-- name: HasUnconsumedMessage :one
SELECT EXISTS (
    SELECT 1 FROM notifications
    WHERE destination_uuid = @destination_uuid AND topic = @topic::text AND consumed = false
);

-- name: ConsumeOldestMessage :one
WITH oldest_entry AS (
    SELECT n.message_uuid FROM notifications n
    WHERE n.destination_uuid = @destination_uuid AND n.topic = @topic::text AND n.consumed = false
    ORDER BY n.created_at_epoch_ms ASC
    LIMIT 1
)
UPDATE notifications
SET consumed = true
WHERE message_uuid = (SELECT message_uuid FROM oldest_entry)
RETURNING message, serialization;

-- name: GetAllNotifications :many
SELECT topic, message, serialization, created_at_epoch_ms, consumed
FROM notifications
WHERE destination_uuid = $1
ORDER BY created_at_epoch_ms;
