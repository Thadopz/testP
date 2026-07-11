-- name: RecordInboxEvent :execrows
INSERT INTO inbox_events (consumer_name, event_id, shard_id, offset_value, processed_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (consumer_name, event_id) DO NOTHING;

-- name: CreateOutboxEvent :exec
INSERT INTO outbox_events (
    event_id, event_type, aggregate_type, aggregate_id,
    shard_id, occurred_at, payload, topic, message_key
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (event_id) DO NOTHING;

-- name: ClaimOutboxEvents :many
UPDATE outbox_events
SET claimed_by = sqlc.arg(worker_id),
    claimed_until = sqlc.arg(lease_until),
    attempts = attempts + 1
WHERE event_id IN (
    SELECT event_id
    FROM outbox_events
    WHERE published_at IS NULL
      AND outbox_events.claimed_until < sqlc.arg(now_value)
    ORDER BY occurred_at
    FOR UPDATE SKIP LOCKED
    LIMIT sqlc.arg(batch_size)
)
RETURNING *;

-- name: MarkOutboxEventPublished :exec
UPDATE outbox_events
SET published_at = $2,
    claimed_by = '',
    claimed_until = 0,
    last_error = ''
WHERE event_id = $1
  AND claimed_by = $3;

-- name: MarkOutboxEventFailed :exec
UPDATE outbox_events
SET claimed_by = '',
    claimed_until = 0,
    last_error = $2
WHERE event_id = $1
  AND claimed_by = $3;
