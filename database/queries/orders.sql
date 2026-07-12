-- name: GetOrder :one
SELECT * FROM orders
WHERE order_id = $1;

-- name: GetOrderForUpdate :one
SELECT * FROM orders
WHERE order_id = $1
FOR UPDATE;

-- name: ListMissedOrdersForRetry :many
SELECT order_id, shard_id
FROM orders
WHERE order_id BETWEEN sqlc.arg(start_id) AND sqlc.arg(end_id)
  AND status = 'missed'
  AND attempt < sqlc.arg(attempt)
ORDER BY order_id;

-- name: UpsertOrder :exec
INSERT INTO orders (
    order_id, shard_id, status, x, y, attempt,
    cancel_reason, retry_reason, miss_reason,
    rider_id, score, last_event_id, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6,
    $7, $8, $9, $10, $11, $12, $13
)
ON CONFLICT (order_id) DO UPDATE SET
    shard_id = EXCLUDED.shard_id,
    status = EXCLUDED.status,
    x = EXCLUDED.x,
    y = EXCLUDED.y,
    attempt = EXCLUDED.attempt,
    cancel_reason = EXCLUDED.cancel_reason,
    retry_reason = EXCLUDED.retry_reason,
    miss_reason = EXCLUDED.miss_reason,
    rider_id = EXCLUDED.rider_id,
    score = EXCLUDED.score,
    last_event_id = EXCLUDED.last_event_id,
    updated_at = EXCLUDED.updated_at;

-- name: MarkOrderMatched :execrows
UPDATE orders
SET status = 'matched',
    rider_id = $2,
    score = $3,
    last_event_id = $4,
    updated_at = $5
WHERE order_id = $1
  AND status = 'match_pending';

-- name: MarkOrderMissed :execrows
UPDATE orders
SET status = 'missed',
    miss_reason = $2,
    last_event_id = $3,
    updated_at = $4
WHERE order_id = $1
  AND status = 'match_pending';
