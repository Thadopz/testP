-- name: GetShardCheckpoint :one
SELECT * FROM shard_checkpoints
WHERE consumer_name = $1
  AND topic = $2
  AND shard_id = $3;

-- name: UpsertShardCheckpoint :exec
INSERT INTO shard_checkpoints (
    consumer_name, topic, shard_id, offset_value, epoch, node_id, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (consumer_name, topic, shard_id) DO UPDATE SET
    offset_value = EXCLUDED.offset_value,
    epoch = EXCLUDED.epoch,
    node_id = EXCLUDED.node_id,
    updated_at = EXCLUDED.updated_at
WHERE shard_checkpoints.epoch <= EXCLUDED.epoch;
