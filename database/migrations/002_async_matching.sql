ALTER TABLE inbox_events DROP CONSTRAINT inbox_events_pkey;
ALTER TABLE inbox_events ADD COLUMN consumer_name TEXT NOT NULL DEFAULT 'order-worker';
ALTER TABLE inbox_events ADD PRIMARY KEY (consumer_name, event_id);

ALTER TABLE outbox_events
    ADD COLUMN topic TEXT NOT NULL DEFAULT 'order-events',
    ADD COLUMN message_key TEXT NOT NULL DEFAULT '',
    ADD COLUMN claimed_by TEXT NOT NULL DEFAULT '',
    ADD COLUMN claimed_until BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN last_error TEXT NOT NULL DEFAULT '';

DROP INDEX outbox_events_pending_idx;
CREATE INDEX outbox_events_pending_idx
    ON outbox_events (occurred_at)
    WHERE published_at IS NULL;

ALTER TABLE shard_checkpoints DROP CONSTRAINT shard_checkpoints_pkey;
ALTER TABLE shard_checkpoints
    ADD COLUMN consumer_name TEXT NOT NULL DEFAULT 'order-worker',
    ADD COLUMN topic TEXT NOT NULL DEFAULT 'order-events';
ALTER TABLE shard_checkpoints ADD PRIMARY KEY (consumer_name, topic, shard_id);

