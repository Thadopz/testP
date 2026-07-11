CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY,
    shard_id INTEGER NOT NULL,
    status TEXT NOT NULL,
    x INTEGER NOT NULL,
    y INTEGER NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    cancel_reason TEXT NOT NULL DEFAULT '',
    retry_reason TEXT NOT NULL DEFAULT '',
    miss_reason TEXT NOT NULL DEFAULT '',
    rider_id BIGINT NOT NULL DEFAULT 0,
    score INTEGER NOT NULL DEFAULT 0,
    last_event_id TEXT NOT NULL DEFAULT '',
    updated_at BIGINT NOT NULL
);

CREATE TABLE riders (
    uid BIGINT PRIMARY KEY,
    x INTEGER NOT NULL,
    y INTEGER NOT NULL,
    online BOOLEAN NOT NULL,
    cell_id BIGINT NOT NULL,
    count BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE inbox_events (
    event_id TEXT PRIMARY KEY,
    shard_id INTEGER NOT NULL,
    offset_value BIGINT NOT NULL,
    processed_at BIGINT NOT NULL
);

CREATE TABLE outbox_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    shard_id INTEGER NOT NULL,
    occurred_at BIGINT NOT NULL,
    payload BYTEA NOT NULL,
    published_at BIGINT
);

CREATE INDEX outbox_events_pending_idx
    ON outbox_events (occurred_at)
    WHERE published_at IS NULL;

CREATE TABLE shard_checkpoints (
    shard_id INTEGER PRIMARY KEY,
    offset_value BIGINT NOT NULL,
    epoch BIGINT NOT NULL,
    node_id INTEGER NOT NULL,
    updated_at BIGINT NOT NULL
);

