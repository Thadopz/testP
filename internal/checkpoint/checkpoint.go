package checkpoint

import "context"

type Checkpoint struct {
	NodeID int
	Offset map[int]int64
}

type ShardCheckpoint struct {
	ShardID   int
	Offset    int64
	Epoch     int64
	NodeID    int
	UpdatedAt int64
}

type Store interface {
	SaveCheckpoint(ctx context.Context, checkpoint Checkpoint) error
	LoadCheckpoint(ctx context.Context, nodeID int) (Checkpoint, bool, error)
}

type ShardStore interface {
	SaveShardCheckpoint(ctx context.Context, checkpoint ShardCheckpoint) error
	LoadShardCheckpoint(ctx context.Context, shardID int) (ShardCheckpoint, bool, error)
}
