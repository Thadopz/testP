package checkpoint

import "context"

type ShardCheckpoint struct {
	ShardID   int
	Offset    int64
	Epoch     int64
	NodeID    int
	UpdatedAt int64
}

type ShardStore interface {
	SaveShardCheckpoint(ctx context.Context, checkpoint ShardCheckpoint) error
	LoadShardCheckpoint(ctx context.Context, shardID int) (ShardCheckpoint, bool, error)
}
