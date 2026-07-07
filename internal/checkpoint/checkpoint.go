package checkpoint

import "context"

type Checkpoint struct {
	NodeID int
	Offset map[int]int64
}

type Store interface {
	SaveCheckpoint(ctx context.Context, checkpoint Checkpoint) error
	LoadCheckpoint(ctx context.Context, nodeID int) (Checkpoint, bool, error)
}
