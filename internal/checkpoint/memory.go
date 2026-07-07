package checkpoint

import (
	"context"
	"sync"
)

type MemoryStore struct {
	mu          sync.Mutex
	checkpoints map[int]Checkpoint
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		checkpoints: make(map[int]Checkpoint),
	}
}

func (s *MemoryStore) SaveCheckpoint(ctx context.Context, checkpoint Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.checkpoints == nil {
		s.checkpoints = make(map[int]Checkpoint)
	}

	s.checkpoints[checkpoint.NodeID] = copyCheckpoint(checkpoint)
	return nil
}

func (s *MemoryStore) LoadCheckpoint(ctx context.Context, nodeID int) (Checkpoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	checkpoint, ok := s.checkpoints[nodeID]
	if !ok {
		return Checkpoint{}, false, nil
	}

	return copyCheckpoint(checkpoint), true, nil
}

func copyCheckpoint(checkpoint Checkpoint) Checkpoint {
	copied := Checkpoint{
		NodeID: checkpoint.NodeID,
		Offset: make(map[int]int64, len(checkpoint.Offset)),
	}

	for shardID, offset := range checkpoint.Offset {
		copied.Offset[shardID] = offset
	}

	return copied
}
