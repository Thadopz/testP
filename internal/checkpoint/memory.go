package checkpoint

import (
	"context"
	"sync"
)

type MemoryStore struct {
	mu               sync.Mutex
	shardCheckpoints map[int]ShardCheckpoint
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		shardCheckpoints: make(map[int]ShardCheckpoint),
	}
}

func (s *MemoryStore) SaveShardCheckpoint(ctx context.Context, checkpoint ShardCheckpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.shardCheckpoints == nil {
		s.shardCheckpoints = make(map[int]ShardCheckpoint)
	}

	s.shardCheckpoints[checkpoint.ShardID] = checkpoint
	return nil
}

func (s *MemoryStore) LoadShardCheckpoint(ctx context.Context, shardID int) (ShardCheckpoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return ShardCheckpoint{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	checkpoint, ok := s.shardCheckpoints[shardID]
	if !ok {
		return ShardCheckpoint{}, false, nil
	}

	return checkpoint, true, nil
}
