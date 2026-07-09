package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type FileStore struct {
	mu  sync.Mutex
	dir string
}

func NewFileStore(dir string) *FileStore {
	return &FileStore{
		dir: dir,
	}
}

func (s *FileStore) SaveCheckpoint(ctx context.Context, checkpoint Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return fmt.Errorf("create checkpoint dir: %w", err)
	}

	copied := copyCheckpoint(checkpoint)
	data, err := json.MarshalIndent(copied, "", "  ")
	if err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}

	targetPath := s.checkpointPath(checkpoint.NodeID)
	tempPath := targetPath + ".tmp"

	//为了确保稳定落盘，不要用writeFile
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open temp checkpoint file: %w", err)
	}
	defer os.Remove(tempPath)

	if _, err := file.Write(data); err != nil {
		file.Close()
		return fmt.Errorf("write temp checkpoint file: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync temp checkpoint file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temp checkpoint file: %w", err)
	}

	if err := os.Rename(tempPath, targetPath); err != nil {
		return fmt.Errorf("replace checkpoint file: %w", err)
	}

	return nil
}

func (s *FileStore) LoadCheckpoint(ctx context.Context, nodeID int) (Checkpoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.checkpointPath(nodeID)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Checkpoint{}, false, nil
	}
	if err != nil {
		return Checkpoint{}, false, fmt.Errorf("read checkpoint file: %w", err)
	}

	checkpoint := Checkpoint{}
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return Checkpoint{}, true, fmt.Errorf("decode checkpoint file: %w", err)
	}

	return copyCheckpoint(checkpoint), true, nil
}

func (s *FileStore) SaveShardCheckpoint(ctx context.Context, checkpoint ShardCheckpoint) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return fmt.Errorf("create checkpoint dir: %w", err)
	}

	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("encode shard checkpoint: %w", err)
	}

	targetPath := s.shardCheckpointPath(checkpoint.ShardID)
	tempPath := targetPath + ".tmp"

	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open temp shard checkpoint file: %w", err)
	}
	defer os.Remove(tempPath)

	if _, err := file.Write(data); err != nil {
		file.Close()
		return fmt.Errorf("write temp shard checkpoint file: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync temp shard checkpoint file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temp shard checkpoint file: %w", err)
	}

	if err := os.Rename(tempPath, targetPath); err != nil {
		return fmt.Errorf("replace shard checkpoint file: %w", err)
	}

	return nil
}

func (s *FileStore) LoadShardCheckpoint(ctx context.Context, shardID int) (ShardCheckpoint, bool, error) {
	if err := ctx.Err(); err != nil {
		return ShardCheckpoint{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.shardCheckpointPath(shardID))
	if errors.Is(err, os.ErrNotExist) {
		return ShardCheckpoint{}, false, nil
	}
	if err != nil {
		return ShardCheckpoint{}, false, fmt.Errorf("read shard checkpoint file: %w", err)
	}

	checkpoint := ShardCheckpoint{}
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return ShardCheckpoint{}, true, fmt.Errorf("decode shard checkpoint file: %w", err)
	}

	return checkpoint, true, nil
}

func (s *FileStore) checkpointPath(nodeID int) string {
	fileName := fmt.Sprintf("node-%d.json", nodeID)
	return filepath.Join(s.dir, fileName)
}

func (s *FileStore) shardCheckpointPath(shardID int) string {
	fileName := fmt.Sprintf("shard-%d.json", shardID)
	return filepath.Join(s.dir, fileName)
}
