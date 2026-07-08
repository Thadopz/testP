package ownership

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
)

type FileOwnershipStore struct {
	mu   sync.Mutex
	path string
}

func NewFileOwnershipStore(dir string) *FileOwnershipStore {
	return &FileOwnershipStore{
		path: filepath.Join(dir, "ownership.json"),
	}
}

func (s *FileOwnershipStore) OwnerOf(shardID int) (Ownership, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	owners, err := s.load()
	if err != nil {
		return Ownership{}, false, err
	}

	ownership, ok := owners[shardID]
	return ownership, ok, nil
}

func (s *FileOwnershipStore) ShardsForNode(nodeID int) ([]Ownership, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	owners, err := s.load()
	if err != nil {
		return nil, err
	}

	ownerships := make([]Ownership, 0)
	for _, ownership := range owners {
		if ownership.NodeID == nodeID {
			ownerships = append(ownerships, ownership)
		}
	}

	slices.SortFunc(ownerships, func(a, b Ownership) int {
		return cmp.Compare(a.ShardID, b.ShardID)
	})

	return ownerships, nil
}

func (s *FileOwnershipStore) Assign(shardID int, nodeID int) error {
	if shardID < 0 {
		return fmt.Errorf("shard id must be >= 0: %d", shardID)
	}
	if nodeID <= 0 {
		return fmt.Errorf("node id must be > 0: %d", nodeID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	owners, err := s.load()
	if err != nil {
		return err
	}

	ownership, ok := owners[shardID]
	if !ok {
		ownership.Epoch = 1
	} else {
		ownership.Epoch += 1
	}

	ownership.ShardID = shardID
	ownership.NodeID = nodeID
	owners[shardID] = ownership

	return s.save(owners)
}

func (s *FileOwnershipStore) load() (map[int]Ownership, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return make(map[int]Ownership), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read ownership file: %w", err)
	}

	ownersByTextID := make(map[string]Ownership)
	if err := json.Unmarshal(data, &ownersByTextID); err != nil {
		return nil, fmt.Errorf("decode ownership file: %w", err)
	}

	owners := make(map[int]Ownership, len(ownersByTextID))
	for textShardID, ownership := range ownersByTextID {
		shardID, err := strconv.Atoi(textShardID)
		if err != nil {
			return nil, fmt.Errorf("parse shard id %q: %w", textShardID, err)
		}
		ownership.ShardID = shardID
		owners[shardID] = ownership
	}

	return owners, nil
}

func (s *FileOwnershipStore) save(owners map[int]Ownership) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0755); err != nil {
		return fmt.Errorf("create ownership dir: %w", err)
	}

	ownersByTextID := make(map[string]Ownership, len(owners))
	for shardID, ownership := range owners {
		ownersByTextID[fmt.Sprintf("%d", shardID)] = ownership
	}

	data, err := json.MarshalIndent(ownersByTextID, "", "  ")
	if err != nil {
		return fmt.Errorf("encode ownership file: %w", err)
	}

	tempPath := s.path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open temp ownership file: %w", err)
	}
	defer os.Remove(tempPath)

	if _, err := file.Write(data); err != nil {
		file.Close()
		return fmt.Errorf("write temp ownership file: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync temp ownership file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temp ownership file: %w", err)
	}

	if err := os.Rename(tempPath, s.path); err != nil {
		return fmt.Errorf("replace ownership file: %w", err)
	}

	return nil
}
