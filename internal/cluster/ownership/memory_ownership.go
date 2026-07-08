package ownership

import (
	"cmp"
	"fmt"
	"slices"
	"sync"
)

type MemoryOwnershipStore struct {
	mu     sync.RWMutex
	owners map[int]Ownership
}

func NewMemoryOwnershipStore() *MemoryOwnershipStore {
	return &MemoryOwnershipStore{
		owners: make(map[int]Ownership),
	}
}

func (m *MemoryOwnershipStore) OwnerOf(shardID int) (Ownership, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ownership, ok := m.owners[shardID]
	return ownership, ok, nil
}

func (m *MemoryOwnershipStore) ShardsForNode(nodeID int) ([]Ownership, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ownerships := make([]Ownership, 0)
	for _, v := range m.owners {
		if v.NodeID == nodeID {
			ownerships = append(ownerships, v)
		}
	}

	slices.SortFunc(ownerships, func(a, b Ownership) int {
		return cmp.Compare(a.ShardID, b.ShardID)
	})

	return ownerships, nil
}

func (m *MemoryOwnershipStore) Assign(shardID int, nodeID int) error {
	if shardID < 0 {
		return fmt.Errorf("shard id must be >= 0: %d", shardID)
	}
	if nodeID <= 0 {
		return fmt.Errorf("node id must be > 0: %d", nodeID)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	ownership, ok := m.owners[shardID]
	if !ok {
		ownership.Epoch = 1
	} else {
		ownership.Epoch += 1
	}

	ownership.ShardID = shardID
	ownership.NodeID = nodeID
	m.owners[shardID] = ownership

	return nil
}
