package membership

import (
	"cmp"
	"fmt"
	"slices"
	"sync"
)

type MemoryMembershipStore struct {
	mu    sync.RWMutex
	alive map[int]bool
}

func NewMemoryMembershipStore() *MemoryMembershipStore {
	return &MemoryMembershipStore{
		alive: make(map[int]bool),
	}
}

func (m *MemoryMembershipStore) MarkAlive(nodeID int) error {
	if nodeID <= 0 {
		return fmt.Errorf("node id must be > 0: %d", nodeID)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.alive[nodeID] = true
	return nil
}

func (m *MemoryMembershipStore) MarkDead(nodeID int) error {
	if nodeID <= 0 {
		return fmt.Errorf("node id must be > 0: %d", nodeID)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.alive, nodeID)
	return nil
}

func (m *MemoryMembershipStore) AliveNodes() ([]int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodeIDs := make([]int, 0, len(m.alive))
	for nodeID := range m.alive {
		nodeIDs = append(nodeIDs, nodeID)
	}

	slices.SortFunc(nodeIDs, func(a, b int) int {
		return cmp.Compare(a, b)
	})

	return nodeIDs, nil
}

func (m *MemoryMembershipStore) IsAlive(nodeID int) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.alive[nodeID], nil
}
