package ownership

import (
	"sort"
	clusterlayout "testP/internal/cluster/layout"
	"testing"
)

func TestAssignLayoutSeedsOwnershipStore(t *testing.T) {
	layout, err := clusterlayout.NewModuloLayout([]int{1, 2}, 5)
	if err != nil {
		t.Fatalf("NewModuloLayout returned error: %v", err)
	}
	store := newSeedTestOwnershipStore()

	if err := AssignLayout(store, layout); err != nil {
		t.Fatalf("AssignLayout returned error: %v", err)
	}

	nodeOneShards, err := store.ShardsForNode(1)
	if err != nil {
		t.Fatalf("ShardsForNode returned error: %v", err)
	}
	if len(nodeOneShards) != 3 ||
		nodeOneShards[0].ShardID != 0 ||
		nodeOneShards[1].ShardID != 2 ||
		nodeOneShards[2].ShardID != 4 {
		t.Fatalf("node 1 shards mismatch: got %+v, want shards [0 2 4]", nodeOneShards)
	}

	nodeTwoShards, err := store.ShardsForNode(2)
	if err != nil {
		t.Fatalf("ShardsForNode returned error: %v", err)
	}
	if len(nodeTwoShards) != 2 ||
		nodeTwoShards[0].ShardID != 1 ||
		nodeTwoShards[1].ShardID != 3 {
		t.Fatalf("node 2 shards mismatch: got %+v, want shards [1 3]", nodeTwoShards)
	}
}

type seedTestOwnershipStore struct {
	owners map[int]Ownership
}

func newSeedTestOwnershipStore() *seedTestOwnershipStore {
	return &seedTestOwnershipStore{
		owners: make(map[int]Ownership),
	}
}

func (s *seedTestOwnershipStore) OwnerOf(shardID int) (Ownership, bool, error) {
	ownership, ok := s.owners[shardID]
	return ownership, ok, nil
}

func (s *seedTestOwnershipStore) ShardsForNode(nodeID int) ([]Ownership, error) {
	ownerships := make([]Ownership, 0)
	for _, ownership := range s.owners {
		if ownership.NodeID == nodeID {
			ownerships = append(ownerships, ownership)
		}
	}
	sort.Slice(ownerships, func(i, j int) bool {
		return ownerships[i].ShardID < ownerships[j].ShardID
	})
	return ownerships, nil
}

func (s *seedTestOwnershipStore) Assign(shardID int, nodeID int) error {
	ownership := s.owners[shardID]
	ownership.ShardID = shardID
	ownership.NodeID = nodeID
	ownership.Epoch++
	if ownership.Epoch == 0 {
		ownership.Epoch = 1
	}
	s.owners[shardID] = ownership
	return nil
}
