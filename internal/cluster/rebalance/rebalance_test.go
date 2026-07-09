package rebalance

import (
	"sort"
	"testP/internal/cluster/ownership"
	"testing"
)

func TestRebalanceOnceAppliesPlannerMove(t *testing.T) {
	ownershipStore := newRebalanceTestOwnershipStore()
	membershipStore := newRebalanceTestMembershipStore()
	membershipStore.alive[1] = true
	membershipStore.alive[2] = true
	assignOwnerships(t, ownershipStore, []ownership.Ownership{
		{ShardID: 0, NodeID: 1},
		{ShardID: 1, NodeID: 1},
		{ShardID: 2, NodeID: 1},
	})

	controller := NewControllerWithPlanner(
		ownershipStore,
		membershipStore,
		PlannerFunc(func(snapshot Snapshot) (Move, bool, error) {
			return Move{ShardID: 2, FromNodeID: 1, ToNodeID: 2}, true, nil
		}),
	)

	move, moved, err := controller.RebalanceOnce()
	if err != nil {
		t.Fatalf("RebalanceOnce returned error: %v", err)
	}
	if !moved {
		t.Fatal("expected RebalanceOnce to move one shard")
	}
	if move.ShardID != 2 || move.FromNodeID != 1 || move.ToNodeID != 2 {
		t.Fatalf("move mismatch: got %+v", move)
	}

	currentOwnership, ok, err := ownershipStore.OwnerOf(2)
	if err != nil {
		t.Fatalf("OwnerOf returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected shard 2 ownership to exist")
	}
	if currentOwnership.NodeID != 2 || currentOwnership.Epoch != 2 {
		t.Fatalf("ownership mismatch: got %+v, want node=2 epoch=2", currentOwnership)
	}
}

func assignOwnerships(t *testing.T, store ownership.OwnershipStore, ownerships []ownership.Ownership) {
	t.Helper()

	for _, currentOwnership := range ownerships {
		if err := store.Assign(currentOwnership.ShardID, currentOwnership.NodeID); err != nil {
			t.Fatalf("Assign returned error: %v", err)
		}
	}
}

func assertOwner(t *testing.T, store ownership.OwnershipStore, shardID int, wantNodeID int, wantEpoch int64) {
	t.Helper()

	currentOwnership, ok, err := store.OwnerOf(shardID)
	if err != nil {
		t.Fatalf("OwnerOf returned error: %v", err)
	}
	if !ok {
		t.Fatalf("expected shard %d ownership to exist", shardID)
	}
	if currentOwnership.NodeID != wantNodeID || currentOwnership.Epoch != wantEpoch {
		t.Fatalf("ownership mismatch: got %+v, want node=%d epoch=%d", currentOwnership, wantNodeID, wantEpoch)
	}
}

type rebalanceTestOwnershipStore struct {
	owners map[int]ownership.Ownership
}

func newRebalanceTestOwnershipStore() *rebalanceTestOwnershipStore {
	return &rebalanceTestOwnershipStore{
		owners: make(map[int]ownership.Ownership),
	}
}

func (s *rebalanceTestOwnershipStore) OwnerOf(shardID int) (ownership.Ownership, bool, error) {
	currentOwnership, ok := s.owners[shardID]
	return currentOwnership, ok, nil
}

func (s *rebalanceTestOwnershipStore) ShardsForNode(nodeID int) ([]ownership.Ownership, error) {
	ownerships := make([]ownership.Ownership, 0)
	for _, currentOwnership := range s.owners {
		if currentOwnership.NodeID == nodeID {
			ownerships = append(ownerships, currentOwnership)
		}
	}
	sort.Slice(ownerships, func(i, j int) bool {
		return ownerships[i].ShardID < ownerships[j].ShardID
	})
	return ownerships, nil
}

func (s *rebalanceTestOwnershipStore) AllOwnerships() ([]ownership.Ownership, error) {
	ownerships := make([]ownership.Ownership, 0, len(s.owners))
	for _, currentOwnership := range s.owners {
		ownerships = append(ownerships, currentOwnership)
	}
	sort.Slice(ownerships, func(i, j int) bool {
		return ownerships[i].ShardID < ownerships[j].ShardID
	})
	return ownerships, nil
}

func (s *rebalanceTestOwnershipStore) Assign(shardID int, nodeID int) error {
	currentOwnership := s.owners[shardID]
	currentOwnership.ShardID = shardID
	currentOwnership.NodeID = nodeID
	currentOwnership.Epoch++
	if currentOwnership.Epoch == 0 {
		currentOwnership.Epoch = 1
	}
	s.owners[shardID] = currentOwnership
	return nil
}

type rebalanceTestMembershipStore struct {
	alive map[int]bool
}

func newRebalanceTestMembershipStore() *rebalanceTestMembershipStore {
	return &rebalanceTestMembershipStore{
		alive: make(map[int]bool),
	}
}

func (s *rebalanceTestMembershipStore) MarkAlive(nodeID int) error {
	s.alive[nodeID] = true
	return nil
}

func (s *rebalanceTestMembershipStore) MarkDead(nodeID int) error {
	delete(s.alive, nodeID)
	return nil
}

func (s *rebalanceTestMembershipStore) AliveNodes() ([]int, error) {
	nodeIDs := make([]int, 0, len(s.alive))
	for nodeID := range s.alive {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Ints(nodeIDs)
	return nodeIDs, nil
}

func (s *rebalanceTestMembershipStore) IsAlive(nodeID int) (bool, error) {
	return s.alive[nodeID], nil
}
