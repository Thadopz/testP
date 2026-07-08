package failover

import (
	"testP/internal/cluster/membership"
	"testP/internal/cluster/ownership"
	"testing"
)

func TestFailoverDeadNodeReassignsDeadNodeShards(t *testing.T) {
	ownershipStore := ownership.NewMemoryOwnershipStore()
	membershipStore := membership.NewMemoryMembershipStore()

	assignOwnerships(t, ownershipStore, []ownership.Ownership{
		{ShardID: 0, NodeID: 1},
		{ShardID: 1, NodeID: 2},
		{ShardID: 2, NodeID: 2},
		{ShardID: 3, NodeID: 3},
	})
	markAliveNodes(t, membershipStore, []int{1, 3})

	controller := NewFailoverController(ownershipStore, membershipStore)
	if err := controller.FailoverDeadNode(2); err != nil {
		t.Fatalf("FailoverDeadNode returned error: %v", err)
	}

	deadNodeShards, err := ownershipStore.ShardsForNode(2)
	if err != nil {
		t.Fatalf("ShardsForNode returned error: %v", err)
	}
	if len(deadNodeShards) != 0 {
		t.Fatalf("expected node 2 to own no shards, got %+v", deadNodeShards)
	}

	assertOwnerIsAlive(t, ownershipStore, 1, []int{1, 3}, 2)
	assertOwnerIsAlive(t, ownershipStore, 2, []int{1, 3}, 2)
}

func TestFailoverDeadNodeReturnsErrorWhenNoAliveNodeCanTakeOver(t *testing.T) {
	ownershipStore := ownership.NewMemoryOwnershipStore()
	membershipStore := membership.NewMemoryMembershipStore()

	assignOwnerships(t, ownershipStore, []ownership.Ownership{
		{ShardID: 1, NodeID: 2},
	})

	controller := NewFailoverController(ownershipStore, membershipStore)
	err := controller.FailoverDeadNode(2)
	if err == nil {
		t.Fatal("expected FailoverDeadNode to return an error")
	}
}

func TestFailoverDeadNodeWithNoOwnedShardsDoesNothing(t *testing.T) {
	ownershipStore := ownership.NewMemoryOwnershipStore()
	membershipStore := membership.NewMemoryMembershipStore()
	markAliveNodes(t, membershipStore, []int{1})

	controller := NewFailoverController(ownershipStore, membershipStore)
	if err := controller.FailoverDeadNode(2); err != nil {
		t.Fatalf("FailoverDeadNode returned error: %v", err)
	}
}

func TestChooseFailoverTargetUsesShardIDConsistently(t *testing.T) {
	aliveNodes := []int{1, 3, 5}

	firstTarget, err := chooseFailoverTarget(aliveNodes, 42)
	if err != nil {
		t.Fatalf("chooseFailoverTarget returned error: %v", err)
	}
	secondTarget, err := chooseFailoverTarget(aliveNodes, 42)
	if err != nil {
		t.Fatalf("chooseFailoverTarget returned error: %v", err)
	}

	if firstTarget != secondTarget {
		t.Fatalf("target mismatch: got %d then %d for same shard", firstTarget, secondTarget)
	}
	if !containsNodeID(aliveNodes, firstTarget) {
		t.Fatalf("target node %d is not in alive nodes %v", firstTarget, aliveNodes)
	}
}

func TestChooseFailoverTargetRejectsEmptyAliveNodes(t *testing.T) {
	_, err := chooseFailoverTarget(nil, 1)
	if err == nil {
		t.Fatal("expected chooseFailoverTarget to return an error")
	}
}

func assignOwnerships(t *testing.T, store ownership.OwnershipStore, ownerships []ownership.Ownership) {
	t.Helper()

	for _, ownership := range ownerships {
		if err := store.Assign(ownership.ShardID, ownership.NodeID); err != nil {
			t.Fatalf("Assign returned error: %v", err)
		}
	}
}

func markAliveNodes(t *testing.T, store membership.MembershipStore, nodeIDs []int) {
	t.Helper()

	for _, nodeID := range nodeIDs {
		if err := store.MarkAlive(nodeID); err != nil {
			t.Fatalf("MarkAlive returned error: %v", err)
		}
	}
}

func assertOwner(t *testing.T, store ownership.OwnershipStore, shardID int, expectedNodeID int, expectedEpoch int64) {
	t.Helper()

	ownership, ok, err := store.OwnerOf(shardID)
	if err != nil {
		t.Fatalf("OwnerOf returned error: %v", err)
	}
	if !ok {
		t.Fatalf("expected shard %d to have owner", shardID)
	}
	if ownership.NodeID != expectedNodeID || ownership.Epoch != expectedEpoch {
		t.Fatalf("ownership mismatch for shard %d: got %+v, want node=%d epoch=%d", shardID, ownership, expectedNodeID, expectedEpoch)
	}
}

func assertOwnerIsAlive(t *testing.T, store ownership.OwnershipStore, shardID int, aliveNodes []int, expectedEpoch int64) {
	t.Helper()

	ownership, ok, err := store.OwnerOf(shardID)
	if err != nil {
		t.Fatalf("OwnerOf returned error: %v", err)
	}
	if !ok {
		t.Fatalf("expected shard %d to have owner", shardID)
	}
	if !containsNodeID(aliveNodes, ownership.NodeID) {
		t.Fatalf("ownership mismatch for shard %d: got node=%d, want one of %v", shardID, ownership.NodeID, aliveNodes)
	}
	if ownership.Epoch != expectedEpoch {
		t.Fatalf("epoch mismatch for shard %d: got %d, want %d", shardID, ownership.Epoch, expectedEpoch)
	}
}

func containsNodeID(nodeIDs []int, expectedNodeID int) bool {
	for _, nodeID := range nodeIDs {
		if nodeID == expectedNodeID {
			return true
		}
	}
	return false
}
