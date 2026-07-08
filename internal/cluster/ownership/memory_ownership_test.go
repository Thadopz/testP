package ownership

import "testing"

func TestMemoryOwnershipStoreAssignAndOwnerOf(t *testing.T) {
	store := NewMemoryOwnershipStore()

	err := store.Assign(3, 1)
	if err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	ownership, ok, err := store.OwnerOf(3)
	if err != nil {
		t.Fatalf("OwnerOf returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected owner to exist")
	}
	if ownership.ShardID != 3 || ownership.NodeID != 1 || ownership.Epoch != 1 {
		t.Fatalf("ownership mismatch: got %+v, want shard=3 node=1 epoch=1", ownership)
	}
}

func TestMemoryOwnershipStoreOwnerOfUnknownShard(t *testing.T) {
	store := NewMemoryOwnershipStore()

	_, ok, err := store.OwnerOf(99)
	if err != nil {
		t.Fatalf("OwnerOf returned error: %v", err)
	}
	if ok {
		t.Fatal("expected unknown shard to return ok=false")
	}
}

func TestMemoryOwnershipStoreAssignIncrementsEpoch(t *testing.T) {
	store := NewMemoryOwnershipStore()

	if err := store.Assign(3, 1); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}
	if err := store.Assign(3, 2); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	ownership, ok, err := store.OwnerOf(3)
	if err != nil {
		t.Fatalf("OwnerOf returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected owner to exist")
	}
	if ownership.NodeID != 2 || ownership.Epoch != 2 {
		t.Fatalf("ownership mismatch: got %+v, want node=2 epoch=2", ownership)
	}
}

func TestMemoryOwnershipStoreShardsForNodeFiltersAndSorts(t *testing.T) {
	store := NewMemoryOwnershipStore()

	assignments := []struct {
		shardID int
		nodeID  int
	}{
		{shardID: 4, nodeID: 1},
		{shardID: 1, nodeID: 2},
		{shardID: 0, nodeID: 1},
		{shardID: 2, nodeID: 1},
	}

	for _, assignment := range assignments {
		if err := store.Assign(assignment.shardID, assignment.nodeID); err != nil {
			t.Fatalf("Assign returned error: %v", err)
		}
	}

	ownerships, err := store.ShardsForNode(1)
	if err != nil {
		t.Fatalf("ShardsForNode returned error: %v", err)
	}

	expectedShardIDs := []int{0, 2, 4}
	if len(ownerships) != len(expectedShardIDs) {
		t.Fatalf("ownership count mismatch: got %d, want %d", len(ownerships), len(expectedShardIDs))
	}

	for i, expectedShardID := range expectedShardIDs {
		if ownerships[i].ShardID != expectedShardID {
			t.Fatalf("shard mismatch at index %d: got %d, want %d", i, ownerships[i].ShardID, expectedShardID)
		}
		if ownerships[i].NodeID != 1 {
			t.Fatalf("node mismatch at index %d: got %d, want 1", i, ownerships[i].NodeID)
		}
		if ownerships[i].Epoch != 1 {
			t.Fatalf("epoch mismatch at index %d: got %d, want 1", i, ownerships[i].Epoch)
		}
	}
}

func TestMemoryOwnershipStoreShardsForNodeReturnsEmptyForUnknownNode(t *testing.T) {
	store := NewMemoryOwnershipStore()

	if err := store.Assign(0, 1); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	ownerships, err := store.ShardsForNode(2)
	if err != nil {
		t.Fatalf("ShardsForNode returned error: %v", err)
	}
	if len(ownerships) != 0 {
		t.Fatalf("expected no ownerships, got %+v", ownerships)
	}
}

func TestMemoryOwnershipStoreAssignRejectsNegativeShardID(t *testing.T) {
	store := NewMemoryOwnershipStore()

	err := store.Assign(-1, 1)
	if err == nil {
		t.Fatal("expected Assign to return an error")
	}
}

func TestMemoryOwnershipStoreAssignRejectsInvalidNodeID(t *testing.T) {
	store := NewMemoryOwnershipStore()

	err := store.Assign(0, 0)
	if err == nil {
		t.Fatal("expected Assign to return an error")
	}
}
