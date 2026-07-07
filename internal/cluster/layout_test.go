package cluster

import "testing"

func TestNewModuloLayoutAssignsOwners(t *testing.T) {
	layout, err := NewModuloLayout([]int{1, 2}, 5)
	if err != nil {
		t.Fatalf("NewModuloLayout returned error: %v", err)
	}

	expectedOwners := map[int]int{
		0: 1,
		1: 2,
		2: 1,
		3: 2,
		4: 1,
	}

	for shardID, expectedNodeID := range expectedOwners {
		nodeID, ok := layout.OwnerOf(shardID)
		if !ok {
			t.Fatalf("owner for shard %d not found", shardID)
		}
		if nodeID != expectedNodeID {
			t.Fatalf("owner mismatch for shard %d: got %d, want %d", shardID, nodeID, expectedNodeID)
		}
	}
}

func TestLayoutShardsForNode(t *testing.T) {
	layout, err := NewModuloLayout([]int{1, 2}, 5)
	if err != nil {
		t.Fatalf("NewModuloLayout returned error: %v", err)
	}

	shardIDs := layout.ShardsForNode(1)

	if len(shardIDs) != 3 || shardIDs[0] != 0 || shardIDs[1] != 2 || shardIDs[2] != 4 {
		t.Fatalf("shards mismatch: got %v, want [0 2 4]", shardIDs)
	}
}

func TestLayoutOwnerOfUnknownShardReturnsFalse(t *testing.T) {
	layout, err := NewModuloLayout([]int{1}, 2)
	if err != nil {
		t.Fatalf("NewModuloLayout returned error: %v", err)
	}

	_, ok := layout.OwnerOf(99)
	if ok {
		t.Fatal("expected unknown shard to return false")
	}
}

func TestNewModuloLayoutRejectsEmptyNodes(t *testing.T) {
	_, err := NewModuloLayout(nil, 2)
	if err == nil {
		t.Fatal("expected NewModuloLayout to return an error")
	}
}

func TestNewModuloLayoutRejectsInvalidShardCount(t *testing.T) {
	_, err := NewModuloLayout([]int{1}, 0)
	if err == nil {
		t.Fatal("expected NewModuloLayout to return an error")
	}
}

func TestNewModuloLayoutRejectsInvalidNodeID(t *testing.T) {
	_, err := NewModuloLayout([]int{0}, 2)
	if err == nil {
		t.Fatal("expected NewModuloLayout to return an error")
	}
}

func TestNewModuloLayoutRejectsDuplicateNodeID(t *testing.T) {
	_, err := NewModuloLayout([]int{1, 1}, 2)
	if err == nil {
		t.Fatal("expected NewModuloLayout to return an error")
	}
}
