package ownership

import (
	clusterlayout "testP/internal/cluster/layout"
	"testing"
)

func TestAssignLayoutSeedsOwnershipStore(t *testing.T) {
	layout, err := clusterlayout.NewModuloLayout([]int{1, 2}, 5)
	if err != nil {
		t.Fatalf("NewModuloLayout returned error: %v", err)
	}
	store := NewMemoryOwnershipStore()

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
