package checkpoint

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryStoreSaveThenLoadShardCheckpoint(t *testing.T) {
	store := NewMemoryStore()

	err := store.SaveShardCheckpoint(context.Background(), ShardCheckpoint{
		ShardID: 1,
		Offset:  100,
		Epoch:   3,
		NodeID:  10,
	})
	if err != nil {
		t.Fatalf("SaveShardCheckpoint returned error: %v", err)
	}

	loaded, found, err := store.LoadShardCheckpoint(context.Background(), 1)
	if err != nil {
		t.Fatalf("LoadShardCheckpoint returned error: %v", err)
	}
	if !found {
		t.Fatal("expected shard checkpoint to be found")
	}
	if loaded.Offset != 100 || loaded.Epoch != 3 || loaded.NodeID != 10 {
		t.Fatalf("shard checkpoint mismatch: got %+v", loaded)
	}
}

func TestMemoryStoreLoadUnknownShardCheckpointReturnsNotFound(t *testing.T) {
	store := NewMemoryStore()

	_, found, err := store.LoadShardCheckpoint(context.Background(), 99)
	if err != nil {
		t.Fatalf("LoadShardCheckpoint returned error: %v", err)
	}
	if found {
		t.Fatal("expected shard checkpoint to be missing")
	}
}

func TestMemoryStoreShardCheckpointReturnsContextError(t *testing.T) {
	store := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.SaveShardCheckpoint(ctx, ShardCheckpoint{ShardID: 1, Offset: 1, Epoch: 1, NodeID: 1})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SaveShardCheckpoint error mismatch: got %v, want %v", err, context.Canceled)
	}

	_, _, err = store.LoadShardCheckpoint(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadShardCheckpoint error mismatch: got %v, want %v", err, context.Canceled)
	}
}
