package checkpoint

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryStoreSaveThenLoadCheckpoint(t *testing.T) {
	store := NewMemoryStore()

	checkpoint := Checkpoint{
		NodeID: 10,
		Offset: map[int]int64{
			1: 100,
			2: 200,
		},
	}

	err := store.SaveCheckpoint(context.Background(), checkpoint)
	if err != nil {
		t.Fatalf("SaveCheckpoint returned error: %v", err)
	}

	loaded, found, err := store.LoadCheckpoint(context.Background(), 10)
	if err != nil {
		t.Fatalf("LoadCheckpoint returned error: %v", err)
	}

	if !found {
		t.Fatal("expected checkpoint to be found")
	}

	if loaded.NodeID != 10 {
		t.Fatalf("node id mismatch: got %d, want %d", loaded.NodeID, 10)
	}

	if loaded.Offset[1] != 100 {
		t.Fatalf("shard 1 offset mismatch: got %d, want %d", loaded.Offset[1], int64(100))
	}

	if loaded.Offset[2] != 200 {
		t.Fatalf("shard 2 offset mismatch: got %d, want %d", loaded.Offset[2], int64(200))
	}
}

func TestMemoryStoreLoadUnknownCheckpointReturnsNotFound(t *testing.T) {
	store := NewMemoryStore()

	_, found, err := store.LoadCheckpoint(context.Background(), 99)
	if err != nil {
		t.Fatalf("LoadCheckpoint returned error: %v", err)
	}

	if found {
		t.Fatal("expected checkpoint to be missing")
	}
}

func TestMemoryStoreCopiesCheckpointOnSave(t *testing.T) {
	store := NewMemoryStore()

	checkpoint := Checkpoint{
		NodeID: 10,
		Offset: map[int]int64{
			1: 100,
		},
	}

	err := store.SaveCheckpoint(context.Background(), checkpoint)
	if err != nil {
		t.Fatalf("SaveCheckpoint returned error: %v", err)
	}

	checkpoint.Offset[1] = 999

	loaded, found, err := store.LoadCheckpoint(context.Background(), 10)
	if err != nil {
		t.Fatalf("LoadCheckpoint returned error: %v", err)
	}
	if !found {
		t.Fatal("expected checkpoint to be found")
	}

	if loaded.Offset[1] != 100 {
		t.Fatalf("stored offset changed: got %d, want %d", loaded.Offset[1], int64(100))
	}
}

func TestMemoryStoreCopiesCheckpointOnLoad(t *testing.T) {
	store := NewMemoryStore()

	checkpoint := Checkpoint{
		NodeID: 10,
		Offset: map[int]int64{
			1: 100,
		},
	}

	err := store.SaveCheckpoint(context.Background(), checkpoint)
	if err != nil {
		t.Fatalf("SaveCheckpoint returned error: %v", err)
	}

	loaded, found, err := store.LoadCheckpoint(context.Background(), 10)
	if err != nil {
		t.Fatalf("LoadCheckpoint returned error: %v", err)
	}
	if !found {
		t.Fatal("expected checkpoint to be found")
	}

	loaded.Offset[1] = 999

	loadedAgain, found, err := store.LoadCheckpoint(context.Background(), 10)
	if err != nil {
		t.Fatalf("LoadCheckpoint returned error: %v", err)
	}
	if !found {
		t.Fatal("expected checkpoint to be found")
	}

	if loadedAgain.Offset[1] != 100 {
		t.Fatalf("stored offset changed: got %d, want %d", loadedAgain.Offset[1], int64(100))
	}
}

func TestMemoryStoreSaveReturnsContextError(t *testing.T) {
	store := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.SaveCheckpoint(ctx, Checkpoint{NodeID: 10})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SaveCheckpoint error mismatch: got %v, want %v", err, context.Canceled)
	}
}

func TestMemoryStoreLoadReturnsContextError(t *testing.T) {
	store := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := store.LoadCheckpoint(ctx, 10)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadCheckpoint error mismatch: got %v, want %v", err, context.Canceled)
	}
}

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
