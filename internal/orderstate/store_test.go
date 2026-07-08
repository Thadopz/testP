package orderstate

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryStoreSaveThenLoad(t *testing.T) {
	store := NewMemoryStore()
	state := testState()

	if err := store.Save(context.Background(), state); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	loaded, found, err := store.Load(context.Background(), state.OrderID)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !found {
		t.Fatal("expected order state to be found")
	}
	assertState(t, loaded, state)
}

func TestFileStorePersistsState(t *testing.T) {
	dir := t.TempDir()
	state := testState()

	firstStore := NewFileStore(dir)
	if err := firstStore.Save(context.Background(), state); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	secondStore := NewFileStore(dir)
	loaded, found, err := secondStore.Load(context.Background(), state.OrderID)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !found {
		t.Fatal("expected order state to be found")
	}
	assertState(t, loaded, state)
}

func TestFileStoreLoadUnknownOrderReturnsNotFound(t *testing.T) {
	store := NewFileStore(t.TempDir())

	_, found, err := store.Load(context.Background(), 999)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if found {
		t.Fatal("expected order state to be missing")
	}
}

func TestFileStoreRejectsInvalidOrderID(t *testing.T) {
	store := NewFileStore(t.TempDir())

	err := store.Save(context.Background(), State{OrderID: 0})
	if err == nil {
		t.Fatal("expected Save to return an error")
	}
}

func TestStoreReturnsContextError(t *testing.T) {
	store := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := store.Save(ctx, testState()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Save error mismatch: got %v, want %v", err, context.Canceled)
	}

	_, _, err := store.Load(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Load error mismatch: got %v, want %v", err, context.Canceled)
	}
}

func testState() State {
	return State{
		OrderID:      1001,
		ShardID:      3,
		Status:       StatusSubmitted,
		X:            10,
		Y:            20,
		Attempt:      1,
		LastEventID:  "event-1",
		UpdatedAt:    123,
		CancelReason: "",
		RetryReason:  "",
	}
}

func assertState(t *testing.T, got State, want State) {
	t.Helper()

	if got != want {
		t.Fatalf("state mismatch: got %+v, want %+v", got, want)
	}
}
