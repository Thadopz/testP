package node

import (
	"context"
	"errors"
	"testP/internal/checkpoint"
	clusterownership "testP/internal/cluster/ownership"
	"testP/internal/eventlog"
	"testP/internal/model"
	"testing"
)

func TestRunnerRestartsFromFileCheckpoint(t *testing.T) {
	baseDir := t.TempDir()
	eventLog := eventlog.NewFileEventLog(baseDir+"/events", &eventlog.JSONEventCodec{})
	checkpointStore := checkpoint.NewFileStore(baseDir + "/checkpoints")

	appendRecoveryEvent(t, eventLog, recoveryEvent("event-1", 1))
	appendRecoveryEvent(t, eventLog, recoveryEvent("event-2", 1))

	firstApplier := &fakeApplier{}
	provider := newRecoveryShardProvider([]clusterownership.Ownership{
		{ShardID: 1, NodeID: 10, Epoch: 1},
	})
	firstRunner := NewRunner(10, provider, eventLog, firstApplier, checkpointStore)

	firstCtx, firstCancel := context.WithCancel(context.Background())
	firstErrCh := make(chan error, 1)
	go func() {
		firstErrCh <- firstRunner.Run(firstCtx)
	}()

	waitForCheckpointOffset(t, checkpointStore, 10, 1, 2)
	firstCancel()

	err := <-firstErrCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("first Run returned error: %v", err)
	}

	if appliedEventIDs(firstApplier) != "event-1,event-2" {
		t.Fatalf("first runner applied events mismatch: got %q", appliedEventIDs(firstApplier))
	}

	loaded, found, err := checkpointStore.LoadShardCheckpoint(context.Background(), 1)
	if err != nil {
		t.Fatalf("LoadShardCheckpoint returned error: %v", err)
	}
	if !found {
		t.Fatal("expected checkpoint to be found")
	}
	if loaded.Offset != 2 {
		t.Fatalf("checkpoint offset mismatch: got %d, want %d", loaded.Offset, int64(2))
	}

	appendRecoveryEvent(t, eventLog, recoveryEvent("event-3", 1))

	secondApplier := &fakeApplier{}
	secondRunner := NewRunner(10, provider, eventLog, secondApplier, checkpointStore)

	secondCtx, secondCancel := context.WithCancel(context.Background())
	secondErrCh := make(chan error, 1)
	go func() {
		secondErrCh <- secondRunner.Run(secondCtx)
	}()

	waitForCheckpointOffset(t, checkpointStore, 10, 1, 3)
	secondCancel()

	err = <-secondErrCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("second Run returned error: %v", err)
	}

	if appliedEventIDs(secondApplier) != "event-3" {
		t.Fatalf("second runner applied events mismatch: got %q", appliedEventIDs(secondApplier))
	}
}

type recoveryShardProvider struct {
	ownerships []clusterownership.Ownership
}

func newRecoveryShardProvider(ownerships []clusterownership.Ownership) *recoveryShardProvider {
	return &recoveryShardProvider{
		ownerships: append([]clusterownership.Ownership(nil), ownerships...),
	}
}

func (p *recoveryShardProvider) ShardsForNode(nodeID int) ([]clusterownership.Ownership, error) {
	result := make([]clusterownership.Ownership, 0)
	for _, ownership := range p.ownerships {
		if ownership.NodeID == nodeID {
			result = append(result, ownership)
		}
	}
	return result, nil
}

func appendRecoveryEvent(t *testing.T, eventLog eventlog.Appender, event model.Event) {
	t.Helper()

	_, err := eventLog.Append(context.Background(), event)
	if err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
}

func recoveryEvent(id string, shardID int) model.Event {
	return model.Event{
		ID:            id,
		Type:          model.EventOrderCreated,
		AggregateType: "order",
		AggregateID:   id,
		ShardID:       shardID,
		OccurredAt:    1234567890,
		Payload:       []byte(`{"order_id":1,"x":1,"y":2}`),
	}
}

func appliedEventIDs(applier *fakeApplier) string {
	applier.mu.Lock()
	defer applier.mu.Unlock()

	result := ""
	for index, event := range applier.events {
		if index > 0 {
			result += ","
		}
		result += event.ID
	}

	return result
}
