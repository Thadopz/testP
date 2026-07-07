package node

import (
	"context"
	"testP/internal/checkpoint"
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
	firstRunner := NewRunner(10, []int{1}, eventLog, firstApplier, checkpointStore)

	err := firstRunner.Run(context.Background())
	if err != nil {
		t.Fatalf("first Run returned error: %v", err)
	}

	if appliedEventIDs(firstApplier) != "event-1,event-2" {
		t.Fatalf("first runner applied events mismatch: got %q", appliedEventIDs(firstApplier))
	}

	loaded, found, err := checkpointStore.LoadCheckpoint(context.Background(), 10)
	if err != nil {
		t.Fatalf("LoadCheckpoint returned error: %v", err)
	}
	if !found {
		t.Fatal("expected checkpoint to be found")
	}
	if loaded.Offset[1] != 2 {
		t.Fatalf("checkpoint offset mismatch: got %d, want %d", loaded.Offset[1], int64(2))
	}

	appendRecoveryEvent(t, eventLog, recoveryEvent("event-3", 1))

	secondApplier := &fakeApplier{}
	secondRunner := NewRunner(10, []int{1}, eventLog, secondApplier, checkpointStore)

	err = secondRunner.Run(context.Background())
	if err != nil {
		t.Fatalf("second Run returned error: %v", err)
	}

	if appliedEventIDs(secondApplier) != "event-3" {
		t.Fatalf("second runner applied events mismatch: got %q", appliedEventIDs(secondApplier))
	}
}

func appendRecoveryEvent(t *testing.T, eventLog eventlog.EventLog, event model.Event) {
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
