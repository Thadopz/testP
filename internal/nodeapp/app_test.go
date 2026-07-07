package nodeapp

import (
	"context"
	"testP/internal/checkpoint"
	"testP/internal/eventlog"
	"testP/internal/model"
	"testing"
)

func TestRunWithResultReplaysFileEventLogAndUsesCheckpoint(t *testing.T) {
	dataDir := t.TempDir()
	codec := &eventlog.JSONEventCodec{}
	fileEventLog := eventlog.NewFileEventLog(dataDir+"/events", codec)
	checkpointStore := checkpoint.NewFileStore(dataDir + "/checkpoints")

	appendOrderCreatedEvent(t, fileEventLog, codec, "event-1", 1, 1)
	appendOrderCreatedEvent(t, fileEventLog, codec, "event-2", 1, 2)

	cfg := Config{
		NodeID:   10,
		ShardIDs: []int{1},
		DataDir:  dataDir,
		Riders:   20,
		Workers:  1,
		Seed:     1,
	}

	firstResult, err := RunWithResult(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunWithResult returned error: %v", err)
	}
	if firstResult.Submitted != 2 {
		t.Fatalf("first submitted count mismatch: got %d, want %d", firstResult.Submitted, int64(2))
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

	appendOrderCreatedEvent(t, fileEventLog, codec, "event-3", 1, 3)

	secondResult, err := RunWithResult(context.Background(), cfg)
	if err != nil {
		t.Fatalf("RunWithResult returned error: %v", err)
	}
	if secondResult.Submitted != 1 {
		t.Fatalf("second submitted count mismatch: got %d, want %d", secondResult.Submitted, int64(1))
	}
}

func appendOrderCreatedEvent(t *testing.T, eventLog eventlog.EventLog, codec eventlog.EventCodec, eventID string, shardID int, orderID int64) {
	t.Helper()

	payload, err := codec.EncodePayload(model.OrderCreated{
		OrderID: orderID,
		X:       10,
		Y:       20,
	})
	if err != nil {
		t.Fatalf("EncodePayload returned error: %v", err)
	}

	_, err = eventLog.Append(context.Background(), model.Event{
		ID:            eventID,
		Type:          model.EventOrderCreated,
		AggregateType: "order",
		AggregateID:   eventID,
		ShardID:       shardID,
		OccurredAt:    1234567890,
		Payload:       payload,
	})
	if err != nil {
		t.Fatalf("Append returned error: %v", err)
	}
}
