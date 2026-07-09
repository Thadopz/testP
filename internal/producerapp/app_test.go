package producerapp

import (
	"context"
	"testP/internal/eventlog"
	"testP/internal/model"
	"testing"
)

func TestRunWritesOrderCreatedEvents(t *testing.T) {
	dataDir := t.TempDir()
	fileEventLog := eventlog.NewFileEventLog(dataDir+"/events", &eventlog.JSONEventCodec{})

	result, err := Run(context.Background(), Config{
		DataDir:  dataDir,
		EventLog: fileEventLog,
		Orders:   3,
		Seed:     1,
		StartID:  10,
		AreaSize: 1000,
		CellSize: 100,
		Shards:   4,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if result.Orders != 3 {
		t.Fatalf("orders mismatch: got %d, want %d", result.Orders, 3)
	}
	if result.FirstID != 10 {
		t.Fatalf("first id mismatch: got %d, want %d", result.FirstID, int64(10))
	}
	if result.LastID != 12 {
		t.Fatalf("last id mismatch: got %d, want %d", result.LastID, int64(12))
	}

	totalRecords := 0
	for shardID := 0; shardID < 4; shardID++ {
		recordCh, err := fileEventLog.ReadFrom(context.Background(), eventlog.Position{ShardID: shardID, Offset: 0})
		if err != nil {
			t.Fatalf("ReadFrom returned error: %v", err)
		}
		for range recordCh {
			totalRecords++
		}
	}

	if totalRecords != 3 {
		t.Fatalf("record count mismatch: got %d, want %d", totalRecords, 3)
	}
}

func TestRunRejectsNegativeOrders(t *testing.T) {
	_, err := Run(context.Background(), Config{
		DataDir:  t.TempDir(),
		EventLog: &recordingEventLog{},
		Orders:   -1,
	})
	if err == nil {
		t.Fatal("expected Run to return an error")
	}
}

func TestRunRequiresEventLog(t *testing.T) {
	_, err := Run(context.Background(), Config{
		DataDir: t.TempDir(),
		Orders:  1,
	})
	if err == nil {
		t.Fatal("expected Run to return an error")
	}
}

func TestRunUsesInjectedEventLog(t *testing.T) {
	eventLog := &recordingEventLog{}

	_, err := Run(context.Background(), Config{
		DataDir:  t.TempDir(),
		EventLog: eventLog,
		Orders:   2,
		Seed:     1,
		StartID:  10,
		AreaSize: 1000,
		CellSize: 100,
		Shards:   4,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if len(eventLog.events) != 2 {
		t.Fatalf("event count mismatch: got %d, want 2", len(eventLog.events))
	}
	if eventLog.events[0].Type != model.EventOrderCreated {
		t.Fatalf("event type mismatch: got %q", eventLog.events[0].Type)
	}
}

type recordingEventLog struct {
	events []model.Event
}

func (l *recordingEventLog) Append(ctx context.Context, event model.Event) (eventlog.Position, error) {
	position := eventlog.Position{
		ShardID: event.ShardID,
		Offset:  int64(len(l.events)),
	}
	l.events = append(l.events, event)
	return position, nil
}
