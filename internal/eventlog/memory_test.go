package eventlog

import (
	"context"
	"errors"
	"testing"
	"testP/internal/model"
)

func TestMemoryEventLogAppendAssignsIncreasingOffsets(t *testing.T) {
	eventLog := newTestMemoryEventLog(2)

	firstPosition, err := eventLog.Append(context.Background(), testEvent("event-1", 2))
	if err != nil {
		t.Fatalf("Append first event returned error: %v", err)
	}

	secondPosition, err := eventLog.Append(context.Background(), testEvent("event-2", 2))
	if err != nil {
		t.Fatalf("Append second event returned error: %v", err)
	}

	if firstPosition != (Position{ShardID: 2, Offset: 0}) {
		t.Fatalf("first position mismatch: got %+v, want %+v", firstPosition, Position{ShardID: 2, Offset: 0})
	}

	if secondPosition != (Position{ShardID: 2, Offset: 1}) {
		t.Fatalf("second position mismatch: got %+v, want %+v", secondPosition, Position{ShardID: 2, Offset: 1})
	}
}

func TestMemoryEventLogReadFromReturnsRecordsFromOffset(t *testing.T) {
	eventLog := newTestMemoryEventLog(3)

	appendTestEvent(t, eventLog, testEvent("event-1", 3))
	appendTestEvent(t, eventLog, testEvent("event-2", 3))
	appendTestEvent(t, eventLog, testEvent("event-3", 3))

	recordCh, err := eventLog.ReadFrom(context.Background(), Position{ShardID: 3, Offset: 1})
	if err != nil {
		t.Fatalf("ReadFrom returned error: %v", err)
	}

	records := collectRecords(recordCh)

	if len(records) != 2 {
		t.Fatalf("record count mismatch: got %d, want %d", len(records), 2)
	}

	if records[0].Event.ID != "event-2" {
		t.Fatalf("first record id mismatch: got %q, want %q", records[0].Event.ID, "event-2")
	}

	if records[1].Event.ID != "event-3" {
		t.Fatalf("second record id mismatch: got %q, want %q", records[1].Event.ID, "event-3")
	}
}

func TestMemoryEventLogKeepsShardsSeparate(t *testing.T) {
	eventLog := newTestMemoryEventLog(1, 2)

	appendTestEvent(t, eventLog, testEvent("shard-1-event", 1))
	appendTestEvent(t, eventLog, testEvent("shard-2-event", 2))

	recordCh, err := eventLog.ReadFrom(context.Background(), Position{ShardID: 1, Offset: 0})
	if err != nil {
		t.Fatalf("ReadFrom returned error: %v", err)
	}

	records := collectRecords(recordCh)

	if len(records) != 1 {
		t.Fatalf("record count mismatch: got %d, want %d", len(records), 1)
	}

	if records[0].Event.ID != "shard-1-event" {
		t.Fatalf("record id mismatch: got %q, want %q", records[0].Event.ID, "shard-1-event")
	}
}

func TestMemoryEventLogAppendReturnsContextErrorWhenCanceled(t *testing.T) {
	eventLog := newTestMemoryEventLog(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := eventLog.Append(ctx, testEvent("event-1", 1))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Append error mismatch: got %v, want %v", err, context.Canceled)
	}
}

func TestMemoryEventLogReadFromRejectsUnknownShard(t *testing.T) {
	eventLog := newTestMemoryEventLog(1)

	_, err := eventLog.ReadFrom(context.Background(), Position{ShardID: 99, Offset: 0})
	if err == nil {
		t.Fatal("expected ReadFrom to return an error")
	}
}

func TestMemoryEventLogReadFromRejectsOffsetPastEnd(t *testing.T) {
	eventLog := newTestMemoryEventLog(1)
	appendTestEvent(t, eventLog, testEvent("event-1", 1))

	_, err := eventLog.ReadFrom(context.Background(), Position{ShardID: 1, Offset: 2})
	if err == nil {
		t.Fatal("expected ReadFrom to return an error")
	}
}

func newTestMemoryEventLog(shardIDs ...int) *MemoryEventLog {
	eventLog := &MemoryEventLog{
		records: make(map[int][]Record),
	}

	for _, shardID := range shardIDs {
		eventLog.records[shardID] = nil
	}

	return eventLog
}

func appendTestEvent(t *testing.T, eventLog *MemoryEventLog, event model.Event) Position {
	t.Helper()

	position, err := eventLog.Append(context.Background(), event)
	if err != nil {
		t.Fatalf("Append returned error: %v", err)
	}

	return position
}

func collectRecords(recordCh <-chan Record) []Record {
	records := make([]Record, 0)

	for record := range recordCh {
		records = append(records, record)
	}

	return records
}

func testEvent(id string, shardID int) model.Event {
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
