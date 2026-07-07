package eventlog

import (
	"context"
	"errors"
	"testP/internal/model"
	"testing"
	"time"
)

func TestFileEventLogAppendAssignsIncreasingOffsets(t *testing.T) {

	eventLog := NewFileEventLog(t.TempDir(), &JSONEventCodec{})

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

func TestFileEventLogReadFromReturnsRecordsFromOffset(t *testing.T) {

	eventLog := NewFileEventLog(t.TempDir(), &JSONEventCodec{})

	appendFileTestEvent(t, eventLog, testEvent("event-1", 3))
	appendFileTestEvent(t, eventLog, testEvent("event-2", 3))
	appendFileTestEvent(t, eventLog, testEvent("event-3", 3))

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

func TestFileEventLogKeepsShardsSeparate(t *testing.T) {

	eventLog := NewFileEventLog(t.TempDir(), &JSONEventCodec{})

	appendFileTestEvent(t, eventLog, testEvent("shard-1-event", 1))
	appendFileTestEvent(t, eventLog, testEvent("shard-2-event", 2))

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

func TestFileEventLogReadFromMissingShardReturnsEmptyChannel(t *testing.T) {
	eventLog := NewFileEventLog(t.TempDir(), &JSONEventCodec{})

	recordCh, err := eventLog.ReadFrom(context.Background(), Position{ShardID: 99, Offset: 0})
	if err != nil {
		t.Fatalf("ReadFrom returned error: %v", err)
	}

	records := collectRecords(recordCh)
	if len(records) != 0 {
		t.Fatalf("record count mismatch: got %d, want %d", len(records), 0)
	}
}

func TestFileEventLogAppendReturnsContextErrorWhenCanceled(t *testing.T) {
	eventLog := NewFileEventLog(t.TempDir(), &JSONEventCodec{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := eventLog.Append(ctx, testEvent("event-1", 1))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Append error mismatch: got %v, want %v", err, context.Canceled)
	}
}

func TestFileEventLogReadFromReturnsContextErrorWhenCanceled(t *testing.T) {
	eventLog := NewFileEventLog(t.TempDir(), &JSONEventCodec{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := eventLog.ReadFrom(ctx, Position{ShardID: 1, Offset: 0})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadFrom error mismatch: got %v, want %v", err, context.Canceled)
	}
}

func TestFileEventLogTailFromReadsExistingEvent(t *testing.T) {
	eventLog := NewFileEventLog(t.TempDir(), &JSONEventCodec{})
	eventLog.SetPollInterval(time.Millisecond)
	appendFileTestEvent(t, eventLog, testEvent("event-1", 1))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recordCh, err := eventLog.TailFrom(ctx, Position{ShardID: 1, Offset: 0})
	if err != nil {
		t.Fatalf("TailFrom returned error: %v", err)
	}

	record := waitForRecord(t, recordCh)
	if record.Event.ID != "event-1" {
		t.Fatalf("record id mismatch: got %q, want %q", record.Event.ID, "event-1")
	}
}

func TestFileEventLogTailFromReceivesAppendedEvent(t *testing.T) {
	eventLog := NewFileEventLog(t.TempDir(), &JSONEventCodec{})
	eventLog.SetPollInterval(time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recordCh, err := eventLog.TailFrom(ctx, Position{ShardID: 1, Offset: 0})
	if err != nil {
		t.Fatalf("TailFrom returned error: %v", err)
	}

	appendFileTestEvent(t, eventLog, testEvent("event-1", 1))

	record := waitForRecord(t, recordCh)
	if record.Event.ID != "event-1" {
		t.Fatalf("record id mismatch: got %q, want %q", record.Event.ID, "event-1")
	}
}

func TestFileEventLogTailFromWaitsForMissingShardFile(t *testing.T) {
	eventLog := NewFileEventLog(t.TempDir(), &JSONEventCodec{})
	eventLog.SetPollInterval(time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	recordCh, err := eventLog.TailFrom(ctx, Position{ShardID: 99, Offset: 0})
	if err != nil {
		t.Fatalf("TailFrom returned error: %v", err)
	}

	appendFileTestEvent(t, eventLog, testEvent("event-1", 99))

	record := waitForRecord(t, recordCh)
	if record.Event.ID != "event-1" {
		t.Fatalf("record id mismatch: got %q, want %q", record.Event.ID, "event-1")
	}
}

func TestFileEventLogTailFromClosesChannelWhenCanceled(t *testing.T) {
	eventLog := NewFileEventLog(t.TempDir(), &JSONEventCodec{})
	eventLog.SetPollInterval(time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	recordCh, err := eventLog.TailFrom(ctx, Position{ShardID: 1, Offset: 0})
	if err != nil {
		t.Fatalf("TailFrom returned error: %v", err)
	}

	cancel()

	select {
	case _, ok := <-recordCh:
		if ok {
			t.Fatal("expected tail channel to be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for tail channel to close")
	}
}

func TestFileEventLogTailFromReturnsContextErrorWhenCanceled(t *testing.T) {
	eventLog := NewFileEventLog(t.TempDir(), &JSONEventCodec{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := eventLog.TailFrom(ctx, Position{ShardID: 1, Offset: 0})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("TailFrom error mismatch: got %v, want %v", err, context.Canceled)
	}
}

func appendFileTestEvent(t *testing.T, eventLog *FileEventLog, event model.Event) Position {
	t.Helper()

	position, err := eventLog.Append(context.Background(), event)
	if err != nil {
		t.Fatalf("Append returned error: %v", err)
	}

	return position
}

func waitForRecord(t *testing.T, recordCh <-chan Record) Record {
	t.Helper()

	select {
	case record, ok := <-recordCh:
		if !ok {
			t.Fatal("record channel closed before receiving a record")
		}
		return record
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for record")
	}

	return Record{}
}
