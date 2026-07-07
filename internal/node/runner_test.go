package node

import (
	"context"
	"errors"
	"sync"
	"testP/internal/checkpoint"
	"testP/internal/eventlog"
	"testP/internal/model"
	"testing"
	"time"
)

func TestRunnerStartsReadingEachShardFromSavedOffset(t *testing.T) {
	eventLog := &fakeEventLog{
		recordsByShard: map[int][]eventlog.Record{
			1: {
				testRecord("event-1", 1, 5),
			},
			2: {
				testRecord("event-2", 2, 8),
			},
		},
	}
	applier := &fakeApplier{}
	runner := NewRunner(10, []int{1, 2}, eventLog, applier, nil)
	runner.nextStep[1] = 5
	runner.nextStep[2] = 8

	err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if len(eventLog.readPositions) != 2 {
		t.Fatalf("read position count mismatch: got %d, want %d", len(eventLog.readPositions), 2)
	}

	if eventLog.readPositions[0] != (eventlog.Position{ShardID: 1, Offset: 5}) {
		t.Fatalf("first read position mismatch: got %+v", eventLog.readPositions[0])
	}

	if eventLog.readPositions[1] != (eventlog.Position{ShardID: 2, Offset: 8}) {
		t.Fatalf("second read position mismatch: got %+v", eventLog.readPositions[1])
	}
}

func TestRunnerAdvancesOffsetAfterApplySucceeds(t *testing.T) {
	eventLog := &fakeEventLog{
		recordsByShard: map[int][]eventlog.Record{
			1: {
				testRecord("event-1", 1, 0),
				testRecord("event-2", 1, 1),
			},
		},
	}
	applier := &fakeApplier{}
	runner := NewRunner(10, []int{1}, eventLog, applier, nil)

	err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if runner.nextStep[1] != 2 {
		t.Fatalf("next offset mismatch: got %d, want %d", runner.nextStep[1], 2)
	}
}

func TestRunnerDoesNotAdvanceOffsetWhenApplyFails(t *testing.T) {
	eventLog := &fakeEventLog{
		recordsByShard: map[int][]eventlog.Record{
			1: {
				testRecord("event-1", 1, 0),
			},
		},
	}
	applier := &fakeApplier{err: errors.New("apply failed")}
	runner := NewRunner(10, []int{1}, eventLog, applier, nil)

	err := runner.Run(context.Background())
	if err == nil {
		t.Fatal("expected Run to return an error")
	}

	if runner.nextStep[1] != 0 {
		t.Fatalf("next offset mismatch: got %d, want %d", runner.nextStep[1], 0)
	}
}

func TestRunnerReturnsReadFromError(t *testing.T) {
	expectedErr := errors.New("read failed")
	eventLog := &fakeEventLog{err: expectedErr}
	applier := &fakeApplier{}
	runner := NewRunner(10, []int{1}, eventLog, applier, nil)

	err := runner.Run(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("Run error mismatch: got %v, want %v", err, expectedErr)
	}
}

func TestRunnerLoadsCheckpointBeforeReading(t *testing.T) {
	eventLog := &fakeEventLog{
		recordsByShard: map[int][]eventlog.Record{
			1: {
				testRecord("event-1", 1, 4),
			},
		},
	}
	applier := &fakeApplier{}
	store := checkpoint.NewMemoryStore()

	err := store.SaveCheckpoint(context.Background(), checkpoint.Checkpoint{
		NodeID: 10,
		Offset: map[int]int64{
			1: 4,
		},
	})
	if err != nil {
		t.Fatalf("SaveCheckpoint returned error: %v", err)
	}

	runner := NewRunner(10, []int{1}, eventLog, applier, store)

	err = runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if len(eventLog.readPositions) != 1 {
		t.Fatalf("read position count mismatch: got %d, want %d", len(eventLog.readPositions), 1)
	}

	if eventLog.readPositions[0] != (eventlog.Position{ShardID: 1, Offset: 4}) {
		t.Fatalf("read position mismatch: got %+v", eventLog.readPositions[0])
	}
}

func TestRunnerSavesCheckpointAfterApplySucceeds(t *testing.T) {
	eventLog := &fakeEventLog{
		recordsByShard: map[int][]eventlog.Record{
			1: {
				testRecord("event-1", 1, 0),
			},
		},
	}
	applier := &fakeApplier{}
	store := checkpoint.NewMemoryStore()
	runner := NewRunner(10, []int{1}, eventLog, applier, store)

	err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	waitForCheckpointOffset(t, store, 10, 1, 1)
}

type fakeEventLog struct {
	recordsByShard map[int][]eventlog.Record
	readPositions  []eventlog.Position
	err            error
}

func (f *fakeEventLog) Append(ctx context.Context, event model.Event) (eventlog.Position, error) {
	return eventlog.Position{}, nil
}

func (f *fakeEventLog) ReadFrom(ctx context.Context, position eventlog.Position) (<-chan eventlog.Record, error) {
	f.readPositions = append(f.readPositions, position)
	if f.err != nil {
		return nil, f.err
	}

	recordCh := make(chan eventlog.Record)
	records := append([]eventlog.Record(nil), f.recordsByShard[position.ShardID]...)

	go func() {
		defer close(recordCh)
		for _, record := range records {
			select {
			case <-ctx.Done():
				return
			case recordCh <- record:
			}
		}
	}()

	return recordCh, nil
}

type fakeApplier struct {
	mu     sync.Mutex
	events []model.Event
	err    error
}

func (f *fakeApplier) Apply(ctx context.Context, event model.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.events = append(f.events, event)
	return f.err
}

func testRecord(eventID string, shardID int, offset int64) eventlog.Record {
	return eventlog.Record{
		Position: eventlog.Position{
			ShardID: shardID,
			Offset:  offset,
		},
		Event: model.Event{
			ID:      eventID,
			Type:    model.EventOrderCreated,
			ShardID: shardID,
		},
	}
}

func waitForAppliedEvents(t *testing.T, applier *fakeApplier, count int) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		applier.mu.Lock()
		appliedCount := len(applier.events)
		applier.mu.Unlock()

		if appliedCount >= count {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("applied event count mismatch: got %d, want at least %d", appliedCount, count)
		case <-ticker.C:
		}
	}
}

func waitForCheckpointOffset(t *testing.T, store checkpoint.Store, nodeID int, shardID int, expectedOffset int64) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		loaded, found, err := store.LoadCheckpoint(context.Background(), nodeID)
		if err != nil {
			t.Fatalf("LoadCheckpoint returned error: %v", err)
		}

		if found && loaded.Offset[shardID] == expectedOffset {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("checkpoint offset mismatch: got found=%v checkpoint=%+v, want shard %d offset %d", found, loaded, shardID, expectedOffset)
		case <-ticker.C:
		}
	}
}
