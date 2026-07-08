package node

import (
	"context"
	"errors"
	"sync"
	"testP/internal/checkpoint"
	clusterownership "testP/internal/cluster/ownership"
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

func TestRunnerTailReturnsErrorWhenEventLogDoesNotSupportTail(t *testing.T) {
	runner := NewRunner(10, []int{1}, &readOnlyEventLog{}, &fakeApplier{}, nil)
	runner.SetTail(true)

	err := runner.Run(context.Background())
	if err == nil {
		t.Fatal("expected Run to return an error")
	}
}

func TestRunnerTailAppliesNewRecordAndStopsOnCancel(t *testing.T) {
	eventLog := newTailFakeEventLog()
	applier := &fakeApplier{}
	store := checkpoint.NewMemoryStore()
	runner := NewRunner(10, []int{1}, eventLog, applier, store)
	runner.SetTail(true)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- runner.Run(ctx)
	}()

	waitForTailReader(t, eventLog)
	eventLog.send(testRecord("event-1", 1, 0))
	waitForAppliedEvents(t, applier, 1)
	waitForCheckpointOffset(t, store, 10, 1, 1)

	cancel()

	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error mismatch: got %v, want %v", err, context.Canceled)
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

func TestDynamicRunnerRequiresTailMode(t *testing.T) {
	provider := newFakeShardProvider([]clusterownership.Ownership{
		{ShardID: 1, NodeID: 10, Epoch: 1},
	})
	runner := NewDynamicRunner(10, provider, &readOnlyEventLog{}, &fakeApplier{}, nil)

	err := runner.Run(context.Background())
	if err == nil {
		t.Fatal("expected Run to return an error")
	}
}

func TestDynamicRunnerRefreshOnceStartsOwnedShard(t *testing.T) {
	provider := newFakeShardProvider([]clusterownership.Ownership{
		{ShardID: 1, NodeID: 10, Epoch: 1},
	})
	eventLog := newDynamicTailFakeEventLog()
	runner := NewDynamicRunner(10, provider, eventLog, &fakeApplier{}, nil)
	defer runner.stopAllShards()

	errCh := make(chan error, 10)
	err := runner.refreshOnce(context.Background(), errCh)
	if err != nil {
		t.Fatalf("refreshOnce returned error: %v", err)
	}

	worker, ok := runner.activeWorker(1)
	if !ok {
		t.Fatal("expected shard 1 to be active")
	}
	if worker.epoch != 1 {
		t.Fatalf("worker epoch mismatch: got %d, want 1", worker.epoch)
	}

	positions := eventLog.tailPositions()
	if len(positions) != 1 {
		t.Fatalf("tail position count mismatch: got %d, want 1", len(positions))
	}
	if positions[0] != (eventlog.Position{ShardID: 1, Offset: 0}) {
		t.Fatalf("tail position mismatch: got %+v, want shard 1 offset 0", positions[0])
	}
}

func TestDynamicRunnerRefreshOnceUsesSavedOffset(t *testing.T) {
	provider := newFakeShardProvider([]clusterownership.Ownership{
		{ShardID: 2, NodeID: 10, Epoch: 1},
	})
	eventLog := newDynamicTailFakeEventLog()
	runner := NewDynamicRunner(10, provider, eventLog, &fakeApplier{}, nil)
	runner.SetTail(true)
	runner.nextStep[2] = 7
	defer runner.stopAllShards()

	errCh := make(chan error, 10)
	err := runner.refreshOnce(context.Background(), errCh)
	if err != nil {
		t.Fatalf("refreshOnce returned error: %v", err)
	}

	positions := eventLog.tailPositions()
	if len(positions) != 1 {
		t.Fatalf("tail position count mismatch: got %d, want 1", len(positions))
	}
	if positions[0] != (eventlog.Position{ShardID: 2, Offset: 7}) {
		t.Fatalf("tail position mismatch: got %+v, want shard 2 offset 7", positions[0])
	}
}

func TestDynamicRunnerRefreshOnceStopsRemovedShard(t *testing.T) {
	provider := newFakeShardProvider([]clusterownership.Ownership{
		{ShardID: 1, NodeID: 10, Epoch: 1},
	})
	eventLog := newDynamicTailFakeEventLog()
	runner := NewDynamicRunner(10, provider, eventLog, &fakeApplier{}, nil)
	runner.SetTail(true)

	errCh := make(chan error, 10)
	if err := runner.refreshOnce(context.Background(), errCh); err != nil {
		t.Fatalf("first refreshOnce returned error: %v", err)
	}

	provider.setOwnerships(nil)
	if err := runner.refreshOnce(context.Background(), errCh); err != nil {
		t.Fatalf("second refreshOnce returned error: %v", err)
	}

	if _, ok := runner.activeWorker(1); ok {
		t.Fatal("expected shard 1 to be stopped")
	}
}

func TestDynamicRunnerRefreshOnceRestartsShardWhenEpochChanges(t *testing.T) {
	provider := newFakeShardProvider([]clusterownership.Ownership{
		{ShardID: 1, NodeID: 10, Epoch: 1},
	})
	eventLog := newDynamicTailFakeEventLog()
	runner := NewDynamicRunner(10, provider, eventLog, &fakeApplier{}, nil)
	runner.SetTail(true)
	defer runner.stopAllShards()

	errCh := make(chan error, 10)
	if err := runner.refreshOnce(context.Background(), errCh); err != nil {
		t.Fatalf("first refreshOnce returned error: %v", err)
	}

	provider.setOwnerships([]clusterownership.Ownership{
		{ShardID: 1, NodeID: 10, Epoch: 2},
	})
	if err := runner.refreshOnce(context.Background(), errCh); err != nil {
		t.Fatalf("second refreshOnce returned error: %v", err)
	}

	worker, ok := runner.activeWorker(1)
	if !ok {
		t.Fatal("expected shard 1 to be active")
	}
	if worker.epoch != 2 {
		t.Fatalf("worker epoch mismatch: got %d, want 2", worker.epoch)
	}

	positions := eventLog.tailPositions()
	if len(positions) != 2 {
		t.Fatalf("tail position count mismatch: got %d, want 2", len(positions))
	}
}

func TestDynamicRunnerRefreshOnceReturnsProviderError(t *testing.T) {
	expectedErr := errors.New("provider failed")
	provider := &fakeShardProvider{err: expectedErr}
	runner := NewDynamicRunner(10, provider, newDynamicTailFakeEventLog(), &fakeApplier{}, nil)
	runner.SetTail(true)

	err := runner.refreshOnce(context.Background(), make(chan error, 1))
	if !errors.Is(err, expectedErr) {
		t.Fatalf("refreshOnce error mismatch: got %v, want %v", err, expectedErr)
	}
}

func TestDynamicRunnerRunAppliesRecordAndSavesCheckpoint(t *testing.T) {
	provider := newFakeShardProvider([]clusterownership.Ownership{
		{ShardID: 1, NodeID: 10, Epoch: 1},
	})
	eventLog := newDynamicTailFakeEventLog()
	applier := &fakeApplier{}
	store := checkpoint.NewMemoryStore()
	runner := NewDynamicRunner(10, provider, eventLog, applier, store)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	waitForDynamicTailPositionCount(t, eventLog, 1)
	eventLog.send(1, testRecord("event-1", 1, 0))
	waitForAppliedEvents(t, applier, 1)
	waitForCheckpointOffset(t, store, 10, 1, 1)

	cancel()
	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error mismatch: got %v, want %v", err, context.Canceled)
	}
}

func TestDynamicRunnerRunAddsShardAfterProviderChanges(t *testing.T) {
	provider := newFakeShardProvider(nil)
	eventLog := newDynamicTailFakeEventLog()
	applier := &fakeApplier{}
	runner := NewDynamicRunner(10, provider, eventLog, applier, nil)
	runner.refreshInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	provider.setOwnerships([]clusterownership.Ownership{
		{ShardID: 2, NodeID: 10, Epoch: 1},
	})

	waitForActiveWorker(t, runner, 2)
	waitForDynamicTailPositionCount(t, eventLog, 1)
	eventLog.send(2, testRecord("event-2", 2, 0))
	waitForAppliedEvents(t, applier, 1)

	cancel()
	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error mismatch: got %v, want %v", err, context.Canceled)
	}
}

func TestDynamicRunnerRunRemovesShardAfterProviderChanges(t *testing.T) {
	provider := newFakeShardProvider([]clusterownership.Ownership{
		{ShardID: 1, NodeID: 10, Epoch: 1},
	})
	eventLog := newDynamicTailFakeEventLog()
	runner := NewDynamicRunner(10, provider, eventLog, &fakeApplier{}, nil)
	runner.refreshInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.Run(ctx)
	}()

	waitForActiveWorker(t, runner, 1)
	provider.setOwnerships(nil)
	waitForInactiveWorker(t, runner, 1)

	cancel()
	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error mismatch: got %v, want %v", err, context.Canceled)
	}
}

func TestDynamicRunnerRecoveredOldOwnerStopsAfterFailover(t *testing.T) {
	ownershipStore := clusterownership.NewMemoryOwnershipStore()
	if err := ownershipStore.Assign(1, 2); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	eventLog := newDynamicTailFakeEventLog()
	oldOwner := NewDynamicRunner(2, ownershipStore, eventLog, &fakeApplier{}, nil)
	newOwner := NewDynamicRunner(1, ownershipStore, eventLog, &fakeApplier{}, nil)
	oldOwner.refreshInterval = time.Millisecond
	newOwner.refreshInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oldErrCh := make(chan error, 1)
	newErrCh := make(chan error, 1)
	go func() {
		oldErrCh <- oldOwner.Run(ctx)
	}()
	go func() {
		newErrCh <- newOwner.Run(ctx)
	}()

	waitForActiveWorker(t, oldOwner, 1)
	if _, ok := newOwner.activeWorker(1); ok {
		t.Fatal("expected node 1 to be inactive before failover")
	}

	if err := ownershipStore.Assign(1, 1); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	waitForInactiveWorker(t, oldOwner, 1)
	waitForActiveWorker(t, newOwner, 1)

	cancel()
	oldErr := <-oldErrCh
	if !errors.Is(oldErr, context.Canceled) {
		t.Fatalf("old owner Run error mismatch: got %v, want %v", oldErr, context.Canceled)
	}
	newErr := <-newErrCh
	if !errors.Is(newErr, context.Canceled) {
		t.Fatalf("new owner Run error mismatch: got %v, want %v", newErr, context.Canceled)
	}
}

func TestDynamicRunnerFenceStopsOldOwnerBeforeApply(t *testing.T) {
	ownershipStore := clusterownership.NewMemoryOwnershipStore()
	if err := ownershipStore.Assign(1, 2); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	eventLog := newDynamicTailFakeEventLog()
	applier := &fakeApplier{}
	store := checkpoint.NewMemoryStore()
	oldOwner := NewDynamicRunner(2, ownershipStore, eventLog, applier, store)
	oldOwner.refreshInterval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- oldOwner.Run(ctx)
	}()

	waitForActiveWorker(t, oldOwner, 1)

	if err := ownershipStore.Assign(1, 1); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}
	eventLog.send(1, testRecord("event-1", 1, 0))

	waitForInactiveWorker(t, oldOwner, 1)
	if appliedEventCount(applier) != 0 {
		t.Fatalf("applied event count mismatch: got %d, want 0", appliedEventCount(applier))
	}

	loaded, found, err := store.LoadCheckpoint(context.Background(), 2)
	if err != nil {
		t.Fatalf("LoadCheckpoint returned error: %v", err)
	}
	if found && loaded.Offset[1] != 0 {
		t.Fatalf("checkpoint should not advance after fence loss: got %+v", loaded)
	}

	cancel()
	err = <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error mismatch: got %v, want %v", err, context.Canceled)
	}
}

func TestDynamicRunnerFencePreventsCheckpointAfterApplyIfEpochChanges(t *testing.T) {
	ownershipStore := clusterownership.NewMemoryOwnershipStore()
	if err := ownershipStore.Assign(1, 2); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	eventLog := newDynamicTailFakeEventLog()
	applier := &ownershipChangingApplier{
		ownershipStore: ownershipStore,
		shardID:        1,
		newNodeID:      1,
	}
	store := checkpoint.NewMemoryStore()
	oldOwner := NewDynamicRunner(2, ownershipStore, eventLog, applier, store)
	oldOwner.refreshInterval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- oldOwner.Run(ctx)
	}()

	waitForActiveWorker(t, oldOwner, 1)
	eventLog.send(1, testRecord("event-1", 1, 0))

	waitForInactiveWorker(t, oldOwner, 1)
	if applier.count() != 1 {
		t.Fatalf("applied event count mismatch: got %d, want 1", applier.count())
	}

	loaded, found, err := store.LoadCheckpoint(context.Background(), 2)
	if err != nil {
		t.Fatalf("LoadCheckpoint returned error: %v", err)
	}
	if found && loaded.Offset[1] != 0 {
		t.Fatalf("checkpoint should not advance after epoch changes: got %+v", loaded)
	}

	cancel()
	err = <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error mismatch: got %v, want %v", err, context.Canceled)
	}
}

type readOnlyEventLog struct{}

func (r *readOnlyEventLog) Append(ctx context.Context, event model.Event) (eventlog.Position, error) {
	return eventlog.Position{}, nil
}

func (r *readOnlyEventLog) ReadFrom(ctx context.Context, position eventlog.Position) (<-chan eventlog.Record, error) {
	recordCh := make(chan eventlog.Record)
	close(recordCh)
	return recordCh, nil
}

type fakeEventLog struct {
	recordsByShard map[int][]eventlog.Record
	readPositions  []eventlog.Position
	err            error
}

type dynamicTailFakeEventLog struct {
	mu        sync.Mutex
	positions []eventlog.Position
	streams   map[int]chan eventlog.Record
}

type tailFakeEventLog struct {
	readyCh  chan struct{}
	recordCh chan eventlog.Record
}

func newTailFakeEventLog() *tailFakeEventLog {
	return &tailFakeEventLog{
		readyCh:  make(chan struct{}),
		recordCh: make(chan eventlog.Record),
	}
}

func newDynamicTailFakeEventLog() *dynamicTailFakeEventLog {
	return &dynamicTailFakeEventLog{
		streams: make(map[int]chan eventlog.Record),
	}
}

func (f *tailFakeEventLog) Append(ctx context.Context, event model.Event) (eventlog.Position, error) {
	return eventlog.Position{}, nil
}

func (f *tailFakeEventLog) ReadFrom(ctx context.Context, position eventlog.Position) (<-chan eventlog.Record, error) {
	recordCh := make(chan eventlog.Record)
	close(recordCh)
	return recordCh, nil
}

func (f *tailFakeEventLog) TailFrom(ctx context.Context, position eventlog.Position) (<-chan eventlog.Record, error) {
	close(f.readyCh)
	return f.recordCh, nil
}

func (f *tailFakeEventLog) send(record eventlog.Record) {
	f.recordCh <- record
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

func (f *dynamicTailFakeEventLog) Append(ctx context.Context, event model.Event) (eventlog.Position, error) {
	return eventlog.Position{}, nil
}

func (f *dynamicTailFakeEventLog) ReadFrom(ctx context.Context, position eventlog.Position) (<-chan eventlog.Record, error) {
	recordCh := make(chan eventlog.Record)
	close(recordCh)
	return recordCh, nil
}

func (f *dynamicTailFakeEventLog) TailFrom(ctx context.Context, position eventlog.Position) (<-chan eventlog.Record, error) {
	recordCh := make(chan eventlog.Record)

	f.mu.Lock()
	f.positions = append(f.positions, position)
	f.streams[position.ShardID] = recordCh
	f.mu.Unlock()

	return recordCh, nil
}

func (f *dynamicTailFakeEventLog) tailPositions() []eventlog.Position {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]eventlog.Position(nil), f.positions...)
}

func (f *dynamicTailFakeEventLog) send(shardID int, record eventlog.Record) {
	f.mu.Lock()
	recordCh := f.streams[shardID]
	f.mu.Unlock()

	recordCh <- record
}

type fakeShardProvider struct {
	mu         sync.Mutex
	ownerships []clusterownership.Ownership
	err        error
}

func newFakeShardProvider(ownerships []clusterownership.Ownership) *fakeShardProvider {
	return &fakeShardProvider{
		ownerships: append([]clusterownership.Ownership(nil), ownerships...),
	}
}

func (f *fakeShardProvider) ShardsForNode(nodeID int) ([]clusterownership.Ownership, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}

	return append([]clusterownership.Ownership(nil), f.ownerships...), nil
}

func (f *fakeShardProvider) setOwnerships(ownerships []clusterownership.Ownership) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.ownerships = append([]clusterownership.Ownership(nil), ownerships...)
}

func (n *Node) activeWorker(shardID int) (shardWorker, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	worker, ok := n.active[shardID]
	if !ok {
		return shardWorker{}, false
	}

	return *worker, true
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

type ownershipChangingApplier struct {
	mu             sync.Mutex
	events         []model.Event
	ownershipStore clusterownership.OwnershipStore
	shardID        int
	newNodeID      int
	err            error
}

func (a *ownershipChangingApplier) Apply(ctx context.Context, event model.Event) error {
	a.mu.Lock()
	a.events = append(a.events, event)
	a.mu.Unlock()

	if a.err != nil {
		return a.err
	}

	return a.ownershipStore.Assign(a.shardID, a.newNodeID)
}

func (a *ownershipChangingApplier) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()

	return len(a.events)
}

func appliedEventCount(applier *fakeApplier) int {
	applier.mu.Lock()
	defer applier.mu.Unlock()

	return len(applier.events)
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

func waitForTailReader(t *testing.T, eventLog *tailFakeEventLog) {
	t.Helper()

	select {
	case <-eventLog.readyCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for tail reader")
	}
}

func waitForDynamicTailPositionCount(t *testing.T, eventLog *dynamicTailFakeEventLog, count int) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		positions := eventLog.tailPositions()
		if len(positions) >= count {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("tail position count mismatch: got %d, want at least %d", len(positions), count)
		case <-ticker.C:
		}
	}
}

func waitForActiveWorker(t *testing.T, runner *Node, shardID int) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		if _, ok := runner.activeWorker(shardID); ok {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for shard %d to become active", shardID)
		case <-ticker.C:
		}
	}
}

func waitForInactiveWorker(t *testing.T, runner *Node, shardID int) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		if _, ok := runner.activeWorker(shardID); !ok {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for shard %d to become inactive", shardID)
		case <-ticker.C:
		}
	}
}
