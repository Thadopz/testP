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

func TestDynamicRunnerRefreshOnceStartsOwnedShard(t *testing.T) {
	provider := newFakeShardProvider([]clusterownership.Ownership{
		{ShardID: 1, NodeID: 10, Epoch: 1},
	})
	eventLog := newDynamicTailFakeEventLog()
	runner := NewRunner(10, provider, eventLog, &fakeApplier{}, nil)
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
	runner := NewRunner(10, provider, eventLog, &fakeApplier{}, nil)
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
	runner := NewRunner(10, provider, eventLog, &fakeApplier{}, nil)

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
	runner := NewRunner(10, provider, eventLog, &fakeApplier{}, nil)
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
	runner := NewRunner(10, provider, newDynamicTailFakeEventLog(), &fakeApplier{}, nil)

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
	runner := NewRunner(10, provider, eventLog, applier, store)

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
	runner := NewRunner(10, provider, eventLog, applier, nil)
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
	runner := NewRunner(10, provider, eventLog, &fakeApplier{}, nil)
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
	ownershipStore := newFakeShardProvider(nil)
	if err := ownershipStore.Assign(1, 2); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	eventLog := newDynamicTailFakeEventLog()
	oldOwner := NewRunner(2, ownershipStore, eventLog, &fakeApplier{}, nil)
	newOwner := NewRunner(1, ownershipStore, eventLog, &fakeApplier{}, nil)
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

func TestDynamicRunnerFailoverUsesSharedShardCheckpoint(t *testing.T) {
	ownershipStore := newFakeShardProvider(nil)
	if err := ownershipStore.Assign(1, 2); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	eventLog := newDynamicTailFakeEventLog()
	checkpointStore := checkpoint.NewMemoryStore()
	oldApplier := &fakeApplier{}
	newApplier := &fakeApplier{}
	oldOwner := NewRunner(2, ownershipStore, eventLog, oldApplier, checkpointStore)
	newOwner := NewRunner(1, ownershipStore, eventLog, newApplier, checkpointStore)
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
	waitForDynamicTailPositionCount(t, eventLog, 1)
	assertTailPosition(t, eventLog, 0, eventlog.Position{ShardID: 1, Offset: 0})

	eventLog.send(1, testRecord("event-1", 1, 0))
	eventLog.send(1, testRecord("event-2", 1, 1))
	waitForAppliedEvents(t, oldApplier, 2)
	waitForCheckpointOffset(t, checkpointStore, 2, 1, 2)

	if err := ownershipStore.Assign(1, 1); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	waitForInactiveWorker(t, oldOwner, 1)
	waitForActiveWorker(t, newOwner, 1)
	waitForDynamicTailPositionCount(t, eventLog, 2)
	assertTailPosition(t, eventLog, 1, eventlog.Position{ShardID: 1, Offset: 2})

	eventLog.send(1, testRecord("event-3", 1, 2))
	waitForAppliedEvents(t, newApplier, 1)
	waitForCheckpointOffset(t, checkpointStore, 1, 1, 3)

	if appliedEventCount(oldApplier) != 2 {
		t.Fatalf("old owner applied event count mismatch: got %d, want 2", appliedEventCount(oldApplier))
	}
	if appliedEventCount(newApplier) != 1 {
		t.Fatalf("new owner applied event count mismatch: got %d, want 1", appliedEventCount(newApplier))
	}

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
	ownershipStore := newFakeShardProvider(nil)
	if err := ownershipStore.Assign(1, 2); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	eventLog := newDynamicTailFakeEventLog()
	applier := &fakeApplier{}
	store := checkpoint.NewMemoryStore()
	oldOwner := NewRunner(2, ownershipStore, eventLog, applier, store)
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

	loaded, found, err := store.LoadShardCheckpoint(context.Background(), 1)
	if err != nil {
		t.Fatalf("LoadShardCheckpoint returned error: %v", err)
	}
	if found && loaded.Offset != 0 {
		t.Fatalf("checkpoint should not advance after fence loss: got %+v", loaded)
	}

	cancel()
	err = <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error mismatch: got %v, want %v", err, context.Canceled)
	}
}

func TestDynamicRunnerFencePreventsCheckpointAfterApplyIfEpochChanges(t *testing.T) {
	ownershipStore := newFakeShardProvider(nil)
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
	oldOwner := NewRunner(2, ownershipStore, eventLog, applier, store)
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

	loaded, found, err := store.LoadShardCheckpoint(context.Background(), 1)
	if err != nil {
		t.Fatalf("LoadShardCheckpoint returned error: %v", err)
	}
	if found && loaded.Offset != 0 {
		t.Fatalf("checkpoint should not advance after epoch changes: got %+v", loaded)
	}

	cancel()
	err = <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error mismatch: got %v, want %v", err, context.Canceled)
	}
}

type dynamicTailFakeEventLog struct {
	mu        sync.Mutex
	positions []eventlog.Position
	streams   map[int]chan eventlog.Record
}

func newDynamicTailFakeEventLog() *dynamicTailFakeEventLog {
	return &dynamicTailFakeEventLog{
		streams: make(map[int]chan eventlog.Record),
	}
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

	ownerships := make([]clusterownership.Ownership, 0)
	for _, ownership := range f.ownerships {
		if ownership.NodeID == nodeID {
			ownerships = append(ownerships, ownership)
		}
	}
	return ownerships, nil
}

func (f *fakeShardProvider) setOwnerships(ownerships []clusterownership.Ownership) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.ownerships = append([]clusterownership.Ownership(nil), ownerships...)
}

func (f *fakeShardProvider) OwnerOf(shardID int) (clusterownership.Ownership, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, ownership := range f.ownerships {
		if ownership.ShardID == shardID {
			return ownership, true, nil
		}
	}
	return clusterownership.Ownership{}, false, nil
}

func (f *fakeShardProvider) Assign(shardID int, nodeID int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for i, ownership := range f.ownerships {
		if ownership.ShardID == shardID {
			ownership.NodeID = nodeID
			ownership.Epoch++
			f.ownerships[i] = ownership
			return nil
		}
	}

	f.ownerships = append(f.ownerships, clusterownership.Ownership{
		ShardID: shardID,
		NodeID:  nodeID,
		Epoch:   1,
	})
	return nil
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

func waitForCheckpointOffset(t *testing.T, store checkpoint.ShardStore, nodeID int, shardID int, expectedOffset int64) {
	t.Helper()

	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		loaded, found, err := store.LoadShardCheckpoint(context.Background(), shardID)
		if err != nil {
			t.Fatalf("LoadShardCheckpoint returned error: %v", err)
		}

		if found && loaded.Offset == expectedOffset {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("checkpoint offset mismatch: got found=%v checkpoint=%+v, want shard %d offset %d", found, loaded, shardID, expectedOffset)
		case <-ticker.C:
		}
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

func assertTailPosition(t *testing.T, eventLog *dynamicTailFakeEventLog, index int, expected eventlog.Position) {
	t.Helper()

	positions := eventLog.tailPositions()
	if index >= len(positions) {
		t.Fatalf("tail position %d not found in %+v", index, positions)
	}
	if positions[index] != expected {
		t.Fatalf("tail position mismatch at index %d: got %+v, want %+v", index, positions[index], expected)
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
