package nodeapp

import (
	"context"
	"sort"
	"testP/internal/checkpoint"
	clusterownership "testP/internal/cluster/ownership"
	"testP/internal/eventlog"
	"testP/internal/model"
	"testP/internal/orderstate"
	"testP/internal/shard"
	"testing"
	"time"
)

func TestRunWithResultReplaysFileEventLogAndUsesCheckpoint(t *testing.T) {
	dataDir := t.TempDir()
	codec := &eventlog.JSONEventCodec{}
	fileEventLog := eventlog.NewFileEventLog(dataDir+"/events", codec)
	checkpointStore := checkpoint.NewMemoryStore()
	ownershipStore := newNodeappTestOwnershipStore()
	if err := ownershipStore.Assign(1, 10); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	appendOrderCreatedEvent(t, fileEventLog, codec, "event-1", 1, 1)
	appendOrderCreatedEvent(t, fileEventLog, codec, "event-2", 1, 2)

	cfg := Config{
		NodeID:          10,
		ShardProvider:   ownershipStore,
		DataDir:         dataDir,
		CheckpointStore: checkpointStore,
		Riders:          20,
		Workers:         1,
		Seed:            1,
		RefreshInterval: time.Millisecond,
	}

	firstCtx, firstCancel := context.WithCancel(context.Background())
	firstResultCh := make(chan Result, 1)
	firstErrCh := make(chan error, 1)
	go func() {
		result, err := RunWithResult(firstCtx, cfg)
		firstResultCh <- result
		firstErrCh <- err
	}()

	waitForNodeappCheckpointOffset(t, checkpointStore, 10, 1, 2)
	firstCancel()

	err := <-firstErrCh
	if err != nil {
		t.Fatalf("RunWithResult returned error: %v", err)
	}
	firstResult := <-firstResultCh
	if firstResult.Submitted != 2 {
		t.Fatalf("first submitted count mismatch: got %d, want %d", firstResult.Submitted, int64(2))
	}
	assertShardMetric(t, firstResult.ShardMetrics, 1, 1, 2, 2, 0)

	orderStore := orderstate.NewFileStore(dataDir + "/orders")
	state, found, err := orderStore.Load(context.Background(), 1)
	if err != nil {
		t.Fatalf("Load order state returned error: %v", err)
	}
	if !found {
		t.Fatal("expected order state to be found")
	}
	if state.Status != orderstate.StatusSubmitted {
		t.Fatalf("order status mismatch: got %q, want %q", state.Status, orderstate.StatusSubmitted)
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

	appendOrderCreatedEvent(t, fileEventLog, codec, "event-3", 1, 3)

	secondCtx, secondCancel := context.WithCancel(context.Background())
	secondResultCh := make(chan Result, 1)
	secondErrCh := make(chan error, 1)
	go func() {
		result, err := RunWithResult(secondCtx, cfg)
		secondResultCh <- result
		secondErrCh <- err
	}()

	waitForNodeappCheckpointOffset(t, checkpointStore, 10, 1, 3)
	secondCancel()

	err = <-secondErrCh
	if err != nil {
		t.Fatalf("RunWithResult returned error: %v", err)
	}
	secondResult := <-secondResultCh
	if secondResult.Submitted != 1 {
		t.Fatalf("second submitted count mismatch: got %d, want %d", secondResult.Submitted, int64(1))
	}
	assertShardMetric(t, secondResult.ShardMetrics, 1, 1, 3, 3, 0)
}

func TestRunWithResultTailProcessesEventAppendedAfterStart(t *testing.T) {
	dataDir := t.TempDir()
	codec := &eventlog.JSONEventCodec{}
	fileEventLog := eventlog.NewFileEventLog(dataDir+"/events", codec)
	checkpointStore := checkpoint.NewMemoryStore()
	ownershipStore := newNodeappTestOwnershipStore()
	if err := ownershipStore.Assign(1, 10); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultCh := make(chan Result, 1)
	errCh := make(chan error, 1)

	go func() {
		result, err := RunWithResult(ctx, Config{
			NodeID:          10,
			ShardProvider:   ownershipStore,
			DataDir:         dataDir,
			CheckpointStore: checkpointStore,
			Riders:          20,
			Workers:         1,
			Seed:            1,
			RefreshInterval: time.Millisecond,
		})
		resultCh <- result
		errCh <- err
	}()

	appendOrderCreatedEvent(t, fileEventLog, codec, "event-1", 1, 1)
	waitForNodeappCheckpointOffset(t, checkpointStore, 10, 1, 1)
	cancel()

	err := <-errCh
	if err != nil {
		t.Fatalf("RunWithResult returned error: %v", err)
	}

	result := <-resultCh
	if result.Submitted != 1 {
		t.Fatalf("submitted count mismatch: got %d, want %d", result.Submitted, int64(1))
	}
	assertShardMetric(t, result.ShardMetrics, 1, 1, 1, 1, 0)
}

func TestRunWithResultUsesInjectedShardCheckpointStore(t *testing.T) {
	dataDir := t.TempDir()
	codec := &eventlog.JSONEventCodec{}
	fileEventLog := eventlog.NewFileEventLog(dataDir+"/events", codec)
	checkpointStore := checkpoint.NewMemoryStore()
	ownershipStore := newNodeappTestOwnershipStore()
	if err := ownershipStore.Assign(1, 10); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}
	if err := checkpointStore.SaveShardCheckpoint(context.Background(), checkpoint.ShardCheckpoint{
		ShardID: 1,
		Offset:  1,
		Epoch:   1,
		NodeID:  2,
	}); err != nil {
		t.Fatalf("SaveShardCheckpoint returned error: %v", err)
	}

	appendOrderCreatedEvent(t, fileEventLog, codec, "event-1", 1, 1)
	appendOrderCreatedEvent(t, fileEventLog, codec, "event-2", 1, 2)

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan Result, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := RunWithResult(ctx, Config{
			NodeID:          10,
			ShardProvider:   ownershipStore,
			DataDir:         dataDir,
			CheckpointStore: checkpointStore,
			Riders:          20,
			Workers:         1,
			Seed:            1,
			RefreshInterval: time.Millisecond,
		})
		resultCh <- result
		errCh <- err
	}()

	waitForNodeappCheckpointOffset(t, checkpointStore, 10, 1, 2)
	cancel()

	err := <-errCh
	if err != nil {
		t.Fatalf("RunWithResult returned error: %v", err)
	}

	result := <-resultCh
	if result.Submitted != 1 {
		t.Fatalf("submitted count mismatch: got %d, want 1", result.Submitted)
	}
	assertShardMetric(t, result.ShardMetrics, 1, 1, 2, 2, 0)
}

func TestRunWithResultReportsMetricsWhileRunning(t *testing.T) {
	dataDir := t.TempDir()
	checkpointStore := checkpoint.NewMemoryStore()
	ownershipStore := newNodeappTestOwnershipStore()
	if err := ownershipStore.Assign(1, 10); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	metricCh := make(chan Result, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := RunWithResult(ctx, Config{
			NodeID:          10,
			ShardProvider:   ownershipStore,
			DataDir:         dataDir,
			CheckpointStore: checkpointStore,
			Riders:          20,
			Workers:         1,
			Seed:            1,
			RefreshInterval: time.Millisecond,
			MetricsInterval: time.Millisecond,
			MetricsSink: func(result Result, err error) {
				if err != nil {
					t.Errorf("MetricsSink received error: %v", err)
					return
				}
				select {
				case metricCh <- result:
				default:
				}
			},
		})
		errCh <- err
	}()

	select {
	case result := <-metricCh:
		if result.NodeID != 10 {
			t.Fatalf("metric node id mismatch: got %d, want 10", result.NodeID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runtime metrics")
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("RunWithResult returned error: %v", err)
	}
}

func TestRunWithResultTailConsumesMatchResultEvent(t *testing.T) {
	dataDir := t.TempDir()
	codec := &eventlog.JSONEventCodec{}
	fileEventLog := eventlog.NewFileEventLog(dataDir+"/events", codec)
	orderStore := orderstate.NewFileStore(dataDir + "/orders")
	checkpointStore := checkpoint.NewMemoryStore()
	riderCount := 1
	cellSize := autoCellSize(defaultAreaSize, riderCount)
	layout := shard.NewLayout(defaultAreaSize, cellSize, defaultShardCount)
	shardID := layout.ShardID(10, 20)
	ownershipStore := newNodeappTestOwnershipStore()
	if err := ownershipStore.Assign(shardID, 10); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultCh := make(chan Result, 1)
	errCh := make(chan error, 1)

	go func() {
		result, err := RunWithResult(ctx, Config{
			NodeID:          10,
			ShardProvider:   ownershipStore,
			DataDir:         dataDir,
			CheckpointStore: checkpointStore,
			Riders:          riderCount,
			Workers:         1,
			Seed:            1,
			RefreshInterval: time.Millisecond,
		})
		resultCh <- result
		errCh <- err
	}()

	appendOrderCreatedEventWithXY(t, fileEventLog, codec, "event-1", shardID, 1, 10, 20)
	waitForOrderFinalStatus(t, orderStore, 1)
	cancel()

	err := <-errCh
	if err != nil {
		t.Fatalf("RunWithResult returned error: %v", err)
	}

	result := <-resultCh
	if result.Matched+result.Missed != 1 {
		t.Fatalf("result count mismatch: matched=%d missed=%d, want total 1", result.Matched, result.Missed)
	}
}

func TestRunWithResultUsesInjectedOrderStateStore(t *testing.T) {
	dataDir := t.TempDir()
	codec := &eventlog.JSONEventCodec{}
	fileEventLog := eventlog.NewFileEventLog(dataDir+"/events", codec)
	orderStore := orderstate.NewMemoryStore()
	checkpointStore := checkpoint.NewMemoryStore()
	ownershipStore := newNodeappTestOwnershipStore()
	if err := ownershipStore.Assign(1, 10); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := RunWithResult(ctx, Config{
			NodeID:          10,
			ShardProvider:   ownershipStore,
			DataDir:         dataDir,
			CheckpointStore: checkpointStore,
			OrderStateStore: orderStore,
			Riders:          20,
			Workers:         1,
			Seed:            1,
			RefreshInterval: time.Millisecond,
		})
		errCh <- err
	}()

	appendOrderCreatedEvent(t, fileEventLog, codec, "event-1", 1, 1)
	waitForOrderStateStatus(t, orderStore, 1, orderstate.StatusSubmitted)
	cancel()

	if err := <-errCh; err != nil {
		t.Fatalf("RunWithResult returned error: %v", err)
	}
}

func TestRunWithResultDynamicProviderProcessesShardAssignedAfterStart(t *testing.T) {
	dataDir := t.TempDir()
	codec := &eventlog.JSONEventCodec{}
	fileEventLog := eventlog.NewFileEventLog(dataDir+"/events", codec)
	checkpointStore := checkpoint.NewMemoryStore()
	ownershipStore := newNodeappTestOwnershipStore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultCh := make(chan Result, 1)
	errCh := make(chan error, 1)

	go func() {
		result, err := RunWithResult(ctx, Config{
			NodeID:          10,
			ShardProvider:   ownershipStore,
			DataDir:         dataDir,
			CheckpointStore: checkpointStore,
			Riders:          20,
			Workers:         1,
			Seed:            1,
			RefreshInterval: time.Millisecond,
		})
		resultCh <- result
		errCh <- err
	}()

	if err := ownershipStore.Assign(1, 10); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}
	appendOrderCreatedEvent(t, fileEventLog, codec, "event-1", 1, 1)
	waitForNodeappCheckpointOffset(t, checkpointStore, 10, 1, 1)
	cancel()

	err := <-errCh
	if err != nil {
		t.Fatalf("RunWithResult returned error: %v", err)
	}

	result := <-resultCh
	if result.Submitted != 1 {
		t.Fatalf("submitted count mismatch: got %d, want %d", result.Submitted, int64(1))
	}
	if len(result.ShardIDs) != 1 || result.ShardIDs[0] != 1 {
		t.Fatalf("result shard ids mismatch: got %v, want [1]", result.ShardIDs)
	}
	assertShardMetric(t, result.ShardMetrics, 1, 1, 1, 1, 0)
}

func TestRunWithResultRequiresShardProvider(t *testing.T) {
	_, err := RunWithResult(context.Background(), Config{
		NodeID:          10,
		DataDir:         t.TempDir(),
		CheckpointStore: checkpoint.NewMemoryStore(),
		Riders:          20,
		Workers:         1,
		Seed:            1,
	})
	if err == nil {
		t.Fatal("expected RunWithResult to return an error")
	}
}

type nodeappTestOwnershipStore struct {
	owners map[int]clusterownership.Ownership
}

func newNodeappTestOwnershipStore() *nodeappTestOwnershipStore {
	return &nodeappTestOwnershipStore{
		owners: make(map[int]clusterownership.Ownership),
	}
}

func (s *nodeappTestOwnershipStore) OwnerOf(shardID int) (clusterownership.Ownership, bool, error) {
	ownership, ok := s.owners[shardID]
	return ownership, ok, nil
}

func (s *nodeappTestOwnershipStore) ShardsForNode(nodeID int) ([]clusterownership.Ownership, error) {
	ownerships := make([]clusterownership.Ownership, 0)
	for _, ownership := range s.owners {
		if ownership.NodeID == nodeID {
			ownerships = append(ownerships, ownership)
		}
	}
	sort.Slice(ownerships, func(i, j int) bool {
		return ownerships[i].ShardID < ownerships[j].ShardID
	})
	return ownerships, nil
}

func (s *nodeappTestOwnershipStore) Assign(shardID int, nodeID int) error {
	ownership := s.owners[shardID]
	ownership.ShardID = shardID
	ownership.NodeID = nodeID
	ownership.Epoch++
	if ownership.Epoch == 0 {
		ownership.Epoch = 1
	}
	s.owners[shardID] = ownership
	return nil
}

func TestBuildShardMetricsReportsLag(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	fileEventLog := eventlog.NewFileEventLog(t.TempDir(), codec)
	checkpointStore := checkpoint.NewMemoryStore()

	appendOrderCreatedEvent(t, fileEventLog, codec, "event-1", 1, 1)
	appendOrderCreatedEvent(t, fileEventLog, codec, "event-2", 1, 2)
	appendOrderCreatedEvent(t, fileEventLog, codec, "event-3", 1, 3)
	if err := checkpointStore.SaveShardCheckpoint(context.Background(), checkpoint.ShardCheckpoint{
		ShardID: 1,
		Offset:  1,
		Epoch:   1,
		NodeID:  10,
	}); err != nil {
		t.Fatalf("SaveShardCheckpoint returned error: %v", err)
	}

	metrics, err := buildShardMetrics(context.Background(), 10, []int{1}, fileEventLog, checkpointStore, nil)
	if err != nil {
		t.Fatalf("buildShardMetrics returned error: %v", err)
	}

	assertShardMetric(t, metrics, 1, 0, 1, 3, 2)
}

func appendOrderCreatedEvent(t *testing.T, eventLog eventlog.Appender, codec eventlog.EventCodec, eventID string, shardID int, orderID int64) {
	t.Helper()

	appendOrderCreatedEventWithXY(t, eventLog, codec, eventID, shardID, orderID, 10, 20)
}

func appendOrderCreatedEventWithXY(
	t *testing.T,
	eventLog eventlog.Appender,
	codec eventlog.EventCodec,
	eventID string,
	shardID int,
	orderID int64,
	x int,
	y int,
) {
	t.Helper()

	payload, err := codec.EncodePayload(model.OrderCreated{
		OrderID: orderID,
		X:       x,
		Y:       y,
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

func waitForOrderFinalStatus(t *testing.T, store orderstate.Store, orderID int64) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		state, found, err := store.Load(context.Background(), orderID)
		if err != nil {
			t.Fatalf("Load order state returned error: %v", err)
		}
		if found && (state.Status == orderstate.StatusMatched || state.Status == orderstate.StatusMissed) {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("order state did not reach final status: found=%v state=%+v", found, state)
		case <-ticker.C:
		}
	}
}

func waitForOrderStateStatus(t *testing.T, store orderstate.Store, orderID int64, expected orderstate.Status) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		state, found, err := store.Load(context.Background(), orderID)
		if err != nil {
			t.Fatalf("Load order state returned error: %v", err)
		}
		if found && state.Status == expected {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("order state status mismatch: found=%v state=%+v want=%q", found, state, expected)
		case <-ticker.C:
		}
	}
}

func assertShardMetric(
	t *testing.T,
	metrics []ShardMetric,
	shardID int,
	expectedEpoch int64,
	expectedCheckpointOffset int64,
	expectedEventLogOffset int64,
	expectedLag int64,
) {
	t.Helper()

	for _, metric := range metrics {
		if metric.ShardID != shardID {
			continue
		}
		if metric.Epoch != expectedEpoch ||
			metric.CheckpointOffset != expectedCheckpointOffset ||
			metric.EventLogOffset != expectedEventLogOffset ||
			metric.Lag != expectedLag {
			t.Fatalf("shard metric mismatch: got %+v", metric)
		}
		return
	}

	t.Fatalf("shard metric for shard %d not found in %+v", shardID, metrics)
}

func waitForNodeappCheckpointOffset(t *testing.T, store checkpoint.ShardStore, nodeID int, shardID int, expectedOffset int64) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
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
