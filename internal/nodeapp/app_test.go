package nodeapp

import (
	"context"
	"testP/internal/checkpoint"
	clusterownership "testP/internal/cluster/ownership"
	"testP/internal/eventlog"
	"testP/internal/model"
	"testP/internal/orderstate"
	"testing"
	"time"
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
	assertShardMetric(t, firstResult.ShardMetrics, 1, 0, 2, 2, 0)

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
	assertShardMetric(t, secondResult.ShardMetrics, 1, 0, 3, 3, 0)
}

func TestRunWithResultTailProcessesEventAppendedAfterStart(t *testing.T) {
	dataDir := t.TempDir()
	codec := &eventlog.JSONEventCodec{}
	fileEventLog := eventlog.NewFileEventLog(dataDir+"/events", codec)
	checkpointStore := checkpoint.NewFileStore(dataDir + "/checkpoints")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultCh := make(chan Result, 1)
	errCh := make(chan error, 1)

	go func() {
		result, err := RunWithResult(ctx, Config{
			NodeID:   10,
			ShardIDs: []int{1},
			DataDir:  dataDir,
			Riders:   20,
			Workers:  1,
			Seed:     1,
			Tail:     true,
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
	assertShardMetric(t, result.ShardMetrics, 1, 0, 1, 1, 0)
}

func TestRunWithResultDynamicProviderProcessesShardAssignedAfterStart(t *testing.T) {
	dataDir := t.TempDir()
	codec := &eventlog.JSONEventCodec{}
	fileEventLog := eventlog.NewFileEventLog(dataDir+"/events", codec)
	checkpointStore := checkpoint.NewFileStore(dataDir + "/checkpoints")
	ownershipStore := clusterownership.NewMemoryOwnershipStore()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultCh := make(chan Result, 1)
	errCh := make(chan error, 1)

	go func() {
		result, err := RunWithResult(ctx, Config{
			NodeID:          10,
			ShardProvider:   ownershipStore,
			DataDir:         dataDir,
			Riders:          20,
			Workers:         1,
			Seed:            1,
			Tail:            true,
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

func TestRunWithResultDynamicProviderRequiresTail(t *testing.T) {
	_, err := RunWithResult(context.Background(), Config{
		NodeID:        10,
		ShardProvider: clusterownership.NewMemoryOwnershipStore(),
		DataDir:       t.TempDir(),
		Riders:        20,
		Workers:       1,
		Seed:          1,
	})
	if err == nil {
		t.Fatal("expected RunWithResult to return an error")
	}
}

func TestBuildShardMetricsReportsLag(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	fileEventLog := eventlog.NewFileEventLog(t.TempDir(), codec)
	checkpointStore := checkpoint.NewMemoryStore()

	appendOrderCreatedEvent(t, fileEventLog, codec, "event-1", 1, 1)
	appendOrderCreatedEvent(t, fileEventLog, codec, "event-2", 1, 2)
	appendOrderCreatedEvent(t, fileEventLog, codec, "event-3", 1, 3)
	if err := checkpointStore.SaveCheckpoint(context.Background(), checkpoint.Checkpoint{
		NodeID: 10,
		Offset: map[int]int64{
			1: 1,
		},
	}); err != nil {
		t.Fatalf("SaveCheckpoint returned error: %v", err)
	}

	metrics, err := buildShardMetrics(context.Background(), 10, []int{1}, fileEventLog, checkpointStore, nil)
	if err != nil {
		t.Fatalf("buildShardMetrics returned error: %v", err)
	}

	assertShardMetric(t, metrics, 1, 0, 1, 3, 2)
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

func waitForNodeappCheckpointOffset(t *testing.T, store checkpoint.Store, nodeID int, shardID int, expectedOffset int64) {
	t.Helper()

	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
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
