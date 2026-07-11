package producerapp

import (
	"context"
	"fmt"
	"testP/internal/eventlog"
	"testP/internal/model"
	"testing"
)

func TestRunWritesOrderCreatedEvents(t *testing.T) {
	dataDir := t.TempDir()
	fileEventLog := eventlog.NewFileEventLog(dataDir+"/events", &eventlog.JSONEventCodec{})
	metricsRecorder := newProducerTestMetricsRecorder()

	result, err := Run(context.Background(), Config{
		DataDir:  dataDir,
		EventLog: fileEventLog,
		Orders:   3,
		Seed:     1,
		StartID:  10,
		AreaSize: 1000,
		CellSize: 100,
		Shards:   4,
		Metrics:  metricsRecorder,
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
	if metricsRecorder.totalProducerEvents() != 3 {
		t.Fatalf("producer event metric mismatch: got %d, want 3", metricsRecorder.totalProducerEvents())
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

func TestRunUsesBatchAppender(t *testing.T) {
	eventLog := &recordingBatchEventLog{}

	_, err := Run(context.Background(), Config{
		DataDir:   t.TempDir(),
		EventLog:  eventLog,
		Orders:    5,
		BatchSize: 2,
		Seed:      1,
		StartID:   10,
		AreaSize:  1000,
		CellSize:  100,
		Shards:    4,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if eventLog.appendCalls != 0 {
		t.Fatalf("append calls = %d, want 0", eventLog.appendCalls)
	}
	if eventLog.batchCalls != 3 {
		t.Fatalf("batch calls = %d, want 3", eventLog.batchCalls)
	}
	if len(eventLog.events) != 5 {
		t.Fatalf("event count = %d, want 5", len(eventLog.events))
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

type recordingBatchEventLog struct {
	events      []model.Event
	appendCalls int
	batchCalls  int
}

func (l *recordingBatchEventLog) Append(ctx context.Context, event model.Event) (eventlog.Position, error) {
	l.appendCalls++
	position := eventlog.Position{
		ShardID: event.ShardID,
		Offset:  int64(len(l.events)),
	}
	l.events = append(l.events, event)
	return position, nil
}

func (l *recordingBatchEventLog) AppendBatch(ctx context.Context, events []model.Event) ([]eventlog.Position, error) {
	l.batchCalls++
	positions := make([]eventlog.Position, 0, len(events))
	for _, event := range events {
		position := eventlog.Position{
			ShardID: event.ShardID,
			Offset:  int64(len(l.events)),
		}
		l.events = append(l.events, event)
		positions = append(positions, position)
	}
	return positions, nil
}

type producerTestMetricsRecorder struct {
	producerEvents map[[2]string]int
	producerErrors map[string]int
}

func newProducerTestMetricsRecorder() *producerTestMetricsRecorder {
	return &producerTestMetricsRecorder{
		producerEvents: make(map[[2]string]int),
		producerErrors: make(map[string]int),
	}
}

func (r *producerTestMetricsRecorder) SetNodeOwnedShards(nodeID int, count int) {}

func (r *producerTestMetricsRecorder) SetNodeSubmitted(nodeID int, value int64) {}

func (r *producerTestMetricsRecorder) SetNodeMatched(nodeID int, value int64) {}

func (r *producerTestMetricsRecorder) SetNodeMissed(nodeID int, value int64) {}

func (r *producerTestMetricsRecorder) SetNodeOnlineRiders(nodeID int, value int) {}

func (r *producerTestMetricsRecorder) SetShardCheckpointOffset(nodeID int, shardID int, offset int64) {
}

func (r *producerTestMetricsRecorder) SetShardEventLogOffset(nodeID int, shardID int, offset int64) {}

func (r *producerTestMetricsRecorder) SetShardLag(nodeID int, shardID int, lag int64) {}

func (r *producerTestMetricsRecorder) SetShardEpoch(nodeID int, shardID int, epoch int64) {}

func (r *producerTestMetricsRecorder) IncEventApply(nodeID int, shardID int, eventType string) {}

func (r *producerTestMetricsRecorder) IncEventApplyError(nodeID int, shardID int, eventType string) {}

func (r *producerTestMetricsRecorder) IncFencingReject(nodeID int, shardID int) {}

func (r *producerTestMetricsRecorder) SetControllerLeader(controllerID string, leader bool) {}

func (r *producerTestMetricsRecorder) IncControllerSweep(controllerID string) {}

func (r *producerTestMetricsRecorder) IncControllerSweepError(controllerID string, reason string) {}

func (r *producerTestMetricsRecorder) IncFailover(controllerID string, deadNodeID int) {}

func (r *producerTestMetricsRecorder) SetAliveNodes(controllerID string, count int) {}

func (r *producerTestMetricsRecorder) SetOwnedShards(controllerID string, count int) {}

func (r *producerTestMetricsRecorder) SetShardsWithoutOwner(controllerID string, count int) {}

func (r *producerTestMetricsRecorder) IncProducerEvent(eventType string, shardID int) {
	r.producerEvents[[2]string{eventType, fmt.Sprintf("%d", shardID)}]++
}

func (r *producerTestMetricsRecorder) IncProducerError(reason string) {
	r.producerErrors[reason]++
}

func (r *producerTestMetricsRecorder) totalProducerEvents() int {
	total := 0
	for _, count := range r.producerEvents {
		total += count
	}
	return total
}
