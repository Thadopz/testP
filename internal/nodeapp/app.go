package nodeapp

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"runtime"
	"testP/internal/applier"
	"testP/internal/checkpoint"
	clusterownership "testP/internal/cluster/ownership"
	"testP/internal/engine"
	"testP/internal/eventlog"
	"testP/internal/metrics"
	"testP/internal/model"
	"testP/internal/node"
	"testP/internal/orderstate"
	"time"
)

const (
	defaultAreaSize   = 100000
	defaultShardCount = 64
	defaultBufferSize = 128
	defaultLoadWeight = 10000
)

type Config struct {
	NodeID          int
	ShardProvider   clusterownership.ShardProvider
	DataDir         string
	EventLog        eventlog.EventLog
	CheckpointStore checkpoint.ShardStore
	OrderStateStore orderstate.Store
	MetricsInterval time.Duration
	MetricsSink     func(Result, error)
	MetricsRecorder metrics.Recorder
	Riders          int
	Workers         int
	Seed            int64
	RefreshInterval time.Duration
}

type Result struct {
	NodeID        int
	ShardIDs      []int
	EventLogDir   string
	CheckpointDir string
	OrderStateDir string
	Submitted     int64
	Matched       int64
	Missed        int64
	OnlineRiders  int
	ShardMetrics  []ShardMetric
}

type ShardMetric struct {
	ShardID          int
	NodeID           int
	Epoch            int64
	CheckpointOffset int64
	EventLogOffset   int64
	Lag              int64
}

func Run(ctx context.Context, cfg Config) error {
	_, err := RunWithResult(ctx, cfg)
	return err
}

func RunWithResult(ctx context.Context, cfg Config) (Result, error) {
	cfg = withDefaults(cfg)
	if err := validateConfig(cfg); err != nil {
		return Result{}, err
	}

	runtime.GOMAXPROCS(cfg.Workers)

	eventLogDir := filepath.Join(cfg.DataDir, "events")
	checkpointDir := filepath.Join(cfg.DataDir, "checkpoints")
	orderStateDir := filepath.Join(cfg.DataDir, "orders")

	codec := &eventlog.JSONEventCodec{}
	activeEventLog := cfg.EventLog
	if activeEventLog == nil {
		activeEventLog = eventlog.NewFileEventLog(eventLogDir, codec)
	}

	cellSize := autoCellSize(defaultAreaSize, cfg.Riders)
	riders := generateRiders(rand.New(rand.NewSource(cfg.Seed)), cfg.Riders, defaultAreaSize)
	matchingEngine := engine.NewShardedEngine(
		riders,
		defaultShardCount,
		defaultBufferSize,
		cellSize,
		defaultAreaSize,
		defaultLoadWeight,
	)
	matchingEngine.SetResultSink(newMatchResultEventSink(activeEventLog, codec))

	matchingEngine.Start(cfg.Workers)
	defer func() {
		matchingEngine.Close()
		matchingEngine.Wait()
	}()

	orderStateStore := cfg.OrderStateStore
	if orderStateStore == nil {
		orderStateStore = orderstate.NewFileStore(orderStateDir)
	}

	eventApplier := applier.NewEventApplierWithOrderStore(codec, matchingEngine, orderStateStore)
	if reader, ok := cfg.ShardProvider.(applier.OwnershipReader); ok {
		eventApplier = applier.NewFencedEventApplierWithOrderStore(codec, matchingEngine, cfg.NodeID, reader, orderStateStore)
	}

	runner := node.NewRunner(cfg.NodeID, cfg.ShardProvider, activeEventLog, eventApplier, cfg.CheckpointStore)
	runner.SetRefreshInterval(cfg.RefreshInterval)
	runner.SetMetricsRecorder(cfg.MetricsRecorder)

	if cfg.MetricsSink != nil && cfg.MetricsInterval > 0 {
		go reportMetrics(ctx, cfg, activeEventLog, matchingEngine, eventLogDir, checkpointDir, orderStateDir)
	}

	if err := runner.Run(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			return Result{}, err
		}
	}

	return collectResult(context.Background(), cfg, activeEventLog, matchingEngine, eventLogDir, checkpointDir, orderStateDir)
}

func reportMetrics(
	ctx context.Context,
	cfg Config,
	activeEventLog eventlog.EventLog,
	matchingEngine *engine.ShardedEngine,
	eventLogDir string,
	checkpointDir string,
	orderStateDir string,
) {
	emit := func() {
		result, err := collectResult(ctx, cfg, activeEventLog, matchingEngine, eventLogDir, checkpointDir, orderStateDir)
		cfg.MetricsSink(result, err)
	}

	emit()

	ticker := time.NewTicker(cfg.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			emit()
		}
	}
}

func collectResult(
	ctx context.Context,
	cfg Config,
	activeEventLog eventlog.EventLog,
	matchingEngine *engine.ShardedEngine,
	eventLogDir string,
	checkpointDir string,
	orderStateDir string,
) (Result, error) {
	ownerships, err := cfg.ShardProvider.ShardsForNode(cfg.NodeID)
	if err != nil {
		return Result{}, err
	}
	shardIDs := ownershipsToShardIDs(ownerships)

	offsetReader, ok := activeEventLog.(eventlog.OffsetReader)
	if !ok {
		return Result{}, fmt.Errorf("eventlog does not support end offset metrics")
	}
	shardMetrics, err := buildShardMetrics(ctx, cfg.NodeID, shardIDs, offsetReader, cfg.CheckpointStore, cfg.ShardProvider)
	if err != nil {
		return Result{}, err
	}

	result := Result{
		NodeID:        cfg.NodeID,
		ShardIDs:      shardIDs,
		EventLogDir:   eventLogDir,
		CheckpointDir: checkpointDir,
		OrderStateDir: orderStateDir,
		Submitted:     matchingEngine.Submitted(),
		Matched:       matchingEngine.Matched(),
		Missed:        matchingEngine.Missed(),
		OnlineRiders:  matchingEngine.OnlineRiders(),
		ShardMetrics:  shardMetrics,
	}
	recordResultMetrics(cfg.MetricsRecorder, result)
	return result, nil
}

func recordResultMetrics(recorder metrics.Recorder, result Result) {
	if recorder == nil {
		return
	}

	recorder.SetNodeOwnedShards(result.NodeID, len(result.ShardIDs))
	recorder.SetNodeSubmitted(result.NodeID, result.Submitted)
	recorder.SetNodeMatched(result.NodeID, result.Matched)
	recorder.SetNodeMissed(result.NodeID, result.Missed)
	recorder.SetNodeOnlineRiders(result.NodeID, result.OnlineRiders)

	for _, metric := range result.ShardMetrics {
		recorder.SetShardCheckpointOffset(result.NodeID, metric.ShardID, metric.CheckpointOffset)
		recorder.SetShardEventLogOffset(result.NodeID, metric.ShardID, metric.EventLogOffset)
		recorder.SetShardLag(result.NodeID, metric.ShardID, metric.Lag)
		recorder.SetShardEpoch(result.NodeID, metric.ShardID, metric.Epoch)
	}
}

func withDefaults(cfg Config) Config {
	if cfg.NodeID == 0 {
		cfg.NodeID = 1
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	if cfg.Riders <= 0 {
		cfg.Riders = 100
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 2
	}
	if cfg.Seed == 0 {
		cfg.Seed = 1
	}
	return cfg
}

func validateConfig(cfg Config) error {
	if cfg.ShardProvider == nil {
		return fmt.Errorf("shard provider is required")
	}
	if cfg.CheckpointStore == nil {
		return fmt.Errorf("checkpoint store is required")
	}
	return nil
}

func ownershipsToShardIDs(ownerships []clusterownership.Ownership) []int {
	shardIDs := make([]int, 0, len(ownerships))
	for _, ownership := range ownerships {
		shardIDs = append(shardIDs, ownership.ShardID)
	}
	return shardIDs
}

func buildShardMetrics(
	ctx context.Context,
	nodeID int,
	shardIDs []int,
	offsetReader eventlog.OffsetReader,
	checkpointStore checkpoint.ShardStore,
	shardProvider clusterownership.ShardProvider,
) ([]ShardMetric, error) {
	epochs := make(map[int]int64)
	if shardProvider != nil {
		ownerships, err := shardProvider.ShardsForNode(nodeID)
		if err != nil {
			return nil, err
		}
		for _, currentOwnership := range ownerships {
			epochs[currentOwnership.ShardID] = currentOwnership.Epoch
		}
	}

	metrics := make([]ShardMetric, 0, len(shardIDs))
	for _, shardID := range shardIDs {
		checkpointOffset := int64(0)
		if checkpointStore != nil {
			loadedCheckpoint, found, err := checkpointStore.LoadShardCheckpoint(ctx, shardID)
			if err != nil {
				return nil, err
			}
			if found {
				checkpointOffset = loadedCheckpoint.Offset
			}
		}

		eventLogOffset, err := offsetReader.EndOffset(ctx, shardID)
		if err != nil {
			return nil, err
		}
		lag := eventLogOffset - checkpointOffset
		if lag < 0 {
			lag = 0
		}

		metrics = append(metrics, ShardMetric{
			ShardID:          shardID,
			NodeID:           nodeID,
			Epoch:            epochs[shardID],
			CheckpointOffset: checkpointOffset,
			EventLogOffset:   eventLogOffset,
			Lag:              lag,
		})
	}

	return metrics, nil
}

type matchResultEventSink struct {
	eventLog eventlog.Appender
	codec    eventlog.EventCodec
}

func newMatchResultEventSink(eventLog eventlog.Appender, codec eventlog.EventCodec) *matchResultEventSink {
	return &matchResultEventSink{
		eventLog: eventLog,
		codec:    codec,
	}
}

func (s *matchResultEventSink) SaveMatchResult(result engine.MatchResult) {
	if s == nil || s.eventLog == nil || s.codec == nil {
		return
	}

	event, err := s.buildEvent(result)
	if err != nil {
		return
	}

	_, _ = s.eventLog.Append(context.Background(), event)
}

func (s *matchResultEventSink) buildEvent(result engine.MatchResult) (model.Event, error) {
	if result.Matched {
		return s.buildMatchedEvent(result)
	}
	return s.buildMissedEvent(result)
}

func (s *matchResultEventSink) buildMatchedEvent(result engine.MatchResult) (model.Event, error) {
	payload, err := s.codec.EncodePayload(model.OrderMatched{
		OrderID: result.OrderID,
		RiderID: result.RiderID,
		Score:   result.Score,
	})
	if err != nil {
		return model.Event{}, err
	}

	return model.Event{
		ID:            fmt.Sprintf("order-%d-matched", result.OrderID),
		Type:          model.EventOrderMatched,
		AggregateType: "order",
		AggregateID:   fmt.Sprintf("%d", result.OrderID),
		ShardID:       result.ShardID,
		OccurredAt:    time.Now().Unix(),
		Payload:       payload,
	}, nil
}

func (s *matchResultEventSink) buildMissedEvent(result engine.MatchResult) (model.Event, error) {
	payload, err := s.codec.EncodePayload(model.OrderMissed{
		OrderID: result.OrderID,
		Reason:  "no_rider_found",
	})
	if err != nil {
		return model.Event{}, err
	}

	return model.Event{
		ID:            fmt.Sprintf("order-%d-missed", result.OrderID),
		Type:          model.EventOrderMissed,
		AggregateType: "order",
		AggregateID:   fmt.Sprintf("%d", result.OrderID),
		ShardID:       result.ShardID,
		OccurredAt:    time.Now().Unix(),
		Payload:       payload,
	}, nil
}

func generateRiders(rng *rand.Rand, count int, areaSize int) []*model.Rider {
	riders := make([]*model.Rider, 0, count)

	for i := 0; i < count; i++ {
		riders = append(riders, &model.Rider{
			UID: int64(i + 1),
			X:   rng.Intn(areaSize),
			Y:   rng.Intn(areaSize),
		})
	}

	return riders
}

func autoCellSize(areaSize int, riderCount int) int {
	if riderCount <= 0 {
		return areaSize
	}

	const targetRidersPerCell = 20.0
	cell := float64(areaSize) / math.Sqrt(float64(riderCount)/targetRidersPerCell)
	if cell < 1 {
		return 1
	}
	if cell > float64(areaSize) {
		return areaSize
	}

	return int(cell)
}
