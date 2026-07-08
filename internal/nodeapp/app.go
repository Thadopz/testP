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
	ShardIDs        []int
	ShardProvider   clusterownership.ShardProvider
	DataDir         string
	Riders          int
	Workers         int
	Seed            int64
	Tail            bool
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

	matchingEngine.Start(cfg.Workers)
	defer func() {
		matchingEngine.Close()
		matchingEngine.Wait()
	}()

	codec := &eventlog.JSONEventCodec{}
	fileEventLog := eventlog.NewFileEventLog(eventLogDir, codec)
	checkpointStore := checkpoint.NewFileStore(checkpointDir)
	orderStateStore := orderstate.NewFileStore(orderStateDir)
	eventApplier := applier.NewEventApplierWithOrderStore(codec, matchingEngine, orderStateStore)
	var runner *node.Node
	if cfg.ShardProvider == nil {
		runner = node.NewRunner(cfg.NodeID, cfg.ShardIDs, fileEventLog, eventApplier, checkpointStore)
	} else {
		if reader, ok := cfg.ShardProvider.(applier.OwnershipReader); ok {
			eventApplier = applier.NewFencedEventApplierWithOrderStore(codec, matchingEngine, cfg.NodeID, reader, orderStateStore)
		}
		runner = node.NewDynamicRunner(cfg.NodeID, cfg.ShardProvider, fileEventLog, eventApplier, checkpointStore)
		runner.SetRefreshInterval(cfg.RefreshInterval)
	}
	runner.SetTail(cfg.Tail)

	if err := runner.Run(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			return Result{}, err
		}
	}

	shardIDs := append([]int(nil), cfg.ShardIDs...)
	if cfg.ShardProvider != nil {
		ownerships, err := cfg.ShardProvider.ShardsForNode(cfg.NodeID)
		if err != nil {
			return Result{}, err
		}
		shardIDs = ownershipsToShardIDs(ownerships)
	}

	shardMetrics, err := buildShardMetrics(context.Background(), cfg.NodeID, shardIDs, fileEventLog, checkpointStore, cfg.ShardProvider)
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

	return result, nil
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
	if cfg.ShardProvider != nil && !cfg.Tail {
		return fmt.Errorf("dynamic shard provider requires tail mode")
	}
	if cfg.ShardProvider != nil {
		return nil
	}
	if len(cfg.ShardIDs) == 0 {
		return fmt.Errorf("at least one shard id is required")
	}
	for _, shardID := range cfg.ShardIDs {
		if shardID < 0 || shardID >= defaultShardCount {
			return fmt.Errorf("shard id %d out of range [0,%d)", shardID, defaultShardCount)
		}
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
	checkpointStore checkpoint.Store,
	shardProvider clusterownership.ShardProvider,
) ([]ShardMetric, error) {
	loadedCheckpoint, found, err := checkpointStore.LoadCheckpoint(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	checkpointOffsets := map[int]int64{}
	if found {
		checkpointOffsets = loadedCheckpoint.Offset
	}

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
		checkpointOffset := checkpointOffsets[shardID]
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
