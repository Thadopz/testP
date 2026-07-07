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
	"testP/internal/engine"
	"testP/internal/eventlog"
	"testP/internal/model"
	"testP/internal/node"
)

const (
	defaultAreaSize   = 100000
	defaultShardCount = 64
	defaultBufferSize = 128
	defaultLoadWeight = 10000
)

type Config struct {
	NodeID   int
	ShardIDs []int
	DataDir  string
	Riders   int
	Workers  int
	Seed     int64
	Tail     bool
}

type Result struct {
	NodeID        int
	ShardIDs      []int
	EventLogDir   string
	CheckpointDir string
	Submitted     int64
	Matched       int64
	Missed        int64
	OnlineRiders  int
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
	eventApplier := applier.NewEventApplier(codec, matchingEngine)
	runner := node.NewRunner(cfg.NodeID, cfg.ShardIDs, fileEventLog, eventApplier, checkpointStore)
	runner.SetTail(cfg.Tail)

	if err := runner.Run(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			return Result{}, err
		}
	}

	result := Result{
		NodeID:        cfg.NodeID,
		ShardIDs:      append([]int(nil), cfg.ShardIDs...),
		EventLogDir:   eventLogDir,
		CheckpointDir: checkpointDir,
		Submitted:     matchingEngine.Submitted(),
		Matched:       matchingEngine.Matched(),
		Missed:        matchingEngine.Missed(),
		OnlineRiders:  matchingEngine.OnlineRiders(),
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
