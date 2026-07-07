package producerapp

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testP/internal/eventlog"
	"testP/internal/model"
	"testP/internal/shard"
)

const (
	defaultAreaSize   = 100000
	defaultCellSize   = 10000
	defaultShardCount = 64
)

type Config struct {
	DataDir  string
	Orders   int
	Seed     int64
	StartID  int64
	AreaSize int
	CellSize int
	Shards   int
}

type Result struct {
	EventLogDir string
	Orders      int
	FirstID     int64
	LastID      int64
}

func Run(ctx context.Context, cfg Config) (Result, error) {
	cfg = withDefaults(cfg)
	if err := validateConfig(cfg); err != nil {
		return Result{}, err
	}

	codec := &eventlog.JSONEventCodec{}
	eventLogDir := filepath.Join(cfg.DataDir, "events")
	fileEventLog := eventlog.NewFileEventLog(eventLogDir, codec)
	layout := shard.NewLayout(cfg.AreaSize, cfg.CellSize, cfg.Shards)
	rng := rand.New(rand.NewSource(cfg.Seed))

	for index := 0; index < cfg.Orders; index++ {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}

		orderID := cfg.StartID + int64(index)
		x := rng.Intn(cfg.AreaSize)
		y := rng.Intn(cfg.AreaSize)
		shardID := layout.ShardID(x, y)

		payload, err := codec.EncodePayload(model.OrderCreated{
			OrderID: orderID,
			X:       x,
			Y:       y,
		})
		if err != nil {
			return Result{}, fmt.Errorf("encode order payload: %w", err)
		}

		_, err = fileEventLog.Append(ctx, model.Event{
			ID:            fmt.Sprintf("order-%d-created", orderID),
			Type:          model.EventOrderCreated,
			AggregateType: "order",
			AggregateID:   fmt.Sprintf("%d", orderID),
			ShardID:       shardID,
			OccurredAt:    int64(index + 1),
			Payload:       payload,
		})
		if err != nil {
			return Result{}, fmt.Errorf("append order event: %w", err)
		}
	}

	result := Result{
		EventLogDir: eventLogDir,
		Orders:      cfg.Orders,
		FirstID:     cfg.StartID,
		LastID:      cfg.StartID + int64(cfg.Orders) - 1,
	}
	if cfg.Orders == 0 {
		result.LastID = 0
	}

	return result, nil
}

func withDefaults(cfg Config) Config {
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	if cfg.Seed == 0 {
		cfg.Seed = 1
	}
	if cfg.StartID == 0 {
		cfg.StartID = 1
	}
	if cfg.AreaSize <= 0 {
		cfg.AreaSize = defaultAreaSize
	}
	if cfg.CellSize <= 0 {
		cfg.CellSize = defaultCellSize
	}
	if cfg.Shards <= 0 {
		cfg.Shards = defaultShardCount
	}
	return cfg
}

func validateConfig(cfg Config) error {
	if cfg.Orders < 0 {
		return fmt.Errorf("orders must be >= 0")
	}
	if cfg.StartID <= 0 {
		return fmt.Errorf("start id must be > 0")
	}
	return nil
}
