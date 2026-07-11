package producerapp

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testP/internal/eventlog"
	"testP/internal/metrics"
	"testP/internal/model"
	"testP/internal/shard"
)

const (
	defaultAreaSize   = 100000
	defaultCellSize   = 10000
	defaultShardCount = 64
)

type Config struct {
	DataDir   string
	EventLog  eventlog.Appender
	Orders    int
	Seed      int64
	StartID   int64
	AreaSize  int
	CellSize  int
	Shards    int
	BatchSize int
	Metrics   metrics.Recorder
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
	activeEventLog := cfg.EventLog
	layout := shard.NewLayout(cfg.AreaSize, cfg.CellSize, cfg.Shards)
	rng := rand.New(rand.NewSource(cfg.Seed))
	batch := make([]model.Event, 0, cfg.BatchSize)

	for index := 0; index < cfg.Orders; index++ {
		if err := ctx.Err(); err != nil {
			recordProducerError(cfg.Metrics, "context")
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
			recordProducerError(cfg.Metrics, "encode")
			return Result{}, fmt.Errorf("encode order payload: %w", err)
		}

		event := model.Event{
			ID:            fmt.Sprintf("order-%d-created", orderID),
			Type:          model.EventOrderCreated,
			AggregateType: "order",
			AggregateID:   fmt.Sprintf("%d", orderID),
			ShardID:       shardID,
			OccurredAt:    int64(index + 1),
			Payload:       payload,
		}

		batch = append(batch, event)
		if len(batch) >= cfg.BatchSize {
			if err := appendOrderEvents(ctx, activeEventLog, batch); err != nil {
				recordProducerError(cfg.Metrics, "append")
				return Result{}, err
			}
			recordProducerEvents(cfg.Metrics, batch)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := appendOrderEvents(ctx, activeEventLog, batch); err != nil {
			recordProducerError(cfg.Metrics, "append")
			return Result{}, err
		}
		recordProducerEvents(cfg.Metrics, batch)
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

func appendOrderEvents(ctx context.Context, appender eventlog.Appender, events []model.Event) error {
	if batchAppender, ok := appender.(eventlog.BatchAppender); ok {
		if _, err := batchAppender.AppendBatch(ctx, events); err != nil {
			return fmt.Errorf("append order event batch: %w", err)
		}
		return nil
	}

	for _, event := range events {
		if _, err := appender.Append(ctx, event); err != nil {
			return fmt.Errorf("append order event: %w", err)
		}
	}
	return nil
}

func recordProducerEvents(recorder metrics.Recorder, events []model.Event) {
	for _, event := range events {
		recordProducerEvent(recorder, event)
	}
}

func recordProducerEvent(recorder metrics.Recorder, event model.Event) {
	if recorder == nil {
		return
	}
	recorder.IncProducerEvent(string(event.Type), event.ShardID)
}

func recordProducerError(recorder metrics.Recorder, reason string) {
	if recorder == nil {
		return
	}
	recorder.IncProducerError(reason)
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
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	return cfg
}

func validateConfig(cfg Config) error {
	if cfg.EventLog == nil {
		return fmt.Errorf("eventlog is required")
	}
	if cfg.Orders < 0 {
		return fmt.Errorf("orders must be >= 0")
	}
	if cfg.StartID <= 0 {
		return fmt.Errorf("start id must be > 0")
	}
	return nil
}
