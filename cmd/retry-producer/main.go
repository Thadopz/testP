package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	db "testP/internal/database"
	"testP/internal/eventlog"
	"testP/internal/model"
)

func main() {
	startID := flag.Int64("start-id", 1, "first order id")
	endID := flag.Int64("end-id", 1, "last order id")
	attempt := flag.Int("attempt", 1, "retry attempt number")
	reason := flag.String("reason", "benchmark_retry", "retry reason")
	batchSize := flag.Int("batch-size", 100, "number of events per Kafka batch")
	postgresURL := flag.String("postgres-url", "postgres://testp:testp@127.0.0.1:5432/testp?sslmode=disable", "PostgreSQL connection URL")
	kafkaBrokers := flag.String("kafka-brokers", "127.0.0.1:9092", "comma-separated Kafka brokers")
	kafkaTopic := flag.String("kafka-topic", model.TopicOrderEvents, "Kafka topic for retry events")
	flag.Parse()

	if err := validateArguments(*startID, *endID, *attempt, *batchSize); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sent, err := sendRetryEvents(ctx, retryConfig{
		StartID:      *startID,
		EndID:        *endID,
		Attempt:      *attempt,
		Reason:       *reason,
		BatchSize:    *batchSize,
		PostgresURL:  *postgresURL,
		KafkaBrokers: splitNonEmpty(*kafkaBrokers),
		KafkaTopic:   strings.TrimSpace(*kafkaTopic),
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "retry producer failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("attempt: %d\n", *attempt)
	fmt.Printf("retry_events_sent: %d\n", sent)
}

type retryConfig struct {
	StartID      int64
	EndID        int64
	Attempt      int
	Reason       string
	BatchSize    int
	PostgresURL  string
	KafkaBrokers []string
	KafkaTopic   string
}

func sendRetryEvents(ctx context.Context, config retryConfig) (int, error) {
	pool, err := pgxpool.New(ctx, config.PostgresURL)
	if err != nil {
		return 0, fmt.Errorf("open postgres: %w", err)
	}
	defer pool.Close()

	orders, err := db.New(pool).ListMissedOrdersForRetry(ctx, db.ListMissedOrdersForRetryParams{
		StartID: config.StartID,
		EndID:   config.EndID,
		Attempt: int32(config.Attempt),
	})
	if err != nil {
		return 0, fmt.Errorf("list missed orders: %w", err)
	}

	log, err := eventlog.NewKafkaEventLog(eventlog.KafkaConfig{
		Brokers: config.KafkaBrokers,
		Topic:   config.KafkaTopic,
		Codec:   &eventlog.JSONEventCodec{},
	})
	if err != nil {
		return 0, err
	}
	defer log.Close()

	codec := &eventlog.JSONEventCodec{}
	for start := 0; start < len(orders); start += config.BatchSize {
		end := start + config.BatchSize
		if end > len(orders) {
			end = len(orders)
		}

		events := make([]model.Event, 0, end-start)
		for _, order := range orders[start:end] {
			event, err := buildRetryEvent(codec, order.OrderID, int(order.ShardID), config.Attempt, config.Reason)
			if err != nil {
				return 0, err
			}
			events = append(events, event)
		}

		if _, err := log.AppendBatch(ctx, events); err != nil {
			return start, fmt.Errorf("append retry batch: %w", err)
		}
	}

	return len(orders), nil
}

func buildRetryEvent(codec eventlog.EventCodec, orderID int64, shardID int, attempt int, reason string) (model.Event, error) {
	payload, err := codec.EncodePayload(model.OrderRetryRequest{
		OrderID: orderID,
		Attempt: attempt,
		Reason:  reason,
	})
	if err != nil {
		return model.Event{}, fmt.Errorf("encode retry for order %d: %w", orderID, err)
	}
	return model.Event{
		ID:            fmt.Sprintf("order-%d-retry-%d", orderID, attempt),
		Type:          model.EventOrderRetryRequest,
		AggregateType: "order",
		AggregateID:   fmt.Sprintf("%d", orderID),
		ShardID:       shardID,
		OccurredAt:    time.Now().Unix(),
		Payload:       payload,
	}, nil
}

func validateArguments(startID int64, endID int64, attempt int, batchSize int) error {
	if startID <= 0 || endID < startID {
		return fmt.Errorf("invalid order id range: %d..%d", startID, endID)
	}
	if attempt <= 0 {
		return fmt.Errorf("attempt must be > 0")
	}
	if batchSize <= 0 {
		return fmt.Errorf("batch size must be > 0")
	}
	return nil
}

func splitNonEmpty(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}
