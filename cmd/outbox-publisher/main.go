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

	"testP/internal/database"
	"testP/internal/eventlog"
	"testP/internal/outbox"
)

func main() {
	postgresURL := flag.String("postgres-url", "postgres://testp:testp@127.0.0.1:5432/testp?sslmode=disable", "PostgreSQL connection URL")
	kafkaBrokers := flag.String("kafka-brokers", "127.0.0.1:9092", "comma-separated Kafka broker addresses")
	workerID := flag.String("worker-id", "outbox-1", "outbox publisher id")
	orderTopic := flag.String("order-topic", "order-events", "physical Kafka order topic")
	matchTopic := flag.String("match-topic", "match-requests", "physical Kafka match request topic")
	batchSize := flag.Int("batch-size", 100, "maximum events per poll")
	pollInterval := flag.Duration("poll-interval", time.Second, "outbox polling interval")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := database.Open(ctx, *postgresURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer pool.Close()

	targets, closeTargets, err := newKafkaTargets(strings.Split(*kafkaBrokers, ","), *orderTopic, *matchTopic)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer closeTargets()

	publisher := outbox.NewPublisher(pool, targets, outbox.Config{
		WorkerID:     *workerID,
		BatchSize:    int32(*batchSize),
		PollInterval: *pollInterval,
	})
	if err := publisher.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newKafkaTargets(
	brokers []string,
	orderTopic string,
	matchTopic string,
) (map[string]eventlog.Appender, func(), error) {
	codec := &eventlog.JSONEventCodec{}
	targets := make(map[string]eventlog.Appender)
	logs := make([]*eventlog.KafkaEventLog, 0, 2)
	physicalTopics := map[string]string{
		"order-events":   orderTopic,
		"match-requests": matchTopic,
	}
	for logicalTopic, physicalTopic := range physicalTopics {
		log, err := eventlog.NewKafkaEventLog(eventlog.KafkaConfig{
			Brokers: brokers,
			Topic:   physicalTopic,
			Codec:   codec,
		})
		if err != nil {
			for _, opened := range logs {
				_ = opened.Close()
			}
			return nil, func() {}, err
		}
		logs = append(logs, log)
		targets[logicalTopic] = log
	}
	return targets, func() {
		for _, log := range logs {
			_ = log.Close()
		}
	}, nil
}
