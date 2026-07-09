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
	"testP/internal/eventlog"
	appmetrics "testP/internal/metrics"
	"testP/internal/producerapp"
)

func main() {
	dataDir := flag.String("data-dir", "./data", "data directory")
	orderCount := flag.Int("orders", 100, "number of order_created events to write")
	seed := flag.Int64("seed", 1, "random seed")
	startID := flag.Int64("start-id", 1, "first order id")
	metricsAddr := flag.String("metrics-addr", ":9103", "Prometheus metrics listen address; set empty to disable")
	kafkaBrokersText := flag.String("kafka-brokers", "127.0.0.1:9092", "comma-separated Kafka broker addresses")
	kafkaTopic := flag.String("kafka-topic", "order-events", "Kafka topic for order events")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var metricsRecorder appmetrics.Recorder
	if strings.TrimSpace(*metricsAddr) != "" {
		prometheusRecorder := appmetrics.NewPrometheusRecorder(nil)
		metricsRecorder = prometheusRecorder
		go func() {
			err := appmetrics.RunServer(ctx, *metricsAddr, prometheusRecorder.Handler())
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintf(os.Stderr, "metrics server stopped: %v\n", err)
			}
		}()
	}

	activeEventLog, err := buildProducerEventLog(*kafkaBrokersText, *kafkaTopic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid eventlog config: %v\n", err)
		os.Exit(2)
	}

	result, err := producerapp.Run(ctx, producerapp.Config{
		DataDir:  *dataDir,
		EventLog: activeEventLog,
		Orders:   *orderCount,
		Seed:     *seed,
		StartID:  *startID,
		Metrics:  metricsRecorder,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "producer failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("eventlog_dir: %s\n", result.EventLogDir)
	fmt.Printf("eventlog: kafka\n")
	fmt.Printf("orders: %d\n", result.Orders)
	fmt.Printf("first_id: %d\n", result.FirstID)
	fmt.Printf("last_id: %d\n", result.LastID)
}

func buildProducerEventLog(brokersText string, topic string) (eventlog.Appender, error) {
	return eventlog.NewKafkaEventLog(eventlog.KafkaConfig{
		Brokers: parseBrokerList(brokersText),
		Topic:   strings.TrimSpace(topic),
		Codec:   &eventlog.JSONEventCodec{},
	})
}

func parseBrokerList(text string) []string {
	parts := strings.Split(text, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}
