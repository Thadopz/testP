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
	"testP/internal/checkpoint"
	clustermembership "testP/internal/cluster/membership"
	clusterownership "testP/internal/cluster/ownership"
	"testP/internal/eventlog"
	"testP/internal/nodeapp"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultShardCount = 64

type nodeEtcdRuntime struct {
	ownership  clusterownership.OwnershipStore
	membership clustermembership.MembershipStore
	checkpoint checkpoint.ShardStore
	close      func() error
}

func main() {
	nodeID := flag.Int("node-id", 1, "node id")
	dataDir := flag.String("data-dir", "./data", "data directory")
	riderCount := flag.Int("riders", 100, "initial rider count")
	workerCount := flag.Int("workers", 2, "worker count")
	seed := flag.Int64("seed", 1, "random seed")
	heartbeatInterval := flag.Duration("heartbeat-interval", time.Second, "heartbeat interval")
	etcdEndpoints := flag.String("etcd-endpoints", "127.0.0.1:2379", "comma separated etcd endpoints")
	etcdPrefix := flag.String("etcd-prefix", "/testp", "etcd key prefix")
	membershipTTL := flag.Duration("membership-ttl", 5*time.Second, "etcd membership ttl")
	kafkaBrokersText := flag.String("kafka-brokers", "127.0.0.1:9092", "comma-separated Kafka broker addresses")
	kafkaTopic := flag.String("kafka-topic", "order-events", "Kafka topic for order events")
	flag.Parse()

	etcdRuntime, err := newNodeEtcdRuntime(*etcdEndpoints, *etcdPrefix, *membershipTTL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid etcd config: %v\n", err)
		os.Exit(2)
	}
	defer etcdRuntime.close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	activeEventLog, err := buildNodeEventLog(*kafkaBrokersText, *kafkaTopic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid eventlog config: %v\n", err)
		os.Exit(2)
	}

	go func() {
		err := runMembershipHeartbeat(ctx, etcdRuntime.membership, *nodeID, *heartbeatInterval)
		if err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintf(os.Stderr, "heartbeat stopped: %v\n", err)
		}
	}()

	result, err := nodeapp.RunWithResult(ctx, nodeapp.Config{
		NodeID:          *nodeID,
		ShardProvider:   etcdRuntime.ownership,
		DataDir:         *dataDir,
		EventLog:        activeEventLog,
		CheckpointStore: etcdRuntime.checkpoint,
		Riders:          *riderCount,
		Workers:         *workerCount,
		Seed:            *seed,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "node failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("node_id: %d\n", result.NodeID)
	fmt.Printf("shards: %v\n", result.ShardIDs)
	fmt.Printf("eventlog_dir: %s\n", result.EventLogDir)
	fmt.Printf("eventlog: kafka\n")
	fmt.Printf("checkpoint_dir: %s\n", result.CheckpointDir)
	fmt.Printf("order_state_dir: %s\n", result.OrderStateDir)
	fmt.Printf("ownership_backend: etcd\n")
	fmt.Printf("etcd_endpoints: %s\n", *etcdEndpoints)
	fmt.Printf("etcd_prefix: %s\n", *etcdPrefix)
	fmt.Printf("submitted: %d\n", result.Submitted)
	fmt.Printf("matched: %d\n", result.Matched)
	fmt.Printf("missed: %d\n", result.Missed)
	fmt.Printf("online_riders: %d\n", result.OnlineRiders)
	for _, metric := range result.ShardMetrics {
		fmt.Printf(
			"shard_metric: shard=%d node=%d epoch=%d checkpoint_offset=%d eventlog_offset=%d lag=%d\n",
			metric.ShardID,
			metric.NodeID,
			metric.Epoch,
			metric.CheckpointOffset,
			metric.EventLogOffset,
			metric.Lag,
		)
	}
}

func newNodeEtcdRuntime(endpointsText string, prefix string, ttl time.Duration) (nodeEtcdRuntime, error) {
	endpoints, err := parseEtcdEndpoints(endpointsText)
	if err != nil {
		return nodeEtcdRuntime{}, err
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		return nodeEtcdRuntime{}, fmt.Errorf("connect etcd: %w", err)
	}
	return nodeEtcdRuntime{
		ownership:  clusterownership.NewEtcdOwnershipStore(client, prefix),
		membership: clustermembership.NewEtcdMembershipStoreWithTTL(client, prefix, ttl),
		checkpoint: checkpoint.NewEtcdStore(client, prefix),
		close:      client.Close,
	}, nil
}

func buildNodeEventLog(brokersText string, topic string) (eventlog.EventLog, error) {
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

func parseEtcdEndpoints(text string) ([]string, error) {
	parts := strings.Split(text, ",")
	endpoints := make([]string, 0, len(parts))
	for _, part := range parts {
		endpoint := strings.TrimSpace(part)
		if endpoint == "" {
			continue
		}
		endpoints = append(endpoints, endpoint)
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("etcd endpoints must not be empty")
	}
	return endpoints, nil
}

func runMembershipHeartbeat(ctx context.Context, store clustermembership.MembershipStore, nodeID int, interval time.Duration) error {
	if store == nil {
		return fmt.Errorf("membership store is required")
	}
	if interval <= 0 {
		interval = time.Second
	}

	if err := store.MarkAlive(nodeID); err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := store.MarkAlive(nodeID); err != nil {
				return err
			}
		}
	}
}
