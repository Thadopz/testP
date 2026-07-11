package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"testP/internal/checkpoint"
	"testP/internal/cluster/membership"
	"testP/internal/cluster/ownership"
	"testP/internal/database"
	"testP/internal/eventlog"
	"testP/internal/matching"
	"testP/internal/matchworker"
	"testP/internal/model"
	"testP/internal/node"
)

const (
	areaSize   = 100000
	shardCount = 64
	loadWeight = 10000
)

func main() {
	nodeID := flag.Int("node-id", 1, "logical node id")
	riderCount := flag.Int("riders", 100, "initial rider count")
	seed := flag.Int64("seed", 1, "initial rider seed")
	refreshInterval := flag.Duration("refresh-interval", time.Second, "ownership refresh interval")
	heartbeatInterval := flag.Duration("heartbeat-interval", time.Second, "membership heartbeat interval")
	membershipTTL := flag.Duration("membership-ttl", 5*time.Second, "membership ttl")
	etcdEndpoints := flag.String("etcd-endpoints", "127.0.0.1:2379", "comma-separated etcd endpoints")
	etcdPrefix := flag.String("etcd-prefix", "/testp", "etcd key prefix")
	kafkaBrokers := flag.String("kafka-brokers", "127.0.0.1:9092", "comma-separated Kafka brokers")
	matchTopic := flag.String("match-topic", model.TopicMatchRequests, "match request topic")
	postgresURL := flag.String("postgres-url", "postgres://testp:testp@127.0.0.1:5432/testp?sslmode=disable", "PostgreSQL connection URL")
	candidateLimit := flag.Int("candidate-limit", 10, "maximum candidates per order")
	maxRiderOrders := flag.Int64("max-rider-orders", 3, "maximum active orders per rider")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   splitValues(*etcdEndpoints),
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		exitError(err)
	}
	defer etcdClient.Close()
	ownershipStore := ownership.NewEtcdOwnershipStore(etcdClient, *etcdPrefix)
	membershipStore := membership.NewEtcdMembershipStoreWithTTL(etcdClient, *etcdPrefix, *membershipTTL)

	pool, err := database.Open(ctx, *postgresURL)
	if err != nil {
		exitError(err)
	}
	defer pool.Close()
	queries := database.New(pool)

	eventLog, err := eventlog.NewKafkaEventLog(eventlog.KafkaConfig{
		Brokers: splitValues(*kafkaBrokers),
		Topic:   *matchTopic,
		Codec:   &eventlog.JSONEventCodec{},
	})
	if err != nil {
		exitError(err)
	}
	defer eventLog.Close()

	cellSize := automaticCellSize(areaSize, *riderCount)
	index := matching.NewIndex(shardCount, cellSize, areaSize, loadWeight)
	worker := matchworker.New(pool, &eventlog.JSONEventCodec{}, index, ownershipStore, matchworker.Config{
		NodeID:         *nodeID,
		CandidateLimit: *candidateLimit,
		MaxRiderOrders: *maxRiderOrders,
		Topic:          *matchTopic,
	})
	checkpointStore := checkpoint.NewPostgresStoreForConsumer(
		queries,
		model.ConsumerMatcherWorker,
		*matchTopic,
	)
	runner := node.NewRunner(*nodeID, ownershipStore, eventLog, worker, checkpointStore)
	runner.SetRefreshInterval(*refreshInterval)
	runner.SetShardLifecycleHooks(
		func(shardID int) error {
			riders := ridersForShard(*seed, *riderCount, areaSize, index, shardID)
			if err := index.ReplaceShard(shardID, riders); err != nil {
				return err
			}
			for _, rider := range riders {
				if err := queries.UpsertRider(ctx, database.UpsertRiderParams{
					Uid: rider.UID, X: int32(rider.X), Y: int32(rider.Y),
					Online: true, CellID: rider.CellID, Count: rider.Count,
				}); err != nil {
					return err
				}
			}
			return nil
		},
		index.RemoveShard,
	)

	go heartbeat(ctx, membershipStore, *nodeID, *heartbeatInterval)
	if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		exitError(err)
	}
}

func ridersForShard(seed int64, count int, areaSize int, index *matching.Index, shardID int) []*model.Rider {
	rng := rand.New(rand.NewSource(seed))
	riders := make([]*model.Rider, 0)
	for riderIndex := 0; riderIndex < count; riderIndex++ {
		x := rng.Intn(areaSize)
		y := rng.Intn(areaSize)
		if index.Layout().ShardID(x, y) == shardID {
			riders = append(riders, &model.Rider{UID: int64(riderIndex + 1), X: x, Y: y})
		}
	}
	return riders
}

func heartbeat(ctx context.Context, store membership.MembershipStore, nodeID int, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	for {
		if err := store.MarkAlive(nodeID); err != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

func automaticCellSize(areaSize int, riderCount int) int {
	if riderCount <= 0 {
		return areaSize
	}
	cell := float64(areaSize) / math.Sqrt(float64(riderCount)/20.0)
	if cell < 1 {
		return 1
	}
	if cell > float64(areaSize) {
		return areaSize
	}
	return int(cell)
}

func splitValues(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func exitError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
