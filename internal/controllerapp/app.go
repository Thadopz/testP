package controllerapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	clusterelection "testP/internal/cluster/election"
	"testP/internal/cluster/failover"
	clusterlayout "testP/internal/cluster/layout"
	"testP/internal/cluster/membership"
	"testP/internal/cluster/ownership"
	"testP/internal/cluster/rebalance"
	appmetrics "testP/internal/metrics"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Config struct {
	ControllerID  string
	EtcdEndpoints string
	EtcdPrefix    string
	ElectionTTL   time.Duration
	MembershipTTL time.Duration
	SweepInterval time.Duration
	ShardCount    int
	MetricsAddr   string
	Output        io.Writer
	ErrorOutput   io.Writer
}

type stores struct {
	ownership  ownership.OwnershipStore
	membership membership.MembershipStore
	election   *clusterelection.EtcdElection
	close      func() error
}

func Run(ctx context.Context, cfg Config) error {
	cfg = withDefaults(cfg)

	stores, err := newStores(cfg.EtcdEndpoints, cfg.EtcdPrefix, cfg.ControllerID, cfg.MembershipTTL, cfg.ElectionTTL)
	if err != nil {
		return fmt.Errorf("create controller stores: %w", err)
	}
	defer stores.close()

	failoverController := failover.NewFailoverController(stores.ownership, stores.membership)
	rebalanceController := rebalance.NewController(stores.ownership, stores.membership)
	metricsRecorder := startMetrics(ctx, cfg)

	ticker := time.NewTicker(cfg.SweepInterval)
	defer ticker.Stop()

	printStartup(cfg)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			sweepWithMetrics(
				ctx,
				stores.ownership,
				stores.membership,
				failoverController,
				rebalanceController,
				stores.election,
				cfg.ShardCount,
				cfg.ControllerID,
				metricsRecorder,
				cfg.Output,
				cfg.ErrorOutput,
			)
		}
	}
}

func withDefaults(cfg Config) Config {
	if strings.TrimSpace(cfg.ControllerID) == "" {
		cfg.ControllerID = "controller"
	}
	if strings.TrimSpace(cfg.EtcdEndpoints) == "" {
		cfg.EtcdEndpoints = "127.0.0.1:2379"
	}
	if strings.TrimSpace(cfg.EtcdPrefix) == "" {
		cfg.EtcdPrefix = "/testp"
	}
	if cfg.ElectionTTL <= 0 {
		cfg.ElectionTTL = 5 * time.Second
	}
	if cfg.MembershipTTL <= 0 {
		cfg.MembershipTTL = 5 * time.Second
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = time.Second
	}
	if cfg.ShardCount <= 0 {
		cfg.ShardCount = 64
	}
	return cfg
}

func newStores(endpointsText string, prefix string, controllerID string, membershipTTL time.Duration, electionTTL time.Duration) (stores, error) {
	endpoints, err := ParseEtcdEndpoints(endpointsText)
	if err != nil {
		return stores{}, err
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		return stores{}, fmt.Errorf("connect etcd: %w", err)
	}

	leaderElection, err := clusterelection.NewEtcdElection(client, prefix, controllerID, electionTTL)
	if err != nil {
		client.Close()
		return stores{}, err
	}

	return stores{
		ownership:  ownership.NewEtcdOwnershipStore(client, prefix),
		membership: membership.NewEtcdMembershipStoreWithTTL(client, prefix, membershipTTL),
		election:   leaderElection,
		close: func() error {
			closeCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = leaderElection.Resign(closeCtx)
			return client.Close()
		},
	}, nil
}

func ParseEtcdEndpoints(text string) ([]string, error) {
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

func startMetrics(ctx context.Context, cfg Config) appmetrics.Recorder {
	if strings.TrimSpace(cfg.MetricsAddr) == "" {
		return nil
	}

	prometheusRecorder := appmetrics.NewPrometheusRecorder(nil)
	go func() {
		err := appmetrics.RunServer(ctx, cfg.MetricsAddr, prometheusRecorder.Handler())
		if err != nil && !errors.Is(err, context.Canceled) {
			writeLine(cfg.ErrorOutput, "metrics server stopped: %v\n", err)
		}
	}()
	return prometheusRecorder
}

func printStartup(cfg Config) {
	writeLine(cfg.Output, "controller_backend: etcd\n")
	writeLine(cfg.Output, "controller_id: %s\n", cfg.ControllerID)
	writeLine(cfg.Output, "etcd_endpoints: %s\n", cfg.EtcdEndpoints)
	writeLine(cfg.Output, "etcd_prefix: %s\n", cfg.EtcdPrefix)
	writeLine(cfg.Output, "membership_ttl: %s\n", cfg.MembershipTTL.String())
	writeLine(cfg.Output, "election_ttl: %s\n", cfg.ElectionTTL.String())
	writeLine(cfg.Output, "shards: %d\n", cfg.ShardCount)
}

func Sweep(
	ctx context.Context,
	ownershipStore ownership.OwnershipStore,
	membershipStore membership.MembershipStore,
	failoverController *failover.FailoverController,
	leaderElection *clusterelection.EtcdElection,
	shardCount int,
) {
	rebalanceController := rebalance.NewController(ownershipStore, membershipStore)
	sweepWithMetrics(ctx, ownershipStore, membershipStore, failoverController, rebalanceController, leaderElection, shardCount, "", nil, nil, nil)
}

func sweepWithMetrics(
	ctx context.Context,
	ownershipStore ownership.OwnershipStore,
	membershipStore membership.MembershipStore,
	failoverController *failover.FailoverController,
	rebalanceController *rebalance.Controller,
	leaderElection *clusterelection.EtcdElection,
	shardCount int,
	controllerID string,
	recorder appmetrics.Recorder,
	output io.Writer,
	errorOutput io.Writer,
) {
	if recorder != nil {
		recorder.IncControllerSweep(controllerID)
	}
	if leaderElection == nil {
		recordSweepError(recorder, controllerID, "missing_election")
		writeLine(errorOutput, "sweep failed: controller election is required\n")
		return
	}
	leader, err := leaderElection.TryAcquire(ctx)
	if err != nil {
		recordSweepError(recorder, controllerID, "election")
		writeLine(errorOutput, "election failed: %v\n", err)
		return
	}
	if recorder != nil {
		recorder.SetControllerLeader(controllerID, leader)
	}
	if !leader {
		return
	}

	if err := EnsureInitialOwnership(ownershipStore, membershipStore, shardCount); err != nil {
		recordSweepError(recorder, controllerID, "initial_ownership")
		writeLine(errorOutput, "initial ownership failed: %v\n", err)
		return
	}

	deadNodeIDs, err := failoverController.FailoverMissingOwners()
	if err != nil {
		recordSweepError(recorder, controllerID, "failover")
		writeLine(errorOutput, "sweep failed: %v\n", err)
		return
	}
	for _, nodeID := range deadNodeIDs {
		if recorder != nil {
			recorder.IncFailover(controllerID, nodeID)
		}
		writeLine(output, "node_dead: %d\n", nodeID)
	}

	if rebalanceController != nil {
		move, moved, err := rebalanceController.RebalanceOnce()
		if err != nil {
			recordSweepError(recorder, controllerID, "rebalance")
			writeLine(errorOutput, "rebalance failed: %v\n", err)
			return
		}
		if moved {
			writeLine(
				output,
				"shard_rebalanced: shard=%d from=%d to=%d\n",
				move.ShardID,
				move.FromNodeID,
				move.ToNodeID,
			)
		}
	}
	recordClusterMetrics(recorder, controllerID, ownershipStore, membershipStore, shardCount)
}

func recordSweepError(recorder appmetrics.Recorder, controllerID string, reason string) {
	if recorder == nil {
		return
	}
	recorder.IncControllerSweepError(controllerID, reason)
}

func recordClusterMetrics(
	recorder appmetrics.Recorder,
	controllerID string,
	ownershipStore ownership.OwnershipStore,
	membershipStore membership.MembershipStore,
	shardCount int,
) {
	if recorder == nil {
		return
	}

	aliveNodes, err := membershipStore.AliveNodes()
	if err == nil {
		recorder.SetAliveNodes(controllerID, len(aliveNodes))
	} else {
		recorder.IncControllerSweepError(controllerID, "metrics_alive_nodes")
	}

	lister, ok := ownershipStore.(ownership.OwnershipLister)
	if !ok {
		recorder.IncControllerSweepError(controllerID, "metrics_ownership_lister")
		return
	}
	ownerships, err := lister.AllOwnerships()
	if err != nil {
		recorder.IncControllerSweepError(controllerID, "metrics_ownership")
		return
	}

	ownedShardIDs := make(map[int]bool, len(ownerships))
	for _, currentOwnership := range ownerships {
		if currentOwnership.ShardID >= 0 && currentOwnership.ShardID < shardCount {
			ownedShardIDs[currentOwnership.ShardID] = true
		}
	}
	recorder.SetOwnedShards(controllerID, len(ownedShardIDs))

	missingCount := shardCount - len(ownedShardIDs)
	if missingCount < 0 {
		missingCount = 0
	}
	recorder.SetShardsWithoutOwner(controllerID, missingCount)
}

func EnsureInitialOwnership(ownershipStore ownership.OwnershipStore, membershipStore membership.MembershipStore, shardCount int) error {
	lister, ok := ownershipStore.(ownership.OwnershipLister)
	if !ok {
		return fmt.Errorf("ownership store cannot list all ownerships")
	}

	currentOwnerships, err := lister.AllOwnerships()
	if err != nil {
		return err
	}
	if len(currentOwnerships) > 0 {
		return nil
	}

	aliveNodes, err := membershipStore.AliveNodes()
	if err != nil {
		return err
	}
	if len(aliveNodes) == 0 {
		return nil
	}

	layout, err := clusterlayout.NewModuloLayout(aliveNodes, shardCount)
	if err != nil {
		return err
	}
	for _, shardID := range layout.ShardIDs() {
		nodeID, ok := layout.OwnerOf(shardID)
		if !ok {
			return fmt.Errorf("owner for shard %d not found", shardID)
		}
		if err := ownershipStore.Assign(shardID, nodeID); err != nil {
			return err
		}
	}

	return nil
}

func writeLine(w io.Writer, format string, args ...any) {
	if w == nil {
		return
	}
	fmt.Fprintf(w, format, args...)
}
