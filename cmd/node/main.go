package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testP/internal/cluster/heartbeat"
	clusterlayout "testP/internal/cluster/layout"
	clusterownership "testP/internal/cluster/ownership"
	"testP/internal/nodeapp"
	"time"
)

const defaultShardCount = 64

func main() {
	nodeID := flag.Int("node-id", 1, "node id")
	shardsText := flag.String("shards", "0", "comma-separated shard ids")
	nodesText := flag.String("nodes", "", "comma-separated node ids for automatic shard ownership")
	dataDir := flag.String("data-dir", "./data", "data directory")
	riderCount := flag.Int("riders", 100, "initial rider count")
	workerCount := flag.Int("workers", 2, "worker count")
	seed := flag.Int64("seed", 1, "random seed")
	tail := flag.Bool("tail", false, "keep running and wait for appended events")
	dynamic := flag.Bool("dynamic", false, "use dynamic shard ownership refresh")
	heartbeatAddr := flag.String("heartbeat-addr", "", "controller heartbeat RPC address")
	heartbeatInterval := flag.Duration("heartbeat-interval", time.Second, "heartbeat interval")
	flag.Parse()

	ownershipDir := filepath.Join(*dataDir, "ownership")
	shardIDs, shardProvider, err := resolveShardAssignment(*nodeID, *shardsText, *nodesText, defaultShardCount, *dynamic, ownershipDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid shard assignment: %v\n", err)
		os.Exit(2)
	}
	if *dynamic && !*tail {
		fmt.Fprintln(os.Stderr, "invalid shard assignment: -dynamic requires -tail")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if strings.TrimSpace(*heartbeatAddr) != "" {
		go func() {
			err := heartbeat.RunHeartbeatClient(ctx, *heartbeatAddr, *nodeID, *heartbeatInterval)
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintf(os.Stderr, "heartbeat stopped: %v\n", err)
			}
		}()
	}

	result, err := nodeapp.RunWithResult(ctx, nodeapp.Config{
		NodeID:        *nodeID,
		ShardIDs:      shardIDs,
		ShardProvider: shardProvider,
		DataDir:       *dataDir,
		Riders:        *riderCount,
		Workers:       *workerCount,
		Seed:          *seed,
		Tail:          *tail,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "node failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("node_id: %d\n", result.NodeID)
	fmt.Printf("shards: %v\n", result.ShardIDs)
	fmt.Printf("eventlog_dir: %s\n", result.EventLogDir)
	fmt.Printf("checkpoint_dir: %s\n", result.CheckpointDir)
	fmt.Printf("order_state_dir: %s\n", result.OrderStateDir)
	if *dynamic {
		fmt.Printf("ownership_dir: %s\n", ownershipDir)
	}
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

func parseShardIDs(text string) ([]int, error) {
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("empty shard list")
	}

	parts := strings.Split(text, ",")
	shardIDs := make([]int, 0, len(parts))

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			return nil, fmt.Errorf("empty shard id")
		}

		shardID, err := strconv.Atoi(trimmed)
		if err != nil {
			return nil, fmt.Errorf("parse shard id %q: %w", trimmed, err)
		}
		if shardID < 0 {
			return nil, fmt.Errorf("negative shard id %d", shardID)
		}

		shardIDs = append(shardIDs, shardID)
	}

	return shardIDs, nil
}

func parseNodeIDs(text string) ([]int, error) {
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("empty node list")
	}

	return parsePositiveIDs(text, "node")
}

func resolveShardIDs(nodeID int, shardsText string, nodesText string, shardCount int) ([]int, error) {
	shardIDs, _, err := resolveShardAssignment(nodeID, shardsText, nodesText, shardCount, false, "")
	return shardIDs, err
}

func resolveShardAssignment(nodeID int, shardsText string, nodesText string, shardCount int, dynamic bool, ownershipDir string) ([]int, clusterownership.ShardProvider, error) {
	if strings.TrimSpace(nodesText) == "" {
		if dynamic {
			store := clusterownership.NewFileOwnershipStore(ownershipDir)
			ownerships, err := store.ShardsForNode(nodeID)
			if err != nil {
				return nil, nil, err
			}
			return ownershipsToShardIDs(ownerships), store, nil
		}
		shardIDs, err := parseShardIDs(shardsText)
		return shardIDs, nil, err
	}

	nodeIDs, err := parseNodeIDs(nodesText)
	if err != nil {
		return nil, nil, err
	}

	layout, err := clusterlayout.NewModuloLayout(nodeIDs, shardCount)
	if err != nil {
		return nil, nil, err
	}

	store := clusterownership.OwnershipStore(clusterownership.NewMemoryOwnershipStore())
	if dynamic {
		store = clusterownership.NewFileOwnershipStore(ownershipDir)
	}

	if err := assignMissingLayout(store, layout); err != nil {
		return nil, nil, err
	}

	ownerships, err := store.ShardsForNode(nodeID)
	if err != nil {
		return nil, nil, err
	}

	shardIDs := make([]int, 0, len(ownerships))
	for _, ownership := range ownerships {
		shardIDs = append(shardIDs, ownership.ShardID)
	}
	if len(shardIDs) == 0 && !dynamic {
		return nil, nil, fmt.Errorf("node %d owns no shards", nodeID)
	}

	if dynamic {
		return shardIDs, store, nil
	}

	return shardIDs, nil, nil
}

func assignMissingLayout(store clusterownership.OwnershipStore, layout clusterlayout.Layout) error {
	for _, shardID := range layout.ShardIDs() {
		if _, ok, err := store.OwnerOf(shardID); err != nil {
			return err
		} else if ok {
			continue
		}

		nodeID, ok := layout.OwnerOf(shardID)
		if !ok {
			return fmt.Errorf("owner for shard %d not found", shardID)
		}
		if err := store.Assign(shardID, nodeID); err != nil {
			return err
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

func parsePositiveIDs(text string, name string) ([]int, error) {
	parts := strings.Split(text, ",")
	ids := make([]int, 0, len(parts))

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			return nil, fmt.Errorf("empty %s id", name)
		}

		id, err := strconv.Atoi(trimmed)
		if err != nil {
			return nil, fmt.Errorf("parse %s id %q: %w", name, trimmed, err)
		}
		if id <= 0 {
			return nil, fmt.Errorf("%s id must be > 0: %d", name, id)
		}

		ids = append(ids, id)
	}

	return ids, nil
}
