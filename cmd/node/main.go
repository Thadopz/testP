package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"testP/internal/cluster"
	"testP/internal/nodeapp"
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
	flag.Parse()

	shardIDs, err := resolveShardIDs(*nodeID, *shardsText, *nodesText, defaultShardCount)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid shard assignment: %v\n", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	result, err := nodeapp.RunWithResult(ctx, nodeapp.Config{
		NodeID:   *nodeID,
		ShardIDs: shardIDs,
		DataDir:  *dataDir,
		Riders:   *riderCount,
		Workers:  *workerCount,
		Seed:     *seed,
		Tail:     *tail,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "node failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("node_id: %d\n", result.NodeID)
	fmt.Printf("shards: %v\n", result.ShardIDs)
	fmt.Printf("eventlog_dir: %s\n", result.EventLogDir)
	fmt.Printf("checkpoint_dir: %s\n", result.CheckpointDir)
	fmt.Printf("submitted: %d\n", result.Submitted)
	fmt.Printf("matched: %d\n", result.Matched)
	fmt.Printf("missed: %d\n", result.Missed)
	fmt.Printf("online_riders: %d\n", result.OnlineRiders)
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
	if strings.TrimSpace(nodesText) == "" {
		return parseShardIDs(shardsText)
	}

	nodeIDs, err := parseNodeIDs(nodesText)
	if err != nil {
		return nil, err
	}

	layout, err := cluster.NewModuloLayout(nodeIDs, shardCount)
	if err != nil {
		return nil, err
	}

	shardIDs := layout.ShardsForNode(nodeID)
	if len(shardIDs) == 0 {
		return nil, fmt.Errorf("node %d owns no shards", nodeID)
	}

	return shardIDs, nil
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
