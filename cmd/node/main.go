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
	"testP/internal/nodeapp"
)

func main() {
	nodeID := flag.Int("node-id", 1, "node id")
	shardsText := flag.String("shards", "0", "comma-separated shard ids")
	dataDir := flag.String("data-dir", "./data", "data directory")
	riderCount := flag.Int("riders", 100, "initial rider count")
	workerCount := flag.Int("workers", 2, "worker count")
	seed := flag.Int64("seed", 1, "random seed")
	flag.Parse()

	shardIDs, err := parseShardIDs(*shardsText)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid -shards: %v\n", err)
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
