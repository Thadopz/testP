package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testP/internal/cluster/failover"
	"testP/internal/cluster/heartbeat"
	"testP/internal/cluster/membership"
	"testP/internal/cluster/ownership"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "heartbeat RPC listen address")
	dataDir := flag.String("data-dir", "./data", "data directory")
	heartbeatTimeout := flag.Duration("heartbeat-timeout", 5*time.Second, "heartbeat timeout")
	sweepInterval := flag.Duration("sweep-interval", time.Second, "dead node sweep interval")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ownershipStore := ownership.NewFileOwnershipStore(filepath.Join(*dataDir, "ownership"))
	membershipStore := membership.NewMemoryMembershipStore()
	failoverController := failover.NewFailoverController(ownershipStore, membershipStore)
	heartbeatService := heartbeat.NewHeartbeatService(membershipStore, failoverController, *heartbeatTimeout)

	errCh := make(chan error, 1)
	go func() {
		errCh <- heartbeat.ServeHeartbeatRPC(ctx, *addr, heartbeatService)
	}()

	ticker := time.NewTicker(*sweepInterval)
	defer ticker.Stop()

	fmt.Printf("controller_listen: %s\n", *addr)
	fmt.Printf("ownership_dir: %s\n", filepath.Join(*dataDir, "ownership"))

	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintf(os.Stderr, "controller failed: %v\n", err)
				os.Exit(1)
			}
			return
		case <-ticker.C:
			deadNodeIDs, err := heartbeatService.SweepDeadNodes()
			if err != nil {
				fmt.Fprintf(os.Stderr, "sweep failed: %v\n", err)
				continue
			}
			for _, nodeID := range deadNodeIDs {
				fmt.Printf("node_dead: %d\n", nodeID)
			}
		}
	}
}
