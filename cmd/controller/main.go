package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	clusterelection "testP/internal/cluster/election"
	"testP/internal/cluster/failover"
	clusterlayout "testP/internal/cluster/layout"
	"testP/internal/cluster/membership"
	"testP/internal/cluster/ownership"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type controllerStores struct {
	ownership  ownership.OwnershipStore
	membership membership.MembershipStore
	election   *clusterelection.EtcdElection
	close      func() error
}

func main() {
	controllerID := flag.String("controller-id", defaultControllerID(), "controller id for etcd election")
	etcdEndpoints := flag.String("etcd-endpoints", "127.0.0.1:2379", "comma separated etcd endpoints")
	etcdPrefix := flag.String("etcd-prefix", "/testp", "etcd key prefix")
	electionTTL := flag.Duration("election-ttl", 5*time.Second, "etcd controller election ttl")
	membershipTTL := flag.Duration("membership-ttl", 5*time.Second, "etcd membership ttl")
	sweepInterval := flag.Duration("sweep-interval", time.Second, "dead node sweep interval")
	shardCount := flag.Int("shards", 64, "number of shards to assign")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stores, err := newControllerStores(*etcdEndpoints, *etcdPrefix, *controllerID, *membershipTTL, *electionTTL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create controller stores failed: %v\n", err)
		os.Exit(1)
	}
	defer stores.close()

	failoverController := failover.NewFailoverController(stores.ownership, stores.membership)

	ticker := time.NewTicker(*sweepInterval)
	defer ticker.Stop()

	fmt.Printf("controller_backend: etcd\n")
	fmt.Printf("controller_id: %s\n", *controllerID)
	fmt.Printf("etcd_endpoints: %s\n", *etcdEndpoints)
	fmt.Printf("etcd_prefix: %s\n", *etcdPrefix)
	fmt.Printf("membership_ttl: %s\n", membershipTTL.String())
	fmt.Printf("election_ttl: %s\n", electionTTL.String())
	fmt.Printf("shards: %d\n", *shardCount)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweepController(ctx, stores.ownership, stores.membership, failoverController, stores.election, *shardCount)
		}
	}
}

func newControllerStores(endpointsText string, prefix string, controllerID string, membershipTTL time.Duration, electionTTL time.Duration) (controllerStores, error) {
	endpoints, err := parseEtcdEndpoints(endpointsText)
	if err != nil {
		return controllerStores{}, err
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		return controllerStores{}, fmt.Errorf("connect etcd: %w", err)
	}

	leaderElection, err := clusterelection.NewEtcdElection(client, prefix, controllerID, electionTTL)
	if err != nil {
		client.Close()
		return controllerStores{}, err
	}

	return controllerStores{
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

func sweepController(
	ctx context.Context,
	ownershipStore ownership.OwnershipStore,
	membershipStore membership.MembershipStore,
	failoverController *failover.FailoverController,
	leaderElection *clusterelection.EtcdElection,
	shardCount int,
) {
	if leaderElection == nil {
		fmt.Fprintln(os.Stderr, "sweep failed: controller election is required")
		return
	}
	leader, err := leaderElection.TryAcquire(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "election failed: %v\n", err)
		return
	}
	if !leader {
		return
	}

	if err := ensureInitialOwnership(ownershipStore, membershipStore, shardCount); err != nil {
		fmt.Fprintf(os.Stderr, "initial ownership failed: %v\n", err)
		return
	}

	deadNodeIDs, err := failoverController.FailoverMissingOwners()
	if err != nil {
		fmt.Fprintf(os.Stderr, "sweep failed: %v\n", err)
		return
	}
	for _, nodeID := range deadNodeIDs {
		fmt.Printf("node_dead: %d\n", nodeID)
	}
}

func ensureInitialOwnership(ownershipStore ownership.OwnershipStore, membershipStore membership.MembershipStore, shardCount int) error {
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

func defaultControllerID() string {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		hostname = "controller"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}
