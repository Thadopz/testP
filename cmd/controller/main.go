package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testP/internal/controllerapp"
	"time"
)

func main() {
	controllerID := flag.String("controller-id", defaultControllerID(), "controller id for etcd election")
	etcdEndpoints := flag.String("etcd-endpoints", "127.0.0.1:2379", "comma separated etcd endpoints")
	etcdPrefix := flag.String("etcd-prefix", "/testp", "etcd key prefix")
	electionTTL := flag.Duration("election-ttl", 5*time.Second, "etcd controller election ttl")
	membershipTTL := flag.Duration("membership-ttl", 5*time.Second, "etcd membership ttl")
	sweepInterval := flag.Duration("sweep-interval", time.Second, "dead node sweep interval")
	shardCount := flag.Int("shards", 64, "number of shards to assign")
	metricsAddr := flag.String("metrics-addr", ":9102", "Prometheus metrics listen address; set empty to disable")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := controllerapp.Run(ctx, controllerapp.Config{
		ControllerID:  *controllerID,
		EtcdEndpoints: *etcdEndpoints,
		EtcdPrefix:    *etcdPrefix,
		ElectionTTL:   *electionTTL,
		MembershipTTL: *membershipTTL,
		SweepInterval: *sweepInterval,
		ShardCount:    *shardCount,
		MetricsAddr:   *metricsAddr,
		Output:        os.Stdout,
		ErrorOutput:   os.Stderr,
	})
	if err != nil && err != context.Canceled {
		fmt.Fprintf(os.Stderr, "controller failed: %v\n", err)
		os.Exit(1)
	}
}

func defaultControllerID() string {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		hostname = "controller"
	}
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}
