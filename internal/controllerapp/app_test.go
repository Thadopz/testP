package controllerapp

import (
	"context"
	"net"
	"net/url"
	"reflect"
	clusterelection "testP/internal/cluster/election"
	"testP/internal/cluster/failover"
	"testP/internal/cluster/membership"
	"testP/internal/cluster/ownership"
	"testP/internal/cluster/rebalance"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestParseEtcdEndpoints(t *testing.T) {
	endpoints, err := ParseEtcdEndpoints("127.0.0.1:2379, 127.0.0.1:2381")
	if err != nil {
		t.Fatalf("ParseEtcdEndpoints returned error: %v", err)
	}
	if len(endpoints) != 2 {
		t.Fatalf("endpoint count mismatch: got %d, want 2", len(endpoints))
	}
	if endpoints[0] != "127.0.0.1:2379" || endpoints[1] != "127.0.0.1:2381" {
		t.Fatalf("endpoints mismatch: got %v", endpoints)
	}
}

func TestParseEtcdEndpointsRejectsEmptyInput(t *testing.T) {
	_, err := ParseEtcdEndpoints(" , ")
	if err == nil {
		t.Fatal("expected ParseEtcdEndpoints to return an error")
	}
}

func TestSweepKeepsOwnershipAfterLeaderSwitch(t *testing.T) {
	client := startControllerEtcd(t)
	prefix := "/test-controller-leader-switch"
	ownershipStore := ownership.NewEtcdOwnershipStore(client, prefix)
	membershipStore := membership.NewEtcdMembershipStore(client, prefix)
	failoverController := failover.NewFailoverController(ownershipStore, membershipStore)

	if err := membershipStore.MarkAlive(1); err != nil {
		t.Fatalf("MarkAlive returned error: %v", err)
	}
	if err := membershipStore.MarkAlive(2); err != nil {
		t.Fatalf("MarkAlive returned error: %v", err)
	}

	firstElection, err := clusterelection.NewEtcdElection(client, prefix, "controller-1", time.Second)
	if err != nil {
		t.Fatalf("NewEtcdElection returned error: %v", err)
	}
	secondElection, err := clusterelection.NewEtcdElection(client, prefix, "controller-2", time.Second)
	if err != nil {
		t.Fatalf("NewEtcdElection returned error: %v", err)
	}

	Sweep(context.Background(), ownershipStore, membershipStore, failoverController, firstElection, 6)
	before := mustListOwnerships(t, ownershipStore)
	if len(before) != 6 {
		t.Fatalf("ownership count mismatch: got %d, want 6", len(before))
	}

	if err := firstElection.Resign(context.Background()); err != nil {
		t.Fatalf("Resign returned error: %v", err)
	}

	Sweep(context.Background(), ownershipStore, membershipStore, failoverController, secondElection, 6)
	after := mustListOwnerships(t, ownershipStore)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("ownership changed after leader switch:\nbefore=%+v\nafter=%+v", before, after)
	}
}

func TestSweepRecoversAfterEtcdTemporarilyUnavailable(t *testing.T) {
	etcd := startRestartableControllerEtcd(t)
	prefix := "/test-controller-etcd-recovery"
	client := newControllerEtcdClient(t, etcd.clientURL.String())
	ownershipStore := ownership.NewEtcdOwnershipStore(client, prefix)
	membershipStore := membership.NewEtcdMembershipStore(client, prefix)
	failoverController := failover.NewFailoverController(ownershipStore, membershipStore)
	leaderElection, err := clusterelection.NewEtcdElection(client, prefix, "controller-1", time.Second)
	if err != nil {
		t.Fatalf("NewEtcdElection returned error: %v", err)
	}

	if err := membershipStore.MarkAlive(1); err != nil {
		t.Fatalf("MarkAlive returned error: %v", err)
	}
	Sweep(context.Background(), ownershipStore, membershipStore, failoverController, leaderElection, 4)
	before := mustListOwnerships(t, ownershipStore)
	if len(before) != 4 {
		t.Fatalf("ownership count mismatch: got %d, want 4", len(before))
	}

	etcd.stop()
	Sweep(context.Background(), ownershipStore, membershipStore, failoverController, leaderElection, 4)

	client.Close()
	time.Sleep(1200 * time.Millisecond)
	etcd.restart(t)

	recoveredClient := newControllerEtcdClient(t, etcd.clientURL.String())
	recoveredOwnershipStore := ownership.NewEtcdOwnershipStore(recoveredClient, prefix)
	recoveredMembershipStore := membership.NewEtcdMembershipStore(recoveredClient, prefix)
	recoveredFailoverController := failover.NewFailoverController(recoveredOwnershipStore, recoveredMembershipStore)
	recoveredElection, err := clusterelection.NewEtcdElection(recoveredClient, prefix, "controller-1", time.Second)
	if err != nil {
		t.Fatalf("NewEtcdElection after restart returned error: %v", err)
	}
	if err := recoveredMembershipStore.MarkAlive(1); err != nil {
		t.Fatalf("MarkAlive after restart returned error: %v", err)
	}

	Sweep(context.Background(), recoveredOwnershipStore, recoveredMembershipStore, recoveredFailoverController, recoveredElection, 4)
	after := mustListOwnerships(t, recoveredOwnershipStore)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("ownership changed after etcd recovery:\nbefore=%+v\nafter=%+v", before, after)
	}
}

func TestSweepRecordsMetrics(t *testing.T) {
	client := startControllerEtcd(t)
	prefix := "/test-controller-metrics"
	ownershipStore := ownership.NewEtcdOwnershipStore(client, prefix)
	membershipStore := membership.NewEtcdMembershipStore(client, prefix)
	failoverController := failover.NewFailoverController(ownershipStore, membershipStore)
	rebalanceController := rebalance.NewController(ownershipStore, membershipStore)
	recorder := &controllerTestMetricsRecorder{}

	if err := membershipStore.MarkAlive(1); err != nil {
		t.Fatalf("MarkAlive returned error: %v", err)
	}
	if err := membershipStore.MarkAlive(2); err != nil {
		t.Fatalf("MarkAlive returned error: %v", err)
	}
	assignments := []struct {
		shardID int
		nodeID  int
	}{
		{shardID: 0, nodeID: 1},
		{shardID: 1, nodeID: 99},
		{shardID: 2, nodeID: 2},
		{shardID: 3, nodeID: 99},
	}
	for _, assignment := range assignments {
		if err := ownershipStore.Assign(assignment.shardID, assignment.nodeID); err != nil {
			t.Fatalf("Assign returned error: %v", err)
		}
	}

	leaderElection, err := clusterelection.NewEtcdElection(client, prefix, "controller-1", time.Second)
	if err != nil {
		t.Fatalf("NewEtcdElection returned error: %v", err)
	}

	sweepWithMetrics(
		context.Background(),
		ownershipStore,
		membershipStore,
		failoverController,
		rebalanceController,
		leaderElection,
		4,
		"controller-1",
		recorder,
		nil,
		nil,
	)

	if !recorder.leader {
		t.Fatal("expected controller leader metric to be true")
	}
	if recorder.sweeps != 1 {
		t.Fatalf("sweep count mismatch: got %d, want 1", recorder.sweeps)
	}
	if recorder.failovers[99] != 1 {
		t.Fatalf("failover count mismatch: got %d, want 1", recorder.failovers[99])
	}
	if recorder.aliveNodes != 2 {
		t.Fatalf("alive nodes mismatch: got %d, want 2", recorder.aliveNodes)
	}
	if recorder.ownedShards != 4 {
		t.Fatalf("owned shards mismatch: got %d, want 4", recorder.ownedShards)
	}
	if recorder.shardsWithoutOwner != 0 {
		t.Fatalf("shards without owner mismatch: got %d, want 0", recorder.shardsWithoutOwner)
	}
}

func mustListOwnerships(t *testing.T, store ownership.OwnershipStore) []ownership.Ownership {
	t.Helper()

	lister, ok := store.(ownership.OwnershipLister)
	if !ok {
		t.Fatal("ownership store cannot list all ownerships")
	}
	ownerships, err := lister.AllOwnerships()
	if err != nil {
		t.Fatalf("AllOwnerships returned error: %v", err)
	}
	return ownerships
}

type controllerTestMetricsRecorder struct {
	leader             bool
	sweeps             int
	sweepErrors        map[string]int
	failovers          map[int]int
	aliveNodes         int
	ownedShards        int
	shardsWithoutOwner int
}

func (r *controllerTestMetricsRecorder) SetNodeOwnedShards(nodeID int, count int) {}

func (r *controllerTestMetricsRecorder) SetNodeSubmitted(nodeID int, value int64) {}

func (r *controllerTestMetricsRecorder) SetNodeMatched(nodeID int, value int64) {}

func (r *controllerTestMetricsRecorder) SetNodeMissed(nodeID int, value int64) {}

func (r *controllerTestMetricsRecorder) SetNodeOnlineRiders(nodeID int, value int) {}

func (r *controllerTestMetricsRecorder) SetShardCheckpointOffset(nodeID int, shardID int, offset int64) {
}

func (r *controllerTestMetricsRecorder) SetShardEventLogOffset(nodeID int, shardID int, offset int64) {
}

func (r *controllerTestMetricsRecorder) SetShardLag(nodeID int, shardID int, lag int64) {}

func (r *controllerTestMetricsRecorder) SetShardEpoch(nodeID int, shardID int, epoch int64) {}

func (r *controllerTestMetricsRecorder) IncEventApply(nodeID int, shardID int, eventType string) {}

func (r *controllerTestMetricsRecorder) IncEventApplyError(nodeID int, shardID int, eventType string) {
}

func (r *controllerTestMetricsRecorder) IncFencingReject(nodeID int, shardID int) {}

func (r *controllerTestMetricsRecorder) SetControllerLeader(controllerID string, leader bool) {
	r.leader = leader
}

func (r *controllerTestMetricsRecorder) IncControllerSweep(controllerID string) {
	r.sweeps++
}

func (r *controllerTestMetricsRecorder) IncControllerSweepError(controllerID string, reason string) {
	if r.sweepErrors == nil {
		r.sweepErrors = make(map[string]int)
	}
	r.sweepErrors[reason]++
}

func (r *controllerTestMetricsRecorder) IncFailover(controllerID string, deadNodeID int) {
	if r.failovers == nil {
		r.failovers = make(map[int]int)
	}
	r.failovers[deadNodeID]++
}

func (r *controllerTestMetricsRecorder) SetAliveNodes(controllerID string, count int) {
	r.aliveNodes = count
}

func (r *controllerTestMetricsRecorder) SetOwnedShards(controllerID string, count int) {
	r.ownedShards = count
}

func (r *controllerTestMetricsRecorder) SetShardsWithoutOwner(controllerID string, count int) {
	r.shardsWithoutOwner = count
}

func (r *controllerTestMetricsRecorder) IncProducerEvent(eventType string, shardID int) {}

func (r *controllerTestMetricsRecorder) IncProducerError(reason string) {}

type restartableControllerEtcd struct {
	dir       string
	clientURL url.URL
	peerURL   url.URL
	server    *embed.Etcd
}

func startRestartableControllerEtcd(t *testing.T) *restartableControllerEtcd {
	t.Helper()

	etcd := &restartableControllerEtcd{
		dir:       t.TempDir(),
		clientURL: freeControllerEtcdURL(t),
		peerURL:   freeControllerEtcdURL(t),
	}
	etcd.restart(t)
	t.Cleanup(etcd.stop)
	return etcd
}

func (e *restartableControllerEtcd) restart(t *testing.T) {
	t.Helper()

	cfg := embed.NewConfig()
	cfg.Dir = e.dir
	cfg.Name = "test-node"
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{e.clientURL}
	cfg.AdvertiseClientUrls = []url.URL{e.clientURL}
	cfg.ListenPeerUrls = []url.URL{e.peerURL}
	cfg.AdvertisePeerUrls = []url.URL{e.peerURL}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	server, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("StartEtcd returned error: %v", err)
	}
	e.server = server

	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(15 * time.Second):
		server.Close()
		t.Fatal("timed out waiting for embedded etcd")
	}
}

func (e *restartableControllerEtcd) stop() {
	if e.server == nil {
		return
	}
	e.server.Close()
	e.server = nil
}

func startControllerEtcd(t *testing.T) *clientv3.Client {
	t.Helper()

	etcd := startRestartableControllerEtcd(t)
	return newControllerEtcdClient(t, etcd.clientURL.String())
}

func newControllerEtcdClient(t *testing.T, endpoint string) *clientv3.Client {
	t.Helper()

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("clientv3.New returned error: %v", err)
	}
	t.Cleanup(func() {
		client.Close()
	})

	waitForControllerEtcdClient(t, client, endpoint)
	return client
}

func waitForControllerEtcdClient(t *testing.T, client *clientv3.Client, endpoint string) {
	t.Helper()

	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := client.Status(ctx, endpoint)
		cancel()
		if err == nil {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for etcd client to recover: %v", err)
		case <-ticker.C:
		}
	}
}

func freeControllerEtcdURL(t *testing.T) url.URL {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("Close listener returned error: %v", err)
	}

	parsed, err := url.Parse("http://" + address)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	return *parsed
}
