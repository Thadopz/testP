package ownership

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestEtcdOwnershipStoreAssignAndOwnerOf(t *testing.T) {
	client := startOwnershipEtcd(t)
	store := NewEtcdOwnershipStore(client, "/test")

	if err := store.Assign(3, 1); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	ownership, ok, err := store.OwnerOf(3)
	if err != nil {
		t.Fatalf("OwnerOf returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected owner to exist")
	}
	if ownership.ShardID != 3 || ownership.NodeID != 1 || ownership.Epoch != 1 {
		t.Fatalf("ownership mismatch: got %+v, want shard=3 node=1 epoch=1", ownership)
	}
}

func TestEtcdOwnershipStoreAssignIncrementsEpoch(t *testing.T) {
	client := startOwnershipEtcd(t)
	store := NewEtcdOwnershipStore(client, "/test")

	if err := store.Assign(3, 1); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}
	if err := store.Assign(3, 2); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	ownership, ok, err := store.OwnerOf(3)
	if err != nil {
		t.Fatalf("OwnerOf returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected owner to exist")
	}
	if ownership.NodeID != 2 || ownership.Epoch != 2 {
		t.Fatalf("ownership mismatch: got %+v, want node=2 epoch=2", ownership)
	}
}

func TestEtcdOwnershipStoreShardsForNodeFiltersAndSorts(t *testing.T) {
	client := startOwnershipEtcd(t)
	store := NewEtcdOwnershipStore(client, "/test")

	assignments := []struct {
		shardID int
		nodeID  int
	}{
		{shardID: 4, nodeID: 1},
		{shardID: 1, nodeID: 2},
		{shardID: 0, nodeID: 1},
		{shardID: 2, nodeID: 1},
	}

	for _, assignment := range assignments {
		if err := store.Assign(assignment.shardID, assignment.nodeID); err != nil {
			t.Fatalf("Assign returned error: %v", err)
		}
	}

	ownerships, err := store.ShardsForNode(1)
	if err != nil {
		t.Fatalf("ShardsForNode returned error: %v", err)
	}

	expectedShardIDs := []int{0, 2, 4}
	if len(ownerships) != len(expectedShardIDs) {
		t.Fatalf("ownership count mismatch: got %d, want %d", len(ownerships), len(expectedShardIDs))
	}
	for i, expectedShardID := range expectedShardIDs {
		if ownerships[i].ShardID != expectedShardID {
			t.Fatalf("shard mismatch at index %d: got %d, want %d", i, ownerships[i].ShardID, expectedShardID)
		}
	}
}

func TestEtcdOwnershipStoreOwnerOfUnknownShard(t *testing.T) {
	client := startOwnershipEtcd(t)
	store := NewEtcdOwnershipStore(client, "/test")

	_, ok, err := store.OwnerOf(99)
	if err != nil {
		t.Fatalf("OwnerOf returned error: %v", err)
	}
	if ok {
		t.Fatal("expected unknown shard to return ok=false")
	}
}

func startOwnershipEtcd(t *testing.T) *clientv3.Client {
	t.Helper()

	clientURL := freeEtcdURL(t)
	peerURL := freeEtcdURL(t)

	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	cfg.Name = "test-node"
	cfg.LogLevel = "error"
	cfg.ListenClientUrls = []url.URL{clientURL}
	cfg.AdvertiseClientUrls = []url.URL{clientURL}
	cfg.ListenPeerUrls = []url.URL{peerURL}
	cfg.AdvertisePeerUrls = []url.URL{peerURL}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	server, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatalf("StartEtcd returned error: %v", err)
	}
	t.Cleanup(server.Close)

	select {
	case <-server.Server.ReadyNotify():
	case <-time.After(15 * time.Second):
		server.Close()
		t.Fatal("timed out waiting for embedded etcd")
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientURL.String()},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("clientv3.New returned error: %v", err)
	}
	t.Cleanup(func() {
		client.Close()
	})

	if _, err := client.Status(context.Background(), clientURL.String()); err != nil {
		t.Fatalf("etcd status returned error: %v", err)
	}

	return client
}

func freeEtcdURL(t *testing.T) url.URL {
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
