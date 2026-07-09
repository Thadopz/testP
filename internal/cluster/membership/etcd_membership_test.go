package membership

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestEtcdMembershipStoreTracksAliveNodes(t *testing.T) {
	client := startMembershipEtcd(t)
	store := NewEtcdMembershipStore(client, "/test")

	if err := store.MarkAlive(2); err != nil {
		t.Fatalf("MarkAlive returned error: %v", err)
	}
	if err := store.MarkAlive(1); err != nil {
		t.Fatalf("MarkAlive returned error: %v", err)
	}

	aliveNodes, err := store.AliveNodes()
	if err != nil {
		t.Fatalf("AliveNodes returned error: %v", err)
	}
	if len(aliveNodes) != 2 || aliveNodes[0] != 1 || aliveNodes[1] != 2 {
		t.Fatalf("alive nodes mismatch: got %v, want [1 2]", aliveNodes)
	}
}

func TestEtcdMembershipStoreMarkDeadRemovesNode(t *testing.T) {
	client := startMembershipEtcd(t)
	store := NewEtcdMembershipStore(client, "/test")

	if err := store.MarkAlive(1); err != nil {
		t.Fatalf("MarkAlive returned error: %v", err)
	}
	if err := store.MarkDead(1); err != nil {
		t.Fatalf("MarkDead returned error: %v", err)
	}

	alive, err := store.IsAlive(1)
	if err != nil {
		t.Fatalf("IsAlive returned error: %v", err)
	}
	if alive {
		t.Fatal("expected node 1 to be dead")
	}
}

func TestEtcdMembershipStoreTTLExpiresNode(t *testing.T) {
	client := startMembershipEtcd(t)
	store := NewEtcdMembershipStoreWithTTL(client, "/test", time.Second)

	if err := store.MarkAlive(1); err != nil {
		t.Fatalf("MarkAlive returned error: %v", err)
	}

	alive, err := store.IsAlive(1)
	if err != nil {
		t.Fatalf("IsAlive returned error: %v", err)
	}
	if !alive {
		t.Fatal("expected node 1 to be alive")
	}

	waitForNodeDead(t, store, 1)
}

func TestEtcdMembershipStoreRejectsInvalidNodeID(t *testing.T) {
	client := startMembershipEtcd(t)
	store := NewEtcdMembershipStore(client, "/test")

	if err := store.MarkAlive(0); err == nil {
		t.Fatal("expected MarkAlive to return an error")
	}
	if err := store.MarkDead(0); err == nil {
		t.Fatal("expected MarkDead to return an error")
	}
}

func waitForNodeDead(t *testing.T, store *EtcdMembershipStore, nodeID int) {
	t.Helper()

	deadline := time.After(4 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		alive, err := store.IsAlive(nodeID)
		if err != nil {
			t.Fatalf("IsAlive returned error: %v", err)
		}
		if !alive {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("node %d did not expire", nodeID)
		case <-ticker.C:
		}
	}
}

func startMembershipEtcd(t *testing.T) *clientv3.Client {
	t.Helper()

	clientURL := freeMembershipEtcdURL(t)
	peerURL := freeMembershipEtcdURL(t)

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

func freeMembershipEtcdURL(t *testing.T) url.URL {
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
