package election

import (
	"context"
	"net"
	"net/url"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestEtcdElectionAllowsOnlyOneLeader(t *testing.T) {
	client := startElectionEtcd(t)

	first, err := NewEtcdElection(client, "/test", "controller-1", time.Second)
	if err != nil {
		t.Fatalf("NewEtcdElection returned error: %v", err)
	}
	second, err := NewEtcdElection(client, "/test", "controller-2", time.Second)
	if err != nil {
		t.Fatalf("NewEtcdElection returned error: %v", err)
	}

	won, err := first.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("first TryAcquire returned error: %v", err)
	}
	if !won || !first.IsLeader() {
		t.Fatal("expected first controller to become leader")
	}

	won, err = second.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("second TryAcquire returned error: %v", err)
	}
	if won || second.IsLeader() {
		t.Fatal("expected second controller to stay follower")
	}
}

func TestEtcdElectionAllowsNewLeaderAfterResign(t *testing.T) {
	client := startElectionEtcd(t)

	first, err := NewEtcdElection(client, "/test", "controller-1", time.Second)
	if err != nil {
		t.Fatalf("NewEtcdElection returned error: %v", err)
	}
	second, err := NewEtcdElection(client, "/test", "controller-2", time.Second)
	if err != nil {
		t.Fatalf("NewEtcdElection returned error: %v", err)
	}

	won, err := first.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("first TryAcquire returned error: %v", err)
	}
	if !won {
		t.Fatal("expected first controller to become leader")
	}

	if err := first.Resign(context.Background()); err != nil {
		t.Fatalf("Resign returned error: %v", err)
	}

	won, err = second.TryAcquire(context.Background())
	if err != nil {
		t.Fatalf("second TryAcquire returned error: %v", err)
	}
	if !won || !second.IsLeader() {
		t.Fatal("expected second controller to become leader")
	}
}

func TestEtcdElectionRejectsEmptyControllerID(t *testing.T) {
	client := startElectionEtcd(t)

	_, err := NewEtcdElection(client, "/test", " ", time.Second)
	if err == nil {
		t.Fatal("expected NewEtcdElection to return an error")
	}
}

func startElectionEtcd(t *testing.T) *clientv3.Client {
	t.Helper()

	clientURL := freeElectionEtcdURL(t)
	peerURL := freeElectionEtcdURL(t)

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

func freeElectionEtcdURL(t *testing.T) url.URL {
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
