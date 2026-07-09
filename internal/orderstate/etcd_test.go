package orderstate

import (
	"context"
	"errors"
	"net"
	"net/url"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

func TestEtcdStoreSaveThenLoad(t *testing.T) {
	client := startOrderStateEtcd(t)
	store := NewEtcdStore(client, "/test")

	state := State{
		OrderID:     1001,
		ShardID:     3,
		Status:      StatusMatched,
		RiderID:     99,
		Score:       88,
		LastEventID: "event-1",
	}

	if err := store.Save(context.Background(), state); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}

	loaded, found, err := store.Load(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !found {
		t.Fatal("expected order state to be found")
	}
	if loaded != state {
		t.Fatalf("state mismatch: got %+v, want %+v", loaded, state)
	}
}

func TestEtcdStoreLoadUnknownOrderReturnsNotFound(t *testing.T) {
	client := startOrderStateEtcd(t)
	store := NewEtcdStore(client, "/test")

	_, found, err := store.Load(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if found {
		t.Fatal("expected unknown order to be missing")
	}
}

func TestEtcdStoreKeepsPrefixesSeparate(t *testing.T) {
	client := startOrderStateEtcd(t)
	firstStore := NewEtcdStore(client, "/first")
	secondStore := NewEtcdStore(client, "/second")

	if err := firstStore.Save(context.Background(), State{OrderID: 1, Status: StatusSubmitted}); err != nil {
		t.Fatalf("first Save returned error: %v", err)
	}
	if err := secondStore.Save(context.Background(), State{OrderID: 1, Status: StatusMissed}); err != nil {
		t.Fatalf("second Save returned error: %v", err)
	}

	firstState, found, err := firstStore.Load(context.Background(), 1)
	if err != nil {
		t.Fatalf("first Load returned error: %v", err)
	}
	if !found {
		t.Fatal("expected first state to be found")
	}

	secondState, found, err := secondStore.Load(context.Background(), 1)
	if err != nil {
		t.Fatalf("second Load returned error: %v", err)
	}
	if !found {
		t.Fatal("expected second state to be found")
	}

	if firstState.Status != StatusSubmitted {
		t.Fatalf("first state mismatch: got %+v", firstState)
	}
	if secondState.Status != StatusMissed {
		t.Fatalf("second state mismatch: got %+v", secondState)
	}
}

func TestEtcdStoreReturnsContextError(t *testing.T) {
	client := startOrderStateEtcd(t)
	store := NewEtcdStore(client, "/test")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.Save(ctx, State{OrderID: 1, Status: StatusSubmitted})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Save error mismatch: got %v, want context.Canceled", err)
	}

	_, _, err = store.Load(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Load error mismatch: got %v, want context.Canceled", err)
	}
}

func startOrderStateEtcd(t *testing.T) *clientv3.Client {
	t.Helper()

	clientURL := freeOrderStateEtcdURL(t)
	peerURL := freeOrderStateEtcdURL(t)

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

func freeOrderStateEtcdURL(t *testing.T) url.URL {
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
