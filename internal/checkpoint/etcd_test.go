package checkpoint

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

func TestEtcdStoreSaveThenLoadShardCheckpoint(t *testing.T) {
	client := startCheckpointEtcd(t)
	store := NewEtcdStore(client, "/test")

	checkpoint := ShardCheckpoint{
		ShardID:   3,
		Offset:    100,
		Epoch:     2,
		NodeID:    10,
		UpdatedAt: 1234567890,
	}

	if err := store.SaveShardCheckpoint(context.Background(), checkpoint); err != nil {
		t.Fatalf("SaveShardCheckpoint returned error: %v", err)
	}

	loaded, found, err := store.LoadShardCheckpoint(context.Background(), 3)
	if err != nil {
		t.Fatalf("LoadShardCheckpoint returned error: %v", err)
	}
	if !found {
		t.Fatal("expected shard checkpoint to be found")
	}
	if loaded != checkpoint {
		t.Fatalf("checkpoint mismatch: got %+v, want %+v", loaded, checkpoint)
	}
}

func TestEtcdStoreLoadUnknownShardCheckpointReturnsNotFound(t *testing.T) {
	client := startCheckpointEtcd(t)
	store := NewEtcdStore(client, "/test")

	_, found, err := store.LoadShardCheckpoint(context.Background(), 99)
	if err != nil {
		t.Fatalf("LoadShardCheckpoint returned error: %v", err)
	}
	if found {
		t.Fatal("expected unknown shard checkpoint to return found=false")
	}
}

func TestEtcdStoreKeepsPrefixesSeparate(t *testing.T) {
	client := startCheckpointEtcd(t)
	firstStore := NewEtcdStore(client, "/first")
	secondStore := NewEtcdStore(client, "/second")

	if err := firstStore.SaveShardCheckpoint(context.Background(), ShardCheckpoint{
		ShardID: 1,
		Offset:  10,
		Epoch:   1,
		NodeID:  10,
	}); err != nil {
		t.Fatalf("first SaveShardCheckpoint returned error: %v", err)
	}
	if err := secondStore.SaveShardCheckpoint(context.Background(), ShardCheckpoint{
		ShardID: 1,
		Offset:  20,
		Epoch:   1,
		NodeID:  20,
	}); err != nil {
		t.Fatalf("second SaveShardCheckpoint returned error: %v", err)
	}

	firstCheckpoint, found, err := firstStore.LoadShardCheckpoint(context.Background(), 1)
	if err != nil {
		t.Fatalf("first LoadShardCheckpoint returned error: %v", err)
	}
	if !found {
		t.Fatal("expected first checkpoint to be found")
	}

	secondCheckpoint, found, err := secondStore.LoadShardCheckpoint(context.Background(), 1)
	if err != nil {
		t.Fatalf("second LoadShardCheckpoint returned error: %v", err)
	}
	if !found {
		t.Fatal("expected second checkpoint to be found")
	}

	if firstCheckpoint.Offset != 10 || firstCheckpoint.NodeID != 10 {
		t.Fatalf("first checkpoint mismatch: got %+v", firstCheckpoint)
	}
	if secondCheckpoint.Offset != 20 || secondCheckpoint.NodeID != 20 {
		t.Fatalf("second checkpoint mismatch: got %+v", secondCheckpoint)
	}
}

func TestEtcdStoreAllowsNewerCheckpoint(t *testing.T) {
	client := startCheckpointEtcd(t)
	store := NewEtcdStore(client, "/test")

	if err := store.SaveShardCheckpoint(context.Background(), ShardCheckpoint{
		ShardID: 1,
		Offset:  10,
		Epoch:   1,
		NodeID:  10,
	}); err != nil {
		t.Fatalf("first SaveShardCheckpoint returned error: %v", err)
	}

	if err := store.SaveShardCheckpoint(context.Background(), ShardCheckpoint{
		ShardID: 1,
		Offset:  11,
		Epoch:   1,
		NodeID:  10,
	}); err != nil {
		t.Fatalf("same epoch SaveShardCheckpoint returned error: %v", err)
	}

	if err := store.SaveShardCheckpoint(context.Background(), ShardCheckpoint{
		ShardID: 1,
		Offset:  12,
		Epoch:   2,
		NodeID:  20,
	}); err != nil {
		t.Fatalf("new epoch SaveShardCheckpoint returned error: %v", err)
	}

	loaded, found, err := store.LoadShardCheckpoint(context.Background(), 1)
	if err != nil {
		t.Fatalf("LoadShardCheckpoint returned error: %v", err)
	}
	if !found {
		t.Fatal("expected checkpoint to be found")
	}
	if loaded.Offset != 12 || loaded.Epoch != 2 || loaded.NodeID != 20 {
		t.Fatalf("checkpoint mismatch: got %+v", loaded)
	}
}

func TestEtcdStoreRejectsStaleCheckpoint(t *testing.T) {
	client := startCheckpointEtcd(t)
	store := NewEtcdStore(client, "/test")

	if err := store.SaveShardCheckpoint(context.Background(), ShardCheckpoint{
		ShardID: 1,
		Offset:  10,
		Epoch:   2,
		NodeID:  20,
	}); err != nil {
		t.Fatalf("SaveShardCheckpoint returned error: %v", err)
	}

	err := store.SaveShardCheckpoint(context.Background(), ShardCheckpoint{
		ShardID: 1,
		Offset:  11,
		Epoch:   1,
		NodeID:  10,
	})
	if !errors.Is(err, ErrStaleShardCheckpoint) {
		t.Fatalf("old epoch error mismatch: got %v, want ErrStaleShardCheckpoint", err)
	}

	err = store.SaveShardCheckpoint(context.Background(), ShardCheckpoint{
		ShardID: 1,
		Offset:  9,
		Epoch:   2,
		NodeID:  20,
	})
	if !errors.Is(err, ErrStaleShardCheckpoint) {
		t.Fatalf("older offset error mismatch: got %v, want ErrStaleShardCheckpoint", err)
	}

	err = store.SaveShardCheckpoint(context.Background(), ShardCheckpoint{
		ShardID: 1,
		Offset:  9,
		Epoch:   3,
		NodeID:  30,
	})
	if !errors.Is(err, ErrStaleShardCheckpoint) {
		t.Fatalf("new epoch older offset error mismatch: got %v, want ErrStaleShardCheckpoint", err)
	}

	loaded, found, err := store.LoadShardCheckpoint(context.Background(), 1)
	if err != nil {
		t.Fatalf("LoadShardCheckpoint returned error: %v", err)
	}
	if !found {
		t.Fatal("expected checkpoint to be found")
	}
	if loaded.Offset != 10 || loaded.Epoch != 2 || loaded.NodeID != 20 {
		t.Fatalf("checkpoint should not change after stale writes: got %+v", loaded)
	}
}

func TestEtcdStoreRejectsInvalidShardCheckpoint(t *testing.T) {
	client := startCheckpointEtcd(t)
	store := NewEtcdStore(client, "/test")

	tests := []struct {
		name       string
		checkpoint ShardCheckpoint
	}{
		{
			name: "negative shard",
			checkpoint: ShardCheckpoint{
				ShardID: -1,
				Offset:  1,
				Epoch:   1,
				NodeID:  1,
			},
		},
		{
			name: "negative offset",
			checkpoint: ShardCheckpoint{
				ShardID: 1,
				Offset:  -1,
				Epoch:   1,
				NodeID:  1,
			},
		},
		{
			name: "zero epoch",
			checkpoint: ShardCheckpoint{
				ShardID: 1,
				Offset:  1,
				Epoch:   0,
				NodeID:  1,
			},
		},
		{
			name: "zero node",
			checkpoint: ShardCheckpoint{
				ShardID: 1,
				Offset:  1,
				Epoch:   1,
				NodeID:  0,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := store.SaveShardCheckpoint(context.Background(), test.checkpoint)
			if err == nil {
				t.Fatal("expected SaveShardCheckpoint to return an error")
			}
		})
	}
}

func TestEtcdStoreReturnsContextError(t *testing.T) {
	client := startCheckpointEtcd(t)
	store := NewEtcdStore(client, "/test")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.SaveShardCheckpoint(ctx, ShardCheckpoint{
		ShardID: 1,
		Offset:  1,
		Epoch:   1,
		NodeID:  1,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SaveShardCheckpoint error mismatch: got %v, want context.Canceled", err)
	}

	_, _, err = store.LoadShardCheckpoint(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadShardCheckpoint error mismatch: got %v, want context.Canceled", err)
	}
}

func startCheckpointEtcd(t *testing.T) *clientv3.Client {
	t.Helper()

	clientURL := freeCheckpointEtcdURL(t)
	peerURL := freeCheckpointEtcdURL(t)

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

func freeCheckpointEtcdURL(t *testing.T) url.URL {
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
