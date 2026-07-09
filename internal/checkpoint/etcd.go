package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var ErrStaleShardCheckpoint = errors.New("stale shard checkpoint")

const defaultEtcdCheckpointTimeout = 3 * time.Second
const maxEtcdCheckpointRetries = 8

type EtcdStore struct {
	client  *clientv3.Client
	prefix  string
	timeout time.Duration
}

func NewEtcdStore(client *clientv3.Client, prefix string) *EtcdStore {
	return &EtcdStore{
		client:  client,
		prefix:  cleanEtcdPrefix(prefix),
		timeout: defaultEtcdCheckpointTimeout,
	}
}

func (s *EtcdStore) SaveShardCheckpoint(ctx context.Context, checkpoint ShardCheckpoint) error {
	if err := validateShardCheckpoint(checkpoint); err != nil {
		return err
	}
	if s.client == nil {
		return fmt.Errorf("etcd client is required")
	}

	key := s.shardCheckpointKey(checkpoint.ShardID)
	for attempt := 0; attempt < maxEtcdCheckpointRetries; attempt++ {
		saved, err := s.trySaveShardCheckpoint(ctx, key, checkpoint)
		if err != nil {
			return err
		}
		if saved {
			return nil
		}
	}

	return fmt.Errorf("save shard checkpoint %d failed after retries", checkpoint.ShardID)
}

func (s *EtcdStore) LoadShardCheckpoint(ctx context.Context, shardID int) (ShardCheckpoint, bool, error) {
	if shardID < 0 {
		return ShardCheckpoint{}, false, fmt.Errorf("shard id must be >= 0: %d", shardID)
	}
	if s.client == nil {
		return ShardCheckpoint{}, false, fmt.Errorf("etcd client is required")
	}

	requestCtx, cancel := s.withTimeout(ctx)
	defer cancel()

	resp, err := s.client.Get(requestCtx, s.shardCheckpointKey(shardID))
	if err != nil {
		return ShardCheckpoint{}, false, fmt.Errorf("get shard checkpoint from etcd: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return ShardCheckpoint{}, false, nil
	}

	checkpoint, err := decodeShardCheckpoint(resp.Kvs[0].Value)
	if err != nil {
		return ShardCheckpoint{}, true, err
	}
	checkpoint.ShardID = shardID
	return checkpoint, true, nil
}

func (s *EtcdStore) trySaveShardCheckpoint(ctx context.Context, key string, next ShardCheckpoint) (bool, error) {
	requestCtx, cancel := s.withTimeout(ctx)
	defer cancel()

	resp, err := s.client.Get(requestCtx, key)
	if err != nil {
		return false, fmt.Errorf("get shard checkpoint before save: %w", err)
	}

	var compare clientv3.Cmp
	var current ShardCheckpoint
	found := len(resp.Kvs) > 0
	if found {
		current, err = decodeShardCheckpoint(resp.Kvs[0].Value)
		if err != nil {
			return false, err
		}
		compare = clientv3.Compare(clientv3.ModRevision(key), "=", resp.Kvs[0].ModRevision)
	} else {
		compare = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	}

	if !shouldSaveShardCheckpoint(current, found, next) {
		return false, ErrStaleShardCheckpoint
	}

	data, err := json.Marshal(next)
	if err != nil {
		return false, fmt.Errorf("encode shard checkpoint: %w", err)
	}

	txnResp, err := s.client.Txn(requestCtx).
		If(compare).
		Then(clientv3.OpPut(key, string(data))).
		Commit()
	if err != nil {
		return false, fmt.Errorf("save shard checkpoint in etcd: %w", err)
	}

	return txnResp.Succeeded, nil
}

func shouldSaveShardCheckpoint(current ShardCheckpoint, found bool, next ShardCheckpoint) bool {
	if !found {
		return true
	}
	if next.Offset < current.Offset {
		return false
	}
	if next.Epoch < current.Epoch {
		return false
	}
	return true
}

func validateShardCheckpoint(checkpoint ShardCheckpoint) error {
	if checkpoint.ShardID < 0 {
		return fmt.Errorf("shard id must be >= 0: %d", checkpoint.ShardID)
	}
	if checkpoint.Offset < 0 {
		return fmt.Errorf("checkpoint offset must be >= 0: %d", checkpoint.Offset)
	}
	if checkpoint.Epoch <= 0 {
		return fmt.Errorf("checkpoint epoch must be > 0: %d", checkpoint.Epoch)
	}
	if checkpoint.NodeID <= 0 {
		return fmt.Errorf("checkpoint node id must be > 0: %d", checkpoint.NodeID)
	}
	return nil
}

func decodeShardCheckpoint(data []byte) (ShardCheckpoint, error) {
	checkpoint := ShardCheckpoint{}
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return ShardCheckpoint{}, fmt.Errorf("decode shard checkpoint: %w", err)
	}
	return checkpoint, nil
}

func (s *EtcdStore) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, s.timeout)
}

func (s *EtcdStore) shardCheckpointKey(shardID int) string {
	return path.Join(s.shardCheckpointPrefix(), strconv.Itoa(shardID))
}

func (s *EtcdStore) shardCheckpointPrefix() string {
	return path.Join(s.prefix, "checkpoints", "shards") + "/"
}

func cleanEtcdPrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return "/testp"
	}
	if !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}
	return path.Clean(prefix)
}
