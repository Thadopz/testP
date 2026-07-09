package ownership

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strconv"
	"time"

	"testP/internal/tools"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultEtcdRequestTimeout = 3 * time.Second
const maxEtcdAssignRetries = 8

type EtcdOwnershipStore struct {
	client  *clientv3.Client
	prefix  string
	timeout time.Duration
}

func NewEtcdOwnershipStore(client *clientv3.Client, prefix string) *EtcdOwnershipStore {
	return &EtcdOwnershipStore{
		client:  client,
		prefix:  tools.CleanEtcdPrefix(prefix),
		timeout: defaultEtcdRequestTimeout,
	}
}

func (s *EtcdOwnershipStore) OwnerOf(shardID int) (Ownership, bool, error) {
	if s.client == nil {
		return Ownership{}, false, fmt.Errorf("etcd client is required")
	}
	if shardID < 0 {
		return Ownership{}, false, fmt.Errorf("shard id must be >= 0: %d", shardID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	resp, err := s.client.Get(ctx, s.shardKey(shardID))
	if err != nil {
		return Ownership{}, false, fmt.Errorf("get ownership from etcd: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return Ownership{}, false, nil
	}

	ownership, err := decodeOwnership(resp.Kvs[0].Value)
	if err != nil {
		return Ownership{}, true, err
	}
	ownership.ShardID = shardID
	return ownership, true, nil
}

func (s *EtcdOwnershipStore) ShardsForNode(nodeID int) ([]Ownership, error) {
	if s.client == nil {
		return nil, fmt.Errorf("etcd client is required")
	}
	if nodeID <= 0 {
		return nil, fmt.Errorf("node id must be > 0: %d", nodeID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	resp, err := s.client.Get(ctx, s.shardsPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list ownership from etcd: %w", err)
	}

	ownerships := make([]Ownership, 0)
	for _, keyValue := range resp.Kvs {
		ownership, err := decodeOwnership(keyValue.Value)
		if err != nil {
			return nil, err
		}
		if ownership.NodeID == nodeID {
			ownerships = append(ownerships, ownership)
		}
	}

	slices.SortFunc(ownerships, func(a, b Ownership) int {
		return cmp.Compare(a.ShardID, b.ShardID)
	})

	return ownerships, nil
}

func (s *EtcdOwnershipStore) AllOwnerships() ([]Ownership, error) {
	if s.client == nil {
		return nil, fmt.Errorf("etcd client is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	resp, err := s.client.Get(ctx, s.shardsPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list ownership from etcd: %w", err)
	}

	ownerships := make([]Ownership, 0, len(resp.Kvs))
	for _, keyValue := range resp.Kvs {
		ownership, err := decodeOwnership(keyValue.Value)
		if err != nil {
			return nil, err
		}
		ownerships = append(ownerships, ownership)
	}

	slices.SortFunc(ownerships, func(a, b Ownership) int {
		return cmp.Compare(a.ShardID, b.ShardID)
	})

	return ownerships, nil
}

func (s *EtcdOwnershipStore) Assign(shardID int, nodeID int) error {
	if s.client == nil {
		return fmt.Errorf("etcd client is required")
	}
	if shardID < 0 {
		return fmt.Errorf("shard id must be >= 0: %d", shardID)
	}
	if nodeID <= 0 {
		return fmt.Errorf("node id must be > 0: %d", nodeID)
	}

	key := s.shardKey(shardID)
	for attempt := 0; attempt < maxEtcdAssignRetries; attempt++ {
		if assigned, err := s.tryAssign(key, shardID, nodeID); err != nil {
			return err
		} else if assigned {
			return nil
		}
	}

	return fmt.Errorf("assign shard %d to node %d failed after retries", shardID, nodeID)
}

func (s *EtcdOwnershipStore) tryAssign(key string, shardID int, nodeID int) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return false, fmt.Errorf("get ownership before assign: %w", err)
	}

	ownership := Ownership{
		ShardID: shardID,
		NodeID:  nodeID,
		Epoch:   1,
	}
	var compare clientv3.Cmp
	if len(resp.Kvs) == 0 {
		compare = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	} else {
		current, err := decodeOwnership(resp.Kvs[0].Value)
		if err != nil {
			return false, err
		}
		ownership.Epoch = current.Epoch + 1
		compare = clientv3.Compare(clientv3.ModRevision(key), "=", resp.Kvs[0].ModRevision)
	}

	data, err := json.Marshal(ownership)
	if err != nil {
		return false, fmt.Errorf("encode ownership: %w", err)
	}

	txnResp, err := s.client.Txn(ctx).
		If(compare).
		Then(clientv3.OpPut(key, string(data))).
		Commit()
	if err != nil {
		return false, fmt.Errorf("assign ownership in etcd: %w", err)
	}

	return txnResp.Succeeded, nil
}

func (s *EtcdOwnershipStore) shardKey(shardID int) string {
	return path.Join(s.shardsPrefix(), strconv.Itoa(shardID))
}

func (s *EtcdOwnershipStore) shardsPrefix() string {
	return path.Join(s.prefix, "ownership", "shards") + "/"
}

func decodeOwnership(data []byte) (Ownership, error) {
	ownership := Ownership{}
	if err := json.Unmarshal(data, &ownership); err != nil {
		return Ownership{}, fmt.Errorf("decode ownership: %w", err)
	}
	return ownership, nil
}
