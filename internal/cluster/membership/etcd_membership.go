package membership

import (
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

type EtcdMembershipStore struct {
	client  *clientv3.Client
	prefix  string
	timeout time.Duration
	ttl     time.Duration
}

type etcdNodeState struct {
	NodeID int
}

func NewEtcdMembershipStore(client *clientv3.Client, prefix string) *EtcdMembershipStore {
	return &EtcdMembershipStore{
		client:  client,
		prefix:  tools.CleanEtcdPrefix(prefix),
		timeout: defaultEtcdRequestTimeout,
	}
}

func NewEtcdMembershipStoreWithTTL(client *clientv3.Client, prefix string, ttl time.Duration) *EtcdMembershipStore {
	store := NewEtcdMembershipStore(client, prefix)
	store.ttl = ttl
	return store
}

func (s *EtcdMembershipStore) MarkAlive(nodeID int) error {
	if s.client == nil {
		return fmt.Errorf("etcd client is required")
	}
	if nodeID <= 0 {
		return fmt.Errorf("node id must be > 0: %d", nodeID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	data, err := json.Marshal(etcdNodeState{NodeID: nodeID})
	if err != nil {
		return fmt.Errorf("encode membership state: %w", err)
	}

	if s.ttl <= 0 {
		_, err = s.client.Put(ctx, s.nodeKey(nodeID), string(data))
		if err != nil {
			return fmt.Errorf("mark node alive in etcd: %w", err)
		}
		return nil
	}

	leaseResp, err := s.client.Grant(ctx, leaseTTLSeconds(s.ttl))
	if err != nil {
		return fmt.Errorf("grant membership lease: %w", err)
	}
	_, err = s.client.Put(ctx, s.nodeKey(nodeID), string(data), clientv3.WithLease(leaseResp.ID))
	if err != nil {
		return fmt.Errorf("mark node alive in etcd: %w", err)
	}
	return nil
}

func (s *EtcdMembershipStore) MarkDead(nodeID int) error {
	if s.client == nil {
		return fmt.Errorf("etcd client is required")
	}
	if nodeID <= 0 {
		return fmt.Errorf("node id must be > 0: %d", nodeID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	_, err := s.client.Delete(ctx, s.nodeKey(nodeID))
	if err != nil {
		return fmt.Errorf("mark node dead in etcd: %w", err)
	}
	return nil
}

func (s *EtcdMembershipStore) AliveNodes() ([]int, error) {
	if s.client == nil {
		return nil, fmt.Errorf("etcd client is required")
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	resp, err := s.client.Get(ctx, s.nodesPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list alive nodes from etcd: %w", err)
	}

	nodeIDs := make([]int, 0, len(resp.Kvs))
	for _, keyValue := range resp.Kvs {
		nodeID, err := decodeNodeID(keyValue.Value)
		if err != nil {
			return nil, err
		}
		nodeIDs = append(nodeIDs, nodeID)
	}

	slices.Sort(nodeIDs)

	return nodeIDs, nil
}

func (s *EtcdMembershipStore) IsAlive(nodeID int) (bool, error) {
	if s.client == nil {
		return false, fmt.Errorf("etcd client is required")
	}
	if nodeID <= 0 {
		return false, fmt.Errorf("node id must be > 0: %d", nodeID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()

	resp, err := s.client.Get(ctx, s.nodeKey(nodeID), clientv3.WithCountOnly())
	if err != nil {
		return false, fmt.Errorf("get alive node from etcd: %w", err)
	}

	return resp.Count > 0, nil
}

func (s *EtcdMembershipStore) nodeKey(nodeID int) string {
	return path.Join(s.nodesPrefix(), strconv.Itoa(nodeID))
}

func (s *EtcdMembershipStore) nodesPrefix() string {
	return path.Join(s.prefix, "membership", "nodes") + "/"
}

func decodeNodeID(data []byte) (int, error) {
	state := etcdNodeState{}
	if err := json.Unmarshal(data, &state); err != nil {
		return 0, fmt.Errorf("decode membership state: %w", err)
	}
	if state.NodeID <= 0 {
		return 0, fmt.Errorf("invalid node id in membership state: %d", state.NodeID)
	}
	return state.NodeID, nil
}

func leaseTTLSeconds(ttl time.Duration) int64 {
	seconds := int64(ttl / time.Second)
	if ttl%time.Second != 0 {
		seconds++
	}
	if seconds <= 0 {
		return 1
	}
	return seconds
}
