package orderstate

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"time"

	"testP/internal/tools"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultEtcdOrderStateTimeout = 3 * time.Second

type EtcdStore struct {
	client  *clientv3.Client
	prefix  string
	timeout time.Duration
}

func NewEtcdStore(client *clientv3.Client, prefix string) *EtcdStore {
	return &EtcdStore{
		client:  client,
		prefix:  tools.CleanEtcdPrefix(prefix),
		timeout: defaultEtcdOrderStateTimeout,
	}
}

func (s *EtcdStore) Load(ctx context.Context, orderID int64) (State, bool, error) {
	if orderID <= 0 {
		return State{}, false, fmt.Errorf("order id must be > 0: %d", orderID)
	}
	if s.client == nil {
		return State{}, false, fmt.Errorf("etcd client is required")
	}

	requestCtx, cancel := s.withTimeout(ctx)
	defer cancel()

	resp, err := s.client.Get(requestCtx, s.orderKey(orderID))
	if err != nil {
		return State{}, false, fmt.Errorf("get order state from etcd: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return State{}, false, nil
	}

	state := State{}
	if err := json.Unmarshal(resp.Kvs[0].Value, &state); err != nil {
		return State{}, true, fmt.Errorf("decode order state: %w", err)
	}
	state.OrderID = orderID
	return state, true, nil
}

func (s *EtcdStore) Save(ctx context.Context, state State) error {
	if state.OrderID <= 0 {
		return fmt.Errorf("order id must be > 0: %d", state.OrderID)
	}
	if s.client == nil {
		return fmt.Errorf("etcd client is required")
	}

	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode order state: %w", err)
	}

	requestCtx, cancel := s.withTimeout(ctx)
	defer cancel()

	_, err = s.client.Put(requestCtx, s.orderKey(state.OrderID), string(data))
	if err != nil {
		return fmt.Errorf("save order state in etcd: %w", err)
	}
	return nil
}

func (s *EtcdStore) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, s.timeout)
}

func (s *EtcdStore) orderKey(orderID int64) string {
	return path.Join(s.ordersPrefix(), strconv.FormatInt(orderID, 10))
}

func (s *EtcdStore) ordersPrefix() string {
	return path.Join(s.prefix, "orderstate", "orders") + "/"
}
