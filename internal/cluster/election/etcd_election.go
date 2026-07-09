package election

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"testP/internal/tools"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const defaultElectionTimeout = 3 * time.Second

type EtcdElection struct {
	client     *clientv3.Client
	key        string
	value      string
	ttl        time.Duration
	timeout    time.Duration
	mu         sync.Mutex
	leader     bool
	leaseID    clientv3.LeaseID
	keepCancel context.CancelFunc
}

func NewEtcdElection(client *clientv3.Client, prefix string, controllerID string, ttl time.Duration) (*EtcdElection, error) {
	if client == nil {
		return nil, fmt.Errorf("etcd client is required")
	}
	controllerID = strings.TrimSpace(controllerID)
	if controllerID == "" {
		return nil, fmt.Errorf("controller id is required")
	}
	if ttl <= 0 {
		ttl = 5 * time.Second
	}

	return &EtcdElection{
		client:  client,
		key:     path.Join(tools.CleanEtcdPrefix(prefix), "controller", "leader"),
		value:   controllerID,
		ttl:     ttl,
		timeout: defaultElectionTimeout,
	}, nil
}

func (e *EtcdElection) TryAcquire(ctx context.Context) (bool, error) {
	if e.IsLeader() {
		return true, nil
	}

	leaseResp, err := e.grantLease(ctx)
	if err != nil {
		return false, err
	}

	won, err := e.putLeaderKey(ctx, leaseResp.ID)
	if err != nil {
		_ = e.revokeLease(ctx, leaseResp.ID)
		return false, err
	}
	if !won {
		_ = e.revokeLease(ctx, leaseResp.ID)
		return false, nil
	}

	keepCtx, keepCancel := context.WithCancel(ctx)
	keepCh, err := e.client.KeepAlive(keepCtx, leaseResp.ID)
	if err != nil {
		keepCancel()
		_ = e.revokeLease(ctx, leaseResp.ID)
		return false, fmt.Errorf("keep election lease alive: %w", err)
	}

	e.mu.Lock()
	e.leader = true
	e.leaseID = leaseResp.ID
	e.keepCancel = keepCancel
	e.mu.Unlock()

	go e.watchKeepAlive(keepCtx, keepCancel, leaseResp.ID, keepCh)

	return true, nil
}

func (e *EtcdElection) IsLeader() bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.leader
}

func (e *EtcdElection) Resign(ctx context.Context) error {
	e.mu.Lock()
	leaseID := e.leaseID
	keepCancel := e.keepCancel
	e.leader = false
	e.leaseID = 0
	e.keepCancel = nil
	e.mu.Unlock()

	if keepCancel != nil {
		keepCancel()
	}
	if leaseID == 0 {
		return nil
	}

	if err := e.revokeLease(ctx, leaseID); err != nil {
		return err
	}
	return nil
}

func (e *EtcdElection) grantLease(ctx context.Context) (*clientv3.LeaseGrantResponse, error) {
	requestCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	leaseResp, err := e.client.Grant(requestCtx, leaseTTLSeconds(e.ttl))
	if err != nil {
		return nil, fmt.Errorf("grant election lease: %w", err)
	}
	return leaseResp, nil
}

func (e *EtcdElection) putLeaderKey(ctx context.Context, leaseID clientv3.LeaseID) (bool, error) {
	requestCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	resp, err := e.client.Txn(requestCtx).
		If(clientv3.Compare(clientv3.CreateRevision(e.key), "=", 0)).
		Then(clientv3.OpPut(e.key, e.value, clientv3.WithLease(leaseID))).
		Commit()
	if err != nil {
		return false, fmt.Errorf("campaign controller leader: %w", err)
	}
	return resp.Succeeded, nil
}

func (e *EtcdElection) revokeLease(ctx context.Context, leaseID clientv3.LeaseID) error {
	requestCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()

	if _, err := e.client.Revoke(requestCtx, leaseID); err != nil {
		return fmt.Errorf("revoke election lease: %w", err)
	}
	return nil
}

func (e *EtcdElection) watchKeepAlive(ctx context.Context, keepCancel context.CancelFunc, leaseID clientv3.LeaseID, keepCh <-chan *clientv3.LeaseKeepAliveResponse) {
	defer keepCancel()

	for {
		select {
		case <-ctx.Done():
			e.clearLeader(leaseID)
			return
		case resp, ok := <-keepCh:
			if !ok || resp == nil {
				e.clearLeader(leaseID)
				return
			}
		}
	}
}

func (e *EtcdElection) clearLeader(leaseID clientv3.LeaseID) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.leaseID != leaseID {
		return
	}
	e.leader = false
	e.leaseID = 0
	e.keepCancel = nil
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
