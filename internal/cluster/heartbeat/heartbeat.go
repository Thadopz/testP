package heartbeat

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"testP/internal/cluster/failover"
	"testP/internal/cluster/membership"
	"time"
)

const defaultHeartbeatTimeout = 5 * time.Second

type HeartbeatRequest struct {
	NodeID int
}

type HeartbeatResponse struct {
	OK bool
}

type HeartbeatSweepRequest struct{}

type HeartbeatSweepResponse struct {
	DeadNodeIDs []int
}

type HeartbeatService struct {
	membership membership.MembershipStore
	failover   *failover.FailoverController
	timeout    time.Duration

	mu       sync.Mutex
	lastSeen map[int]time.Time
	now      func() time.Time
}

func NewHeartbeatService(membershipStore membership.MembershipStore, failoverController *failover.FailoverController, timeout time.Duration) *HeartbeatService {
	if timeout <= 0 {
		timeout = defaultHeartbeatTimeout
	}

	return &HeartbeatService{
		membership: membershipStore,
		failover:   failoverController,
		timeout:    timeout,
		lastSeen:   make(map[int]time.Time),
		now:        time.Now,
	}
}

func (s *HeartbeatService) Ping(req HeartbeatRequest, reply *HeartbeatResponse) error {
	if req.NodeID <= 0 {
		return fmt.Errorf("node id must be > 0: %d", req.NodeID)
	}
	if s.membership == nil {
		return fmt.Errorf("membership store is required")
	}

	if err := s.membership.MarkAlive(req.NodeID); err != nil {
		return err
	}

	s.mu.Lock()
	s.lastSeen[req.NodeID] = s.now()
	s.mu.Unlock()

	reply.OK = true
	return nil
}

func (s *HeartbeatService) Sweep(req HeartbeatSweepRequest, reply *HeartbeatSweepResponse) error {
	deadNodeIDs, err := s.SweepDeadNodes()
	if err != nil {
		return err
	}

	reply.DeadNodeIDs = deadNodeIDs
	return nil
}

func (s *HeartbeatService) SweepDeadNodes() ([]int, error) {
	if s.membership == nil {
		return nil, fmt.Errorf("membership store is required")
	}

	now := s.now()
	deadNodeIDs := make([]int, 0)

	s.mu.Lock()
	for nodeID, lastSeen := range s.lastSeen {
		if now.Sub(lastSeen) > s.timeout {
			deadNodeIDs = append(deadNodeIDs, nodeID)
			delete(s.lastSeen, nodeID)
		}
	}
	s.mu.Unlock()

	for _, nodeID := range deadNodeIDs {
		if err := s.membership.MarkDead(nodeID); err != nil {
			return nil, err
		}
		if s.failover != nil {
			if err := s.failover.FailoverDeadNode(nodeID); err != nil {
				return nil, err
			}
		}
	}

	return deadNodeIDs, nil
}

func (s *HeartbeatService) setNowFunc(now func() time.Time) {
	if now == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.now = now
}

func ServeHeartbeatRPC(ctx context.Context, addr string, service *HeartbeatService) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen heartbeat rpc: %w", err)
	}

	return ServeHeartbeatRPCOnListener(ctx, listener, service)
}

func ServeHeartbeatRPCOnListener(ctx context.Context, listener net.Listener, service *HeartbeatService) error {
	if service == nil {
		return fmt.Errorf("heartbeat service is required")
	}
	if listener == nil {
		return fmt.Errorf("heartbeat listener is required")
	}

	server := rpc.NewServer()
	if err := server.RegisterName("HeartbeatService", service); err != nil {
		return fmt.Errorf("register heartbeat service: %w", err)
	}
	defer listener.Close()

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("accept heartbeat rpc connection: %w", err)
		}

		go server.ServeConn(conn)
	}
}

func SendHeartbeatRPC(ctx context.Context, addr string, nodeID int) error {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("dial heartbeat rpc: %w", err)
	}

	client := rpc.NewClient(conn)
	defer client.Close()

	done := make(chan error, 1)
	go func() {
		var reply HeartbeatResponse
		done <- client.Call("HeartbeatService.Ping", HeartbeatRequest{NodeID: nodeID}, &reply)
	}()

	select {
	case <-ctx.Done():
		client.Close()
		return ctx.Err()
	case err := <-done:
		if err != nil {
			return fmt.Errorf("send heartbeat rpc: %w", err)
		}
		return nil
	}
}

func RunHeartbeatClient(ctx context.Context, addr string, nodeID int, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Second
	}

	_ = SendHeartbeatRPC(ctx, addr, nodeID)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			_ = SendHeartbeatRPC(ctx, addr, nodeID)
		}
	}
}
