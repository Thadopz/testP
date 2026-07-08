package heartbeat

import (
	"context"
	"errors"
	"net"
	"testP/internal/cluster/failover"
	"testP/internal/cluster/membership"
	"testP/internal/cluster/ownership"
	"testing"
	"time"
)

func TestHeartbeatServicePingMarksNodeAlive(t *testing.T) {
	membershipStore := membership.NewMemoryMembershipStore()
	service := NewHeartbeatService(membershipStore, nil, time.Second)

	var reply HeartbeatResponse
	if err := service.Ping(HeartbeatRequest{NodeID: 1}, &reply); err != nil {
		t.Fatalf("Ping returned error: %v", err)
	}
	if !reply.OK {
		t.Fatal("expected heartbeat reply OK")
	}

	alive, err := membershipStore.IsAlive(1)
	if err != nil {
		t.Fatalf("IsAlive returned error: %v", err)
	}
	if !alive {
		t.Fatal("expected node 1 to be alive")
	}
}

func TestHeartbeatServiceSweepMarksTimedOutNodeDeadAndFailsOver(t *testing.T) {
	ownershipStore := ownership.NewMemoryOwnershipStore()
	membershipStore := membership.NewMemoryMembershipStore()
	failoverController := failover.NewFailoverController(ownershipStore, membershipStore)
	service := NewHeartbeatService(membershipStore, failoverController, time.Second)

	if err := ownershipStore.Assign(1, 1); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}
	if err := membershipStore.MarkAlive(2); err != nil {
		t.Fatalf("MarkAlive returned error: %v", err)
	}

	now := time.Unix(100, 0)
	service.setNowFunc(func() time.Time { return now })

	var reply HeartbeatResponse
	if err := service.Ping(HeartbeatRequest{NodeID: 1}, &reply); err != nil {
		t.Fatalf("Ping returned error: %v", err)
	}

	now = now.Add(2 * time.Second)
	deadNodeIDs, err := service.SweepDeadNodes()
	if err != nil {
		t.Fatalf("SweepDeadNodes returned error: %v", err)
	}
	if len(deadNodeIDs) != 1 || deadNodeIDs[0] != 1 {
		t.Fatalf("dead nodes mismatch: got %v, want [1]", deadNodeIDs)
	}

	alive, err := membershipStore.IsAlive(1)
	if err != nil {
		t.Fatalf("IsAlive returned error: %v", err)
	}
	if alive {
		t.Fatal("expected node 1 to be dead")
	}

	owner, ok, err := ownershipStore.OwnerOf(1)
	if err != nil {
		t.Fatalf("OwnerOf returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected shard 1 to have owner")
	}
	if owner.NodeID != 2 || owner.Epoch != 2 {
		t.Fatalf("ownership mismatch: got %+v, want node=2 epoch=2", owner)
	}
}

func TestHeartbeatFromRecoveredNodeDoesNotRollbackFailoverOwnership(t *testing.T) {
	ownershipStore := ownership.NewMemoryOwnershipStore()
	membershipStore := membership.NewMemoryMembershipStore()
	failoverController := failover.NewFailoverController(ownershipStore, membershipStore)
	service := NewHeartbeatService(membershipStore, failoverController, time.Second)

	if err := ownershipStore.Assign(1, 2); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}
	if err := membershipStore.MarkAlive(1); err != nil {
		t.Fatalf("MarkAlive returned error: %v", err)
	}

	now := time.Unix(100, 0)
	service.setNowFunc(func() time.Time { return now })

	if err := service.Ping(HeartbeatRequest{NodeID: 2}, &HeartbeatResponse{}); err != nil {
		t.Fatalf("Ping returned error: %v", err)
	}

	now = now.Add(2 * time.Second)
	if _, err := service.SweepDeadNodes(); err != nil {
		t.Fatalf("SweepDeadNodes returned error: %v", err)
	}

	ownerAfterFailover, ok, err := ownershipStore.OwnerOf(1)
	if err != nil {
		t.Fatalf("OwnerOf returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected shard 1 to have owner")
	}
	if ownerAfterFailover.NodeID != 1 {
		t.Fatalf("owner mismatch after failover: got %d, want 1", ownerAfterFailover.NodeID)
	}

	if err := service.Ping(HeartbeatRequest{NodeID: 2}, &HeartbeatResponse{}); err != nil {
		t.Fatalf("recovered Ping returned error: %v", err)
	}

	alive, err := membershipStore.IsAlive(2)
	if err != nil {
		t.Fatalf("IsAlive returned error: %v", err)
	}
	if !alive {
		t.Fatal("expected recovered node 2 to be alive")
	}

	ownerAfterRecovery, ok, err := ownershipStore.OwnerOf(1)
	if err != nil {
		t.Fatalf("OwnerOf returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected shard 1 to have owner")
	}
	if ownerAfterRecovery.NodeID != 1 || ownerAfterRecovery.Epoch != ownerAfterFailover.Epoch {
		t.Fatalf("ownership rollback after recovery: got %+v, want node=1 epoch=%d", ownerAfterRecovery, ownerAfterFailover.Epoch)
	}
}

func TestHeartbeatServiceRejectsInvalidNodeID(t *testing.T) {
	service := NewHeartbeatService(membership.NewMemoryMembershipStore(), nil, time.Second)

	err := service.Ping(HeartbeatRequest{NodeID: 0}, &HeartbeatResponse{})
	if err == nil {
		t.Fatal("expected Ping to return an error")
	}
}

func TestHeartbeatRPCClientSendsPing(t *testing.T) {
	membershipStore := membership.NewMemoryMembershipStore()
	service := NewHeartbeatService(membershipStore, nil, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- ServeHeartbeatRPCOnListener(ctx, listener, service)
	}()

	if err := SendHeartbeatRPC(context.Background(), listener.Addr().String(), 3); err != nil {
		t.Fatalf("SendHeartbeatRPC returned error: %v", err)
	}

	alive, err := membershipStore.IsAlive(3)
	if err != nil {
		t.Fatalf("IsAlive returned error: %v", err)
	}
	if !alive {
		t.Fatal("expected node 3 to be alive")
	}

	cancel()
	err = <-serverErrCh
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("ServeHeartbeatRPCOnListener returned error: %v", err)
	}
}
