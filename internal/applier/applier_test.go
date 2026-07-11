package applier

import (
	"context"
	"strings"
	"testing"

	clusterownership "testP/internal/cluster/ownership"
	"testP/internal/eventlog"
	"testP/internal/model"
	"testP/internal/orderstate"
)

func TestEventApplierStoresCreatedOrderState(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	orderStore := orderstate.NewMemoryStore()
	applier := NewEventApplierWithOrderStore(codec, orderStore)

	if err := applier.Apply(context.Background(), orderCreatedEvent(t, codec, 1)); err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}

	state, found, err := orderStore.Load(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !found {
		t.Fatal("expected order state to be found")
	}
	if state.Status != orderstate.StatusCreated {
		t.Fatalf("status mismatch: got %q, want %q", state.Status, orderstate.StatusCreated)
	}
	if state.ShardID != 1 || state.X != 10 || state.Y != 20 {
		t.Fatalf("state mismatch: got %+v", state)
	}
}

func TestEventApplierSkipsDuplicateQueuedOrder(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	orderStore := orderstate.NewMemoryStore()
	if err := orderStore.Save(context.Background(), orderstate.State{
		OrderID: 1001,
		Status:  orderstate.StatusMatchPending,
		X:       10,
		Y:       20,
	}); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	applier := NewEventApplierWithOrderStore(codec, orderStore)

	if err := applier.Apply(context.Background(), orderCreatedEvent(t, codec, 1)); err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}

	state, found, err := orderStore.Load(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !found {
		t.Fatal("expected order state to be found")
	}
	if state.Status != orderstate.StatusMatchPending {
		t.Fatalf("status mismatch: got %q, want %q", state.Status, orderstate.StatusMatchPending)
	}
}

func TestEventApplierAppliesOrderCancelledState(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	orderStore := orderstate.NewMemoryStore()
	applier := NewEventApplierWithOrderStore(codec, orderStore)

	payload, err := codec.EncodePayload(model.OrderCancelled{
		OrderID: 1001,
		Reason:  "user_cancelled",
	})
	if err != nil {
		t.Fatalf("EncodePayload returned error: %v", err)
	}
	event := model.Event{
		ID:      "event-cancel",
		Type:    model.EventOrderCancelled,
		ShardID: 1,
		Payload: payload,
	}

	if err := applier.Apply(context.Background(), event); err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}

	state, found, err := orderStore.Load(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !found {
		t.Fatal("expected order state to be found")
	}
	if state.Status != orderstate.StatusCancelled || state.CancelReason != "user_cancelled" {
		t.Fatalf("state mismatch: got %+v", state)
	}
}

func TestEventApplierAppliesOrderRetryState(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	orderStore := orderstate.NewMemoryStore()
	if err := orderStore.Save(context.Background(), orderstate.State{
		OrderID: 1001,
		ShardID: 1,
		Status:  orderstate.StatusMissed,
		X:       10,
		Y:       20,
	}); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	applier := NewEventApplierWithOrderStore(codec, orderStore)

	payload, err := codec.EncodePayload(model.OrderRetryRequest{
		OrderID: 1001,
		Attempt: 2,
		Reason:  "timeout",
	})
	if err != nil {
		t.Fatalf("EncodePayload returned error: %v", err)
	}
	event := model.Event{
		ID:      "event-retry",
		Type:    model.EventOrderRetryRequest,
		ShardID: 1,
		Payload: payload,
	}

	if err := applier.Apply(context.Background(), event); err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}

	state, found, err := orderStore.Load(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !found {
		t.Fatal("expected order state to be found")
	}
	if state.Status != orderstate.StatusRetryRequested || state.Attempt != 2 || state.RetryReason != "timeout" {
		t.Fatalf("state mismatch: got %+v", state)
	}
}

func TestEventApplierAppliesOrderMatchedState(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	orderStore := orderstate.NewMemoryStore()
	applier := NewEventApplierWithOrderStore(codec, orderStore)

	payload, err := codec.EncodePayload(model.OrderMatched{
		OrderID: 1001,
		RiderID: 88,
		Score:   95,
	})
	if err != nil {
		t.Fatalf("EncodePayload returned error: %v", err)
	}
	event := model.Event{
		ID:      "event-matched",
		Type:    model.EventOrderMatched,
		ShardID: 1,
		Payload: payload,
	}

	if err := applier.Apply(context.Background(), event); err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}

	state, found, err := orderStore.Load(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !found {
		t.Fatal("expected order state to be found")
	}
	if state.Status != orderstate.StatusMatched || state.RiderID != 88 || state.Score != 95 {
		t.Fatalf("state mismatch: got %+v", state)
	}
}

func TestEventApplierAppliesOrderMissedState(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	orderStore := orderstate.NewMemoryStore()
	applier := NewEventApplierWithOrderStore(codec, orderStore)

	payload, err := codec.EncodePayload(model.OrderMissed{
		OrderID: 1001,
		Reason:  "no_rider_found",
	})
	if err != nil {
		t.Fatalf("EncodePayload returned error: %v", err)
	}
	event := model.Event{
		ID:      "event-missed",
		Type:    model.EventOrderMissed,
		ShardID: 1,
		Payload: payload,
	}

	if err := applier.Apply(context.Background(), event); err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}

	state, found, err := orderStore.Load(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !found {
		t.Fatal("expected order state to be found")
	}
	if state.Status != orderstate.StatusMissed || state.MissReason != "no_rider_found" {
		t.Fatalf("state mismatch: got %+v", state)
	}
}

func TestEventApplierReturnsErrorForInvalidPayload(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	applier := NewEventApplier(codec)

	event := model.Event{
		ID:      "event-1",
		Type:    model.EventOrderCreated,
		ShardID: 1,
		Payload: []byte(`not-json`),
	}

	err := applier.Apply(context.Background(), event)
	if err == nil {
		t.Fatal("expected Apply to return an error")
	}
}

func TestEventApplierReturnsErrorForUnsupportedEventType(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	applier := NewEventApplier(codec)

	event := model.Event{
		ID:      "event-1",
		Type:    model.EventType("unknown"),
		ShardID: 1,
	}

	err := applier.Apply(context.Background(), event)
	if err == nil {
		t.Fatal("expected Apply to return an error")
	}
	if !strings.Contains(err.Error(), "unsupported event type") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func orderCreatedEvent(t *testing.T, codec eventlog.EventCodec, shardID int) model.Event {
	t.Helper()

	payload, err := codec.EncodePayload(model.OrderCreated{
		OrderID: 1001,
		X:       10,
		Y:       20,
	})
	if err != nil {
		t.Fatalf("EncodePayload returned error: %v", err)
	}

	return model.Event{
		ID:      "event-1",
		Type:    model.EventOrderCreated,
		ShardID: shardID,
		Payload: payload,
	}
}

type applierTestOwnershipStore struct {
	owners map[int]clusterownership.Ownership
}

func newApplierTestOwnershipStore() *applierTestOwnershipStore {
	return &applierTestOwnershipStore{
		owners: make(map[int]clusterownership.Ownership),
	}
}

func (s *applierTestOwnershipStore) OwnerOf(shardID int) (clusterownership.Ownership, bool, error) {
	ownership, ok := s.owners[shardID]
	return ownership, ok, nil
}

func (s *applierTestOwnershipStore) Assign(shardID int, nodeID int) error {
	ownership := s.owners[shardID]
	ownership.ShardID = shardID
	ownership.NodeID = nodeID
	ownership.Epoch++
	if ownership.Epoch == 0 {
		ownership.Epoch = 1
	}
	s.owners[shardID] = ownership
	return nil
}
