package applier

import (
	"context"
	"errors"
	"strings"
	clusterownership "testP/internal/cluster/ownership"
	"testP/internal/eventlog"
	"testP/internal/model"
	"testP/internal/orderstate"
	"testing"
)

func TestEventApplierAppliesOrderCreated(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	engine := &fakeEngine{}
	applier := NewEventApplier(codec, engine)

	payload, err := codec.EncodePayload(model.OrderCreated{
		OrderID: 1001,
		X:       10,
		Y:       20,
	})
	if err != nil {
		t.Fatalf("EncodePayload returned error: %v", err)
	}

	event := model.Event{
		ID:      "event-1",
		Type:    model.EventOrderCreated,
		ShardID: 1,
		Payload: payload,
	}

	err = applier.Apply(context.Background(), event)
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}

	if len(engine.submittedBatches) != 1 {
		t.Fatalf("submitted batch count mismatch: got %d, want %d", len(engine.submittedBatches), 1)
	}

	orders := engine.submittedBatches[0].Orders
	if len(orders) != 1 {
		t.Fatalf("submitted order count mismatch: got %d, want %d", len(orders), 1)
	}

	if orders[0] != (model.Order{ID: 1001, X: 10, Y: 20}) {
		t.Fatalf("submitted order mismatch: got %+v", orders[0])
	}
}

func TestEventApplierAppliesRiderMoved(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	engine := &fakeEngine{}
	applier := NewEventApplier(codec, engine)

	payload, err := codec.EncodePayload(model.RiderEvent{
		Type: model.RiderMove,
		UID:  77,
		X:    30,
		Y:    40,
	})
	if err != nil {
		t.Fatalf("EncodePayload returned error: %v", err)
	}

	event := model.Event{
		ID:      "event-1",
		Type:    model.EventRiderMoved,
		ShardID: 2,
		Payload: payload,
	}

	err = applier.Apply(context.Background(), event)
	if err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}

	if len(engine.riderEvents) != 1 {
		t.Fatalf("rider event count mismatch: got %d, want %d", len(engine.riderEvents), 1)
	}

	expected := model.RiderEvent{
		Type: model.RiderMove,
		UID:  77,
		X:    30,
		Y:    40,
	}
	if engine.riderEvents[0] != expected {
		t.Fatalf("rider event mismatch: got %+v, want %+v", engine.riderEvents[0], expected)
	}
}

func TestEventApplierReturnsSubmitBatchError(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	expectedErr := errors.New("submit failed")
	engine := &fakeEngine{submitErr: expectedErr}
	applier := NewEventApplier(codec, engine)

	payload, err := codec.EncodePayload(model.OrderCreated{
		OrderID: 1001,
		X:       10,
		Y:       20,
	})
	if err != nil {
		t.Fatalf("EncodePayload returned error: %v", err)
	}

	event := model.Event{
		ID:      "event-1",
		Type:    model.EventOrderCreated,
		ShardID: 1,
		Payload: payload,
	}

	err = applier.Apply(context.Background(), event)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("Apply error mismatch: got %v, want %v", err, expectedErr)
	}
}

func TestEventApplierReturnsErrorForInvalidPayload(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	engine := &fakeEngine{}
	applier := NewEventApplier(codec, engine)

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
	engine := &fakeEngine{}
	applier := NewEventApplier(codec, engine)

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

func TestEventApplierStoresSubmittedOrderState(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	engine := &fakeEngine{}
	orderStore := orderstate.NewMemoryStore()
	applier := NewEventApplierWithOrderStore(codec, engine, orderStore)

	event := orderCreatedEvent(t, codec, 1)

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
	if state.Status != orderstate.StatusSubmitted {
		t.Fatalf("status mismatch: got %q, want %q", state.Status, orderstate.StatusSubmitted)
	}
	if state.ShardID != 1 || state.X != 10 || state.Y != 20 {
		t.Fatalf("state mismatch: got %+v", state)
	}
}

func TestEventApplierSkipsDuplicateSubmittedOrder(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	engine := &fakeEngine{}
	orderStore := orderstate.NewMemoryStore()
	if err := orderStore.Save(context.Background(), orderstate.State{
		OrderID: 1001,
		Status:  orderstate.StatusSubmitted,
		X:       10,
		Y:       20,
	}); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	applier := NewEventApplierWithOrderStore(codec, engine, orderStore)

	event := orderCreatedEvent(t, codec, 1)

	if err := applier.Apply(context.Background(), event); err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}
	if len(engine.submittedBatches) != 0 {
		t.Fatalf("submitted batch count mismatch: got %d, want 0", len(engine.submittedBatches))
	}
}

func TestEventApplierAppliesOrderCancelledState(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	engine := &fakeEngine{}
	orderStore := orderstate.NewMemoryStore()
	applier := NewEventApplierWithOrderStore(codec, engine, orderStore)

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

func TestEventApplierAppliesOrderRetryStateAndResubmits(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	engine := &fakeEngine{}
	orderStore := orderstate.NewMemoryStore()
	if err := orderStore.Save(context.Background(), orderstate.State{
		OrderID: 1001,
		ShardID: 1,
		Status:  orderstate.StatusSubmitted,
		X:       10,
		Y:       20,
	}); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	applier := NewEventApplierWithOrderStore(codec, engine, orderStore)

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
	if len(engine.submittedBatches) != 1 {
		t.Fatalf("submitted batch count mismatch: got %d, want 1", len(engine.submittedBatches))
	}

	state, found, err := orderStore.Load(context.Background(), 1001)
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	if !found {
		t.Fatal("expected order state to be found")
	}
	if state.Status != orderstate.StatusSubmitted || state.Attempt != 2 || state.RetryReason != "timeout" {
		t.Fatalf("state mismatch: got %+v", state)
	}
}

func TestEventApplierAppliesOrderMatchedState(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	engine := &fakeEngine{}
	orderStore := orderstate.NewMemoryStore()
	applier := NewEventApplierWithOrderStore(codec, engine, orderStore)

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

func TestEventApplierApplyWithFenceAppliesWhenOwnershipMatches(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	engine := &fakeEngine{}
	ownershipStore := clusterownership.NewMemoryOwnershipStore()
	if err := ownershipStore.Assign(1, 10); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}
	applier := NewFencedEventApplier(codec, engine, 10, ownershipStore)

	event := orderCreatedEvent(t, codec, 1)

	err := applier.ApplyWithFence(context.Background(), event, clusterownership.Ownership{
		ShardID: 1,
		NodeID:  10,
		Epoch:   1,
	})
	if err != nil {
		t.Fatalf("ApplyWithFence returned error: %v", err)
	}
	if len(engine.submittedBatches) != 1 {
		t.Fatalf("submitted batch count mismatch: got %d, want 1", len(engine.submittedBatches))
	}
}

func TestEventApplierApplyWithFenceRejectsStaleOwnerBeforeApply(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	engine := &fakeEngine{}
	ownershipStore := clusterownership.NewMemoryOwnershipStore()
	if err := ownershipStore.Assign(1, 20); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}
	applier := NewFencedEventApplier(codec, engine, 10, ownershipStore)

	event := orderCreatedEvent(t, codec, 1)

	err := applier.ApplyWithFence(context.Background(), event, clusterownership.Ownership{
		ShardID: 1,
		NodeID:  10,
		Epoch:   1,
	})
	if !errors.Is(err, clusterownership.ErrOwnershipFenceLost) {
		t.Fatalf("ApplyWithFence error mismatch: got %v, want ErrOwnershipFenceLost", err)
	}
	if len(engine.submittedBatches) != 0 {
		t.Fatalf("submitted batch count mismatch: got %d, want 0", len(engine.submittedBatches))
	}
}

func TestEventApplierApplyWithFenceRejectsEpochChangeAfterApply(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	ownershipStore := clusterownership.NewMemoryOwnershipStore()
	if err := ownershipStore.Assign(1, 10); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}
	engine := &fakeEngine{
		afterSubmit: func() error {
			return ownershipStore.Assign(1, 20)
		},
	}
	applier := NewFencedEventApplier(codec, engine, 10, ownershipStore)

	event := orderCreatedEvent(t, codec, 1)

	err := applier.ApplyWithFence(context.Background(), event, clusterownership.Ownership{
		ShardID: 1,
		NodeID:  10,
		Epoch:   1,
	})
	if !errors.Is(err, clusterownership.ErrOwnershipFenceLost) {
		t.Fatalf("ApplyWithFence error mismatch: got %v, want ErrOwnershipFenceLost", err)
	}
	if len(engine.submittedBatches) != 1 {
		t.Fatalf("submitted batch count mismatch: got %d, want 1", len(engine.submittedBatches))
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

type fakeEngine struct {
	submittedBatches []model.OrderBatch
	riderEvents      []model.RiderEvent
	submitErr        error
	afterSubmit      func() error
}

func (f *fakeEngine) Start(workerCount int) {}

func (f *fakeEngine) SubmitBatch(ctx context.Context, batch model.OrderBatch) error {
	if f.submitErr != nil {
		return f.submitErr
	}

	f.submittedBatches = append(f.submittedBatches, batch)
	if f.afterSubmit != nil {
		return f.afterSubmit()
	}
	return nil
}

func (f *fakeEngine) ApplyRiderEvent(event model.RiderEvent) {
	f.riderEvents = append(f.riderEvents, event)
}

func (f *fakeEngine) Close() {}

func (f *fakeEngine) Wait() {}

func (f *fakeEngine) Submitted() int64 {
	return 0
}

func (f *fakeEngine) Matched() int64 {
	return 0
}

func (f *fakeEngine) Missed() int64 {
	return 0
}

func (f *fakeEngine) OnlineRiders() int {
	return 0
}
