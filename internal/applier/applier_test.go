package applier

import (
	"context"
	"errors"
	"strings"
	"testP/internal/eventlog"
	"testP/internal/model"
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

type fakeEngine struct {
	submittedBatches []model.OrderBatch
	riderEvents      []model.RiderEvent
	submitErr        error
}

func (f *fakeEngine) Start(workerCount int) {}

func (f *fakeEngine) SubmitBatch(ctx context.Context, batch model.OrderBatch) error {
	if f.submitErr != nil {
		return f.submitErr
	}

	f.submittedBatches = append(f.submittedBatches, batch)
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
