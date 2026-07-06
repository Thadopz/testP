package eventlog

import (
	"testP/internal/model"
	"testing"
)

func TestJSONEventCodecEventRoundTrip(t *testing.T) {

	codec := &JSONEventCodec{}

	event := model.Event{
		ID:            "event-1",
		Type:          model.EventType(model.EventOrderCreated),
		AggregateType: "order",
		AggregateID:   "event-1",
		ShardID:       1,
		OccurredAt:    1234567890,
		Payload:       []byte(`{"order_id":1001,"x":10,"y":20}`),
	}

	encodedEvent, err := codec.EncodeEvent(event)
	if err != nil {
		t.Fatalf("EncodeEvent returned error: %v", err)
	}

	decodedEvent, err := codec.DecodeEvent(encodedEvent)
	if err != nil {
		t.Fatalf("DecodeEvent returned error: %v", err)
	}

	expectedEventType := model.EventType(model.EventOrderCreated)
	if decodedEvent.Type != expectedEventType {
		t.Fatalf("event type mismatch: got %q, want %q", decodedEvent.Type, expectedEventType)
	}

	expectedAggregateID := "event-1"
	if decodedEvent.AggregateID != expectedAggregateID {
		t.Fatalf("aggregate id mismatch: got %q, want %q", decodedEvent.AggregateID, expectedAggregateID)
	}

	if string(decodedEvent.Payload) != string(event.Payload) {
		t.Fatalf("payload mismatch: got %s, want %s", decodedEvent.Payload, event.Payload)
	}
}

func TestJSONEventCodecPayloadRoundTrip(t *testing.T) {
	codec := &JSONEventCodec{}

	payload := model.OrderCreated{
		OrderID: 1,
		X:       1,
		Y:       2,
	}

	encodedPayload, err := codec.EncodePayload(payload)
	if err != nil {
		t.Fatalf("EncodePayload returned error: %v", err)
	}

	var decodedPayload model.OrderCreated
	err = codec.DecodePayload(encodedPayload, &decodedPayload)
	if err != nil {
		t.Fatalf("DecodePayload returned error: %v", err)
	}

	expectedOrderID := 1
	if decodedPayload.OrderID != expectedOrderID {
		t.Fatalf("order id mismatch: got %d, want %d", decodedPayload.OrderID, expectedOrderID)
	}

	expectedX := 1
	if decodedPayload.X != expectedX {
		t.Fatalf("x mismatch: got %d, want %d", decodedPayload.X, expectedX)
	}

	expectedY := 2
	if decodedPayload.Y != expectedY {
		t.Fatalf("y mismatch: got %d, want %d", decodedPayload.Y, expectedY)
	}
}

func TestJSONEventCodecRejectsEventWithoutType(t *testing.T) {

	codec := &JSONEventCodec{}

	event := model.Event{
		ID:      "event-without-type",
		Type:    model.EventType(""),
		ShardID: 1,
	}

	_, err := codec.EncodeEvent(event)
	if err == nil {
		t.Fatal("expected EncodeEvent to return an error")
	}
}
