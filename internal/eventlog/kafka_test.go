package eventlog

import (
	"testing"

	"github.com/segmentio/kafka-go"
	"testP/internal/model"
)

func TestNewKafkaEventLogRejectsMissingBrokers(t *testing.T) {
	_, err := NewKafkaEventLog(KafkaConfig{
		Topic: "orders",
	})
	if err == nil {
		t.Fatal("expected NewKafkaEventLog to return an error")
	}
}

func TestNewKafkaEventLogRejectsMissingTopic(t *testing.T) {
	_, err := NewKafkaEventLog(KafkaConfig{
		Brokers: []string{"127.0.0.1:9092"},
	})
	if err == nil {
		t.Fatal("expected NewKafkaEventLog to return an error")
	}
}

func TestKafkaEventLogEncodesAndDecodesMessage(t *testing.T) {
	eventLog, err := NewKafkaEventLog(KafkaConfig{
		Brokers: []string{"127.0.0.1:9092"},
		Topic:   "orders",
		Codec:   &JSONEventCodec{},
	})
	if err != nil {
		t.Fatalf("NewKafkaEventLog returned error: %v", err)
	}

	event := model.Event{
		ID:            "event-1",
		Type:          model.EventOrderCreated,
		AggregateType: "order",
		AggregateID:   "1001",
		ShardID:       3,
		OccurredAt:    123,
		Payload:       []byte(`{"order_id":1001,"x":10,"y":20}`),
	}

	message, err := eventLog.encodeMessage(event)
	if err != nil {
		t.Fatalf("encodeMessage returned error: %v", err)
	}
	message.Offset = 7

	record, err := eventLog.decodeMessage(3, message)
	if err != nil {
		t.Fatalf("decodeMessage returned error: %v", err)
	}

	if record.Position != (Position{ShardID: 3, Offset: 7}) {
		t.Fatalf("position mismatch: got %+v", record.Position)
	}
	if record.Event.ID != event.ID || record.Event.Type != event.Type || record.Event.ShardID != 3 {
		t.Fatalf("event mismatch: got %+v", record.Event)
	}
}

func TestKafkaEventLogDecodeRejectsInvalidMessage(t *testing.T) {
	eventLog, err := NewKafkaEventLog(KafkaConfig{
		Brokers: []string{"127.0.0.1:9092"},
		Topic:   "orders",
	})
	if err != nil {
		t.Fatalf("NewKafkaEventLog returned error: %v", err)
	}

	_, err = eventLog.decodeMessage(1, kafka.Message{
		Value: []byte(`not-json`),
	})
	if err == nil {
		t.Fatal("expected decodeMessage to return an error")
	}
}
