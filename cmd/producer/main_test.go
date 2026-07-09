package main

import (
	"testP/internal/eventlog"
	"testing"
)

func TestParseBrokerListTrimsEmptyParts(t *testing.T) {
	brokers := parseBrokerList(" 127.0.0.1:9092, ,localhost:9093 ")
	if len(brokers) != 2 || brokers[0] != "127.0.0.1:9092" || brokers[1] != "localhost:9093" {
		t.Fatalf("broker list mismatch: got %v", brokers)
	}
}

func TestBuildProducerEventLogReturnsNilForFileBackend(t *testing.T) {
	eventLog, err := buildProducerEventLog("file", "", "")
	if err != nil {
		t.Fatalf("buildProducerEventLog returned error: %v", err)
	}
	if eventLog != nil {
		t.Fatalf("eventlog mismatch: got %T, want nil", eventLog)
	}
}

func TestBuildProducerEventLogReturnsKafkaEventLog(t *testing.T) {
	eventLog, err := buildProducerEventLog("kafka", "127.0.0.1:9092", "order-events")
	if err != nil {
		t.Fatalf("buildProducerEventLog returned error: %v", err)
	}
	if _, ok := eventLog.(*eventlog.KafkaEventLog); !ok {
		t.Fatalf("eventlog type mismatch: got %T, want *eventlog.KafkaEventLog", eventLog)
	}
}

func TestBuildProducerEventLogRejectsUnknownBackend(t *testing.T) {
	_, err := buildProducerEventLog("bad", "", "")
	if err == nil {
		t.Fatal("expected buildProducerEventLog to return an error")
	}
}
