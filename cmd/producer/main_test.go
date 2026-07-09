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

func TestBuildProducerEventLogReturnsKafkaEventLog(t *testing.T) {
	eventLog, err := buildProducerEventLog("127.0.0.1:9092", "order-events")
	if err != nil {
		t.Fatalf("buildProducerEventLog returned error: %v", err)
	}
	if _, ok := eventLog.(*eventlog.KafkaEventLog); !ok {
		t.Fatalf("eventlog type mismatch: got %T, want *eventlog.KafkaEventLog", eventLog)
	}
}
