package main

import (
	"testing"

	"testP/internal/eventlog"
	"testP/internal/model"
)

func TestBuildRetryEventKeepsOrderShardAndAttempt(t *testing.T) {
	codec := &eventlog.JSONEventCodec{}
	event, err := buildRetryEvent(codec, 1001, 7, 2, "benchmark_retry")
	if err != nil {
		t.Fatalf("buildRetryEvent returned error: %v", err)
	}

	if event.ID != "order-1001-retry-2" || event.Type != model.EventOrderRetryRequest {
		t.Fatalf("event identity mismatch: %+v", event)
	}
	if event.ShardID != 7 || event.AggregateID != "1001" {
		t.Fatalf("event routing mismatch: %+v", event)
	}

	payload := model.OrderRetryRequest{}
	if err := codec.DecodePayload(event.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.OrderID != 1001 || payload.Attempt != 2 || payload.Reason != "benchmark_retry" {
		t.Fatalf("payload mismatch: %+v", payload)
	}
}

func TestValidateArgumentsRejectsInvalidAttempt(t *testing.T) {
	if err := validateArguments(1, 10, 0, 100); err == nil {
		t.Fatal("expected invalid attempt error")
	}
}
