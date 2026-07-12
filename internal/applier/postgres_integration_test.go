package applier

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"testP/internal/cluster/ownership"
	db "testP/internal/database"
	"testP/internal/eventlog"
	"testP/internal/model"
)

func TestPostgresApplierQueuesMatchRequestOnce(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()
	pool, err := db.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer pool.Close()

	codec := &eventlog.JSONEventCodec{}
	ownershipStore := newApplierTestOwnershipStore()
	const shardID = 63
	if err := ownershipStore.Assign(shardID, 10); err != nil {
		t.Fatalf("assign ownership: %v", err)
	}
	currentOwnership, _, _ := ownershipStore.OwnerOf(shardID)

	base := NewFencedEventApplier(codec, 10, ownershipStore, nil)
	applier := NewPostgresApplier(base, pool, 10)
	orderID := time.Now().UnixNano()
	eventID := fmt.Sprintf("postgres-integration-%d", orderID)
	matchEventID := eventID + "-match-requested"
	payload, err := codec.EncodePayload(model.OrderCreated{OrderID: orderID, X: 10, Y: 20})
	if err != nil {
		t.Fatalf("encode payload: %v", err)
	}
	event := model.Event{
		ID:         eventID,
		Type:       model.EventOrderCreated,
		ShardID:    shardID,
		OccurredAt: 100,
		Payload:    payload,
	}
	record := eventlog.Record{
		Position: eventlog.Position{ShardID: shardID, Offset: 7},
		Event:    event,
	}
	defer func() {
		pool.Exec(context.Background(), "DELETE FROM outbox_events WHERE event_id = $1", matchEventID)
		pool.Exec(context.Background(), "DELETE FROM inbox_events WHERE event_id = $1", eventID)
		pool.Exec(context.Background(), "DELETE FROM orders WHERE order_id = $1", orderID)
		pool.Exec(context.Background(), "DELETE FROM shard_checkpoints WHERE shard_id = $1", shardID)
	}()

	if err := applier.ApplyRecordWithFence(ctx, record, currentOwnership); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if err := applier.ApplyRecordWithFence(ctx, record, currentOwnership); err != nil {
		t.Fatalf("duplicate apply: %v", err)
	}

	queries := db.New(pool)
	order, err := queries.GetOrder(ctx, orderID)
	if err != nil {
		t.Fatalf("get order: %v", err)
	}
	if order.Status != "match_pending" {
		t.Fatalf("order status = %q, want match_pending", order.Status)
	}

	checkpoint, err := queries.GetShardCheckpoint(ctx, db.GetShardCheckpointParams{
		ConsumerName: "order-worker",
		Topic:        "order-events",
		ShardID:      shardID,
	})
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if checkpoint.OffsetValue != 8 {
		t.Fatalf("checkpoint offset = %d, want 8", checkpoint.OffsetValue)
	}

	var inboxCount int
	var outboxCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM inbox_events WHERE event_id = $1", eventID).Scan(&inboxCount); err != nil {
		t.Fatalf("count inbox events: %v", err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE event_id = $1", matchEventID).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox events: %v", err)
	}
	if inboxCount != 1 || outboxCount != 1 {
		t.Fatalf("event counts = inbox:%d outbox:%d, want 1 and 1", inboxCount, outboxCount)
	}
	var eventType string
	var topic string
	if err := pool.QueryRow(ctx,
		"SELECT event_type, topic FROM outbox_events WHERE event_id = $1",
		matchEventID,
	).Scan(&eventType, &topic); err != nil {
		t.Fatalf("load match request outbox: %v", err)
	}
	if eventType != string(model.EventMatchRequested) || topic != model.TopicMatchRequests {
		t.Fatalf("outbox event = %s/%s, want match_requested/match-requests", eventType, topic)
	}
}

func TestPostgresApplierAppliesBatchAndSavesLastCheckpoint(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx := context.Background()
	pool, err := db.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer pool.Close()

	codec := &eventlog.JSONEventCodec{}
	ownershipStore := newApplierTestOwnershipStore()
	const shardID = 62
	if err := ownershipStore.Assign(shardID, 10); err != nil {
		t.Fatalf("assign ownership: %v", err)
	}
	currentOwnership, _, _ := ownershipStore.OwnerOf(shardID)
	applier := NewPostgresApplier(NewFencedEventApplier(codec, 10, ownershipStore, nil), pool, 10)

	baseOrderID := time.Now().UnixNano()
	records := make([]eventlog.Record, 0, 2)
	for index := 0; index < 2; index++ {
		orderID := baseOrderID + int64(index)
		eventID := fmt.Sprintf("postgres-batch-%d", orderID)
		payload, encodeErr := codec.EncodePayload(model.OrderCreated{OrderID: orderID, X: index, Y: index})
		if encodeErr != nil {
			t.Fatalf("encode payload: %v", encodeErr)
		}
		records = append(records, eventlog.Record{
			Position: eventlog.Position{ShardID: shardID, Offset: 20 + int64(index)},
			Event: model.Event{
				ID:         eventID,
				Type:       model.EventOrderCreated,
				ShardID:    shardID,
				OccurredAt: 200 + int64(index),
				Payload:    payload,
			},
		})
	}
	defer func() {
		for _, record := range records {
			pool.Exec(context.Background(), "DELETE FROM outbox_events WHERE event_id = $1", record.Event.ID+"-match-requested")
			pool.Exec(context.Background(), "DELETE FROM inbox_events WHERE event_id = $1", record.Event.ID)
		}
		pool.Exec(context.Background(), "DELETE FROM orders WHERE order_id IN ($1, $2)", baseOrderID, baseOrderID+1)
		pool.Exec(context.Background(), "DELETE FROM shard_checkpoints WHERE consumer_name = $1 AND topic = $2 AND shard_id = $3", model.ConsumerOrderWorker, model.TopicOrderEvents, shardID)
	}()

	if err := applier.ApplyRecordsWithFence(ctx, records, currentOwnership); err != nil {
		t.Fatalf("apply batch: %v", err)
	}

	queries := db.New(pool)
	for index := 0; index < 2; index++ {
		order, getErr := queries.GetOrder(ctx, baseOrderID+int64(index))
		if getErr != nil {
			t.Fatalf("get order %d: %v", index, getErr)
		}
		if order.Status != "match_pending" {
			t.Fatalf("order %d status = %q, want match_pending", index, order.Status)
		}
	}
	checkpoint, err := queries.GetShardCheckpoint(ctx, db.GetShardCheckpointParams{
		ConsumerName: model.ConsumerOrderWorker,
		Topic:        model.TopicOrderEvents,
		ShardID:      shardID,
	})
	if err != nil {
		t.Fatalf("get checkpoint: %v", err)
	}
	if checkpoint.OffsetValue != 22 {
		t.Fatalf("checkpoint offset = %d, want 22", checkpoint.OffsetValue)
	}
}

func TestValidateRecordBatchRejectsNonConsecutiveOffsets(t *testing.T) {
	records := []eventlog.Record{
		{Position: eventlog.Position{ShardID: 1, Offset: 1}, Event: model.Event{ID: "one", ShardID: 1}},
		{Position: eventlog.Position{ShardID: 1, Offset: 3}, Event: model.Event{ID: "two", ShardID: 1}},
	}

	err := validateRecordBatch(records, clusterOwnership(1))
	if err == nil {
		t.Fatal("expected non-consecutive offsets to be rejected")
	}
}

func clusterOwnership(shardID int) ownership.Ownership {
	return ownership.Ownership{ShardID: shardID, NodeID: 10, Epoch: 1}
}
