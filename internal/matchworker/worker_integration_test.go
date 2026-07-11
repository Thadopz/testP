package matchworker

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	clusterownership "testP/internal/cluster/ownership"
	db "testP/internal/database"
	"testP/internal/eventlog"
	"testP/internal/matching"
	"testP/internal/model"
)

func TestWorkerMatchesOrderOnce(t *testing.T) {
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
	queries := db.New(pool)

	orderID := time.Now().UnixNano()
	riderID := orderID + 1
	eventID := fmt.Sprintf("match-request-%d", orderID)
	resultEventID := eventID + "-order_matched"
	const shardID = 0
	defer func() {
		pool.Exec(context.Background(), "DELETE FROM outbox_events WHERE event_id = $1", resultEventID)
		pool.Exec(context.Background(), "DELETE FROM inbox_events WHERE consumer_name = $1 AND event_id = $2", model.ConsumerMatcherWorker, eventID)
		pool.Exec(context.Background(), "DELETE FROM orders WHERE order_id = $1", orderID)
		pool.Exec(context.Background(), "DELETE FROM riders WHERE uid = $1", riderID)
		pool.Exec(context.Background(), "DELETE FROM shard_checkpoints WHERE consumer_name = $1 AND topic = $2 AND shard_id = $3", model.ConsumerMatcherWorker, model.TopicMatchRequests, shardID)
	}()

	if err := queries.UpsertOrder(ctx, db.UpsertOrderParams{
		OrderID: orderID, ShardID: shardID, Status: "match_pending",
		X: 1, Y: 1, LastEventID: "created", UpdatedAt: 1,
	}); err != nil {
		t.Fatalf("insert order: %v", err)
	}
	if err := queries.UpsertRider(ctx, db.UpsertRiderParams{
		Uid: riderID, X: 2, Y: 2, Online: true,
	}); err != nil {
		t.Fatalf("insert rider: %v", err)
	}

	index := matching.NewIndex(64, 10000, 100000, 10000)
	index.ReplaceShard(shardID, []*model.Rider{{UID: riderID, X: 2, Y: 2}})
	reader := &testOwnershipReader{ownership: clusterownership.Ownership{
		ShardID: shardID, NodeID: 10, Epoch: 1,
	}}
	codec := &eventlog.JSONEventCodec{}
	worker := New(pool, codec, index, reader, Config{NodeID: 10, MaxRiderOrders: 3})
	payload, err := codec.EncodePayload(model.MatchRequested{OrderID: orderID, X: 1, Y: 1})
	if err != nil {
		t.Fatalf("encode request: %v", err)
	}
	record := eventlog.Record{
		Position: eventlog.Position{ShardID: shardID, Offset: 4},
		Event: model.Event{
			ID: eventID, Type: model.EventMatchRequested,
			AggregateType: "order", AggregateID: fmt.Sprintf("%d", orderID),
			ShardID: shardID, OccurredAt: 2, Payload: payload,
		},
	}

	if err := worker.ApplyRecordWithFence(ctx, record, reader.ownership); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if err := worker.ApplyRecordWithFence(ctx, record, reader.ownership); err != nil {
		t.Fatalf("duplicate apply: %v", err)
	}

	order, err := queries.GetOrder(ctx, orderID)
	if err != nil {
		t.Fatalf("get order: %v", err)
	}
	if order.Status != "matched" || order.RiderID != riderID {
		t.Fatalf("order = %+v, want matched rider %d", order, riderID)
	}
	rider, err := queries.GetRider(ctx, riderID)
	if err != nil {
		t.Fatalf("get rider: %v", err)
	}
	if rider.Count != 1 {
		t.Fatalf("rider count = %d, want 1", rider.Count)
	}
	var outboxCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE event_id = $1", resultEventID).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("outbox count = %d, want 1", outboxCount)
	}
}

type testOwnershipReader struct {
	ownership clusterownership.Ownership
}

func (r *testOwnershipReader) OwnerOf(shardID int) (clusterownership.Ownership, bool, error) {
	if r.ownership.ShardID != shardID {
		return clusterownership.Ownership{}, false, nil
	}
	return r.ownership, true, nil
}
