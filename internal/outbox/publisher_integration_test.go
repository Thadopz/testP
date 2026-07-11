package outbox

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	db "testP/internal/database"
	"testP/internal/eventlog"
	"testP/internal/model"
)

func TestPublisherPublishesAndMarksOutboxEvent(t *testing.T) {
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
	eventID := fmt.Sprintf("outbox-publisher-%d", time.Now().UnixNano())
	defer func() {
		pool.Exec(context.Background(), "DELETE FROM outbox_events WHERE event_id = $1", eventID)
	}()

	if err := queries.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
		EventID: eventID, EventType: string(model.EventMatchRequested),
		AggregateType: "order", AggregateID: "10", ShardID: 1,
		OccurredAt: -1 << 62, Payload: []byte(`{"OrderID":10}`),
		Topic: model.TopicMatchRequests, MessageKey: "10",
	}); err != nil {
		t.Fatalf("create outbox event: %v", err)
	}

	target := &recordingAppender{}
	publisher := NewPublisher(pool, map[string]eventlog.Appender{
		model.TopicMatchRequests: target,
	}, Config{WorkerID: "publisher-test", BatchSize: 1})
	published, err := publisher.PublishOnce(ctx)
	if err != nil {
		t.Fatalf("publish once: %v", err)
	}
	if published != 1 || len(target.events) != 1 {
		t.Fatalf("published = %d, appended = %d, want 1 and 1", published, len(target.events))
	}

	var marked bool
	if err := pool.QueryRow(ctx,
		"SELECT published_at IS NOT NULL FROM outbox_events WHERE event_id = $1",
		eventID,
	).Scan(&marked); err != nil {
		t.Fatalf("load outbox state: %v", err)
	}
	if !marked {
		t.Fatal("expected outbox event to be marked published")
	}
}

type recordingAppender struct {
	events []model.Event
}

func (a *recordingAppender) Append(_ context.Context, event model.Event) (eventlog.Position, error) {
	a.events = append(a.events, event)
	return eventlog.Position{ShardID: event.ShardID}, nil
}
