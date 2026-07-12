package outbox

import (
	"context"
	"fmt"
	"time"

	db "testP/internal/database"
	"testP/internal/eventlog"
	"testP/internal/model"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	WorkerID     string
	BatchSize    int32
	Lease        time.Duration
	PollInterval time.Duration
}

type Publisher struct {
	pool    *pgxpool.Pool
	targets map[string]eventlog.Appender
	config  Config
}

func NewPublisher(pool *pgxpool.Pool, targets map[string]eventlog.Appender, config Config) *Publisher {
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.Lease <= 0 {
		config.Lease = 30 * time.Second
	}
	if config.PollInterval <= 0 {
		config.PollInterval = time.Second
	}
	return &Publisher{pool: pool, targets: targets, config: config}
}

func (p *Publisher) Run(ctx context.Context) error {
	if p.pool == nil {
		return fmt.Errorf("postgres pool is required")
	}
	if p.config.WorkerID == "" {
		return fmt.Errorf("outbox worker id is required")
	}

	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	for {
		if _, err := p.PublishOnce(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *Publisher) PublishOnce(ctx context.Context) (int, error) {
	//抢占
	events, err := p.claim(ctx)
	if err != nil {
		return 0, err
	}

	published := 0
	queries := db.New(p.pool)
	for topic, items := range groupByTopic(events) {
		target := p.targets[topic]
		if target == nil {
			for _, item := range items {
				p.markFailed(ctx, queries, item.EventID, fmt.Errorf("outbox topic %q is not configured", item.Topic))
			}
			continue
		}

		publishedIDs, err := p.publishTopic(ctx, queries, target, items)
		if err != nil {
			return published, err
		}
		if len(publishedIDs) == 0 {
			continue
		}
		if err := p.markPublished(ctx, queries, publishedIDs); err != nil {
			return published, err
		}
		published += len(publishedIDs)
	}
	return published, nil
}

func groupByTopic(events []db.OutboxEvent) map[string][]db.OutboxEvent {
	groups := make(map[string][]db.OutboxEvent)
	for _, event := range events {
		groups[event.Topic] = append(groups[event.Topic], event)
	}
	return groups
}

func (p *Publisher) publishTopic(
	ctx context.Context,
	queries *db.Queries,
	target eventlog.Appender,
	items []db.OutboxEvent,
) ([]string, error) {
	modelEvents := make([]model.Event, 0, len(items))
	for _, item := range items {
		modelEvents = append(modelEvents, outboxToEvent(item))
	}
	//如果实现批处理接口那就批处理，不然就一条条打进去
	if batchTarget, ok := target.(eventlog.BatchAppender); ok {
		if _, err := batchTarget.AppendBatch(ctx, modelEvents); err != nil {
			for _, item := range items {
				p.markFailed(ctx, queries, item.EventID, err)
			}
			return nil, nil
		}
		return outboxEventIDs(items), nil
	}

	publishedIDs := make([]string, 0, len(items))
	for index, event := range modelEvents {
		if _, err := target.Append(ctx, event); err != nil {
			p.markFailed(ctx, queries, items[index].EventID, err)
			continue
		}
		publishedIDs = append(publishedIDs, items[index].EventID)
	}
	return publishedIDs, nil
}

func outboxToEvent(item db.OutboxEvent) model.Event {
	return model.Event{
		ID:            item.EventID,
		Type:          model.EventType(item.EventType),
		AggregateType: item.AggregateType,
		AggregateID:   item.AggregateID,
		ShardID:       int(item.ShardID),
		OccurredAt:    item.OccurredAt,
		Payload:       item.Payload,
	}
}

func outboxEventIDs(items []db.OutboxEvent) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		ids = append(ids, item.EventID)
	}
	return ids
}

func (p *Publisher) markPublished(ctx context.Context, queries *db.Queries, eventIDs []string) error {
	updated, err := queries.MarkOutboxEventsPublished(ctx, db.MarkOutboxEventsPublishedParams{
		PublishedAt: pgtype.Int8{Int64: time.Now().Unix(), Valid: true},
		EventIds:    eventIDs,
		ClaimedBy:   p.config.WorkerID,
	})
	if err != nil {
		return fmt.Errorf("mark outbox events published: %w", err)
	}
	if updated != int64(len(eventIDs)) {
		return fmt.Errorf("marked %d outbox events published, want %d", updated, len(eventIDs))
	}
	return nil
}

func (p *Publisher) claim(ctx context.Context) ([]db.OutboxEvent, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin outbox claim: %w", err)
	}
	defer tx.Rollback(ctx)

	now := time.Now()
	events, err := db.New(tx).ClaimOutboxEvents(ctx, db.ClaimOutboxEventsParams{
		WorkerID:   p.config.WorkerID,
		LeaseUntil: now.Add(p.config.Lease).Unix(),
		NowValue:   now.Unix(),
		BatchSize:  p.config.BatchSize,
	})
	if err != nil {
		return nil, fmt.Errorf("claim outbox events: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit outbox claim: %w", err)
	}
	return events, nil
}

func (p *Publisher) markFailed(ctx context.Context, queries *db.Queries, eventID string, publishErr error) {
	_ = queries.MarkOutboxEventFailed(ctx, db.MarkOutboxEventFailedParams{
		EventID:   eventID,
		LastError: publishErr.Error(),
		ClaimedBy: p.config.WorkerID,
	})
}
