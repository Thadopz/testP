package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	db "testP/internal/database"
	"testP/internal/eventlog"
	"testP/internal/model"
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
	events, err := p.claim(ctx)
	if err != nil {
		return 0, err
	}

	published := 0
	queries := db.New(p.pool)
	for _, item := range events {
		target := p.targets[item.Topic]
		if target == nil {
			p.markFailed(ctx, queries, item.EventID, fmt.Errorf("outbox topic %q is not configured", item.Topic))
			continue
		}

		event := model.Event{
			ID:            item.EventID,
			Type:          model.EventType(item.EventType),
			AggregateType: item.AggregateType,
			AggregateID:   item.AggregateID,
			ShardID:       int(item.ShardID),
			OccurredAt:    item.OccurredAt,
			Payload:       item.Payload,
		}
		if _, err := target.Append(ctx, event); err != nil {
			p.markFailed(ctx, queries, item.EventID, err)
			continue
		}

		err := queries.MarkOutboxEventPublished(ctx, db.MarkOutboxEventPublishedParams{
			EventID:     item.EventID,
			PublishedAt: pgtype.Int8{Int64: time.Now().Unix(), Valid: true},
			ClaimedBy:   p.config.WorkerID,
		})
		if err != nil {
			return published, fmt.Errorf("mark outbox event published: %w", err)
		}
		published++
	}
	return published, nil
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
