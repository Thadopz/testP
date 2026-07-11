package applier

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"testP/internal/cluster/ownership"
	db "testP/internal/database"
	"testP/internal/eventlog"
	"testP/internal/model"
	"testP/internal/orderstate"
)

type PostgresApplier struct {
	base   *EventApplier
	pool   *pgxpool.Pool
	nodeID int
}

func NewPostgresApplier(base *EventApplier, pool *pgxpool.Pool, nodeID int) *PostgresApplier {
	return &PostgresApplier{base: base, pool: pool, nodeID: nodeID}
}

func (a *PostgresApplier) Apply(ctx context.Context, event model.Event) error {
	return a.base.Apply(ctx, event)
}

func (a *PostgresApplier) ApplyRecordWithFence(
	ctx context.Context,
	record eventlog.Record,
	currentOwnership ownership.Ownership,
) error {
	if a.pool == nil {
		return fmt.Errorf("postgres pool is required")
	}
	if record.Event.ID == "" {
		return fmt.Errorf("event id is required")
	}
	if err := a.base.checkFence(record.Event, currentOwnership); err != nil {
		return err
	}

	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin event transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	queries := db.New(tx)
	inserted, err := queries.RecordInboxEvent(ctx, db.RecordInboxEventParams{
		ConsumerName: model.ConsumerOrderWorker,
		EventID:      record.Event.ID,
		ShardID:      int32(record.Position.ShardID),
		OffsetValue:  record.Position.Offset,
		ProcessedAt:  record.Event.OccurredAt,
	})
	if err != nil {
		return fmt.Errorf("record inbox event: %w", err)
	}

	if inserted > 0 {
		transactionApplier := *a.base
		transactionStore := orderstate.NewPostgresStore(queries)
		transactionApplier.orderStore = transactionStore
		if err := transactionApplier.Apply(ctx, record.Event); err != nil {
			return err
		}

		if queuesMatchRequest(record.Event.Type) {
			if err := a.queueMatchRequest(ctx, queries, transactionStore, record.Event); err != nil {
				return err
			}
		}
	}

	if err := a.base.checkFence(record.Event, currentOwnership); err != nil {
		return err
	}
	if err := queries.UpsertShardCheckpoint(ctx, db.UpsertShardCheckpointParams{
		ConsumerName: model.ConsumerOrderWorker,
		Topic:        model.TopicOrderEvents,
		ShardID:      int32(record.Position.ShardID),
		OffsetValue:  record.Position.Offset + 1,
		Epoch:        currentOwnership.Epoch,
		NodeID:       int32(a.nodeID),
		UpdatedAt:    record.Event.OccurredAt,
	}); err != nil {
		return fmt.Errorf("save shard checkpoint: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit event transaction: %w", err)
	}
	return nil
}

func queuesMatchRequest(eventType model.EventType) bool {
	return eventType == model.EventOrderCreated || eventType == model.EventOrderRetryRequest
}

func (a *PostgresApplier) queueMatchRequest(
	ctx context.Context,
	queries *db.Queries,
	store *orderstate.PostgresStore,
	source model.Event,
) error {
	orderID, attempt, err := a.matchRequestIdentity(source)
	if err != nil {
		return err
	}
	state, found, err := store.Load(ctx, orderID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("match request order %d not found", orderID)
	}
	state.Status = orderstate.StatusMatchPending
	if err := store.Save(ctx, state); err != nil {
		return err
	}

	payload, err := a.base.codec.EncodePayload(model.MatchRequested{
		OrderID: orderID,
		X:       state.X,
		Y:       state.Y,
		Attempt: attempt,
	})
	if err != nil {
		return fmt.Errorf("encode match request: %w", err)
	}
	event := model.Event{
		ID:            source.ID + "-match-requested",
		Type:          model.EventMatchRequested,
		AggregateType: "order",
		AggregateID:   fmt.Sprintf("%d", orderID),
		ShardID:       source.ShardID,
		OccurredAt:    source.OccurredAt,
		Payload:       payload,
	}
	return queries.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
		EventID:       event.ID,
		EventType:     string(event.Type),
		AggregateType: event.AggregateType,
		AggregateID:   event.AggregateID,
		ShardID:       int32(event.ShardID),
		OccurredAt:    event.OccurredAt,
		Payload:       event.Payload,
		Topic:         model.TopicMatchRequests,
		MessageKey:    event.AggregateID,
	})
}

func (a *PostgresApplier) matchRequestIdentity(event model.Event) (int64, int, error) {
	switch event.Type {
	case model.EventOrderCreated:
		payload := model.OrderCreated{}
		if err := a.base.codec.DecodePayload(event.Payload, &payload); err != nil {
			return 0, 0, err
		}
		return payload.OrderID, 0, nil
	case model.EventOrderRetryRequest:
		payload := model.OrderRetryRequest{}
		if err := a.base.codec.DecodePayload(event.Payload, &payload); err != nil {
			return 0, 0, err
		}
		return payload.OrderID, payload.Attempt, nil
	default:
		return 0, 0, fmt.Errorf("event %s cannot create a match request", event.Type)
	}
}
