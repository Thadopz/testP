package matchworker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	clusterownership "testP/internal/cluster/ownership"
	db "testP/internal/database"
	"testP/internal/eventlog"
	"testP/internal/matching"
	"testP/internal/model"
	"testP/internal/orderstate"
)

type OwnershipReader interface {
	OwnerOf(shardID int) (clusterownership.Ownership, bool, error)
}

type Config struct {
	NodeID         int
	CandidateLimit int
	MaxRiderOrders int64
	Topic          string
}

type Worker struct {
	pool            *pgxpool.Pool
	codec           eventlog.EventCodec
	index           *matching.Index
	ownershipReader OwnershipReader
	config          Config
}

func New(
	pool *pgxpool.Pool,
	codec eventlog.EventCodec,
	index *matching.Index,
	ownershipReader OwnershipReader,
	config Config,
) *Worker {
	if config.CandidateLimit <= 0 {
		config.CandidateLimit = 10
	}
	if config.MaxRiderOrders <= 0 {
		config.MaxRiderOrders = 3
	}
	if config.Topic == "" {
		config.Topic = model.TopicMatchRequests
	}
	return &Worker{
		pool:            pool,
		codec:           codec,
		index:           index,
		ownershipReader: ownershipReader,
		config:          config,
	}
}

func (w *Worker) Apply(context.Context, model.Event) error {
	return fmt.Errorf("matcher worker requires a record with Kafka position")
}

func (w *Worker) ApplyRecordWithFence(
	ctx context.Context,
	record eventlog.Record,
	ownership clusterownership.Ownership,
) error {
	if record.Event.Type != model.EventMatchRequested {
		return fmt.Errorf("unsupported matcher event type: %s", record.Event.Type)
	}
	if err := w.checkFence(record.Event.ShardID, ownership); err != nil {
		return err
	}

	request := model.MatchRequested{}
	if err := w.codec.DecodePayload(record.Event.Payload, &request); err != nil {
		return fmt.Errorf("decode match request: %w", err)
	}
	candidates := w.index.FindCandidates(model.Order{
		ID: request.OrderID,
		X:  request.X,
		Y:  request.Y,
	}, w.config.CandidateLimit)

	tx, err := w.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin match transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	queries := db.New(tx)

	inserted, err := queries.RecordInboxEvent(ctx, db.RecordInboxEventParams{
		ConsumerName: model.ConsumerMatcherWorker,
		EventID:      record.Event.ID,
		ShardID:      int32(record.Position.ShardID),
		OffsetValue:  record.Position.Offset,
		ProcessedAt:  time.Now().Unix(),
	})
	if err != nil {
		return fmt.Errorf("record matcher inbox event: %w", err)
	}

	var reservedRiderID int64
	var reservedCount int64
	if inserted > 0 {
		order, err := queries.GetOrderForUpdate(ctx, request.OrderID)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("match order %d not found", request.OrderID)
		}
		if err != nil {
			return fmt.Errorf("lock match order: %w", err)
		}
		if order.Status == string(orderstate.StatusMatchPending) {
			reservedRiderID, reservedCount, err = w.matchOrder(ctx, queries, record.Event, request, candidates)
			if err != nil {
				return err
			}
		}
	}

	if err := w.checkFence(record.Event.ShardID, ownership); err != nil {
		return err
	}
	if err := queries.UpsertShardCheckpoint(ctx, db.UpsertShardCheckpointParams{
		ConsumerName: model.ConsumerMatcherWorker,
		Topic:        w.config.Topic,
		ShardID:      int32(record.Position.ShardID),
		OffsetValue:  record.Position.Offset + 1,
		Epoch:        ownership.Epoch,
		NodeID:       int32(w.config.NodeID),
		UpdatedAt:    time.Now().Unix(),
	}); err != nil {
		return fmt.Errorf("save matcher checkpoint: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit match transaction: %w", err)
	}
	if reservedRiderID != 0 {
		w.index.SetRiderCount(reservedRiderID, reservedCount)
	}
	return nil
}

func (w *Worker) matchOrder(
	ctx context.Context,
	queries *db.Queries,
	requestEvent model.Event,
	request model.MatchRequested,
	candidates []matching.Candidate,
) (int64, int64, error) {
	for _, candidate := range candidates {
		rider, err := queries.ReserveRider(ctx, db.ReserveRiderParams{
			Uid:      candidate.RiderID,
			MaxCount: w.config.MaxRiderOrders,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return 0, 0, fmt.Errorf("reserve rider %d: %w", candidate.RiderID, err)
		}

		resultEvent, err := w.matchedEvent(requestEvent, request.OrderID, rider.Uid, candidate.Score)
		if err != nil {
			return 0, 0, err
		}
		updated, err := queries.MarkOrderMatched(ctx, db.MarkOrderMatchedParams{
			OrderID:     request.OrderID,
			RiderID:     rider.Uid,
			Score:       scoreInt32(candidate.Score),
			LastEventID: resultEvent.ID,
			UpdatedAt:   resultEvent.OccurredAt,
		})
		if err != nil {
			return 0, 0, fmt.Errorf("mark order matched: %w", err)
		}
		if updated != 1 {
			return 0, 0, fmt.Errorf("order %d is no longer match pending", request.OrderID)
		}
		if err := createOutboxEvent(ctx, queries, resultEvent, model.TopicOrderEvents); err != nil {
			return 0, 0, err
		}
		return rider.Uid, rider.Count, nil
	}

	resultEvent, err := w.missedEvent(requestEvent, request.OrderID)
	if err != nil {
		return 0, 0, err
	}
	updated, err := queries.MarkOrderMissed(ctx, db.MarkOrderMissedParams{
		OrderID:     request.OrderID,
		MissReason:  "no_available_rider",
		LastEventID: resultEvent.ID,
		UpdatedAt:   resultEvent.OccurredAt,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("mark order missed: %w", err)
	}
	if updated == 1 {
		if err := createOutboxEvent(ctx, queries, resultEvent, model.TopicOrderEvents); err != nil {
			return 0, 0, err
		}
	}
	return 0, 0, nil
}

func (w *Worker) matchedEvent(request model.Event, orderID int64, riderID int64, score int64) (model.Event, error) {
	payload, err := w.codec.EncodePayload(model.OrderMatched{
		OrderID: orderID,
		RiderID: riderID,
		Score:   int(scoreInt32(score)),
	})
	if err != nil {
		return model.Event{}, err
	}
	return resultEvent(request, model.EventOrderMatched, payload), nil
}

func (w *Worker) missedEvent(request model.Event, orderID int64) (model.Event, error) {
	payload, err := w.codec.EncodePayload(model.OrderMissed{
		OrderID: orderID,
		Reason:  "no_available_rider",
	})
	if err != nil {
		return model.Event{}, err
	}
	return resultEvent(request, model.EventOrderMissed, payload), nil
}

func resultEvent(request model.Event, eventType model.EventType, payload []byte) model.Event {
	return model.Event{
		ID:            fmt.Sprintf("%s-%s", request.ID, eventType),
		Type:          eventType,
		AggregateType: "order",
		AggregateID:   request.AggregateID,
		ShardID:       request.ShardID,
		OccurredAt:    time.Now().Unix(),
		Payload:       payload,
	}
}

func createOutboxEvent(ctx context.Context, queries *db.Queries, event model.Event, topic string) error {
	return queries.CreateOutboxEvent(ctx, db.CreateOutboxEventParams{
		EventID:       event.ID,
		EventType:     string(event.Type),
		AggregateType: event.AggregateType,
		AggregateID:   event.AggregateID,
		ShardID:       int32(event.ShardID),
		OccurredAt:    event.OccurredAt,
		Payload:       event.Payload,
		Topic:         topic,
		MessageKey:    event.AggregateID,
	})
}

func (w *Worker) checkFence(shardID int, expected clusterownership.Ownership) error {
	if expected.NodeID != w.config.NodeID || expected.ShardID != shardID {
		return clusterownership.ErrOwnershipFenceLost
	}
	current, found, err := w.ownershipReader.OwnerOf(shardID)
	if err != nil {
		return err
	}
	if !found || current.NodeID != expected.NodeID || current.Epoch != expected.Epoch {
		return clusterownership.ErrOwnershipFenceLost
	}
	return nil
}

func scoreInt32(value int64) int32 {
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	if value < math.MinInt32 {
		return math.MinInt32
	}
	return int32(value)
}
