package orderstate

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	db "testP/internal/database"
)

type PostgresStore struct {
	queries *db.Queries
}

func NewPostgresStore(queries *db.Queries) *PostgresStore {
	return &PostgresStore{queries: queries}
}

func (s *PostgresStore) Load(ctx context.Context, orderID int64) (State, bool, error) {
	order, err := s.queries.GetOrder(ctx, orderID)
	if errors.Is(err, pgx.ErrNoRows) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}

	return State{
		OrderID:      order.OrderID,
		ShardID:      int(order.ShardID),
		Status:       Status(order.Status),
		X:            int(order.X),
		Y:            int(order.Y),
		Attempt:      int(order.Attempt),
		CancelReason: order.CancelReason,
		RetryReason:  order.RetryReason,
		MissReason:   order.MissReason,
		RiderID:      order.RiderID,
		Score:        int(order.Score),
		LastEventID:  order.LastEventID,
		UpdatedAt:    order.UpdatedAt,
	}, true, nil
}

func (s *PostgresStore) Save(ctx context.Context, state State) error {
	return s.queries.UpsertOrder(ctx, db.UpsertOrderParams{
		OrderID:      state.OrderID,
		ShardID:      int32(state.ShardID),
		Status:       string(state.Status),
		X:            int32(state.X),
		Y:            int32(state.Y),
		Attempt:      int32(state.Attempt),
		CancelReason: state.CancelReason,
		RetryReason:  state.RetryReason,
		MissReason:   state.MissReason,
		RiderID:      state.RiderID,
		Score:        int32(state.Score),
		LastEventID:  state.LastEventID,
		UpdatedAt:    state.UpdatedAt,
	})
}
