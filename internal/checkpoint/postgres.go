package checkpoint

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	db "testP/internal/database"
	"testP/internal/model"
)

type PostgresStore struct {
	queries      *db.Queries
	consumerName string
	topic        string
}

func NewPostgresStore(queries *db.Queries) *PostgresStore {
	return NewPostgresStoreForConsumer(queries, model.ConsumerOrderWorker, model.TopicOrderEvents)
}

func NewPostgresStoreForConsumer(queries *db.Queries, consumerName string, topic string) *PostgresStore {
	return &PostgresStore{
		queries:      queries,
		consumerName: consumerName,
		topic:        topic,
	}
}

func (s *PostgresStore) SaveShardCheckpoint(ctx context.Context, value ShardCheckpoint) error {
	return s.queries.UpsertShardCheckpoint(ctx, db.UpsertShardCheckpointParams{
		ConsumerName: s.consumerName,
		Topic:        s.topic,
		ShardID:      int32(value.ShardID),
		OffsetValue:  value.Offset,
		Epoch:        value.Epoch,
		NodeID:       int32(value.NodeID),
		UpdatedAt:    value.UpdatedAt,
	})
}

func (s *PostgresStore) LoadShardCheckpoint(ctx context.Context, shardID int) (ShardCheckpoint, bool, error) {
	value, err := s.queries.GetShardCheckpoint(ctx, db.GetShardCheckpointParams{
		ConsumerName: s.consumerName,
		Topic:        s.topic,
		ShardID:      int32(shardID),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return ShardCheckpoint{}, false, nil
	}
	if err != nil {
		return ShardCheckpoint{}, false, err
	}

	return ShardCheckpoint{
		ShardID:   int(value.ShardID),
		Offset:    value.OffsetValue,
		Epoch:     value.Epoch,
		NodeID:    int(value.NodeID),
		UpdatedAt: value.UpdatedAt,
	}, true, nil
}
