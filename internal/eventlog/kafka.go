package eventlog

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/segmentio/kafka-go"
	"testP/internal/model"
)

type KafkaConfig struct {
	Brokers  []string
	Topic    string
	Codec    EventCodec
	MaxBytes int
	MaxWait  time.Duration
}

type KafkaEventLog struct {
	brokers  []string
	topic    string
	codec    EventCodec
	maxBytes int
	maxWait  time.Duration
}

func NewKafkaEventLog(cfg KafkaConfig) (*KafkaEventLog, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers must not be empty")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka topic must not be empty")
	}
	if cfg.Codec == nil {
		cfg.Codec = &JSONEventCodec{}
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 10e6
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = time.Second
	}

	return &KafkaEventLog{
		brokers:  append([]string(nil), cfg.Brokers...),
		topic:    cfg.Topic,
		codec:    cfg.Codec,
		maxBytes: cfg.MaxBytes,
		maxWait:  cfg.MaxWait,
	}, nil
}

func (l *KafkaEventLog) Append(ctx context.Context, event model.Event) (Position, error) {
	if err := ctx.Err(); err != nil {
		return Position{}, err
	}
	if event.ShardID < 0 {
		return Position{}, fmt.Errorf("shard id must be >= 0: %d", event.ShardID)
	}

	conn, err := kafka.DialLeader(ctx, "tcp", l.brokers[0], l.topic, event.ShardID)
	if err != nil {
		return Position{}, fmt.Errorf("dial kafka leader: %w", err)
	}
	defer conn.Close()

	offset, err := conn.ReadLastOffset()
	if err != nil {
		return Position{}, fmt.Errorf("read kafka last offset: %w", err)
	}

	message, err := l.encodeMessage(event)
	if err != nil {
		return Position{}, err
	}
	if _, err := conn.WriteMessages(message); err != nil {
		return Position{}, fmt.Errorf("write kafka message: %w", err)
	}

	return Position{
		ShardID: event.ShardID,
		Offset:  offset,
	}, nil
}

func (l *KafkaEventLog) ReadFrom(ctx context.Context, position Position) (<-chan Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	endOffset, err := l.EndOffset(ctx, position.ShardID)
	if err != nil {
		return nil, err
	}
	if position.Offset >= endOffset {
		recordCh := make(chan Record)
		close(recordCh)
		return recordCh, nil
	}

	reader := l.newReader(position.ShardID)
	if err := reader.SetOffset(position.Offset); err != nil {
		reader.Close()
		return nil, fmt.Errorf("set kafka offset: %w", err)
	}

	recordCh := make(chan Record)
	go func() {
		defer close(recordCh)
		defer reader.Close()

		for {
			message, err := reader.FetchMessage(ctx)
			if err != nil {
				return
			}
			if message.Offset >= endOffset {
				return
			}

			record, err := l.decodeMessage(position.ShardID, message)
			if err != nil {
				return
			}

			select {
			case <-ctx.Done():
				return
			case recordCh <- record:
			}
		}
	}()

	return recordCh, nil
}

func (l *KafkaEventLog) TailFrom(ctx context.Context, position Position) (<-chan Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	reader := l.newReader(position.ShardID)
	if err := reader.SetOffset(position.Offset); err != nil {
		reader.Close()
		return nil, fmt.Errorf("set kafka offset: %w", err)
	}

	recordCh := make(chan Record)
	go func() {
		defer close(recordCh)
		defer reader.Close()

		for {
			message, err := reader.FetchMessage(ctx)
			if err != nil {
				if err == io.EOF {
					return
				}
				return
			}

			record, err := l.decodeMessage(position.ShardID, message)
			if err != nil {
				return
			}

			select {
			case <-ctx.Done():
				return
			case recordCh <- record:
			}
		}
	}()

	return recordCh, nil
}

func (l *KafkaEventLog) EndOffset(ctx context.Context, shardID int) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if shardID < 0 {
		return 0, fmt.Errorf("shard id must be >= 0: %d", shardID)
	}

	conn, err := kafka.DialLeader(ctx, "tcp", l.brokers[0], l.topic, shardID)
	if err != nil {
		return 0, fmt.Errorf("dial kafka leader: %w", err)
	}
	defer conn.Close()

	offset, err := conn.ReadLastOffset()
	if err != nil {
		return 0, fmt.Errorf("read kafka last offset: %w", err)
	}

	return offset, nil
}

func (l *KafkaEventLog) encodeMessage(event model.Event) (kafka.Message, error) {
	value, err := l.codec.EncodeEvent(event)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("encode kafka event: %w", err)
	}

	return kafka.Message{
		Key:   []byte(event.ID),
		Value: value,
	}, nil
}

func (l *KafkaEventLog) decodeMessage(shardID int, message kafka.Message) (Record, error) {
	event, err := l.codec.DecodeEvent(message.Value)
	if err != nil {
		return Record{}, fmt.Errorf("decode kafka event: %w", err)
	}
	event.ShardID = shardID

	return Record{
		Position: Position{
			ShardID: shardID,
			Offset:  message.Offset,
		},
		Event: event,
	}, nil
}

func (l *KafkaEventLog) newReader(shardID int) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:   l.brokers,
		Topic:     l.topic,
		Partition: shardID,
		MinBytes:  1,
		MaxBytes:  l.maxBytes,
		MaxWait:   l.maxWait,
	})
}
