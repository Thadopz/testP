package eventlog

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"testP/internal/model"

	"github.com/segmentio/kafka-go"
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
	dialer   *kafka.Dialer
	writer   *kafka.Writer
	closeMu  sync.Mutex
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

	dialer := &kafka.Dialer{
		Resolver: localhostResolver{},
	}

	return &KafkaEventLog{
		brokers:  append([]string(nil), cfg.Brokers...),
		topic:    cfg.Topic,
		codec:    cfg.Codec,
		maxBytes: cfg.MaxBytes,
		maxWait:  cfg.MaxWait,
		dialer:   dialer,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        cfg.Topic,
			Balancer:     shardIDBalancer{},
			BatchSize:    100,
			BatchTimeout: 10 * time.Millisecond,
			Transport: &kafka.Transport{
				Dial: localKafkaDial,
			},
		},
	}, nil
}

func (l *KafkaEventLog) Append(ctx context.Context, event model.Event) (Position, error) {
	positions, err := l.AppendBatch(ctx, []model.Event{event})
	if err != nil {
		return Position{}, err
	}
	return positions[0], nil
}

func (l *KafkaEventLog) AppendBatch(ctx context.Context, events []model.Event) ([]Position, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, nil
	}

	messages := make([]kafka.Message, 0, len(events))
	positions := make([]Position, 0, len(events))
	for _, event := range events {
		if event.ShardID < 0 {
			return nil, fmt.Errorf("shard id must be >= 0: %d", event.ShardID)
		}
		msg, err := l.encodeMessage(event)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
		positions = append(positions, Position{
			ShardID: event.ShardID,
			Offset:  -1,
		})
	}

	l.closeMu.Lock()
	writer := l.writer
	l.closeMu.Unlock()
	if writer == nil {
		return nil, fmt.Errorf("kafka eventlog is closed")
	}
	if err := writer.WriteMessages(ctx, messages...); err != nil {
		return nil, fmt.Errorf("write kafka messages: %w", err)
	}

	return positions, nil
}

func (l *KafkaEventLog) Close() error {
	l.closeMu.Lock()
	defer l.closeMu.Unlock()

	if l.writer == nil {
		return nil
	}
	err := l.writer.Close()
	l.writer = nil
	return err
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

	conn, err := l.dialer.DialLeader(ctx, "tcp", l.brokers[0], l.topic, shardID)
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
		Key:   []byte(fmt.Sprintf("%d", event.ShardID)),
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
		Dialer:    l.dialer,
	})
}

type localhostResolver struct{}

// 避免出现妙妙转义导致localhost变成::1,kafka只监听ipv4的地址
func (localhostResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	if host == "localhost" {
		return []string{"127.0.0.1"}, nil
	}
	return net.DefaultResolver.LookupHost(ctx, host)
}

type shardIDBalancer struct{}

func (shardIDBalancer) Balance(message kafka.Message, partitions ...int) int {
	if len(partitions) == 0 {
		return 0
	}
	shardID, err := strconv.Atoi(string(message.Key))
	if err != nil {
		return partitions[0]
	}
	for _, partition := range partitions {
		if partition == shardID {
			return partition
		}
	}
	return partitions[0]
}

// 同lookupHost
func localKafkaDial(ctx context.Context, network string, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err == nil && host == "localhost" {
		address = net.JoinHostPort("127.0.0.1", port)
	}
	return (&net.Dialer{
		Timeout:   3 * time.Second,
		DualStack: true,
	}).DialContext(ctx, network, address)
}
