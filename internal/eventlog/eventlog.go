package eventlog

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testP/internal/model"
)

type Position struct {
	ShardID int
	Offset  int64
}

type Record struct {
	Position Position
	Event    model.Event
}

type EventLog interface {
	Append(ctx context.Context, event model.Event) (Position, error)
	ReadFrom(ctx context.Context, position Position) (<-chan Record, error)
}

type TailEventLog interface {
	EventLog
	TailFrom(ctx context.Context, position Position) (<-chan Record, error)
}

type OffsetReader interface {
	EndOffset(ctx context.Context, shardID int) (int64, error)
}

type MemoryEventLog struct {
	mu      sync.Mutex
	records map[int][]Record
}

func (m *MemoryEventLog) EndOffset(ctx context.Context, shardID int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if m.records == nil {
		m.records = make(map[int][]Record)
	}
	records, ok := m.records[shardID]
	if !ok {
		return 0, fmt.Errorf("Shard Not Found")
	}
	return int64(len(records)), nil
}

func (m *MemoryEventLog) Append(ctx context.Context, event model.Event) (Position, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return Position{}, err
	}
	if m.records == nil {
		m.records = make(map[int][]Record)
	}
	position := Position{}
	if _, ok := m.records[event.ShardID]; !ok {
		return position, fmt.Errorf("Shard Not Found")
	}
	position = Position{event.ShardID, int64(len(m.records[event.ShardID]))}
	m.records[event.ShardID] = append(m.records[event.ShardID], Record{
		Position: position,
		Event:    event,
	})

	return position, nil
}

func (m *MemoryEventLog) ReadFrom(ctx context.Context, position Position) (<-chan Record, error) {
	m.mu.Lock()

	if m.records == nil {
		m.records = make(map[int][]Record)
	}
	if _, ok := m.records[position.ShardID]; !ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("Shard Not Found")
	}
	l := int64(len(m.records[position.ShardID]))
	if position.Offset > l {
		m.mu.Unlock()
		return nil, fmt.Errorf("Offset Out of range")
	}
	RecordCh := make(chan Record)
	tmp := slices.Clone(m.records[position.ShardID][position.Offset:])
	m.mu.Unlock()
	go func() {
		defer close(RecordCh)
		for i := range tmp {
			select {
			case <-ctx.Done():
				return
			case RecordCh <- tmp[i]:
			}
		}
	}()
	return RecordCh, nil
}
