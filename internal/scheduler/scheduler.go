package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testP/internal/matcher"
	"testP/internal/model"
)

type Scheduler struct {
	shards       []*Shard
	shardCount   int
	layout       ShardLayout
	inputCh      chan model.OrderBatch
	riderEventCh chan model.RiderEvent
	wg           sync.WaitGroup
	submitted    atomic.Int64
	dispatched   atomic.Int64
}

func NewScheduler(shardCount, bufferSize, cellSize, areaSize int) *Scheduler {
	if shardCount <= 0 {
		shardCount = 1
	}
	if bufferSize <= 0 {
		bufferSize = 1
	}
	if cellSize <= 0 {
		cellSize = 1
	}

	s := &Scheduler{
		shardCount: shardCount,
		layout:     NewShardLayout(areaSize, cellSize, shardCount),
		inputCh:    make(chan model.OrderBatch, bufferSize),
		shards:     make([]*Shard, shardCount),
	}

	for i := 0; i < shardCount; i++ {
		s.shards[i] = NewShard(i, bufferSize)
	}

	return s
}

func (s *Scheduler) Start() {
	s.wg.Add(1)
	go s.dispatchLoop()
}

func (s *Scheduler) SubmitBatch(batch model.OrderBatch) {
	s.submitted.Add(int64(len(batch.Orders)))
	s.inputCh <- batch
}

func (s *Scheduler) SubmitBatchContext(ctx context.Context, batch model.OrderBatch) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.inputCh <- batch:
		s.submitted.Add(int64(len(batch.Orders)))
		return nil
	}
}

func (s *Scheduler) Close() {
	close(s.inputCh)
	s.wg.Wait()

	for _, shard := range s.shards {
		close(shard.orderCh)
	}
}

func (s *Scheduler) Shards() []*Shard {
	return s.shards
}

func (s *Scheduler) Layout() ShardLayout {
	return s.layout
}

func (s *Scheduler) Submitted() int64 {
	return s.submitted.Load()
}

func (s *Scheduler) Dispatched() int64 {
	return s.dispatched.Load()
}

func (s *Scheduler) dispatchLoop() {
	defer s.wg.Done()

	for batch := range s.inputCh {
		counts := make([]int, s.shardCount)
		for _, order := range batch.Orders {
			shardID := s.shardID(order.X, order.Y)
			counts[shardID]++
		}

		grouped := make([][]int, s.shardCount)
		for shardID, count := range counts {
			if count > 0 {
				grouped[shardID] = make([]int, 0, count)
			}
		}

		for orderIndex, order := range batch.Orders {
			shardID := s.shardID(order.X, order.Y)
			grouped[shardID] = append(grouped[shardID], orderIndex)
		}

		for shardID, indexes := range grouped {
			if len(indexes) == 0 {
				continue
			}

			s.shards[shardID].orderCh <- model.ShardOrderBatch{
				Orders:  batch.Orders,
				Indexes: indexes,
			}
			s.dispatched.Add(int64(len(indexes)))
		}
	}
}

func (s *Scheduler) riderEventLoop(m *matcher.Matcher) {
	for event := range s.riderEventCh {
		m.ApplyRiderEvent(event)
	}
}

func (s *Scheduler) shardID(x, y int) int {
	return s.layout.ShardID(x, y)
}
