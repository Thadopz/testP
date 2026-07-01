package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testP/internal/model"
)

type Scheduler struct {
	shards     []*Shard
	shardCount int
	cellSize   int
	inputCh    chan model.OrderBatch
	wg         sync.WaitGroup
	submitted  atomic.Int64
	dispatched atomic.Int64
}

func NewScheduler(shardCount, bufferSize, cellSize int) *Scheduler {
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
		cellSize:   cellSize,
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

func (s *Scheduler) Submitted() int64 {
	return s.submitted.Load()
}

func (s *Scheduler) Dispatched() int64 {
	return s.dispatched.Load()
}

func (s *Scheduler) dispatchLoop() {
	defer s.wg.Done()

	for batch := range s.inputCh {
		grouped := make([][]model.Order, s.shardCount)

		for _, order := range batch.Orders {
			shardID := s.shardID(order.X, order.Y)
			grouped[shardID] = append(grouped[shardID], order)
		}

		for shardID, orders := range grouped {
			if len(orders) == 0 {
				continue
			}

			s.shards[shardID].orderCh <- model.OrderBatch{Orders: orders}
			s.dispatched.Add(int64(len(orders)))
		}
	}
}

func (s *Scheduler) shardID(x, y int) int {
	cx := x / s.cellSize
	cy := y / s.cellSize
	id := (cx*73856093 ^ cy*19349663) % s.shardCount
	if id < 0 {
		return -id
	}
	return id
}
