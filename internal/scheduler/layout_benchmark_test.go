package scheduler

import (
	"runtime"
	"sync"
	"testing"

	"testP/internal/model"
)

const (
	benchAreaSize   = 100000
	benchCellSize   = 4472
	benchShardCount = 64
	benchBatchSize  = 1024
)

var (
	benchShardIDResult int
	benchNeighborsSink []int
)

func BenchmarkShardLayoutShardID(b *testing.B) {
	layout := NewShardLayout(benchAreaSize, benchCellSize, benchShardCount)
	orders := benchOrders(benchBatchSize, benchAreaSize)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		order := orders[i%len(orders)]
		benchShardIDResult = layout.ShardID(order.X, order.Y)
	}
}

func BenchmarkShardLayoutNeighborShardIDs(b *testing.B) {
	layout := NewShardLayout(benchAreaSize, benchCellSize, benchShardCount)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchNeighborsSink = layout.NeighborShardIDs(i % benchShardCount)
	}
}

func BenchmarkSchedulerDispatchBatch(b *testing.B) {
	s := NewScheduler(benchShardCount, 256, benchCellSize, benchAreaSize)
	batch := model.OrderBatch{Orders: benchOrders(benchBatchSize, benchAreaSize)}
	var drainWG sync.WaitGroup

	for _, shard := range s.shards {
		drainWG.Add(1)
		go func(shard *Shard) {
			defer drainWG.Done()
			for range shard.orderCh {
			}
		}(shard)
	}

	s.Start()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s.SubmitBatch(batch)
	}
	for s.Dispatched() < int64(b.N*len(batch.Orders)) {
		runtime.Gosched()
	}

	b.StopTimer()
	s.Close()
	drainWG.Wait()
}

func benchOrders(count int, areaSize int) []model.Order {
	orders := make([]model.Order, count)
	for i := range orders {
		orders[i] = model.Order{
			ID: int64(i + 1),
			X:  (i*7919 + 17) % areaSize,
			Y:  (i*104729 + 29) % areaSize,
		}
	}
	return orders
}
