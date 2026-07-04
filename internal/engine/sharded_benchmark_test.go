package engine

import (
	"context"
	"sync"
	"testing"

	"testP/internal/matcher"
	"testP/internal/model"
)

const (
	benchAreaSize    = 100000
	benchShardCount  = 64
	benchBufferSize  = 256
	benchCellSize    = 4472
	benchLoadWeight  = 10000
	benchRiderCount  = 10000
	benchOrderBatch  = 1024
	benchHomeShardID = 0
)

var (
	benchEngineSink     *ShardedEngine
	benchCandidateSink  []matcher.RiderCandidate
	benchEngineRider    *model.Rider
	benchEngineError    error
	benchEngineInt64Out int64
)

func BenchmarkNewShardedEngine(b *testing.B) {
	riders := benchEngineRiders(benchRiderCount, benchAreaSize)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchEngineSink = NewShardedEngine(riders, benchShardCount, benchBufferSize, benchCellSize, benchAreaSize, benchLoadWeight)
	}
}

func BenchmarkShardedSubmitBatchRouting(b *testing.B) {
	e := NewShardedEngine(benchEngineRiders(benchRiderCount, benchAreaSize), benchShardCount, benchBufferSize, benchCellSize, benchAreaSize, benchLoadWeight)
	batch := model.OrderBatch{Orders: benchEngineOrders(benchOrderBatch, benchAreaSize)}
	var drainWG sync.WaitGroup

	for _, shard := range e.shards {
		drainWG.Add(1)
		go func(shard *Shard) {
			defer drainWG.Done()
			for range shard.orderCh {
			}
		}(shard)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchEngineError = e.SubmitBatch(context.Background(), batch)
	}

	b.StopTimer()
	e.Close()
	drainWG.Wait()
}

func BenchmarkShardedFindCandidatesHomeShard(b *testing.B) {
	e := NewShardedEngine(benchEngineRiders(benchRiderCount, benchAreaSize), benchShardCount, benchBufferSize, benchCellSize, benchAreaSize, benchLoadWeight)
	orders := benchEngineOrders(1024, benchAreaSize)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		order := orders[i%len(orders)]
		homeShardID := e.layout.ShardID(order.X, order.Y)
		benchCandidateSink = e.findCandidates(homeShardID, &order)
	}
}

func BenchmarkShardedCollectCandidatesNeighborShards(b *testing.B) {
	e := NewShardedEngine(benchEngineRiders(benchRiderCount, benchAreaSize), benchShardCount, benchBufferSize, benchCellSize, benchAreaSize, benchLoadWeight)
	ids := e.neighborShardIDs(benchHomeShardID)
	order := model.Order{ID: 1, X: 10, Y: 10}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		benchCandidateSink = e.collectCandidates(ids, &order, 8)
	}
}

func BenchmarkShardedMatchOne(b *testing.B) {
	e := NewShardedEngine(benchEngineRiders(benchRiderCount, benchAreaSize), benchShardCount, benchBufferSize, benchCellSize, benchAreaSize, benchLoadWeight)
	orders := benchEngineOrders(1024, benchAreaSize)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		order := orders[i%len(orders)]
		homeShardID := e.layout.ShardID(order.X, order.Y)
		e.matchOne(homeShardID, &order)
	}

	benchEngineInt64Out = e.metrics.Matched.Load()
}

func BenchmarkShardedApplyRiderMoveSameShard(b *testing.B) {
	e := NewShardedEngine(benchEngineRiders(benchRiderCount, benchAreaSize), benchShardCount, benchBufferSize, benchCellSize, benchAreaSize, benchLoadWeight)
	events := benchSameShardMoveEvents(1, 1024)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		e.ApplyRiderEvent(events[i%len(events)])
	}

	benchEngineRider = e.ridersByUID[1]
}

func BenchmarkShardedApplyRiderMoveCrossShard(b *testing.B) {
	e := NewShardedEngine(benchEngineRiders(benchRiderCount, benchAreaSize), benchShardCount, benchBufferSize, benchCellSize, benchAreaSize, benchLoadWeight)
	events := []model.RiderEvent{
		{Type: model.RiderMove, UID: 1, X: benchAreaSize - 1, Y: benchAreaSize - 1},
		{Type: model.RiderMove, UID: 1, X: 10, Y: 10},
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		e.ApplyRiderEvent(events[i%len(events)])
	}

	benchEngineRider = e.ridersByUID[1]
}

func benchEngineRiders(count int, areaSize int) []*model.Rider {
	riders := make([]*model.Rider, count)
	riders[0] = &model.Rider{UID: 1, X: 10, Y: 10}
	for i := 1; i < count; i++ {
		riders[i] = &model.Rider{
			UID: int64(i + 1),
			X:   (i*7919 + 17) % areaSize,
			Y:   (i*104729 + 29) % areaSize,
		}
	}
	return riders
}

func benchEngineOrders(count int, areaSize int) []model.Order {
	orders := make([]model.Order, count)
	for i := range orders {
		orders[i] = model.Order{
			ID: int64(i + 1),
			X:  (i*1543 + 71) % areaSize,
			Y:  (i*3253 + 83) % areaSize,
		}
	}
	return orders
}

func benchSameShardMoveEvents(uid int64, count int) []model.RiderEvent {
	events := make([]model.RiderEvent, count)
	for i := range events {
		events[i] = model.RiderEvent{
			Type: model.RiderMove,
			UID:  uid,
			X:    10 + i%100,
			Y:    10 + i%100,
		}
	}
	return events
}
