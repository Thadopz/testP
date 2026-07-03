package matcher

import (
	"testing"

	"testP/internal/model"
)

const (
	benchAreaSize   = 100000
	benchCellSize   = 4472
	benchRiderCount = 10000
)

var (
	benchCandidatesSink []RiderCandidate
	benchRiderSink      *model.Rider
)

func BenchmarkGridFindNearbyCandidatesRadius1(b *testing.B) {
	benchmarkGridFindNearbyCandidates(b, 1)
}

func BenchmarkGridFindNearbyCandidatesRadius3(b *testing.B) {
	benchmarkGridFindNearbyCandidates(b, 3)
}

func BenchmarkGridFindNearbyCandidatesRadius8(b *testing.B) {
	benchmarkGridFindNearbyCandidates(b, 8)
}

func BenchmarkGridFindNearbyCandidatesRange0To1(b *testing.B) {
	benchmarkGridFindNearbyCandidatesInRange(b, -1, 1)
}

func BenchmarkGridFindNearbyCandidatesRange2To3(b *testing.B) {
	benchmarkGridFindNearbyCandidatesInRange(b, 1, 3)
}

func BenchmarkGridFindNearbyCandidatesRange4To8(b *testing.B) {
	benchmarkGridFindNearbyCandidatesInRange(b, 3, 8)
}

func BenchmarkGridMoveRider(b *testing.B) {
	riders := benchRiders(benchRiderCount, benchAreaSize)
	grid := NewGridIndex(riders, benchCellSize)
	moves := benchRiderMoves(riders[0].UID, 1024, benchAreaSize)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		grid.MoveRider(moves[i%len(moves)])
	}

	benchRiderSink = riders[0]
}

func benchmarkGridFindNearbyCandidates(b *testing.B, radius int) {
	riders := benchRiders(benchRiderCount, benchAreaSize)
	grid := NewGridIndex(riders, benchCellSize)
	orders := benchPoints(1024, benchAreaSize)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		point := orders[i%len(orders)]
		benchCandidatesSink = grid.FindNearbyCandidates(point.X, point.Y, radius)
	}
}

func benchmarkGridFindNearbyCandidatesInRange(b *testing.B, innerRadius int, outerRadius int) {
	riders := benchRiders(benchRiderCount, benchAreaSize)
	grid := NewGridIndex(riders, benchCellSize)
	orders := benchPoints(1024, benchAreaSize)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		point := orders[i%len(orders)]
		benchCandidatesSink = grid.FindNearbyCandidatesInRange(point.X, point.Y, innerRadius, outerRadius)
	}
}

func benchRiders(count int, areaSize int) []*model.Rider {
	riders := make([]*model.Rider, count)
	for i := range riders {
		riders[i] = &model.Rider{
			UID: int64(i + 1),
			X:   (i*7919 + 17) % areaSize,
			Y:   (i*104729 + 29) % areaSize,
		}
	}
	return riders
}

func benchRiderMoves(uid int64, count int, areaSize int) []*model.Rider {
	moves := make([]*model.Rider, count)
	for i := range moves {
		moves[i] = &model.Rider{
			UID: uid,
			X:   (i*3571 + 41) % areaSize,
			Y:   (i*4447 + 53) % areaSize,
		}
	}
	return moves
}

func benchPoints(count int, areaSize int) []model.Order {
	points := make([]model.Order, count)
	for i := range points {
		points[i] = model.Order{
			ID: int64(i + 1),
			X:  (i*1543 + 71) % areaSize,
			Y:  (i*3253 + 83) % areaSize,
		}
	}
	return points
}
