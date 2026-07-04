package matcher

import (
	"math"
	"sync/atomic"
	"testP/internal/model"
)

var searchRadii = []int{1, 3, 8}

type Matcher struct {
	grid       *GridIndex
	loadWeight int64
	matched    atomic.Int64
	missed     atomic.Int64
}

func NewMatcher(riders []*model.Rider, cellSize int, loadWeight int64) *Matcher {
	return &Matcher{
		grid:       NewGridIndex(riders, cellSize),
		loadWeight: loadWeight,
	}
}

func (m *Matcher) MatchBatch(batch model.OrderBatch) {
	for i := range batch.Orders {
		m.MatchOne(&batch.Orders[i])
	}
}

func (m *Matcher) MatchShardBatch(batch model.ShardOrderBatch) {
	for _, orderIndex := range batch.Indexes {
		m.MatchOne(&batch.Orders[orderIndex])
	}
}

func (m *Matcher) MatchOne(order *model.Order) *model.Rider {
	innerRadius := -1

	for _, outerRadius := range searchRadii {
		best := m.BestNearbyRiderInRange(order, innerRadius, outerRadius)
		if best != nil {
			atomic.AddInt64(&best.Count, 1)
			m.matched.Add(1)
			return best
		}
		innerRadius = outerRadius
	}

	m.missed.Add(1)
	return nil
}

func (m *Matcher) FindNearbyCandidates(x int, y int, radius int) []RiderCandidate {
	return m.grid.FindNearbyCandidates(x, y, radius)
}

func (m *Matcher) FindNearbyCandidatesInRange(x int, y int, innerRadius int, outerRadius int) []RiderCandidate {
	return m.grid.FindNearbyCandidatesInRange(x, y, innerRadius, outerRadius)
}

func (m *Matcher) BestCandidate(order *model.Order, candidates []RiderCandidate) *model.Rider {
	var best *model.Rider
	bestScore := int64(math.MaxInt64)

	for _, candidate := range candidates {
		score := m.score(order, candidate)
		if score < bestScore {
			best = candidate.Rider
			bestScore = score
		}
	}

	return best
}

func (m *Matcher) BestNearbyRiderInRange(order *model.Order, innerRadius int, outerRadius int) *model.Rider {
	if outerRadius < 0 || outerRadius <= innerRadius {
		return nil
	}

	grid := m.grid
	grid.mu.RLock()
	defer grid.mu.RUnlock()

	cellX := order.X / grid.cellSize
	cellY := order.Y / grid.cellSize
	var best *model.Rider
	bestScore := int64(math.MaxInt64)

	for dx := -outerRadius; dx <= outerRadius; dx++ {
		for dy := -outerRadius; dy <= outerRadius; dy++ {
			if maxInt(absInt(dx), absInt(dy)) <= innerRadius {
				continue
			}

			cellID := grid.cellIDByCell(cellX+dx, cellY+dy)
			for _, rider := range grid.cells[cellID] {
				score := m.scoreRider(order, rider)
				if score < bestScore {
					best = rider
					bestScore = score
				}
			}
		}
	}

	return best
}

func (m *Matcher) BetterRider(order *model.Order, best *model.Rider, candidate *model.Rider) *model.Rider {
	if candidate == nil {
		return best
	}
	if best == nil {
		return candidate
	}

	if m.scoreRider(order, candidate) < m.scoreRider(order, best) {
		return candidate
	}

	return best
}

func (m *Matcher) AddRider(rider *model.Rider) {
	m.grid.AddRider(rider)
}

func (m *Matcher) MoveRider(rider *model.Rider) {
	m.grid.MoveRider(rider)
}

func (m *Matcher) RemoveRider(rider *model.Rider) {
	m.grid.RemoveRider(rider)
}

func (m *Matcher) DeleteRider(rider *model.Rider) {
	m.grid.DeleteRider(rider)
}

func (m *Matcher) ApplyRiderEvent(event model.RiderEvent) {
	rider := &model.Rider{
		UID: event.UID,
		X:   event.X,
		Y:   event.Y,
	}

	switch event.Type {
	case model.RiderOnline:
		m.AddRider(rider)
	case model.RiderMove:
		m.MoveRider(rider)
	case model.RiderOffline:
		m.RemoveRider(rider)
	}
}

func (m *Matcher) OnlineRiders() int {
	return m.grid.OnlineCount()
}

func (m *Matcher) score(order *model.Order, candidate RiderCandidate) int64 {
	dx := int64(order.X - candidate.X)
	dy := int64(order.Y - candidate.Y)
	distance := int64(math.Abs(float64(dx)) + math.Abs(float64(dy)))
	distanceScore := distance / int64(m.grid.cellSize)
	loadScore := candidate.Count * m.loadWeight

	return distanceScore + loadScore
}

func (m *Matcher) scoreRider(order *model.Order, rider *model.Rider) int64 {
	dx := int64(order.X - rider.X)
	dy := int64(order.Y - rider.Y)
	distance := int64(math.Abs(float64(dx)) + math.Abs(float64(dy)))
	distanceScore := distance / int64(m.grid.cellSize)
	loadScore := atomic.LoadInt64(&rider.Count) * m.loadWeight

	return distanceScore + loadScore
}

func (m *Matcher) Matched() int64 {
	return m.matched.Load()
}

func (m *Matcher) Missed() int64 {
	return m.missed.Load()
}
