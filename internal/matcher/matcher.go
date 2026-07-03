package matcher

import (
	"math"
	"sync/atomic"
	"testP/internal/model"
)

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

func (m *Matcher) MatchOne(order *model.Order) *model.Rider {
	candidates := m.FindNearbyCandidates(order.X, order.Y, 1)
	if len(candidates) == 0 {
		candidates = m.FindNearbyCandidates(order.X, order.Y, 3)
	}
	if len(candidates) == 0 {
		candidates = m.FindNearbyCandidates(order.X, order.Y, 8)
	}
	if len(candidates) == 0 {
		m.missed.Add(1)
		return nil
	}

	best := m.BestCandidate(order, candidates)
	if best == nil {
		m.missed.Add(1)
		return nil
	}

	atomic.AddInt64(&best.Count, 1)
	m.matched.Add(1)
	return best
}

func (m *Matcher) FindNearbyCandidates(x int, y int, radius int) []RiderCandidate {
	return m.grid.FindNearbyCandidates(x, y, radius)
}

func (m *Matcher) BestCandidate(order *model.Order, candidates []RiderCandidate) *model.Rider {
	var best *model.Rider
	bestScore := int64(math.MaxInt64)

	for _, candidate := range candidates {
		score := m.scoreCandidate(order, candidate)
		if score < bestScore {
			best = candidate.Rider
			bestScore = score
		}
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

func (m *Matcher) scoreCandidate(order *model.Order, candidate RiderCandidate) int64 {
	dx := int64(order.X - candidate.X)
	dy := int64(order.Y - candidate.Y)
	distance := int64(math.Abs(float64(dx)) + math.Abs(float64(dy)))
	distanceScore := distance / int64(m.grid.cellSize)
	loadScore := candidate.Count * m.loadWeight

	return distanceScore + loadScore
}

func (m *Matcher) Matched() int64 {
	return m.matched.Load()
}

func (m *Matcher) Missed() int64 {
	return m.missed.Load()
}
