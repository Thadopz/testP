package matcher

import (
	"math"
	"sync/atomic"
	"testP/internal/model"
)

var searchRadii = []int{1, 3, 8}

// 用于匹配订单与骑手，是整个流程中最消耗内存，用时的一部分
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

// 已弃用
func (m *Matcher) FindNearbyCandidates(x int, y int, radius int) []RiderCandidate {
	return m.grid.FindNearbyCandidates(x, y, radius)
}

// 已弃用
func (m *Matcher) FindNearbyCandidatesInRange(x int, y int, innerRadius int, outerRadius int) []RiderCandidate {
	return m.grid.FindNearbyCandidatesInRange(x, y, innerRadius, outerRadius)
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
			//在内径中就直接continue，只读外径的
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

func (m *Matcher) DeleteRider(rider *model.Rider) {
	m.grid.DeleteRider(rider)
}

func (m *Matcher) scoreRider(order *model.Order, rider *model.Rider) int64 {
	dx := int64(order.X - rider.X)
	dy := int64(order.Y - rider.Y)
	distance := int64(math.Abs(float64(dx)) + math.Abs(float64(dy)))
	distanceScore := distance / int64(m.grid.cellSize)
	//原子读Count，避免读时改
	loadScore := atomic.LoadInt64(&rider.Count) * m.loadWeight

	return distanceScore + loadScore
}

func (m *Matcher) OnlineRiders() int {
	return m.grid.OnlineCount()
}
