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

// 批处理匹配
func (m *Matcher) MatchBatch(batch model.OrderBatch) {
	for i := range batch.Orders {
		m.MatchOne(&batch.Orders[i])
	}
}

func (m *Matcher) MatchOne(order *model.Order) *model.Rider {
	candidates := m.grid.FindNearbyRiders(order.X, order.Y, 1)
	if len(candidates) == 0 {
		candidates = m.grid.FindNearbyRiders(order.X, order.Y, 3)
	}
	if len(candidates) == 0 {
		candidates = m.grid.FindNearbyRiders(order.X, order.Y, 8)
	}
	if len(candidates) == 0 {
		m.missed.Add(1)
		return nil
	}

	var best *model.Rider
	bestScore := int64(math.MaxInt64)

	for _, rider := range candidates {
		score := m.score(order, rider)
		if score < bestScore {
			best = rider
			bestScore = score
		}
	}

	if best != nil {
		atomic.AddInt64(&best.Count, 1)
		m.matched.Add(1)
	}

	return best
}

// 分数更低者优先，避免在大量订单同时涌入时把订单全部分配到最近的骑手上
// 可能要考虑后续加权调参，可以考虑订单权重按指数增长，具体需要测试结果印证了
// diff: 将距离平方和改为曼哈顿距离，避免dx dy较大时已持有订单数对权重影响不大
func (m *Matcher) score(order *model.Order, rider *model.Rider) int64 {
	dx := int64(order.X - rider.X)
	dy := int64(order.Y - rider.Y)
	distance := int64(math.Abs(float64(dx)) + math.Abs(float64(dy)))
	distanceScore := int64(distance / int64(m.grid.cellSize))

	load := atomic.LoadInt64(&rider.Count)
	loadScore := load * m.loadWeight

	return distanceScore + loadScore
}

func (m *Matcher) Matched() int64 {
	return m.matched.Load()
}

func (m *Matcher) Missed() int64 {
	return m.missed.Load()
}
