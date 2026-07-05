package matcher

import (
	"sync"
	"sync/atomic"
	"testP/internal/model"
)

type RiderCandidate struct {
	Rider *model.Rider
	UID   int64
	X     int
	Y     int
	Count int64
}

// GridIndex 通过cell收集在线的rider
type GridIndex struct {
	cellSize int
	mu       sync.RWMutex
	riders   map[int64]*model.Rider
	cells    map[int64]map[int64]*model.Rider
}

func NewGridIndex(riders []*model.Rider, cellSize int) *GridIndex {
	if cellSize <= 0 {
		cellSize = 1
	}

	g := &GridIndex{
		cellSize: cellSize,
		riders:   make(map[int64]*model.Rider),
		cells:    make(map[int64]map[int64]*model.Rider),
	}

	for _, rider := range riders {
		g.AddRider(rider)
	}

	return g
}

// 已弃用
func (g *GridIndex) FindNearbyCandidates(x, y int, radius int) []RiderCandidate {
	return g.FindNearbyCandidatesInRange(x, y, -1, radius)
}

// 已弃用
func (g *GridIndex) FindNearbyCandidatesInRange(x, y int, innerRadius int, outerRadius int) []RiderCandidate {
	if outerRadius < 0 || outerRadius <= innerRadius {
		return nil
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	cellX := x / g.cellSize
	cellY := y / g.cellSize

	candidates := make([]RiderCandidate, 0)

	for dx := -outerRadius; dx <= outerRadius; dx++ {
		for dy := -outerRadius; dy <= outerRadius; dy++ {
			if maxInt(absInt(dx), absInt(dy)) <= innerRadius {
				continue
			}

			cellID := g.cellIDByCell(cellX+dx, cellY+dy)
			candidates = g.appendCellCandidates(candidates, cellID)
		}
	}

	return candidates
}

func (g *GridIndex) AddRider(rider *model.Rider) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if current, ok := g.riders[rider.UID]; ok {
		delete(g.cells[current.CellID], current.UID)
		current.X = rider.X
		current.Y = rider.Y
		current.OnLine = true
		current.CellID = g.cellID(current.X, current.Y)
		g.addRiderToCell(current, current.CellID)
		return
	}

	rider.CellID = g.cellID(rider.X, rider.Y)
	rider.OnLine = true
	g.riders[rider.UID] = rider
	g.addRiderToCell(rider, rider.CellID)
}

func (g *GridIndex) MoveRider(rider *model.Rider) {
	g.mu.Lock()
	defer g.mu.Unlock()

	current, ok := g.riders[rider.UID]
	if !ok || !current.OnLine {
		return
	}

	oldCellID := current.CellID
	newCellID := g.cellID(rider.X, rider.Y)

	current.X = rider.X
	current.Y = rider.Y
	current.CellID = newCellID

	if oldCellID == newCellID {
		return
	}

	delete(g.cells[oldCellID], current.UID)
	g.addRiderToCell(current, newCellID)
}

func (g *GridIndex) RemoveRider(rider *model.Rider) {
	g.mu.Lock()
	defer g.mu.Unlock()

	current, ok := g.riders[rider.UID]
	if !ok || !current.OnLine {
		return
	}

	current.OnLine = false
	delete(g.cells[current.CellID], current.UID)
}

func (g *GridIndex) DeleteRider(rider *model.Rider) {
	g.mu.Lock()
	defer g.mu.Unlock()

	current, ok := g.riders[rider.UID]
	if !ok {
		return
	}

	current.OnLine = false
	delete(g.cells[current.CellID], current.UID)
	delete(g.riders, current.UID)
}

func (g *GridIndex) OnlineCount() int {
	g.mu.RLock()
	defer g.mu.RUnlock()

	count := 0
	for _, rider := range g.riders {
		if rider.OnLine {
			count++
		}
	}

	return count
}

func (g *GridIndex) cellID(x, y int) int64 {
	return g.cellIDByCell(x/g.cellSize, y/g.cellSize)
}

func (g *GridIndex) cellIDByCell(cellX, cellY int) int64 {
	return int64(cellX)<<32 | int64(uint32(cellY))
}

func (g *GridIndex) addRiderToCell(rider *model.Rider, cellID int64) {
	if g.cells[cellID] == nil {
		g.cells[cellID] = make(map[int64]*model.Rider)
	}
	g.cells[cellID][rider.UID] = rider
}

// 已弃用
func (g *GridIndex) appendCellCandidates(candidates []RiderCandidate, cellID int64) []RiderCandidate {
	for _, rider := range g.cells[cellID] {
		candidates = append(candidates, RiderCandidate{
			Rider: rider,
			UID:   rider.UID,
			X:     rider.X,
			Y:     rider.Y,
			Count: atomic.LoadInt64(&rider.Count),
		})
	}

	return candidates
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func maxInt(left int, right int) int {
	if left > right {
		return left
	}
	return right
}
