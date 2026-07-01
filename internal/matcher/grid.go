package matcher

import "testP/internal/model"

const cellHashWeight = 100000

// 将骑手按网格划分，避免订单全量查找每一个骑手
type GridIndex struct {
	cellSize int
	cells    map[int][]*model.Rider
}

func NewGridIndex(riders []*model.Rider, cellSize int) *GridIndex {
	if cellSize <= 0 {
		cellSize = 1
	}

	g := &GridIndex{
		cellSize: cellSize,
		cells:    make(map[int][]*model.Rider),
	}

	for _, rider := range riders {
		cellID := g.cellID(rider.X, rider.Y)
		g.cells[cellID] = append(g.cells[cellID], rider)
	}

	return g
}

// 查找订单位置相邻的骑手，没有则返回nil
func (g *GridIndex) FindNearbyRiders(x, y int, radius int) []*model.Rider {
	cx := x / g.cellSize
	cy := y / g.cellSize

	var result []*model.Rider

	for dx := -radius; dx <= radius; dx++ {
		for dy := -radius; dy <= radius; dy++ {
			cellID := g.cellIDByCell(cx+dx, cy+dy)
			result = append(result, g.cells[cellID]...)
		}
	}

	return result
}

func (g *GridIndex) cellID(x, y int) int {
	return g.cellIDByCell(x/g.cellSize, y/g.cellSize)
}

func (g *GridIndex) cellIDByCell(cx, cy int) int {
	return cellHashWeight*cx + cy
}
