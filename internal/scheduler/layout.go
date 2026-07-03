package scheduler

import "math"

type ShardLayout struct {
	//整个区域的边长，坐标会按这个范围进行划分
	areaSize int
	//单个网格单元的边长，用于把区域离散化
	cellSize int
	//分片总数
	shardCount int
	//分片在水平方向上的列数
	shardCols int
	//分片在垂直方向上的行数
	shardRows int
	//每个轴上所有的网格单元
	cellsPerAxis int
	//每个分片在 X 方向覆盖的网格数量
	cellsPerShardX int
	//每个分片在 Y 方向覆盖的网格数量
	cellsPerShardY int
}

func NewShardLayout(areaSize int, cellSize int, shardCount int) ShardLayout {
	if areaSize <= 0 {
		areaSize = 1
	}
	if cellSize <= 0 {
		cellSize = 1
	}
	if shardCount <= 0 {
		shardCount = 1
	}

	shardCols := int(math.Ceil(math.Sqrt(float64(shardCount))))
	if shardCols < 1 {
		shardCols = 1
	}

	shardRows := (shardCount + shardCols - 1) / shardCols
	cellsPerAxis := (areaSize + cellSize - 1) / cellSize
	cellsPerShardX := (cellsPerAxis + shardCols - 1) / shardCols
	cellsPerShardY := (cellsPerAxis + shardRows - 1) / shardRows

	if cellsPerShardX < 1 {
		cellsPerShardX = 1
	}
	if cellsPerShardY < 1 {
		cellsPerShardY = 1
	}

	return ShardLayout{
		areaSize:       areaSize,
		cellSize:       cellSize,
		shardCount:     shardCount,
		shardCols:      shardCols,
		shardRows:      shardRows,
		cellsPerAxis:   cellsPerAxis,
		cellsPerShardX: cellsPerShardX,
		cellsPerShardY: cellsPerShardY,
	}
}

func (l ShardLayout) ShardID(x int, y int) int {
	cellX := clampInt(x/l.cellSize, 0, l.cellsPerAxis-1)
	cellY := clampInt(y/l.cellSize, 0, l.cellsPerAxis-1)

	shardX := clampInt(cellX/l.cellsPerShardX, 0, l.shardCols-1)
	shardY := clampInt(cellY/l.cellsPerShardY, 0, l.shardRows-1)
	shardID := shardY*l.shardCols + shardX
	if shardID >= l.shardCount {
		return l.shardCount - 1
	}

	return shardID
}

// 实际上这个函数会返回自己，因为dx dy可以都取0
func (l ShardLayout) NeighborShardIDs(shardID int) []int {
	if shardID < 0 || shardID >= l.shardCount {
		return nil
	}

	shardX := shardID % l.shardCols
	shardY := shardID / l.shardCols
	ids := make([]int, 0, 9)

	for dy := -1; dy <= 1; dy++ {
		for dx := -1; dx <= 1; dx++ {
			neighborX := shardX + dx
			neighborY := shardY + dy
			if neighborX < 0 || neighborX >= l.shardCols || neighborY < 0 || neighborY >= l.shardRows {
				continue
			}

			id := neighborY*l.shardCols + neighborX
			if id < l.shardCount {
				ids = append(ids, id)
			}
		}
	}

	return ids
}

func (l ShardLayout) ShardCols() int {
	return l.shardCols
}

func (l ShardLayout) ShardRows() int {
	return l.shardRows
}

func (l ShardLayout) CellsPerShardX() int {
	return l.cellsPerShardX
}

func (l ShardLayout) CellsPerShardY() int {
	return l.cellsPerShardY
}

// 把value限制在范围内
func clampInt(value int, minValue int, maxValue int) int {
	if maxValue < minValue {
		return minValue
	}
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}

	return value
}
