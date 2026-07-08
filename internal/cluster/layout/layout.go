package layout

import "fmt"

type Layout struct {
	owners map[int]int
}

func NewModuloLayout(nodeIDs []int, shardCount int) (Layout, error) {
	if len(nodeIDs) == 0 {
		return Layout{}, fmt.Errorf("node ids must not be empty")
	}
	if shardCount <= 0 {
		return Layout{}, fmt.Errorf("shard count must be > 0")
	}

	seenNodeIDs := make(map[int]bool, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if nodeID <= 0 {
			return Layout{}, fmt.Errorf("node id must be > 0: %d", nodeID)
		}
		if seenNodeIDs[nodeID] {
			return Layout{}, fmt.Errorf("duplicate node id %d", nodeID)
		}
		seenNodeIDs[nodeID] = true
	}

	owners := make(map[int]int, shardCount)
	for shardID := 0; shardID < shardCount; shardID++ {
		ownerIndex := shardID % len(nodeIDs)
		owners[shardID] = nodeIDs[ownerIndex]
	}

	return Layout{owners: owners}, nil
}

func (l Layout) OwnerOf(shardID int) (int, bool) {
	nodeID, ok := l.owners[shardID]
	return nodeID, ok
}

func (l Layout) ShardIDs() []int {
	shardIDs := make([]int, 0, len(l.owners))

	for shardID := 0; shardID < len(l.owners); shardID++ {
		if _, ok := l.owners[shardID]; ok {
			shardIDs = append(shardIDs, shardID)
		}
	}

	return shardIDs
}

func (l Layout) ShardsForNode(nodeID int) []int {
	shardIDs := make([]int, 0)

	for shardID := 0; shardID < len(l.owners); shardID++ {
		ownerID, ok := l.owners[shardID]
		if ok && ownerID == nodeID {
			shardIDs = append(shardIDs, shardID)
		}
	}

	return shardIDs
}
