package ownership

import (
	"fmt"
	clusterlayout "testP/internal/cluster/layout"
)

func AssignLayout(store OwnershipStore, layout clusterlayout.Layout) error {
	for _, shardID := range layout.ShardIDs() {
		nodeID, ok := layout.OwnerOf(shardID)
		if !ok {
			return fmt.Errorf("owner for shard %d not found", shardID)
		}

		if err := store.Assign(shardID, nodeID); err != nil {
			return err
		}
	}

	return nil
}
