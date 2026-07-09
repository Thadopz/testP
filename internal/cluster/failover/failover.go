package failover

import (
	"fmt"
	"sort"
	"strconv"
	"testP/internal/cluster/membership"
	"testP/internal/cluster/ownership"
	"testP/internal/tools"
)

const failoverHashReplicas = 32

type FailoverController struct {
	ownership  ownership.OwnershipStore
	membership membership.MembershipStore
}

func NewFailoverController(ownershipStore ownership.OwnershipStore, membershipStore membership.MembershipStore) *FailoverController {
	return &FailoverController{
		ownership:  ownershipStore,
		membership: membershipStore,
	}
}

func (f *FailoverController) FailoverDeadNode(deadNodeID int) error {
	if deadNodeID <= 0 {
		return fmt.Errorf("dead node id must be > 0: %d", deadNodeID)
	}
	if f.ownership == nil {
		return fmt.Errorf("ownership store is required")
	}
	if f.membership == nil {
		return fmt.Errorf("membership store is required")
	}

	deadOwnerships, err := f.ownership.ShardsForNode(deadNodeID)
	if err != nil {
		return err
	}
	if len(deadOwnerships) == 0 {
		return nil
	}

	aliveNodes, err := f.membership.AliveNodes()
	if err != nil {
		return err
	}
	aliveNodes = removeNodeID(aliveNodes, deadNodeID)
	if len(aliveNodes) == 0 {
		return fmt.Errorf("no alive node can take over shards from node %d", deadNodeID)
	}

	for _, ownership := range deadOwnerships {
		targetNodeID, err := chooseFailoverTarget(aliveNodes, ownership.ShardID)
		if err != nil {
			return err
		}
		if err := f.ownership.Assign(ownership.ShardID, targetNodeID); err != nil {
			return err
		}
	}

	return nil
}

func (f *FailoverController) FailoverMissingOwners() ([]int, error) {
	if f.ownership == nil {
		return nil, fmt.Errorf("ownership store is required")
	}
	if f.membership == nil {
		return nil, fmt.Errorf("membership store is required")
	}

	lister, ok := f.ownership.(ownership.OwnershipLister)
	if !ok {
		return nil, fmt.Errorf("ownership store cannot list all ownerships")
	}

	allOwnerships, err := lister.AllOwnerships()
	if err != nil {
		return nil, err
	}
	if len(allOwnerships) == 0 {
		return nil, nil
	}

	aliveNodes, err := f.membership.AliveNodes()
	if err != nil {
		return nil, err
	}
	aliveSet := make(map[int]bool, len(aliveNodes))
	for _, nodeID := range aliveNodes {
		aliveSet[nodeID] = true
	}

	deadSet := make(map[int]bool)
	for _, ownership := range allOwnerships {
		if !aliveSet[ownership.NodeID] {
			deadSet[ownership.NodeID] = true
		}
	}

	deadNodeIDs := make([]int, 0, len(deadSet))
	for nodeID := range deadSet {
		deadNodeIDs = append(deadNodeIDs, nodeID)
	}
	sort.Ints(deadNodeIDs)

	for _, nodeID := range deadNodeIDs {
		if err := f.FailoverDeadNode(nodeID); err != nil {
			return deadNodeIDs, err
		}
	}

	return deadNodeIDs, nil
}

func removeNodeID(nodeIDs []int, removedNodeID int) []int {
	kept := make([]int, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if nodeID != removedNodeID {
			kept = append(kept, nodeID)
		}
	}
	return kept
}

func chooseFailoverTarget(aliveNodes []int, shardID int) (int, error) {
	if len(aliveNodes) == 0 {
		return 0, fmt.Errorf("alive nodes must not be empty")
	}

	hashRing := tools.NewConsistentHash(failoverHashReplicas, nil)
	for _, nodeID := range aliveNodes {
		hashRing.Add(strconv.Itoa(nodeID))
	}
	targetText := hashRing.Get(strconv.Itoa(shardID))
	targetNodeID, err := strconv.Atoi(targetText)
	if err != nil {
		return 0, fmt.Errorf("parse target node id %q: %w", targetText, err)
	}

	return targetNodeID, nil
}
