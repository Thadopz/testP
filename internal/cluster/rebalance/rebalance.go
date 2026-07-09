package rebalance

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"testP/internal/cluster/membership"
	"testP/internal/cluster/ownership"
	"testP/internal/tools"
)

type Move struct {
	ShardID    int
	FromNodeID int
	ToNodeID   int
}

type Snapshot struct {
	AliveNodeIDs []int
	Ownerships   []ownership.Ownership
}

type Planner interface {
	Plan(snapshot Snapshot) (Move, bool, error)
}

type PlannerFunc func(snapshot Snapshot) (Move, bool, error)

func (f PlannerFunc) Plan(snapshot Snapshot) (Move, bool, error) {
	return f(snapshot)
}

type Controller struct {
	ownership  ownership.OwnershipStore
	membership membership.MembershipStore
	planner    Planner
}

func NewController(ownershipStore ownership.OwnershipStore, membershipStore membership.MembershipStore) *Controller {
	return NewControllerWithPlanner(ownershipStore, membershipStore, DefaultPlanner{})
}

func NewControllerWithPlanner(
	ownershipStore ownership.OwnershipStore,
	membershipStore membership.MembershipStore,
	planner Planner,
) *Controller {
	return &Controller{
		ownership:  ownershipStore,
		membership: membershipStore,
		planner:    planner,
	}
}

func (c *Controller) RebalanceOnce() (Move, bool, error) {
	if c.ownership == nil {
		return Move{}, false, fmt.Errorf("ownership store is required")
	}
	if c.membership == nil {
		return Move{}, false, fmt.Errorf("membership store is required")
	}
	if c.planner == nil {
		return Move{}, false, nil
	}

	lister, ok := c.ownership.(ownership.OwnershipLister)
	if !ok {
		return Move{}, false, fmt.Errorf("ownership store cannot list all ownerships")
	}

	aliveNodeIDs, err := c.membership.AliveNodes()
	if err != nil {
		return Move{}, false, err
	}
	if len(aliveNodeIDs) == 0 {
		return Move{}, false, nil
	}
	sort.Ints(aliveNodeIDs)

	ownerships, err := lister.AllOwnerships()
	if err != nil {
		return Move{}, false, err
	}
	if len(ownerships) == 0 {
		return Move{}, false, nil
	}

	snapshot := Snapshot{
		AliveNodeIDs: slices.Clone(aliveNodeIDs),
		Ownerships:   slices.Clone(ownerships),
	}
	move, ok, err := c.planner.Plan(snapshot)
	if err != nil {
		return Move{}, false, err
	}
	if !ok {
		return Move{}, false, nil
	}
	if err := validateMove(move, snapshot); err != nil {
		return Move{}, false, err
	}
	if err := c.ownership.Assign(move.ShardID, move.ToNodeID); err != nil {
		return Move{}, false, err
	}

	return move, true, nil
}

type DefaultPlanner struct{}

func (DefaultPlanner) Plan(snapshot Snapshot) (Move, bool, error) {
	hashring := tools.NewConsistentHash(32, nil)
	for _, nodeID := range snapshot.AliveNodeIDs {
		hashring.Add(strconv.Itoa(nodeID))
	}
	for _, ownership := range snapshot.Ownerships {
		targetNodeID, _ := strconv.Atoi(hashring.Get(strconv.Itoa(ownership.ShardID)))
		if ownership.NodeID != targetNodeID {
			return Move{
				ShardID:    ownership.ShardID,
				FromNodeID: ownership.NodeID,
				ToNodeID:   targetNodeID,
			}, true, nil
		}
	}
	return Move{}, false, nil
}

func validateMove(move Move, snapshot Snapshot) error {
	if move.ShardID < 0 {
		return fmt.Errorf("rebalance shard id must be >= 0: %d", move.ShardID)
	}
	if move.FromNodeID <= 0 {
		return fmt.Errorf("rebalance from node id must be > 0: %d", move.FromNodeID)
	}
	if move.ToNodeID <= 0 {
		return fmt.Errorf("rebalance to node id must be > 0: %d", move.ToNodeID)
	}
	if move.FromNodeID == move.ToNodeID {
		return fmt.Errorf("rebalance target must differ from source node: %d", move.FromNodeID)
	}

	aliveSet := make(map[int]bool, len(snapshot.AliveNodeIDs))
	for _, nodeID := range snapshot.AliveNodeIDs {
		aliveSet[nodeID] = true
	}
	if !aliveSet[move.FromNodeID] {
		return fmt.Errorf("rebalance source node is not alive: %d", move.FromNodeID)
	}
	if !aliveSet[move.ToNodeID] {
		return fmt.Errorf("rebalance target node is not alive: %d", move.ToNodeID)
	}

	for _, currentOwnership := range snapshot.Ownerships {
		if currentOwnership.ShardID == move.ShardID {
			if currentOwnership.NodeID != move.FromNodeID {
				return fmt.Errorf(
					"rebalance source mismatch for shard %d: got owner %d, want %d",
					move.ShardID,
					currentOwnership.NodeID,
					move.FromNodeID,
				)
			}
			return nil
		}
	}

	return fmt.Errorf("rebalance shard %d has no current owner", move.ShardID)
}
