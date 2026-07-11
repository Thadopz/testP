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

const defaultHashReplicas = 32

// 进行移动的必要条件
type Move struct {
	ShardID    int
	FromNodeID int
	ToNodeID   int
}

// 进行再分配的必要条件
type Snapshot struct {
	AliveNodeIDs []int
	Ownerships   []ownership.Ownership
}

// 抽象出接口以及 PlannerFunc，之前没试过这种写法这次尝试一下
type Planner interface {
	Plan(snapshot Snapshot) (Move, bool, error)
}

type PlannerFunc func(snapshot Snapshot) (Move, bool, error)

func (f PlannerFunc) Plan(snapshot Snapshot) (Move, bool, error) {
	return f(snapshot)
}

// 负责协调集群的再分配操作
type Controller struct {
	//主要是为了调用ownershipLister的方法，但是ownership本身也是必须的
	ownership ownership.OwnershipStore
	//负责管理存活状态
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
	//必须排序，不然一致性哈希会乱完的
	sort.Ints(aliveNodeIDs)

	ownerships, err := lister.AllOwnerships()
	if err != nil {
		return Move{}, false, err
	}
	if len(ownerships) == 0 {
		return Move{}, false, nil
	}
	//大炮，已组装！
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
	for _, ownership := range snapshot.Ownerships {
		targetNodeID, err := TargetNodeID(snapshot.AliveNodeIDs, ownership.ShardID)
		if err != nil {
			return Move{}, false, err
		}
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

// 关于边界条件的补充
func TargetNodeID(nodeIDs []int, shardID int) (int, error) {
	if len(nodeIDs) == 0 {
		return 0, fmt.Errorf("node ids must not be empty")
	}
	if shardID < 0 {
		return 0, fmt.Errorf("shard id must be >= 0: %d", shardID)
	}

	hashRing := tools.NewConsistentHash(defaultHashReplicas, nil)
	seenNodeIDs := make(map[int]bool, len(nodeIDs))
	//初始化哈希环
	for _, nodeID := range nodeIDs {
		if nodeID <= 0 {
			return 0, fmt.Errorf("node id must be > 0: %d", nodeID)
		}
		//去重
		if seenNodeIDs[nodeID] {
			return 0, fmt.Errorf("duplicate node id: %d", nodeID)
		}
		seenNodeIDs[nodeID] = true
		hashRing.Add(strconv.Itoa(nodeID))
	}
	//这么写真是蠢到没边了，但是不想改成泛型了将就下吧
	targetNodeID, err := strconv.Atoi(hashRing.Get(strconv.Itoa(shardID)))
	if err != nil {
		return 0, fmt.Errorf("parse target node : %w", err)
	}
	return targetNodeID, nil
}

// 检验移动是否有效
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
