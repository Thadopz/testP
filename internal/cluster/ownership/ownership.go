package ownership

import "errors"

var ErrOwnershipFenceLost = errors.New("ownership fence lost")

type Ownership struct {
	ShardID int
	NodeID  int
	Epoch   int64
}

type OwnershipStore interface {
	OwnerOf(shardID int) (Ownership, bool, error)
	ShardsForNode(nodeID int) ([]Ownership, error)
	Assign(shardID int, nodeID int) error
}

type ShardProvider interface {
	ShardsForNode(nodeID int) ([]Ownership, error)
}
