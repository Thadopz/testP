package membership

type MembershipStore interface {
	MarkAlive(nodeID int) error
	MarkDead(nodeID int) error
	AliveNodes() ([]int, error)
	IsAlive(nodeID int) (bool, error)
}
