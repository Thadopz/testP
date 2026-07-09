package orderstate

import "context"

type Status string

const (
	StatusCreated        Status = "created"
	StatusSubmitted      Status = "submitted"
	StatusCancelled      Status = "cancelled"
	StatusRetryRequested Status = "retry_requested"
	StatusMatched        Status = "matched"
	StatusMissed         Status = "missed"
)

type State struct {
	OrderID      int64
	ShardID      int
	Status       Status
	X            int
	Y            int
	Attempt      int
	CancelReason string
	RetryReason  string
	MissReason   string
	RiderID      int64
	Score        int
	LastEventID  string
	UpdatedAt    int64
}

type Store interface {
	Load(ctx context.Context, orderID int64) (State, bool, error)
	Save(ctx context.Context, state State) error
}
