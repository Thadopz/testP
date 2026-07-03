package engine

import (
	"context"
	"sync/atomic"
	"testP/internal/model"
)

type Engine interface {
	Start(workerCount int)
	SubmitBatch(ctx context.Context, batch model.OrderBatch) error
	ApplyRiderEvent(event model.RiderEvent)
	Close()
	Wait()

	Submitted() int64
	Matched() int64
	Missed() int64
	OnlineRiders() int
}

type Metrics struct {
	Submitted atomic.Int64
	Matched   atomic.Int64
	Missed    atomic.Int64
}

type ShardedOptions struct {
	TopK int
}
