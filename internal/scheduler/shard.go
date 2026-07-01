package scheduler

import (
	"testP/internal/model"
)

type Shard struct {
	ID      int
	orderCh chan model.OrderBatch
}

func NewShard(id, bufferSize int) *Shard {
	return &Shard{
		ID:      id,
		orderCh: make(chan model.OrderBatch, bufferSize),
	}
}
