package node

import (
	"context"
	"sync"
	"testP/internal/checkpoint"
	"testP/internal/eventlog"
	"testP/internal/model"
)

type eventApplier interface {
	Apply(ctx context.Context, event model.Event) error
}

type Node struct {
	mu       sync.Mutex
	nodeID   int
	shardIDs []int
	eventlog eventlog.EventLog
	applier  eventApplier
	store    checkpoint.Store
	NextStep map[int]int64
}

func NewRunner(ID int, shards []int, el eventlog.EventLog, ea eventApplier, store checkpoint.Store) *Node {
	if el == nil {
		el = &eventlog.MemoryEventLog{}
	}
	return &Node{
		nodeID:   ID,
		shardIDs: shards,
		eventlog: el,
		applier:  ea,
		store:    store,
		NextStep: make(map[int]int64),
	}
}

func (n *Node) Run(ctx context.Context) error {
	if err := n.loadCheckpoint(ctx); err != nil {
		return err
	}

	for _, id := range n.shardIDs {
		n.mu.Lock()
		nextOffset := n.NextStep[id]
		n.mu.Unlock()

		eventCh, err := n.eventlog.ReadFrom(ctx, eventlog.Position{
			ShardID: id,
			Offset:  nextOffset,
		})
		if err != nil {
			return err
		}

		go n.runShard(ctx, eventCh)
	}

	return nil
}

func (n *Node) runShard(ctx context.Context, eventCh <-chan eventlog.Record) {
	for {
		select {
		case <-ctx.Done():
			return
		case record, ok := <-eventCh:
			if !ok {
				return
			}

			if n.applier == nil {
				return
			}

			if err := n.applier.Apply(ctx, record.Event); err != nil {
				return
			}

			n.mu.Lock()
			n.NextStep[record.Position.ShardID] = record.Position.Offset + 1
			checkpointToSave := checkpoint.Checkpoint{
				NodeID: n.nodeID,
				Offset: copyOffsets(n.NextStep),
			}
			n.mu.Unlock()

			if n.store != nil {
				if err := n.store.SaveCheckpoint(ctx, checkpointToSave); err != nil {
					return
				}
			}
		}
	}
}

func (n *Node) loadCheckpoint(ctx context.Context) error {
	if n.store == nil {
		return nil
	}

	loaded, found, err := n.store.LoadCheckpoint(ctx, n.nodeID)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	for shardID, offset := range loaded.Offset {
		n.NextStep[shardID] = offset
	}

	return nil
}

func copyOffsets(offsets map[int]int64) map[int]int64 {
	copied := make(map[int]int64, len(offsets))
	for shardID, offset := range offsets {
		copied[shardID] = offset
	}
	return copied
}
