package node

import (
	"context"
	"fmt"
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
	nextStep map[int]int64
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
		nextStep: make(map[int]int64),
	}
}

func (n *Node) Run(ctx context.Context) error {
	errCh := make(chan error, len(n.shardIDs))
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := n.loadCheckpoint(runCtx); err != nil {
		return err
	}

	for _, id := range n.shardIDs {
		n.mu.Lock()
		nextOffset := n.nextStep[id]
		n.mu.Unlock()

		eventCh, err := n.eventlog.ReadFrom(runCtx, eventlog.Position{
			ShardID: id,
			Offset:  nextOffset,
		})
		if err != nil {
			return err
		}

		go func() {
			errCh <- n.runShard(runCtx, eventCh)
		}()
	}

	for range n.shardIDs {
		err := <-errCh
		if err != nil {
			cancel()
			return err
		}
	}
	return nil
}

func (n *Node) runShard(ctx context.Context, eventCh <-chan eventlog.Record) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case record, ok := <-eventCh:
			if !ok {
				return nil
			}

			if n.applier == nil {
				return fmt.Errorf("applier not found")
			}

			if err := n.applier.Apply(ctx, record.Event); err != nil {
				return err
			}

			if err := n.advanceCheckpoint(ctx, record.Position); err != nil {
				return err
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
		n.nextStep[shardID] = offset
	}

	return nil
}

func (n *Node) advanceCheckpoint(ctx context.Context, position eventlog.Position) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.nextStep[position.ShardID] = position.Offset + 1
	if n.store == nil {
		return nil
	}

	return n.store.SaveCheckpoint(ctx, checkpoint.Checkpoint{
		NodeID: n.nodeID,
		Offset: copyOffsets(n.nextStep),
	})
}

func copyOffsets(offsets map[int]int64) map[int]int64 {
	copied := make(map[int]int64, len(offsets))
	for shardID, offset := range offsets {
		copied[shardID] = offset
	}
	return copied
}
