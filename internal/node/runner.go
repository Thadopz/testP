package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testP/internal/checkpoint"
	clusterownership "testP/internal/cluster/ownership"
	"testP/internal/eventlog"
	"testP/internal/model"
	"time"
)

type eventApplier interface {
	Apply(ctx context.Context, event model.Event) error
}

type fencedEventApplier interface {
	ApplyWithFence(ctx context.Context, event model.Event, ownership clusterownership.Ownership) error
}

type ownershipReader interface {
	OwnerOf(shardID int) (clusterownership.Ownership, bool, error)
}

type Node struct {
	mu              sync.Mutex
	nodeID          int
	shardIDs        []int
	eventlog        eventlog.EventLog
	applier         eventApplier
	store           checkpoint.Store
	nextStep        map[int]int64
	tail            bool
	active          map[int]*shardWorker
	provider        clusterownership.ShardProvider
	refreshInterval time.Duration
}

type shardWorker struct {
	shardID int
	epoch   int64
	cancel  context.CancelFunc
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

func NewDynamicRunner(ID int,
	shardprovider clusterownership.ShardProvider,
	el eventlog.EventLog,
	ea eventApplier,
	store checkpoint.Store) *Node {

	return &Node{
		nodeID:          ID,
		provider:        shardprovider,
		eventlog:        el,
		applier:         ea,
		store:           store,
		nextStep:        make(map[int]int64),
		active:          make(map[int]*shardWorker),
		tail:            true,
		refreshInterval: time.Second,
	}
}

func (n *Node) SetTail(tail bool) {
	n.tail = tail
}

func (n *Node) SetRefreshInterval(interval time.Duration) {
	if interval > 0 {
		n.refreshInterval = interval
	}
}

func (n *Node) Run(ctx context.Context) error {
	if n.provider != nil {
		return n.runDynamic(ctx)
	}
	return n.runStatic(ctx)
}

func (n *Node) runStatic(ctx context.Context) error {
	errCh := make(chan error, len(n.shardIDs))
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := n.loadCheckpoint(runCtx); err != nil {
		return err
	}

	for _, shardID := range n.shardIDs {
		n.mu.Lock()
		nextOffset := n.nextStep[shardID]
		n.mu.Unlock()

		eventCh, err := n.openRecordStream(runCtx, eventlog.Position{
			ShardID: shardID,
			Offset:  nextOffset,
		})
		if err != nil {
			return err
		}

		go func(eventCh <-chan eventlog.Record) {
			errCh <- n.runShard(runCtx, eventCh)
		}(eventCh)
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

func (n *Node) runDynamic(ctx context.Context) error {
	if err := n.loadCheckpoint(ctx); err != nil {
		return err
	}
	if !n.tail {
		return fmt.Errorf("runDynamic requires tail mode")
	}
	errCh := make(chan error, 16)

	if err := n.refreshOnce(ctx, errCh); err != nil {
		return err
	}

	ticker := time.NewTicker(n.refreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			n.stopAllShards()
			return ctx.Err()

		case err := <-errCh:
			if err != nil {
				n.stopAllShards()
				return err
			}

		case <-ticker.C:
			if err := n.refreshOnce(ctx, errCh); err != nil {
				n.stopAllShards()
				return err
			}
		}
	}
}

func (n *Node) stopAllShards() {
	for k := range n.active {
		n.stopShard(k)
	}
}

func (n *Node) openRecordStream(ctx context.Context, position eventlog.Position) (<-chan eventlog.Record, error) {
	if !n.tail {
		return n.eventlog.ReadFrom(ctx, position)
	}

	tailLog, ok := n.eventlog.(eventlog.TailEventLog)
	if !ok {
		return nil, fmt.Errorf("event log does not support tail")
	}

	return tailLog.TailFrom(ctx, position)
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

func (n *Node) startShard(
	ctx context.Context,
	ownership clusterownership.Ownership,
	errCh chan<- error,
) error {
	shardCtx, cancel := context.WithCancel(ctx)

	n.mu.Lock()
	nextOffset := n.nextStep[ownership.ShardID]
	n.mu.Unlock()

	eventCh, err := n.openRecordStream(shardCtx, eventlog.Position{
		ShardID: ownership.ShardID,
		Offset:  nextOffset,
	})
	if err != nil {
		cancel()
		return err
	}

	n.mu.Lock()
	n.active[ownership.ShardID] = &shardWorker{
		shardID: ownership.ShardID,
		epoch:   ownership.Epoch,
		cancel:  cancel,
	}
	n.mu.Unlock()

	go func() {
		err := n.runDynamicShard(shardCtx, eventCh, ownership)
		if errors.Is(err, clusterownership.ErrOwnershipFenceLost) {
			n.removeActiveShardIfEpochMatches(ownership.ShardID, ownership.Epoch)
			return
		}
		if errors.Is(err, context.Canceled) {
			return
		}
		errCh <- err
	}()

	return nil
}

func (n *Node) runDynamicShard(ctx context.Context, eventCh <-chan eventlog.Record, ownership clusterownership.Ownership) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case record, ok := <-eventCh:
			if !ok {
				return nil
			}

			if err := n.checkShardFence(ownership.ShardID, ownership.Epoch); err != nil {
				return err
			}

			if n.applier == nil {
				return fmt.Errorf("applier not found")
			}

			if err := n.applyDynamicEvent(ctx, record.Event, ownership); err != nil {
				return err
			}

			if err := n.advanceCheckpoint(ctx, record.Position); err != nil {
				return err
			}
		}
	}
}

func (n *Node) applyDynamicEvent(ctx context.Context, event model.Event, ownership clusterownership.Ownership) error {
	if applier, ok := n.applier.(fencedEventApplier); ok {
		return applier.ApplyWithFence(ctx, event, ownership)
	}

	if err := n.checkShardFence(ownership.ShardID, ownership.Epoch); err != nil {
		return err
	}
	if err := n.applier.Apply(ctx, event); err != nil {
		return err
	}
	return n.checkShardFence(ownership.ShardID, ownership.Epoch)
}

func (n *Node) checkShardFence(shardID int, epoch int64) error {
	reader, ok := n.provider.(ownershipReader)
	if !ok {
		return nil
	}

	ownership, found, err := reader.OwnerOf(shardID)
	if err != nil {
		return err
	}
	if !found {
		return clusterownership.ErrOwnershipFenceLost
	}
	if ownership.NodeID != n.nodeID || ownership.Epoch != epoch {
		return clusterownership.ErrOwnershipFenceLost
	}

	return nil
}

func (n *Node) removeActiveShardIfEpochMatches(shardID int, epoch int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	worker, ok := n.active[shardID]
	if !ok {
		return
	}
	if worker.epoch != epoch {
		return
	}

	delete(n.active, shardID)
}

func (n *Node) stopShard(shardID int) {
	n.mu.Lock()
	worker, ok := n.active[shardID]
	if ok {
		delete(n.active, shardID)
	}
	n.mu.Unlock()

	if ok && worker.cancel != nil {
		worker.cancel()
	}
}

func (n *Node) refreshOnce(ctx context.Context, errCh chan<- error) error {
	ownerships, err := n.provider.ShardsForNode(n.nodeID)
	if err != nil {
		return err
	}
	desired := make(map[int]clusterownership.Ownership)
	for _, ownership := range ownerships {
		desired[ownership.ShardID] = ownership
	}
	for shardID, ownership := range desired {
		n.mu.Lock()
		worker, running := n.active[shardID]
		n.mu.Unlock()

		if !running {
			if err := n.startShard(ctx, ownership, errCh); err != nil {
				return err
			}
			continue
		}

		if worker.epoch != ownership.Epoch {
			n.stopShard(shardID)
			if err := n.startShard(ctx, ownership, errCh); err != nil {
				return err
			}
		}
	}

	n.mu.Lock()
	activeShardIDs := make([]int, 0, len(n.active))
	for shardID := range n.active {
		activeShardIDs = append(activeShardIDs, shardID)
	}
	n.mu.Unlock()

	for _, shardID := range activeShardIDs {
		if _, ok := desired[shardID]; !ok {
			n.stopShard(shardID)
		}
	}
	return nil
}
