package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testP/internal/checkpoint"
	clusterownership "testP/internal/cluster/ownership"
	"testP/internal/eventlog"
	"testP/internal/metrics"
	"testP/internal/model"
	"time"
)

// 抽离子接口，方便测试
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
	mu     sync.Mutex
	nodeID int
	//读取日志
	eventlog eventlog.Tailer
	//转义并执行事件
	applier eventApplier
	//持久化接口
	store checkpoint.ShardStore
	//持久化记录offset 意思是下一步要保存什么offset
	nextStep map[int]int64
	//目前自己仍持有的shard附带了一定的取消权，注意这是会过期的，要清理
	active map[int]*shardWorker
	//用来一键导出自己所有权下的shards
	provider clusterownership.ShardProvider
	//指标
	metricsRecorder metrics.Recorder
	refreshInterval time.Duration
	//hook
	onShardStart func(shardID int) error
	onShardStop  func(shardID int)
}

type shardWorker struct {
	shardID int
	epoch   int64
	cancel  context.CancelFunc
}

func NewRunner(ID int,
	shardProvider clusterownership.ShardProvider,
	el eventlog.Tailer,
	ea eventApplier,
	store checkpoint.ShardStore) *Node {

	if el == nil {
		el = &eventlog.MemoryEventLog{}
	}
	return &Node{
		nodeID:          ID,
		provider:        shardProvider,
		eventlog:        el,
		applier:         ea,
		store:           store,
		nextStep:        make(map[int]int64),
		active:          make(map[int]*shardWorker),
		refreshInterval: time.Second,
	}
}

func (n *Node) Run(ctx context.Context) error {
	if n.provider == nil {
		return fmt.Errorf("shard provider is required")
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
	return n.eventlog.TailFrom(ctx, position)
}

func (n *Node) loadShardCheckpoint(ctx context.Context, shardID int) (int64, error) {
	n.mu.Lock()
	cachedOffset := n.nextStep[shardID]
	n.mu.Unlock()

	if n.store == nil {
		return cachedOffset, nil
	}

	loaded, found, err := n.store.LoadShardCheckpoint(ctx, shardID)
	if err != nil {
		return 0, err
	}
	if !found {
		return cachedOffset, nil
	}

	return loaded.Offset, nil
}

func (n *Node) advanceCheckpoint(ctx context.Context, position eventlog.Position, ownership clusterownership.Ownership) error {
	n.mu.Lock()
	n.nextStep[position.ShardID] = position.Offset + 1
	n.mu.Unlock()
	if n.store == nil {
		return nil
	}
	return n.store.SaveShardCheckpoint(ctx, checkpoint.ShardCheckpoint{
		ShardID:   position.ShardID,
		Offset:    position.Offset + 1,
		Epoch:     ownership.Epoch,
		NodeID:    n.nodeID,
		UpdatedAt: time.Now().Unix(),
	})
}

func (n *Node) startShard(
	ctx context.Context,
	ownership clusterownership.Ownership,
	errCh chan<- error,
) error {
	shardCtx, cancel := context.WithCancel(ctx)

	nextOffset, err := n.loadShardCheckpoint(shardCtx, ownership.ShardID)
	if err != nil {
		cancel()
		return err
	}
	if n.onShardStart != nil {
		if err := n.onShardStart(ownership.ShardID); err != nil {
			cancel()
			return err
		}
	}

	n.mu.Lock()
	n.nextStep[ownership.ShardID] = nextOffset
	n.mu.Unlock()
	//我服了怎么又把goroutine扔锁里了
	eventCh, err := n.openRecordStream(shardCtx, eventlog.Position{
		ShardID: ownership.ShardID,
		Offset:  nextOffset,
	})
	if err != nil {
		if n.onShardStop != nil {
			n.onShardStop(ownership.ShardID)
		}
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
		//如果fence失败，说明节点落后了，删除shard退出等待controller重新分配
		if errors.Is(err, clusterownership.ErrOwnershipFenceLost) {
			n.recordFencingReject(ownership.ShardID)
			if n.removeActiveShard(ownership.ShardID, ownership.Epoch) && n.onShardStop != nil {
				n.onShardStop(ownership.ShardID)
			}
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
			if n.applier == nil {
				return fmt.Errorf("applier not found")
			}

			//执行事件，fence检测现已整合进apply动作前后
			if err := n.applyEvent(ctx, record.Event, ownership); err != nil {
				n.recordEventApplyError(record.Event)
				return err
			}
			n.recordEventApply(record.Event)

			//持久化
			if err := n.advanceCheckpoint(ctx, record.Position, ownership); err != nil {
				return err
			}
		}
	}
}

func (n *Node) applyEvent(ctx context.Context, event model.Event, ownership clusterownership.Ownership) error {
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
	//正常情况下，一开始配分ownership时shard必定可以被找到
	if !found {
		return clusterownership.ErrOwnershipFenceLost
	}
	if ownership.NodeID != n.nodeID || ownership.Epoch != epoch {
		return clusterownership.ErrOwnershipFenceLost
	}

	return nil
}
func (n *Node) removeActiveShard(shardID int, epoch int64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	worker, ok := n.active[shardID]
	if !ok {
		return false
	}
	if worker.epoch != epoch {
		return false
	}

	delete(n.active, shardID)
	return true
}

func (n *Node) stopShard(shardID int) {
	n.mu.Lock()
	worker, ok := n.active[shardID]
	if ok {
		delete(n.active, shardID)
	}
	n.mu.Unlock()
	//优雅退出shardWorker
	if ok && worker.cancel != nil {
		worker.cancel()
	}
	if ok && n.onShardStop != nil {
		n.onShardStop(shardID)
	}
}

// refreshOnce会检查当前节点应该运行的shard列表和当前节点正在运行的shard列表，
// desired是当前节点应该运行的shard列表，key是shardID，value是ownership
// active是当前节点正在运行的shard列表，key是shardID，value是shardWorker
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
			//如果当前节点没有运行该shard，则启动
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
func (n *Node) SetRefreshInterval(interval time.Duration) {
	if interval > 0 {
		n.refreshInterval = interval
	}
}

func (n *Node) SetMetricsRecorder(recorder metrics.Recorder) {
	n.metricsRecorder = recorder
}

func (n *Node) SetShardLifecycleHooks(onStart func(shardID int) error, onStop func(shardID int)) {
	n.onShardStart = onStart
	n.onShardStop = onStop
}

func (n *Node) recordEventApply(event model.Event) {
	if n.metricsRecorder == nil {
		return
	}
	n.metricsRecorder.IncEventApply(n.nodeID, event.ShardID, string(event.Type))
}

func (n *Node) recordEventApplyError(event model.Event) {
	if n.metricsRecorder == nil {
		return
	}
	n.metricsRecorder.IncEventApplyError(n.nodeID, event.ShardID, string(event.Type))
}

func (n *Node) recordFencingReject(shardID int) {
	if n.metricsRecorder == nil {
		return
	}
	n.metricsRecorder.IncFencingReject(n.nodeID, shardID)
}
