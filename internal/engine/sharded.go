package engine

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testP/internal/matcher"
	"testP/internal/model"
	"testP/internal/shard"
)

type ShardedEngine struct {
	//网格划分，engine用它才能求出shardID
	layout shard.Layout
	shards []*Shard
	//UID-rider映射对
	ridersByUID map[int64]*model.Rider
	//存储rider处于哪个shard
	riderShard map[int64]int
	//mutex只用于riderEvent的匹配，目前并不是性能瓶颈(详见benchmark)故先将就让事件用全局锁了
	mu sync.RWMutex
	// activeShards 限制匹配仅限于该节点当前拥有的分片
	activeShards []bool
	//指标
	metrics    *Metrics
	resultSink MatchResultSink
	wg         sync.WaitGroup
}

// 每个shard持有一个matcher与订单channel，订单会分散到各个shard中并发处理订单
// 相比于全局锁，分片处理在1w骑手100w订单的情况下快了将近90倍
type Shard struct {
	id      int
	orderCh chan model.ShardOrderBatch
	matcher *matcher.Matcher
}

var candidateSearchRadii = []int{1, 3, 8}

func NewShardedEngine(riders []*model.Rider, shardCount int, bufferSize int, cellSize int, areaSize int, loadWeight int64) *ShardedEngine {
	if shardCount <= 0 {
		shardCount = 1
	}
	if bufferSize <= 0 {
		bufferSize = 1
	}
	//初始化网格
	layout := shard.NewLayout(areaSize, cellSize, shardCount)
	ridersByShard := make([][]*model.Rider, shardCount)
	ridersByUID := make(map[int64]*model.Rider, len(riders))
	riderShard := make(map[int64]int, len(riders))
	//初始化rider
	for _, rider := range riders {
		shardID := layout.ShardID(rider.X, rider.Y)
		ridersByShard[shardID] = append(ridersByShard[shardID], rider)
		ridersByUID[rider.UID] = rider
		riderShard[rider.UID] = shardID
	}
	//初始化shard
	shards := make([]*Shard, shardCount)
	activeShards := make([]bool, shardCount)
	for shardID := 0; shardID < shardCount; shardID++ {
		shards[shardID] = &Shard{
			id:      shardID,
			orderCh: make(chan model.ShardOrderBatch, bufferSize),
			matcher: matcher.NewMatcher(ridersByShard[shardID], cellSize, loadWeight),
		}
		activeShards[shardID] = true
	}

	return &ShardedEngine{
		layout:       layout,
		shards:       shards,
		ridersByUID:  ridersByUID,
		riderShard:   riderShard,
		activeShards: activeShards,
		metrics:      &Metrics{},
	}
}

// ReplaceShardRiders 激活该节点拥有的骑手所在的一个分片
func (e *ShardedEngine) ReplaceShardRiders(shardID int, riders []*model.Rider) error {
	if shardID < 0 || shardID >= len(e.shards) {
		return fmt.Errorf("shard id out of range: %d", shardID)
	}
	for _, rider := range riders {
		if rider == nil {
			continue
		}
		if actualShardID := e.layout.ShardID(rider.X, rider.Y); actualShardID != shardID {
			return fmt.Errorf("rider %d belongs to shard %d, not shard %d", rider.UID, actualShardID, shardID)
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.removeShardRidersLocked(shardID)
	for _, rider := range riders {
		if rider == nil {
			continue
		}

		e.shards[shardID].matcher.AddRider(rider)
		e.ridersByUID[rider.UID] = rider
		e.riderShard[rider.UID] = shardID
	}
	e.activeShards[shardID] = true
	return nil
}

// DeactivateShard 移除该节点失去所有权后的本地骑手状态
func (e *ShardedEngine) DeactivateShard(shardID int) {
	if shardID < 0 || shardID >= len(e.shards) {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.removeShardRidersLocked(shardID)
	e.activeShards[shardID] = false
}

func (e *ShardedEngine) removeShardRidersLocked(shardID int) {
	for riderID, currentShardID := range e.riderShard {
		if currentShardID != shardID {
			continue
		}

		rider := e.ridersByUID[riderID]
		e.shards[shardID].matcher.DeleteRider(rider)
		delete(e.riderShard, riderID)
		delete(e.ridersByUID, riderID)
	}
}

// 启动所有workerLoop
func (e *ShardedEngine) Start(workerCount int) {
	if workerCount <= 0 {
		workerCount = 1
	}

	for workerID := 0; workerID < workerCount; workerID++ {
		e.wg.Add(1)
		go e.workerLoop()
	}
}

func (e *ShardedEngine) SubmitBatch(ctx context.Context, batch model.OrderBatch) error {
	counts := make([]int, len(e.shards))
	for _, order := range batch.Orders {
		shardID := e.layout.ShardID(order.X, order.Y)
		counts[shardID]++
	}
	//提前计算shardID并填入int切片中，同时给grouped预设空间，结果减少了BatchSubmit约61%的内存分配
	grouped := make([][]int, len(e.shards))
	for shardID, count := range counts {
		if count > 0 {
			grouped[shardID] = make([]int, 0, count)
		}
	}
	//diff: grouped内只存batch的index，在前者在BatchSubmit中61%的内存减耗上再次减少了60%分配但是端到端benchmark的速度略慢，
	//代价是batch的生命周期被进一步拉长，遇到大订单迟迟不能被消耗就可能有内存的滞留
	//同时现在再也不能修改订单了，这对鲁棒性的削弱无疑是相当大的
	for orderIndex, order := range batch.Orders {
		shardID := e.layout.ShardID(order.X, order.Y)
		grouped[shardID] = append(grouped[shardID], orderIndex)
	}

	accepted := 0
	for shardID, indexes := range grouped {
		if len(indexes) == 0 {
			continue
		}

		select {
		case <-ctx.Done():
			if accepted > 0 {
				e.metrics.Submitted.Add(int64(accepted))
			}
			return ctx.Err()
		case e.shards[shardID].orderCh <- model.ShardOrderBatch{
			Orders:  batch.Orders,
			Indexes: indexes,
		}:
			accepted += len(indexes)
		}
	}

	if accepted > 0 {
		e.metrics.Submitted.Add(int64(accepted))
	}
	return nil
}

func (e *ShardedEngine) ApplyRiderEvent(event model.RiderEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()

	switch event.Type {
	case model.RiderOnline:
		e.applyOnlineLocked(event)
	case model.RiderMove:
		e.applyMoveLocked(event)
	case model.RiderOffline:
		e.applyOfflineLocked(event)
	}
}

func (e *ShardedEngine) Close() {
	for _, shard := range e.shards {
		close(shard.orderCh)
	}
}

// 暴露wg.Wait供外部调用等待
func (e *ShardedEngine) Wait() {
	e.wg.Wait()
}

// 暴露指标提供给入口
func (e *ShardedEngine) Submitted() int64 {
	return e.metrics.Submitted.Load()
}

func (e *ShardedEngine) Matched() int64 {
	return e.metrics.Matched.Load()
}

func (e *ShardedEngine) Missed() int64 {
	return e.metrics.Missed.Load()
}

func (e *ShardedEngine) OnlineRiders() int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	total := 0
	for shardID, shard := range e.shards {
		if !e.activeShards[shardID] {
			continue
		}
		total += shard.matcher.OnlineRiders()
	}
	return total
}

func (e *ShardedEngine) Layout() shard.Layout {
	return e.layout
}

func (e *ShardedEngine) SetResultSink(sink MatchResultSink) {
	e.resultSink = sink
}

func (e *ShardedEngine) workerLoop() {
	defer e.wg.Done()

	closed := make([]bool, len(e.shards))
	closedCount := 0

	for closedCount < len(e.shards) {
		hasWork := false

		for shardID, shard := range e.shards {
			if closed[shardID] {
				continue
			}

			select {
			case batch, ok := <-shard.orderCh:
				if !ok {
					closed[shardID] = true
					closedCount++
					continue
				}

				hasWork = true
				e.matchBatch(shardID, batch)
			default:
			}
		}

		if !hasWork {
			runtime.Gosched()
		}
	}
}

// 匹配批量订单
func (e *ShardedEngine) matchBatch(homeShardID int, batch model.ShardOrderBatch) {
	for _, orderIndex := range batch.Indexes {
		e.matchOne(homeShardID, &batch.Orders[orderIndex])
	}
}

// 匹配单个订单
func (e *ShardedEngine) matchOne(homeShardID int, order *model.Order) {
	e.mu.RLock()
	if homeShardID < 0 || homeShardID >= len(e.shards) || !e.activeShards[homeShardID] {
		e.mu.RUnlock()
		return
	}

	best := e.findBestRider(homeShardID, order)
	if best == nil {
		e.mu.RUnlock()
		e.metrics.Missed.Add(1)
		e.saveMatchResult(MatchResult{
			OrderID: order.ID,
			ShardID: homeShardID,
			Matched: false,
		})
		return
	}

	atomic.AddInt64(&best.Count, 1)
	e.mu.RUnlock()
	e.metrics.Matched.Add(1)
	e.saveMatchResult(MatchResult{
		OrderID: order.ID,
		ShardID: homeShardID,
		Matched: true,
		RiderID: best.UID,
		Score:   0,
	})
}

func (e *ShardedEngine) saveMatchResult(result MatchResult) {
	if e.resultSink == nil {
		return
	}

	e.resultSink.SaveMatchResult(result)
}

func (e *ShardedEngine) findBestRider(homeShardID int, order *model.Order) *model.Rider {
	//写成切片是为了符合bestRiderInShards的参数要求，避免重新写一个差不多功能的方法
	homeShardIDs := []int{homeShardID}
	//内径，初值为-1，用于环形检测
	innerRadius := -1
	//查找homeshard内有无骑手并从其中选出最佳
	for _, outerRadius := range candidateSearchRadii {
		best := e.bestRiderInShards(homeShardIDs, order, innerRadius, outerRadius)
		if best != nil {
			return best
		}
		innerRadius = outerRadius
	}
	//homeshard中找不到时，查找相邻shard
	neighborShardIDs := e.neighborShardIDs(homeShardID)
	innerRadius = -1
	for _, outerRadius := range candidateSearchRadii {
		best := e.bestRiderInShards(neighborShardIDs, order, innerRadius, outerRadius)
		if best != nil {
			return best
		}
		innerRadius = outerRadius
	}
	//还找不到，查询还没被搜索到的，这里可能会出现相隔十分远还能匹配到的情况，
	// todo:后续需要优化一下
	fallbackShardIDs := e.unsearchedShardIDs(homeShardID, neighborShardIDs)
	return e.bestRiderInShards(fallbackShardIDs, order, -1, 8)
}

// 对于给出的shards中，在其中搜索与order匹配最佳的rider
func (e *ShardedEngine) bestRiderInShards(shardIDs []int, order *model.Order, innerRadius int, outerRadius int) *model.Rider {
	var best *model.Rider
	scorer := e.shards[0].matcher

	for _, shardID := range shardIDs {
		if !e.activeShards[shardID] {
			continue
		}
		candidate := e.shards[shardID].matcher.BestNearbyRiderInRange(order, innerRadius, outerRadius)
		best = scorer.BetterRider(order, best, candidate)
	}

	return best
}

// 正式调用中已弃用，只用于benchmark与test作为对照组
func (e *ShardedEngine) findCandidates(homeShardID int, order *model.Order) []matcher.RiderCandidate {
	homeShardIDs := []int{homeShardID}
	innerRadius := -1
	for _, outerRadius := range candidateSearchRadii {
		candidates := e.collectCandidatesInRange(homeShardIDs, order, innerRadius, outerRadius)
		if len(candidates) > 0 {
			return candidates
		}
		innerRadius = outerRadius
	}

	neighborShardIDs := e.neighborShardIDs(homeShardID)
	innerRadius = -1
	for _, outerRadius := range candidateSearchRadii {
		candidates := e.collectCandidatesInRange(neighborShardIDs, order, innerRadius, outerRadius)
		if len(candidates) > 0 {
			return candidates
		}
		innerRadius = outerRadius
	}

	return e.collectCandidates(e.unsearchedShardIDs(homeShardID, neighborShardIDs), order, 8)
}

func (e *ShardedEngine) neighborShardIDs(homeShardID int) []int {
	neighborShardIDs := e.layout.NeighborShardIDs(homeShardID)
	result := make([]int, 0, len(neighborShardIDs))

	for _, shardID := range neighborShardIDs {
		if shardID != homeShardID {
			result = append(result, shardID)
		}
	}

	return result
}

// 已弃用
func (e *ShardedEngine) collectCandidates(shardIDs []int, order *model.Order, radius int) []matcher.RiderCandidate {
	candidates := make([]matcher.RiderCandidate, 0)

	for _, shardID := range shardIDs {
		candidates = append(candidates, e.shards[shardID].matcher.FindNearbyCandidates(order.X, order.Y, radius)...)
	}

	return candidates
}

// 已弃用
func (e *ShardedEngine) collectCandidatesInRange(shardIDs []int, order *model.Order, innerRadius int, outerRadius int) []matcher.RiderCandidate {
	candidates := make([]matcher.RiderCandidate, 0)

	for _, shardID := range shardIDs {
		candidates = append(candidates, e.shards[shardID].matcher.FindNearbyCandidatesInRange(order.X, order.Y, innerRadius, outerRadius)...)

	}

	return candidates
}

func (e *ShardedEngine) unsearchedShardIDs(homeShardID int, neighborShardIDs []int) []int {
	searched := make([]bool, len(e.shards))
	if homeShardID >= 0 && homeShardID < len(searched) {
		searched[homeShardID] = true
	}
	for _, shardID := range neighborShardIDs {
		if shardID >= 0 && shardID < len(searched) {
			searched[shardID] = true
		}
	}

	ids := make([]int, 0, len(e.shards))
	for shardID := range e.shards {
		if !searched[shardID] {
			ids = append(ids, shardID)
		}
	}

	return ids
}

func (e *ShardedEngine) applyOnlineLocked(event model.RiderEvent) {
	rider := e.riderForEvent(event)
	shardID := e.layout.ShardID(event.X, event.Y)

	if oldShardID, ok := e.riderShard[event.UID]; ok && oldShardID != shardID {
		e.shards[oldShardID].matcher.DeleteRider(rider)
	}

	rider.X = event.X
	rider.Y = event.Y
	e.shards[shardID].matcher.AddRider(rider)
	e.riderShard[event.UID] = shardID
}

func (e *ShardedEngine) applyMoveLocked(event model.RiderEvent) {
	oldShardID, ok := e.riderShard[event.UID]
	if !ok {
		return
	}

	rider := e.riderForEvent(event)
	newShardID := e.layout.ShardID(event.X, event.Y)

	if oldShardID == newShardID {
		e.shards[oldShardID].matcher.MoveRider(&model.Rider{
			UID: event.UID,
			X:   event.X,
			Y:   event.Y,
		})
		return
	}

	e.shards[oldShardID].matcher.DeleteRider(rider)
	rider.X = event.X
	rider.Y = event.Y
	e.shards[newShardID].matcher.AddRider(rider)
	e.riderShard[event.UID] = newShardID
}

func (e *ShardedEngine) applyOfflineLocked(event model.RiderEvent) {
	oldShardID, ok := e.riderShard[event.UID]
	if !ok {
		return
	}

	rider := e.riderForEvent(event)
	e.shards[oldShardID].matcher.DeleteRider(rider)
	delete(e.riderShard, event.UID)
}

func (e *ShardedEngine) riderForEvent(event model.RiderEvent) *model.Rider {
	rider, ok := e.ridersByUID[event.UID]
	if ok {
		return rider
	}

	rider = &model.Rider{
		UID: event.UID,
		X:   event.X,
		Y:   event.Y,
	}
	e.ridersByUID[event.UID] = rider
	return rider
}
