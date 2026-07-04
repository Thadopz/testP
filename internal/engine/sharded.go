package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testP/internal/matcher"
	"testP/internal/model"
	"testP/internal/scheduler"
)

type ShardedEngine struct {
	layout      scheduler.ShardLayout
	shards      []*ShardRuntime
	ridersByUID map[int64]*model.Rider
	riderShard  map[int64]int
	mu          sync.Mutex
	metrics     *Metrics
	topK        int
	wg          sync.WaitGroup
}

type ShardRuntime struct {
	id      int
	orderCh chan model.OrderBatch
	matcher *matcher.Matcher
}

var candidateSearchRadii = []int{1, 3, 8}

func NewShardedEngine(riders []*model.Rider, shardCount int, bufferSize int, cellSize int, areaSize int, loadWeight int64) *ShardedEngine {
	return NewShardedEngineWithOptions(riders, shardCount, bufferSize, cellSize, areaSize, loadWeight, ShardedOptions{})
}

func NewShardedEngineWithOptions(riders []*model.Rider, shardCount int, bufferSize int, cellSize int, areaSize int, loadWeight int64, options ShardedOptions) *ShardedEngine {
	if shardCount <= 0 {
		shardCount = 1
	}
	if bufferSize <= 0 {
		bufferSize = 1
	}

	layout := scheduler.NewShardLayout(areaSize, cellSize, shardCount)
	ridersByShard := make([][]*model.Rider, shardCount)
	ridersByUID := make(map[int64]*model.Rider, len(riders))
	riderShard := make(map[int64]int, len(riders))

	for _, rider := range riders {
		shardID := layout.ShardID(rider.X, rider.Y)
		ridersByShard[shardID] = append(ridersByShard[shardID], rider)
		ridersByUID[rider.UID] = rider
		riderShard[rider.UID] = shardID
	}

	shards := make([]*ShardRuntime, shardCount)
	for shardID := 0; shardID < shardCount; shardID++ {
		shards[shardID] = &ShardRuntime{
			id:      shardID,
			orderCh: make(chan model.OrderBatch, bufferSize),
			matcher: matcher.NewMatcher(ridersByShard[shardID], cellSize, loadWeight),
		}
	}

	return &ShardedEngine{
		layout:      layout,
		shards:      shards,
		ridersByUID: ridersByUID,
		riderShard:  riderShard,
		metrics:     &Metrics{},
		topK:        options.TopK,
	}
}

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
	e.metrics.Submitted.Add(int64(len(batch.Orders)))

	grouped := make([][]model.Order, len(e.shards))
	for _, order := range batch.Orders {
		shardID := e.layout.ShardID(order.X, order.Y)
		grouped[shardID] = append(grouped[shardID], order)
	}

	for shardID, orders := range grouped {
		if len(orders) == 0 {
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case e.shards[shardID].orderCh <- model.OrderBatch{Orders: orders}:
		}
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

func (e *ShardedEngine) Wait() {
	e.wg.Wait()
}

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
	total := 0
	for _, shard := range e.shards {
		total += shard.matcher.OnlineRiders()
	}
	return total
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
			runtimeGosched()
		}
	}
}

func (e *ShardedEngine) matchBatch(homeShardID int, batch model.OrderBatch) {
	for i := range batch.Orders {
		e.matchOne(homeShardID, &batch.Orders[i])
	}
}

func (e *ShardedEngine) matchOne(homeShardID int, order *model.Order) {
	best := e.findBestRider(homeShardID, order)
	if best == nil {
		e.metrics.Missed.Add(1)
		return
	}

	atomic.AddInt64(&best.Count, 1)
	e.metrics.Matched.Add(1)
}

func (e *ShardedEngine) findBestRider(homeShardID int, order *model.Order) *model.Rider {
	if e.topK > 0 {
		candidates := e.findCandidates(homeShardID, order)
		if len(candidates) == 0 {
			return nil
		}
		return e.shards[homeShardID].matcher.BestCandidate(order, candidates)
	}

	homeShardIDs := []int{homeShardID}
	innerRadius := -1
	for _, outerRadius := range candidateSearchRadii {
		best := e.bestRiderInShards(homeShardIDs, order, innerRadius, outerRadius)
		if best != nil {
			return best
		}
		innerRadius = outerRadius
	}

	neighborShardIDs := e.neighborShardIDs(homeShardID)
	innerRadius = -1
	for _, outerRadius := range candidateSearchRadii {
		best := e.bestRiderInShards(neighborShardIDs, order, innerRadius, outerRadius)
		if best != nil {
			return best
		}
		innerRadius = outerRadius
	}

	fallbackShardIDs := e.unsearchedShardIDs(homeShardID, neighborShardIDs)
	return e.bestRiderInShards(fallbackShardIDs, order, -1, 8)
}

func (e *ShardedEngine) bestRiderInShards(shardIDs []int, order *model.Order, innerRadius int, outerRadius int) *model.Rider {
	var best *model.Rider
	scorer := e.shards[0].matcher

	for _, shardID := range shardIDs {
		candidate := e.shards[shardID].matcher.BestNearbyRiderInRange(order, innerRadius, outerRadius)
		best = scorer.BetterRider(order, best, candidate)
	}

	return best
}

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

func (e *ShardedEngine) collectCandidates(shardIDs []int, order *model.Order, radius int) []matcher.RiderCandidate {
	candidates := make([]matcher.RiderCandidate, 0)

	for _, shardID := range shardIDs {
		candidates = append(candidates, e.shards[shardID].matcher.FindNearbyCandidates(order.X, order.Y, radius)...)
		if e.topK > 0 && len(candidates) >= e.topK {
			return candidates[:e.topK]
		}
	}

	return candidates
}

func (e *ShardedEngine) collectCandidatesInRange(shardIDs []int, order *model.Order, innerRadius int, outerRadius int) []matcher.RiderCandidate {
	candidates := make([]matcher.RiderCandidate, 0)

	for _, shardID := range shardIDs {
		candidates = append(candidates, e.shards[shardID].matcher.FindNearbyCandidatesInRange(order.X, order.Y, innerRadius, outerRadius)...)
		if e.topK > 0 && len(candidates) >= e.topK {
			return candidates[:e.topK]
		}
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
