package matching

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"testP/internal/matcher"
	"testP/internal/model"
	"testP/internal/shard"
)

type Candidate struct {
	RiderID int64
	Score   int64
}

type Index struct {
	mu           sync.RWMutex
	layout       shard.Layout
	matchers     []*matcher.Matcher
	activeShards []bool
	riders       map[int64]*model.Rider
	riderShards  map[int64]int
	cellSize     int
	loadWeight   int64
}

func NewIndex(shardCount int, cellSize int, areaSize int, loadWeight int64) *Index {
	if shardCount <= 0 {
		shardCount = 1
	}
	matchers := make([]*matcher.Matcher, shardCount)
	for shardID := range matchers {
		matchers[shardID] = matcher.NewMatcher(nil, cellSize, loadWeight)
	}
	return &Index{
		layout:       shard.NewLayout(areaSize, cellSize, shardCount),
		matchers:     matchers,
		activeShards: make([]bool, shardCount),
		riders:       make(map[int64]*model.Rider),
		riderShards:  make(map[int64]int),
		cellSize:     cellSize,
		loadWeight:   loadWeight,
	}
}

func (i *Index) Layout() shard.Layout {
	return i.layout
}

func (i *Index) ReplaceShard(shardID int, riders []*model.Rider) error {
	if shardID < 0 || shardID >= len(i.matchers) {
		return nil
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	for riderID, currentShardID := range i.riderShards {
		if currentShardID == shardID {
			delete(i.riders, riderID)
			delete(i.riderShards, riderID)
		}
	}
	i.matchers[shardID] = matcher.NewMatcher(riders, i.cellSize, i.loadWeight)
	i.activeShards[shardID] = true
	for _, rider := range riders {
		i.riders[rider.UID] = rider
		i.riderShards[rider.UID] = shardID
	}
	return nil
}

func (i *Index) RemoveShard(shardID int) {
	if shardID < 0 || shardID >= len(i.matchers) {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.activeShards[shardID] = false
	i.matchers[shardID] = matcher.NewMatcher(nil, i.cellSize, i.loadWeight)
	for riderID, currentShardID := range i.riderShards {
		if currentShardID == shardID {
			delete(i.riders, riderID)
			delete(i.riderShards, riderID)
		}
	}
}

func (i *Index) ApplyRiderEvent(event model.RiderEvent) {
	i.mu.Lock()
	defer i.mu.Unlock()

	switch event.Type {
	case model.RiderOnline:
		i.applyOnline(event)
	case model.RiderMove:
		i.applyMove(event)
	case model.RiderOffline:
		i.applyOffline(event)
	}
}

func (i *Index) FindCandidates(order model.Order, limit int) []Candidate {
	if limit <= 0 {
		return nil
	}
	i.mu.RLock()
	defer i.mu.RUnlock()

	homeShardID := i.layout.ShardID(order.X, order.Y)
	groups := [][]int{{homeShardID}, i.neighborShardIDs(homeShardID), i.otherShardIDs(homeShardID)}
	for _, shardIDs := range groups {
		innerRadius := -1
		for _, outerRadius := range []int{1, 3, 8} {
			candidates := i.collect(shardIDs, order, innerRadius, outerRadius)
			if len(candidates) > 0 {
				return bestCandidates(candidates, order, i.cellSize, i.loadWeight, limit)
			}
			innerRadius = outerRadius
		}
	}
	return nil
}

func (i *Index) SetRiderCount(riderID int64, count int64) {
	i.mu.RLock()
	rider := i.riders[riderID]
	i.mu.RUnlock()
	if rider != nil {
		atomic.StoreInt64(&rider.Count, count)
	}
}

func (i *Index) collect(shardIDs []int, order model.Order, innerRadius int, outerRadius int) []matcher.RiderCandidate {
	candidates := make([]matcher.RiderCandidate, 0)
	for _, shardID := range shardIDs {
		if shardID < 0 || shardID >= len(i.matchers) || !i.activeShards[shardID] {
			continue
		}
		candidates = append(candidates, i.matchers[shardID].FindNearbyCandidatesInRange(
			order.X,
			order.Y,
			innerRadius,
			outerRadius,
		)...)
	}
	return candidates
}

func (i *Index) neighborShardIDs(homeShardID int) []int {
	all := i.layout.NeighborShardIDs(homeShardID)
	result := make([]int, 0, len(all))
	for _, shardID := range all {
		if shardID != homeShardID {
			result = append(result, shardID)
		}
	}
	return result
}

func (i *Index) otherShardIDs(homeShardID int) []int {
	neighbors := make(map[int]bool)
	for _, shardID := range i.neighborShardIDs(homeShardID) {
		neighbors[shardID] = true
	}
	result := make([]int, 0, len(i.matchers))
	for shardID := range i.matchers {
		if shardID != homeShardID && !neighbors[shardID] {
			result = append(result, shardID)
		}
	}
	return result
}

func (i *Index) applyOnline(event model.RiderEvent) {
	rider := i.riders[event.UID]
	if rider == nil {
		rider = &model.Rider{UID: event.UID}
		i.riders[event.UID] = rider
	}
	newShardID := i.layout.ShardID(event.X, event.Y)
	if oldShardID, found := i.riderShards[event.UID]; found && oldShardID != newShardID {
		i.matchers[oldShardID].DeleteRider(rider)
	}
	rider.X = event.X
	rider.Y = event.Y
	i.matchers[newShardID].AddRider(rider)
	i.riderShards[event.UID] = newShardID
}

func (i *Index) applyMove(event model.RiderEvent) {
	oldShardID, found := i.riderShards[event.UID]
	if !found {
		return
	}
	rider := i.riders[event.UID]
	newShardID := i.layout.ShardID(event.X, event.Y)
	if oldShardID == newShardID {
		i.matchers[oldShardID].MoveRider(&model.Rider{UID: event.UID, X: event.X, Y: event.Y})
		return
	}
	i.matchers[oldShardID].DeleteRider(rider)
	rider.X = event.X
	rider.Y = event.Y
	i.matchers[newShardID].AddRider(rider)
	i.riderShards[event.UID] = newShardID
}

func (i *Index) applyOffline(event model.RiderEvent) {
	shardID, found := i.riderShards[event.UID]
	if !found {
		return
	}
	rider := i.riders[event.UID]
	i.matchers[shardID].DeleteRider(rider)
	delete(i.riders, event.UID)
	delete(i.riderShards, event.UID)
}

func bestCandidates(
	items []matcher.RiderCandidate,
	order model.Order,
	cellSize int,
	loadWeight int64,
	limit int,
) []Candidate {
	result := make([]Candidate, 0, len(items))
	for _, item := range items {
		distance := int64(math.Abs(float64(order.X-item.X)) + math.Abs(float64(order.Y-item.Y)))
		result = append(result, Candidate{
			RiderID: item.UID,
			Score:   distance/int64(cellSize) + item.Count*loadWeight,
		})
	}
	sort.Slice(result, func(left int, right int) bool {
		if result[left].Score == result[right].Score {
			return result[left].RiderID < result[right].RiderID
		}
		return result[left].Score < result[right].Score
	})
	if len(result) > limit {
		result = result[:limit]
	}
	return result
}
