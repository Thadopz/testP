package engine

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"testP/internal/matcher"
	"testP/internal/model"
)

func TestShardedFindCandidatesHomeShardIncrementalHit(t *testing.T) {
	e := newTestShardedEngine([]*model.Rider{
		{UID: 1, X: 20, Y: 0},
	})
	order := &model.Order{ID: 1, X: 0, Y: 0}

	candidates := e.findCandidates(e.layout.ShardID(order.X, order.Y), order)
	assertCandidateUIDs(t, candidates, []int64{1})
}

func TestShardedFindCandidatesNeighborShardIncrementalHit(t *testing.T) {
	e := newTestShardedEngine([]*model.Rider{
		{UID: 1, X: 30, Y: 0},
	})
	order := &model.Order{ID: 1, X: 0, Y: 0}

	candidates := e.findCandidates(e.layout.ShardID(order.X, order.Y), order)
	assertCandidateUIDs(t, candidates, []int64{1})
}

func TestShardedFindCandidatesRemoteShardFallbackHit(t *testing.T) {
	e := newTestShardedEngine([]*model.Rider{
		{UID: 1, X: 80, Y: 80},
	})
	order := &model.Order{ID: 1, X: 0, Y: 0}

	candidates := e.findCandidates(e.layout.ShardID(order.X, order.Y), order)
	assertCandidateUIDs(t, candidates, []int64{1})
}

func TestSubmitBatchCountsOnlyAcceptedOrdersWhenContextCancels(t *testing.T) {
	e := NewShardedEngine(nil, 2, 1, 10, 20, 1000)
	batch := model.OrderBatch{Orders: []model.Order{
		{ID: 1, X: 0, Y: 0},
		{ID: 2, X: 19, Y: 0},
	}}

	e.shards[1].orderCh <- model.ShardOrderBatch{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := e.SubmitBatch(ctx, batch)
	if err == nil {
		t.Fatal("got nil error, want context cancellation")
	}
	if got := e.Submitted(); got != 1 {
		t.Fatalf("submitted = %d, want 1", got)
	}
}

func TestRiderMoveAcrossShardsUpdatesMatcherIndexes(t *testing.T) {
	e := NewShardedEngine([]*model.Rider{
		{UID: 1, X: 0, Y: 0},
	}, 4, 1, 10, 40, 1000)

	oldShardID := e.layout.ShardID(0, 0)
	newShardID := e.layout.ShardID(39, 39)
	oldOrder := &model.Order{ID: 1, X: 0, Y: 0}
	if got := e.shards[oldShardID].matcher.BestNearbyRiderInRange(oldOrder, -1, 1); got == nil || got.UID != 1 {
		t.Fatalf("initial match UID = %v, want 1", riderUID(got))
	}

	e.ApplyRiderEvent(model.RiderEvent{
		Type: model.RiderMove,
		UID:  1,
		X:    39,
		Y:    39,
	})

	if got := e.OnlineRiders(); got != 1 {
		t.Fatalf("online riders = %d, want 1", got)
	}

	if got := e.shards[oldShardID].matcher.BestNearbyRiderInRange(oldOrder, -1, 1); got != nil {
		t.Fatalf("old shard match UID = %d, want nil", got.UID)
	}

	newOrder := &model.Order{ID: 2, X: 39, Y: 39}
	if got := e.shards[newShardID].matcher.BestNearbyRiderInRange(newOrder, -1, 1); got == nil || got.UID != 1 {
		t.Fatalf("new shard match UID = %v, want 1", riderUID(got))
	}
}

func TestRiderOfflineThenOnlineUpdatesMatcherIndexes(t *testing.T) {
	e := NewShardedEngine([]*model.Rider{
		{UID: 1, X: 0, Y: 0},
	}, 4, 1, 10, 40, 1000)

	oldShardID := e.layout.ShardID(0, 0)
	oldOrder := &model.Order{ID: 1, X: 0, Y: 0}
	if got := e.shards[oldShardID].matcher.BestNearbyRiderInRange(oldOrder, -1, 1); got == nil || got.UID != 1 {
		t.Fatalf("initial match UID = %v, want 1", riderUID(got))
	}

	e.ApplyRiderEvent(model.RiderEvent{
		Type: model.RiderOffline,
		UID:  1,
	})

	if got := e.OnlineRiders(); got != 0 {
		t.Fatalf("online riders after offline = %d, want 0", got)
	}
	if got := e.shards[oldShardID].matcher.BestNearbyRiderInRange(oldOrder, -1, 1); got != nil {
		t.Fatalf("offline rider match UID = %d, want nil", got.UID)
	}

	e.ApplyRiderEvent(model.RiderEvent{
		Type: model.RiderOnline,
		UID:  1,
		X:    39,
		Y:    39,
	})

	if got := e.OnlineRiders(); got != 1 {
		t.Fatalf("online riders after online = %d, want 1", got)
	}

	newOrder := &model.Order{ID: 2, X: 39, Y: 39}
	newShardID := e.layout.ShardID(newOrder.X, newOrder.Y)
	if got := e.shards[newShardID].matcher.BestNearbyRiderInRange(newOrder, -1, 1); got == nil || got.UID != 1 {
		t.Fatalf("new online match UID = %v, want 1", riderUID(got))
	}
}

func newTestShardedEngine(riders []*model.Rider) *ShardedEngine {
	return NewShardedEngine(riders, 9, 4, 10, 90, 1000)
}

func riderUID(rider *model.Rider) any {
	if rider == nil {
		return nil
	}
	return rider.UID
}

func assertCandidateUIDs(t *testing.T, candidates []matcher.RiderCandidate, want []int64) {
	t.Helper()

	got := make([]int64, 0, len(candidates))
	for _, candidate := range candidates {
		got = append(got, candidate.UID)
	}
	sort.Slice(got, func(i int, j int) bool {
		return got[i] < got[j]
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got candidate UIDs %v, want %v", got, want)
	}
}
