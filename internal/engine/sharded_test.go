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

func newTestShardedEngine(riders []*model.Rider) *ShardedEngine {
	return NewShardedEngine(riders, 9, 4, 10, 90, 1000)
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
