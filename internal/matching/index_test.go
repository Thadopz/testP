package matching

import (
	"testing"

	"testP/internal/model"
)

func TestIndexReturnsCandidatesOrderedByDistanceAndLoad(t *testing.T) {
	index := NewIndex(4, 10, 100, 1000)
	if err := index.ReplaceShard(0, []*model.Rider{
		{UID: 1, X: 2, Y: 2, Count: 1},
		{UID: 2, X: 8, Y: 8},
	}); err != nil {
		t.Fatalf("replace shard: %v", err)
	}

	candidates := index.FindCandidates(model.Order{ID: 10, X: 1, Y: 1}, 2)
	if len(candidates) != 2 {
		t.Fatalf("candidate count = %d, want 2", len(candidates))
	}
	if candidates[0].RiderID != 2 {
		t.Fatalf("first rider = %d, want 2", candidates[0].RiderID)
	}
}

func TestIndexAppliesRiderLifecycle(t *testing.T) {
	index := NewIndex(4, 10, 100, 1000)
	index.ReplaceShard(0, nil)
	index.ApplyRiderEvent(model.RiderEvent{Type: model.RiderOnline, UID: 1, X: 1, Y: 1})
	if got := len(index.FindCandidates(model.Order{X: 1, Y: 1}, 10)); got != 1 {
		t.Fatalf("online candidate count = %d, want 1", got)
	}
	index.ApplyRiderEvent(model.RiderEvent{Type: model.RiderOffline, UID: 1})
	if got := len(index.FindCandidates(model.Order{X: 1, Y: 1}, 10)); got != 0 {
		t.Fatalf("offline candidate count = %d, want 0", got)
	}
}
