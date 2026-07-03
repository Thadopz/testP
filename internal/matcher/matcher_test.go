package matcher

import (
	"testing"

	"testP/internal/model"
)

func TestMatchOneUsesIncrementalOuterRingWhenInnerRingMisses(t *testing.T) {
	m := NewMatcher([]*model.Rider{
		{UID: 1, X: 70, Y: 50},
	}, 10, 1000)

	rider := m.MatchOne(&model.Order{ID: 1, X: 50, Y: 50})
	if rider == nil {
		t.Fatal("got nil rider, want UID 1")
	}
	if rider.UID != 1 {
		t.Fatalf("got UID %d, want 1", rider.UID)
	}
}

func TestMatchOneStopsAtFirstRingWithCandidates(t *testing.T) {
	inner := &model.Rider{UID: 1, X: 60, Y: 50, Count: 10}
	outer := &model.Rider{UID: 2, X: 70, Y: 50}
	m := NewMatcher([]*model.Rider{inner, outer}, 10, 1000)

	rider := m.MatchOne(&model.Order{ID: 1, X: 50, Y: 50})
	if rider == nil {
		t.Fatal("got nil rider, want inner rider")
	}
	if rider.UID != 1 {
		t.Fatalf("got UID %d, want 1", rider.UID)
	}
}
