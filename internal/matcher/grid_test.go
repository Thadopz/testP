package matcher

import (
	"reflect"
	"sort"
	"testing"

	"testP/internal/model"
)

func TestFindNearbyCandidatesInRangeMatchesFullRadiusDifference(t *testing.T) {
	grid := NewGridIndex([]*model.Rider{
		{UID: 1, X: 50, Y: 50},
		{UID: 2, X: 60, Y: 50},
		{UID: 3, X: 70, Y: 50},
		{UID: 4, X: 80, Y: 80},
		{UID: 5, X: 90, Y: 50},
		{UID: 6, X: 130, Y: 50},
		{UID: 7, X: 140, Y: 50},
	}, 10)

	full1 := candidateUIDs(grid.FindNearbyCandidates(50, 50, 1))
	range0To1 := candidateUIDs(grid.FindNearbyCandidatesInRange(50, 50, -1, 1))
	if !reflect.DeepEqual(range0To1, full1) {
		t.Fatalf("range -1..1 = %v, want full radius 1 %v", range0To1, full1)
	}

	full3 := candidateUIDs(grid.FindNearbyCandidates(50, 50, 3))
	range2To3 := candidateUIDs(grid.FindNearbyCandidatesInRange(50, 50, 1, 3))
	wantRange2To3 := differenceUIDs(full3, full1)
	if !reflect.DeepEqual(range2To3, wantRange2To3) {
		t.Fatalf("range 2..3 = %v, want %v", range2To3, wantRange2To3)
	}

	full8 := candidateUIDs(grid.FindNearbyCandidates(50, 50, 8))
	range4To8 := candidateUIDs(grid.FindNearbyCandidatesInRange(50, 50, 3, 8))
	wantRange4To8 := differenceUIDs(full8, full3)
	if !reflect.DeepEqual(range4To8, wantRange4To8) {
		t.Fatalf("range 4..8 = %v, want %v", range4To8, wantRange4To8)
	}
}

func TestFindNearbyCandidatesInRangeEmptyRanges(t *testing.T) {
	grid := NewGridIndex([]*model.Rider{
		{UID: 1, X: 50, Y: 50},
	}, 10)

	cases := []struct {
		name        string
		innerRadius int
		outerRadius int
	}{
		{name: "same radius", innerRadius: 1, outerRadius: 1},
		{name: "inverted radius", innerRadius: 3, outerRadius: 1},
		{name: "negative outer radius", innerRadius: -1, outerRadius: -1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidates := grid.FindNearbyCandidatesInRange(50, 50, tc.innerRadius, tc.outerRadius)
			if len(candidates) != 0 {
				t.Fatalf("got %d candidates, want 0", len(candidates))
			}
		})
	}
}

func TestCellIDDoesNotCollideAtOldHashBoundary(t *testing.T) {
	grid := NewGridIndex([]*model.Rider{
		{UID: 1, X: 0, Y: 100000},
		{UID: 2, X: 1, Y: 0},
	}, 1)

	candidates := candidateUIDs(grid.FindNearbyCandidatesInRange(1, 0, -1, 0))
	want := []int64{2}

	if !reflect.DeepEqual(candidates, want) {
		t.Fatalf("got candidate UIDs %v, want %v", candidates, want)
	}
}

func candidateUIDs(candidates []RiderCandidate) []int64 {
	uids := make([]int64, 0, len(candidates))
	for _, candidate := range candidates {
		uids = append(uids, candidate.UID)
	}
	sort.Slice(uids, func(i int, j int) bool {
		return uids[i] < uids[j]
	})
	return uids
}

func differenceUIDs(left []int64, right []int64) []int64 {
	excluded := make(map[int64]bool, len(right))
	for _, uid := range right {
		excluded[uid] = true
	}

	result := make([]int64, 0, len(left))
	for _, uid := range left {
		if !excluded[uid] {
			result = append(result, uid)
		}
	}
	return result
}
