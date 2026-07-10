package main

import "testing"

func TestScenariosForProfileSmoke(t *testing.T) {
	scenarios, err := scenariosForProfile("smoke")
	if err != nil {
		t.Fatalf("scenariosForProfile returned error: %v", err)
	}
	if len(scenarios) != 1 {
		t.Fatalf("scenario count mismatch: got %d, want 1", len(scenarios))
	}
	if scenarios[0].Nodes != 2 || scenarios[0].Riders != 100 || scenarios[0].Orders != 1000 {
		t.Fatalf("smoke scenario mismatch: got %+v", scenarios[0])
	}
}

func TestApplyOverridesKeepsScenarioValuesWhenZero(t *testing.T) {
	scenarios := []Scenario{
		{Name: "case", Nodes: 2, Riders: 100, Orders: 1000},
	}

	got := applyOverrides(scenarios, 0, 0, 0)

	if got[0].Nodes != 2 || got[0].Riders != 100 || got[0].Orders != 1000 {
		t.Fatalf("override mismatch: got %+v", got[0])
	}
}

func TestApplyOverridesReplacesPositiveValues(t *testing.T) {
	scenarios := []Scenario{
		{Name: "case", Nodes: 2, Riders: 100, Orders: 1000},
	}

	got := applyOverrides(scenarios, 4, 500, 2000)

	if got[0].Nodes != 4 || got[0].Riders != 500 || got[0].Orders != 2000 {
		t.Fatalf("override mismatch: got %+v", got[0])
	}
}

func TestParseCommaSeparatedTrimsEmptyValues(t *testing.T) {
	got := parseCommaSeparated("127.0.0.1:9092, , localhost:9093 ")

	if len(got) != 2 {
		t.Fatalf("value count mismatch: got %d, want 2", len(got))
	}
	if got[0] != "127.0.0.1:9092" || got[1] != "localhost:9093" {
		t.Fatalf("values mismatch: got %v", got)
	}
}

func TestSanitizeNamePart(t *testing.T) {
	got := sanitizeNamePart(" 2N_100R/1K Orders ")

	if got != "2n-100r-1k-orders" {
		t.Fatalf("sanitizeNamePart returned %q, want 2n-100r-1k-orders", got)
	}
}

func TestBenchmarkEtcdPrefixKeepsLeadingSlash(t *testing.T) {
	got := benchmarkEtcdPrefix("testp-bench", "smoke")

	if len(got) == 0 || got[0] != '/' {
		t.Fatalf("benchmarkEtcdPrefix returned %q, want leading slash", got)
	}
}

func TestAssignModuloOwnershipDistributesShardsAcrossNodes(t *testing.T) {
	store := newBenchmarkOwnershipStore()

	if err := assignModuloOwnership(store, 3, 8); err != nil {
		t.Fatalf("assignModuloOwnership returned error: %v", err)
	}

	tests := []struct {
		nodeID int
		want   []int
	}{
		{nodeID: 1, want: []int{0, 3, 6}},
		{nodeID: 2, want: []int{1, 4, 7}},
		{nodeID: 3, want: []int{2, 5}},
	}
	for _, tt := range tests {
		got, err := store.ShardsForNode(tt.nodeID)
		if err != nil {
			t.Fatalf("ShardsForNode returned error: %v", err)
		}
		if len(got) != len(tt.want) {
			t.Fatalf("node %d shard count mismatch: got %d, want %d", tt.nodeID, len(got), len(tt.want))
		}
		for i, wantShardID := range tt.want {
			if got[i].ShardID != wantShardID {
				t.Fatalf("node %d shard mismatch at %d: got %d, want %d", tt.nodeID, i, got[i].ShardID, wantShardID)
			}
		}
	}
}
