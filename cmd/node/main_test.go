package main

import "testing"

func TestParseShardIDs(t *testing.T) {
	shardIDs, err := parseShardIDs("0,1,2")
	if err != nil {
		t.Fatalf("parseShardIDs returned error: %v", err)
	}

	if len(shardIDs) != 3 || shardIDs[0] != 0 || shardIDs[1] != 1 || shardIDs[2] != 2 {
		t.Fatalf("shard ids mismatch: got %v, want [0 1 2]", shardIDs)
	}
}

func TestParseShardIDsTrimsSpaces(t *testing.T) {
	shardIDs, err := parseShardIDs(" 0, 2 ")
	if err != nil {
		t.Fatalf("parseShardIDs returned error: %v", err)
	}

	if len(shardIDs) != 2 || shardIDs[0] != 0 || shardIDs[1] != 2 {
		t.Fatalf("shard ids mismatch: got %v, want [0 2]", shardIDs)
	}
}

func TestParseShardIDsRejectsEmptyText(t *testing.T) {
	_, err := parseShardIDs("")
	if err == nil {
		t.Fatal("expected parseShardIDs to return an error")
	}
}

func TestParseShardIDsRejectsNonNumber(t *testing.T) {
	_, err := parseShardIDs("a")
	if err == nil {
		t.Fatal("expected parseShardIDs to return an error")
	}
}

func TestParseShardIDsRejectsNegativeNumber(t *testing.T) {
	_, err := parseShardIDs("-1")
	if err == nil {
		t.Fatal("expected parseShardIDs to return an error")
	}
}

func TestParseNodeIDs(t *testing.T) {
	nodeIDs, err := parseNodeIDs("1,2,3")
	if err != nil {
		t.Fatalf("parseNodeIDs returned error: %v", err)
	}

	if len(nodeIDs) != 3 || nodeIDs[0] != 1 || nodeIDs[1] != 2 || nodeIDs[2] != 3 {
		t.Fatalf("node ids mismatch: got %v, want [1 2 3]", nodeIDs)
	}
}

func TestParseNodeIDsTrimsSpaces(t *testing.T) {
	nodeIDs, err := parseNodeIDs(" 1, 3 ")
	if err != nil {
		t.Fatalf("parseNodeIDs returned error: %v", err)
	}

	if len(nodeIDs) != 2 || nodeIDs[0] != 1 || nodeIDs[1] != 3 {
		t.Fatalf("node ids mismatch: got %v, want [1 3]", nodeIDs)
	}
}

func TestParseNodeIDsRejectsEmptyText(t *testing.T) {
	_, err := parseNodeIDs("")
	if err == nil {
		t.Fatal("expected parseNodeIDs to return an error")
	}
}

func TestParseNodeIDsRejectsNonNumber(t *testing.T) {
	_, err := parseNodeIDs("a")
	if err == nil {
		t.Fatal("expected parseNodeIDs to return an error")
	}
}

func TestParseNodeIDsRejectsNonPositiveNumber(t *testing.T) {
	_, err := parseNodeIDs("0")
	if err == nil {
		t.Fatal("expected parseNodeIDs to return an error")
	}
}

func TestResolveShardIDsUsesManualShardsWhenNodesAreEmpty(t *testing.T) {
	shardIDs, err := resolveShardIDs(1, "0,2", "", 64)
	if err != nil {
		t.Fatalf("resolveShardIDs returned error: %v", err)
	}

	if len(shardIDs) != 2 || shardIDs[0] != 0 || shardIDs[1] != 2 {
		t.Fatalf("shard ids mismatch: got %v, want [0 2]", shardIDs)
	}
}

func TestResolveShardIDsUsesModuloLayout(t *testing.T) {
	shardIDs, err := resolveShardIDs(1, "99", "1,2", 6)
	if err != nil {
		t.Fatalf("resolveShardIDs returned error: %v", err)
	}

	if len(shardIDs) != 3 || shardIDs[0] != 0 || shardIDs[1] != 2 || shardIDs[2] != 4 {
		t.Fatalf("shard ids mismatch: got %v, want [0 2 4]", shardIDs)
	}
}

func TestResolveShardIDsUsesCurrentNodeID(t *testing.T) {
	shardIDs, err := resolveShardIDs(2, "99", "1,2", 6)
	if err != nil {
		t.Fatalf("resolveShardIDs returned error: %v", err)
	}

	if len(shardIDs) != 3 || shardIDs[0] != 1 || shardIDs[1] != 3 || shardIDs[2] != 5 {
		t.Fatalf("shard ids mismatch: got %v, want [1 3 5]", shardIDs)
	}
}

func TestResolveShardIDsRejectsUnknownNode(t *testing.T) {
	_, err := resolveShardIDs(3, "99", "1,2", 6)
	if err == nil {
		t.Fatal("expected resolveShardIDs to return an error")
	}
}
