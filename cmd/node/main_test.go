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
