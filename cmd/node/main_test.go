package main

import (
	"os"
	"path/filepath"
	clusterownership "testP/internal/cluster/ownership"
	"testing"
)

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

func TestResolveShardAssignmentDynamicReturnsProvider(t *testing.T) {
	ownershipDir := t.TempDir()
	shardIDs, provider, err := resolveShardAssignment(1, "99", "1,2", 6, true, ownershipDir)
	if err != nil {
		t.Fatalf("resolveShardAssignment returned error: %v", err)
	}
	if provider == nil {
		t.Fatal("expected dynamic shard provider")
	}
	if len(shardIDs) != 3 || shardIDs[0] != 0 || shardIDs[1] != 2 || shardIDs[2] != 4 {
		t.Fatalf("shard ids mismatch: got %v, want [0 2 4]", shardIDs)
	}

	if _, ok := provider.(*clusterownership.FileOwnershipStore); !ok {
		t.Fatalf("provider type mismatch: got %T, want *clusterownership.FileOwnershipStore", provider)
	}
}

func TestResolveShardAssignmentDynamicReadsExistingFileOwnership(t *testing.T) {
	ownershipDir := t.TempDir()
	store := clusterownership.NewFileOwnershipStore(ownershipDir)
	if err := store.Assign(2, 1); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	shardIDs, provider, err := resolveShardAssignment(1, "0", "", 6, true, ownershipDir)
	if err != nil {
		t.Fatalf("resolveShardAssignment returned error: %v", err)
	}
	if provider == nil {
		t.Fatal("expected dynamic shard provider")
	}
	if len(shardIDs) != 1 || shardIDs[0] != 2 {
		t.Fatalf("shard ids mismatch: got %v, want [2]", shardIDs)
	}
}

func TestResolveShardAssignmentDynamicDoesNotOverwriteExistingOwnership(t *testing.T) {
	ownershipDir := t.TempDir()
	store := clusterownership.NewFileOwnershipStore(ownershipDir)
	if err := store.Assign(0, 2); err != nil {
		t.Fatalf("Assign returned error: %v", err)
	}

	shardIDs, _, err := resolveShardAssignment(1, "99", "1,2", 2, true, ownershipDir)
	if err != nil {
		t.Fatalf("resolveShardAssignment returned error: %v", err)
	}
	if len(shardIDs) != 0 {
		t.Fatalf("shard ids mismatch: got %v, want []", shardIDs)
	}

	ownership, ok, err := store.OwnerOf(0)
	if err != nil {
		t.Fatalf("OwnerOf returned error: %v", err)
	}
	if !ok {
		t.Fatal("expected shard 0 ownership to exist")
	}
	if ownership.NodeID != 2 || ownership.Epoch != 1 {
		t.Fatalf("ownership mismatch: got %+v, want node=2 epoch=1", ownership)
	}
}

func TestResolveShardAssignmentDynamicCreatesOwnershipFile(t *testing.T) {
	ownershipDir := t.TempDir()

	_, _, err := resolveShardAssignment(1, "99", "1,2", 6, true, ownershipDir)
	if err != nil {
		t.Fatalf("resolveShardAssignment returned error: %v", err)
	}

	if !fileExists(filepath.Join(ownershipDir, "ownership.json")) {
		t.Fatal("expected ownership file to exist")
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
