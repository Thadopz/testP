package membership

import "testing"

func TestMemoryMembershipStoreTracksAliveNodes(t *testing.T) {
	store := NewMemoryMembershipStore()

	if err := store.MarkAlive(2); err != nil {
		t.Fatalf("MarkAlive returned error: %v", err)
	}
	if err := store.MarkAlive(1); err != nil {
		t.Fatalf("MarkAlive returned error: %v", err)
	}

	aliveNodes, err := store.AliveNodes()
	if err != nil {
		t.Fatalf("AliveNodes returned error: %v", err)
	}
	if len(aliveNodes) != 2 || aliveNodes[0] != 1 || aliveNodes[1] != 2 {
		t.Fatalf("alive nodes mismatch: got %v, want [1 2]", aliveNodes)
	}
}

func TestMemoryMembershipStoreMarkDeadRemovesNode(t *testing.T) {
	store := NewMemoryMembershipStore()

	if err := store.MarkAlive(1); err != nil {
		t.Fatalf("MarkAlive returned error: %v", err)
	}
	if err := store.MarkDead(1); err != nil {
		t.Fatalf("MarkDead returned error: %v", err)
	}

	alive, err := store.IsAlive(1)
	if err != nil {
		t.Fatalf("IsAlive returned error: %v", err)
	}
	if alive {
		t.Fatal("expected node 1 to be dead")
	}
}

func TestMemoryMembershipStoreRejectsInvalidNodeID(t *testing.T) {
	store := NewMemoryMembershipStore()

	if err := store.MarkAlive(0); err == nil {
		t.Fatal("expected MarkAlive to return an error")
	}
	if err := store.MarkDead(0); err == nil {
		t.Fatal("expected MarkDead to return an error")
	}
}
