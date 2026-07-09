package tools

import "testing"

func TestConsistentHashGetReturnsAddedKey(t *testing.T) {
	hashRing := NewConsistentHash(3, nil)
	hashRing.Add("node-1")

	got := hashRing.Get("shard-1")
	if got != "node-1" {
		t.Fatalf("Get returned %q, want node-1", got)
	}
}

func TestConsistentHashGetReturnsEmptyWhenNoKeyExists(t *testing.T) {
	hashRing := NewConsistentHash(3, nil)

	got := hashRing.Get("shard-1")
	if got != "" {
		t.Fatalf("Get returned %q, want empty string", got)
	}
}

func TestConsistentHashRemove(t *testing.T) {
	hashRing := NewConsistentHash(3, nil)
	hashRing.Add("node-1", "node-2")
	hashRing.Remove("node-1")

	for i := 0; i < 20; i++ {
		got := hashRing.Get("shard-" + string(rune('a'+i)))
		if got == "node-1" {
			t.Fatalf("Get returned removed key %q", got)
		}
	}
}
