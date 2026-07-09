package main

import "testing"

func TestParseEtcdEndpoints(t *testing.T) {
	endpoints, err := parseEtcdEndpoints("127.0.0.1:2379, 127.0.0.1:2381")
	if err != nil {
		t.Fatalf("parseEtcdEndpoints returned error: %v", err)
	}
	if len(endpoints) != 2 {
		t.Fatalf("endpoint count mismatch: got %d, want 2", len(endpoints))
	}
	if endpoints[0] != "127.0.0.1:2379" || endpoints[1] != "127.0.0.1:2381" {
		t.Fatalf("endpoints mismatch: got %v", endpoints)
	}
}

func TestParseEtcdEndpointsRejectsEmptyInput(t *testing.T) {
	_, err := parseEtcdEndpoints(" , ")
	if err == nil {
		t.Fatal("expected parseEtcdEndpoints to return an error")
	}
}
