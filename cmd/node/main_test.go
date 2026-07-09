package main

import (
	"context"
	"errors"
	"sort"
	"testP/internal/eventlog"
	"testing"
	"time"
)

func TestParseBrokerListTrimsEmptyParts(t *testing.T) {
	brokers := parseBrokerList(" 127.0.0.1:9092, ,localhost:9093 ")
	if len(brokers) != 2 || brokers[0] != "127.0.0.1:9092" || brokers[1] != "localhost:9093" {
		t.Fatalf("broker list mismatch: got %v", brokers)
	}
}

func TestParseEtcdEndpoints(t *testing.T) {
	endpoints, err := parseEtcdEndpoints("127.0.0.1:2379, localhost:2381")
	if err != nil {
		t.Fatalf("parseEtcdEndpoints returned error: %v", err)
	}
	if len(endpoints) != 2 || endpoints[0] != "127.0.0.1:2379" || endpoints[1] != "localhost:2381" {
		t.Fatalf("endpoints mismatch: got %v", endpoints)
	}
}

func TestParseEtcdEndpointsRejectsEmptyInput(t *testing.T) {
	_, err := parseEtcdEndpoints(" , ")
	if err == nil {
		t.Fatal("expected parseEtcdEndpoints to return an error")
	}
}

func TestBuildNodeEventLogReturnsKafkaEventLog(t *testing.T) {
	eventLog, err := buildNodeEventLog("127.0.0.1:9092", "order-events")
	if err != nil {
		t.Fatalf("buildNodeEventLog returned error: %v", err)
	}
	if _, ok := eventLog.(*eventlog.KafkaEventLog); !ok {
		t.Fatalf("eventlog type mismatch: got %T, want *eventlog.KafkaEventLog", eventLog)
	}
}

func TestRunMembershipHeartbeatMarksNodeAliveBeforeWaiting(t *testing.T) {
	store := newNodeTestMembershipStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := runMembershipHeartbeat(ctx, store, 1, time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runMembershipHeartbeat error mismatch: got %v, want context.Canceled", err)
	}

	alive, err := store.IsAlive(1)
	if err != nil {
		t.Fatalf("IsAlive returned error: %v", err)
	}
	if !alive {
		t.Fatal("expected node 1 to be marked alive")
	}
}

type nodeTestMembershipStore struct {
	alive map[int]bool
}

func newNodeTestMembershipStore() *nodeTestMembershipStore {
	return &nodeTestMembershipStore{
		alive: make(map[int]bool),
	}
}

func (s *nodeTestMembershipStore) MarkAlive(nodeID int) error {
	s.alive[nodeID] = true
	return nil
}

func (s *nodeTestMembershipStore) MarkDead(nodeID int) error {
	delete(s.alive, nodeID)
	return nil
}

func (s *nodeTestMembershipStore) AliveNodes() ([]int, error) {
	nodeIDs := make([]int, 0, len(s.alive))
	for nodeID := range s.alive {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Ints(nodeIDs)
	return nodeIDs, nil
}

func (s *nodeTestMembershipStore) IsAlive(nodeID int) (bool, error) {
	return s.alive[nodeID], nil
}
