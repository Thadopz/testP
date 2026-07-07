package producerapp

import (
	"context"
	"testP/internal/eventlog"
	"testing"
)

func TestRunWritesOrderCreatedEvents(t *testing.T) {
	dataDir := t.TempDir()

	result, err := Run(context.Background(), Config{
		DataDir:  dataDir,
		Orders:   3,
		Seed:     1,
		StartID:  10,
		AreaSize: 1000,
		CellSize: 100,
		Shards:   4,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if result.Orders != 3 {
		t.Fatalf("orders mismatch: got %d, want %d", result.Orders, 3)
	}
	if result.FirstID != 10 {
		t.Fatalf("first id mismatch: got %d, want %d", result.FirstID, int64(10))
	}
	if result.LastID != 12 {
		t.Fatalf("last id mismatch: got %d, want %d", result.LastID, int64(12))
	}

	fileEventLog := eventlog.NewFileEventLog(result.EventLogDir, &eventlog.JSONEventCodec{})
	totalRecords := 0
	for shardID := 0; shardID < 4; shardID++ {
		recordCh, err := fileEventLog.ReadFrom(context.Background(), eventlog.Position{ShardID: shardID, Offset: 0})
		if err != nil {
			t.Fatalf("ReadFrom returned error: %v", err)
		}
		for range recordCh {
			totalRecords++
		}
	}

	if totalRecords != 3 {
		t.Fatalf("record count mismatch: got %d, want %d", totalRecords, 3)
	}
}

func TestRunRejectsNegativeOrders(t *testing.T) {
	_, err := Run(context.Background(), Config{
		DataDir: t.TempDir(),
		Orders:  -1,
	})
	if err == nil {
		t.Fatal("expected Run to return an error")
	}
}
