package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testP/internal/producerapp"
)

func main() {
	dataDir := flag.String("data-dir", "./data", "data directory")
	orderCount := flag.Int("orders", 100, "number of order_created events to write")
	seed := flag.Int64("seed", 1, "random seed")
	startID := flag.Int64("start-id", 1, "first order id")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	result, err := producerapp.Run(ctx, producerapp.Config{
		DataDir: *dataDir,
		Orders:  *orderCount,
		Seed:    *seed,
		StartID: *startID,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "producer failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("eventlog_dir: %s\n", result.EventLogDir)
	fmt.Printf("orders: %d\n", result.Orders)
	fmt.Printf("first_id: %d\n", result.FirstID)
	fmt.Printf("last_id: %d\n", result.LastID)
}
