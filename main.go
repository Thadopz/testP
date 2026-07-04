package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"sync/atomic"
	"syscall"
	"testP/internal/engine"
	"testP/internal/model"
	"time"
)

const (
	areaSize   = 100000
	shardCount = 64
	bufferSize = 128
	loadWeight = 10000

	orderMinBatch = 1
	orderMaxBatch = 50
	orderMinWait  = 10 * time.Millisecond
	orderMaxWait  = 50 * time.Millisecond

	riderMinWait = 100 * time.Millisecond
	riderMaxWait = 500 * time.Millisecond

	statsInterval = time.Second
)

func main() {
	riderCount := flag.Int("riders", 100, "initial rider count")
	workerCount := flag.Int("workers", 2, "worker count")
	runForText := flag.String("run-for", "0s", "runtime; 0s means until Ctrl+C")
	seed := flag.Int64("seed", 1, "random seed")
	flag.Parse()

	if *riderCount <= 0 {
		*riderCount = 1
	}
	if *workerCount <= 0 {
		*workerCount = 1
	}

	runFor, err := time.ParseDuration(*runForText)
	if err != nil {
		panic(err)
	}

	runtime.GOMAXPROCS(*workerCount)

	cellSize := autoCellSize(areaSize, *riderCount)
	riders := generateRiders(rand.New(rand.NewSource(*seed)), *riderCount, areaSize)
	e := engine.NewShardedEngine(riders, shardCount, bufferSize, cellSize, areaSize, loadWeight)

	e.Start(*workerCount)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if runFor > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, runFor)
		defer cancel()
	}

	start := time.Now()
	statsDone := startStatsPrinter(ctx, start, e)

	go produceRiderEvents(ctx, rand.New(rand.NewSource(*seed+2)), e, riders, areaSize)
	produceOrders(ctx, rand.New(rand.NewSource(*seed+1)), e, areaSize)

	<-statsDone
	e.Close()
	e.Wait()

	printFinalStats(start, riders, e, *workerCount, cellSize)
}

func generateRiders(rng *rand.Rand, count int, areaSize int) []*model.Rider {
	riders := make([]*model.Rider, 0, count)

	for i := 0; i < count; i++ {
		riders = append(riders, &model.Rider{
			UID: int64(i + 1),
			X:   rng.Intn(areaSize),
			Y:   rng.Intn(areaSize),
		})
	}

	return riders
}

func produceOrders(ctx context.Context, rng *rand.Rand, e engine.Engine, areaSize int) {
	var nextOrderID int64 = 1

	for {
		if ctx.Err() != nil {
			return
		}

		batchSize := randomIntRange(rng, orderMinBatch, orderMaxBatch)
		orders := make([]model.Order, 0, batchSize)

		for i := 0; i < batchSize; i++ {
			orders = append(orders, model.Order{
				ID: nextOrderID,
				X:  rng.Intn(areaSize),
				Y:  rng.Intn(areaSize),
			})
			nextOrderID++
		}

		err := e.SubmitBatch(ctx, model.OrderBatch{Orders: orders})
		if err != nil {
			return
		}

		waitRandomDuration(ctx, rng, orderMinWait, orderMaxWait)
	}
}

func produceRiderEvents(ctx context.Context, rng *rand.Rand, e engine.Engine, riders []*model.Rider, areaSize int) {
	online := make([]bool, len(riders))
	for i := range online {
		online[i] = true
	}

	for {
		if ctx.Err() != nil {
			return
		}

		event := randomRiderEvent(rng, riders, online, areaSize)
		e.ApplyRiderEvent(event)
		waitRandomDuration(ctx, rng, riderMinWait, riderMaxWait)
	}
}

func randomRiderEvent(rng *rand.Rand, riders []*model.Rider, online []bool, areaSize int) model.RiderEvent {
	onlineCount := countOnline(online)
	eventType := rng.Intn(100)

	if eventType < 70 && onlineCount > 0 {
		index := randomOnlineIndex(rng, online)
		return model.RiderEvent{
			Type: model.RiderMove,
			UID:  riders[index].UID,
			X:    rng.Intn(areaSize),
			Y:    rng.Intn(areaSize),
		}
	}

	if eventType < 85 && onlineCount > 1 {
		index := randomOnlineIndex(rng, online)
		online[index] = false
		return model.RiderEvent{
			Type: model.RiderOffline,
			UID:  riders[index].UID,
		}
	}

	index := randomOfflineIndex(rng, online)
	if index >= 0 {
		online[index] = true
		return model.RiderEvent{
			Type: model.RiderOnline,
			UID:  riders[index].UID,
			X:    rng.Intn(areaSize),
			Y:    rng.Intn(areaSize),
		}
	}

	index = randomOnlineIndex(rng, online)
	return model.RiderEvent{
		Type: model.RiderMove,
		UID:  riders[index].UID,
		X:    rng.Intn(areaSize),
		Y:    rng.Intn(areaSize),
	}
}

func startStatsPrinter(ctx context.Context, start time.Time, e engine.Engine) <-chan struct{} {
	done := make(chan struct{})

	go func() {
		defer close(done)

		ticker := time.NewTicker(statsInterval)
		defer ticker.Stop()

		var lastMatched int64
		lastTime := start

		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				matched := e.Matched()
				recentRate := ratePerSecond(matched-lastMatched, now.Sub(lastTime))

				fmt.Printf(
					"live elapsed=%s online_riders=%d submitted=%d matched=%d missed=%d recent_rate=%.2f orders/s\n",
					now.Sub(start).Truncate(time.Millisecond),
					e.OnlineRiders(),
					e.Submitted(),
					matched,
					e.Missed(),
					recentRate,
				)

				lastMatched = matched
				lastTime = now
			}
		}
	}()

	return done
}

func printFinalStats(start time.Time, riders []*model.Rider, e *engine.ShardedEngine, workerCount int, cellSize int) {
	elapsed := time.Since(start)
	totalOrders := e.Submitted()
	layout := e.Layout()

	fmt.Printf("riders: %d\n", len(riders))
	fmt.Printf("online_riders: %d\n", e.OnlineRiders())
	fmt.Printf("orders: %d\n", totalOrders)
	fmt.Printf("matched: %d\n", e.Matched())
	fmt.Printf("missed: %d\n", e.Missed())
	fmt.Printf("workers: %d\n", workerCount)
	fmt.Printf("shards: %d\n", shardCount)
	fmt.Printf("shard_layout: %dx%d\n", layout.ShardCols(), layout.ShardRows())
	fmt.Printf("cell_size: %d\n", cellSize)
	fmt.Printf("elapsed: %s\n", elapsed)
	fmt.Printf("throughput: %.2f orders/s\n", ratePerSecond(totalOrders, elapsed))
	printBottomRiders(riders, 10)
}

func countOnline(online []bool) int {
	count := 0
	for _, isOnline := range online {
		if isOnline {
			count++
		}
	}
	return count
}

func randomOnlineIndex(rng *rand.Rand, online []bool) int {
	for {
		index := rng.Intn(len(online))
		if online[index] {
			return index
		}
	}
}

func randomOfflineIndex(rng *rand.Rand, online []bool) int {
	offlineIndexes := make([]int, 0)
	for index, isOnline := range online {
		if !isOnline {
			offlineIndexes = append(offlineIndexes, index)
		}
	}

	if len(offlineIndexes) == 0 {
		return -1
	}

	return offlineIndexes[rng.Intn(len(offlineIndexes))]
}

func waitRandomDuration(ctx context.Context, rng *rand.Rand, minValue time.Duration, maxValue time.Duration) {
	wait := randomDurationRange(rng, minValue, maxValue)
	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func randomIntRange(rng *rand.Rand, minValue int, maxValue int) int {
	if maxValue <= minValue {
		return minValue
	}

	return minValue + rng.Intn(maxValue-minValue+1)
}

func randomDurationRange(rng *rand.Rand, minValue time.Duration, maxValue time.Duration) time.Duration {
	if maxValue <= minValue {
		return minValue
	}

	delta := maxValue - minValue
	return minValue + time.Duration(rng.Int63n(int64(delta)+1))
}

func ratePerSecond(count int64, duration time.Duration) float64 {
	if duration <= 0 {
		return 0
	}

	return float64(count) / duration.Seconds()
}

func autoCellSize(areaSize int, riderCount int) int {
	if riderCount <= 0 {
		return areaSize
	}

	const targetRidersPerCell = 20.0
	cell := float64(areaSize) / math.Sqrt(float64(riderCount)/targetRidersPerCell)
	if cell < 1 {
		return 1
	}
	if cell > float64(areaSize) {
		return areaSize
	}

	return int(cell)
}

func printBottomRiders(riders []*model.Rider, count int) {
	sort.Slice(riders, func(i int, j int) bool {
		left := atomic.LoadInt64(&riders[i].Count)
		right := atomic.LoadInt64(&riders[j].Count)
		if left == right {
			return riders[i].UID < riders[j].UID
		}
		return left < right
	})

	fmt.Println("bottom riders:")
	for i := 0; i < count && i < len(riders); i++ {
		fmt.Printf("uid=%d count=%d\n", riders[i].UID, atomic.LoadInt64(&riders[i].Count))
	}
}
