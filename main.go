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
	"testP/internal/matcher"
	"testP/internal/model"
	"testP/internal/scheduler"
	"time"
)

func main() {
	riderCount := flag.Int("riders", 100, "number of riders")
	orderCount := flag.Int("orders", 10000, "number of orders")
	batchSize := flag.Int("batch", 1000, "orders generated per batch")
	workerCount := flag.Int("workers", 2, "matcher worker count")
	shardCount := flag.Int("shards", 64, "spatial shard count")
	areaSize := flag.Int("area", 100000, "coordinate range: [0, area)")
	cellSize := flag.Int("cell", 0, "grid cell size; 0 means auto")
	bufferSize := flag.Int("buffer", 128, "channel buffer size for batches")
	loadWeight := flag.Int64("load-weight", 10000, "assigned-order penalty in score")
	durationText := flag.String("duration", "0s", "input duration, such as 30s or 1m; 0s means burst mode")
	continuous := flag.Bool("continuous", false, "keep generating random orders until Ctrl+C or -run-for expires")
	runForText := flag.String("run-for", "0s", "continuous mode runtime; 0s means until Ctrl+C")
	minBatchSize := flag.Int("min-batch", 1, "continuous mode minimum batch size")
	maxBatchSize := flag.Int("max-batch", 0, "continuous mode maximum batch size; 0 means -batch")
	minIntervalText := flag.String("min-interval", "10ms", "continuous mode minimum wait between batches")
	maxIntervalText := flag.String("max-interval", "100ms", "continuous mode maximum wait between batches")
	statsIntervalText := flag.String("stats-interval", "1s", "continuous mode stats print interval")
	seed := flag.Int64("seed", 1, "random seed")
	flag.Parse()

	if *batchSize <= 0 {
		*batchSize = 1
	}
	if *areaSize <= 0 {
		*areaSize = 100000
	}
	if *cellSize <= 0 {
		*cellSize = autoCellSize(*areaSize, *riderCount)
	}
	if *workerCount <= 0 {
		*workerCount = 1
	}

	duration, err := time.ParseDuration(*durationText)
	if err != nil {
		panic(err)
	}
	runFor, err := time.ParseDuration(*runForText)
	if err != nil {
		panic(err)
	}
	minInterval, err := time.ParseDuration(*minIntervalText)
	if err != nil {
		panic(err)
	}
	maxInterval, err := time.ParseDuration(*maxIntervalText)
	if err != nil {
		panic(err)
	}
	statsInterval, err := time.ParseDuration(*statsIntervalText)
	if err != nil {
		panic(err)
	}
	if *maxBatchSize <= 0 {
		*maxBatchSize = *batchSize
	}
	if *minBatchSize <= 0 {
		*minBatchSize = 1
	}
	if *maxBatchSize < *minBatchSize {
		*maxBatchSize = *minBatchSize
	}
	if minInterval < 0 {
		minInterval = 0
	}
	if maxInterval < minInterval {
		maxInterval = minInterval
	}

	runtime.GOMAXPROCS(*workerCount)

	rng := rand.New(rand.NewSource(*seed))
	riders := generateRiders(rng, *riderCount, *areaSize)
	m := matcher.NewMatcher(riders, *cellSize, *loadWeight)
	s := scheduler.NewScheduler(*shardCount, *bufferSize, *cellSize)
	pool := scheduler.NewWorkerPool(s.Shards(), m)

	s.Start()
	pool.Start(*workerCount)

	start := time.Now()

	if *continuous {
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		if runFor > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, runFor)
			defer cancel()
		}

		done := make(chan struct{})
		statsStopped := make(chan struct{})
		go func() {
			defer close(statsStopped)
			printLiveStats(ctx, done, start, statsInterval, s, m)
		}()

		produceContinuousOrders(ctx, rng, s, *minBatchSize, *maxBatchSize, *areaSize, minInterval, maxInterval)
		close(done)
		<-statsStopped
	} else {
		produceOrders(rng, s, *orderCount, *batchSize, *areaSize, duration)
	}

	s.Close()
	pool.Wait()
	elapsed := time.Since(start)
	totalOrders := int64(*orderCount)
	if *continuous {
		totalOrders = s.Submitted()
	}

	fmt.Printf("riders: %d\n", *riderCount)
	fmt.Printf("orders: %d\n", totalOrders)
	fmt.Printf("matched: %d\n", m.Matched())
	fmt.Printf("missed: %d\n", m.Missed())
	fmt.Printf("workers: %d\n", *workerCount)
	fmt.Printf("shards: %d\n", *shardCount)
	fmt.Printf("cell_size: %d\n", *cellSize)
	fmt.Printf("submitted: %d\n", s.Submitted())
	fmt.Printf("dispatched: %d\n", s.Dispatched())
	fmt.Printf("elapsed: %s\n", elapsed)
	fmt.Printf("throughput: %.2f orders/s\n", float64(totalOrders)/elapsed.Seconds())
	printBottomRiders(riders, 10)
}

func generateRiders(rng *rand.Rand, n, areaSize int) []*model.Rider {
	riders := make([]*model.Rider, 0, n)

	for i := 0; i < n; i++ {
		riders = append(riders, &model.Rider{
			UID: int64(i + 1),
			X:   rng.Intn(areaSize),
			Y:   rng.Intn(areaSize),
		})
	}

	return riders
}

func produceOrders(rng *rand.Rand, s *scheduler.Scheduler, total, batchSize, areaSize int, duration time.Duration) {
	batchCount := (total + batchSize - 1) / batchSize
	var interval time.Duration

	if duration > 0 && batchCount > 0 {
		interval = duration / time.Duration(batchCount)
	}

	nextSend := time.Now()

	for generated := 0; generated < total; {
		size := batchSize
		if remaining := total - generated; remaining < size {
			size = remaining
		}

		orders := make([]model.Order, 0, size)
		for i := 0; i < size; i++ {
			orders = append(orders, model.Order{
				ID: int64(generated + i + 1),
				X:  rng.Intn(areaSize),
				Y:  rng.Intn(areaSize),
			})
		}

		s.SubmitBatch(model.OrderBatch{Orders: orders})
		generated += size

		if interval > 0 && generated < total {
			nextSend = nextSend.Add(interval)
			if sleepFor := time.Until(nextSend); sleepFor > 0 {
				time.Sleep(sleepFor)
			}
		}
	}
}

func produceContinuousOrders(
	ctx context.Context,
	rng *rand.Rand,
	s *scheduler.Scheduler,
	minBatchSize int,
	maxBatchSize int,
	areaSize int,
	minInterval time.Duration,
	maxInterval time.Duration,
) {
	var nextOrderID int64 = 1

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		size := randomIntRange(rng, minBatchSize, maxBatchSize)
		orders := make([]model.Order, 0, size)
		for i := 0; i < size; i++ {
			orders = append(orders, model.Order{
				ID: nextOrderID,
				X:  rng.Intn(areaSize),
				Y:  rng.Intn(areaSize),
			})
			nextOrderID++
		}

		if err := s.SubmitBatchContext(ctx, model.OrderBatch{Orders: orders}); err != nil {
			return
		}

		wait := randomDurationRange(rng, minInterval, maxInterval)
		if wait <= 0 {
			continue
		}

		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func printLiveStats(
	ctx context.Context,
	done <-chan struct{},
	start time.Time,
	interval time.Duration,
	s *scheduler.Scheduler,
	m *matcher.Matcher,
) {
	if interval <= 0 {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var lastMatched int64
	var lastTime = start

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case now := <-ticker.C:
			matched := m.Matched()
			delta := matched - lastMatched
			seconds := now.Sub(lastTime).Seconds()
			rate := float64(0)
			if seconds > 0 {
				rate = float64(delta) / seconds
			}

			fmt.Printf(
				"live elapsed=%s submitted=%d dispatched=%d matched=%d missed=%d recent_rate=%.2f orders/s\n",
				now.Sub(start).Truncate(time.Millisecond),
				s.Submitted(),
				s.Dispatched(),
				matched,
				m.Missed(),
				rate,
			)

			lastMatched = matched
			lastTime = now
		}
	}
}

func randomIntRange(rng *rand.Rand, minValue, maxValue int) int {
	if maxValue <= minValue {
		return minValue
	}

	return minValue + rng.Intn(maxValue-minValue+1)
}

func randomDurationRange(rng *rand.Rand, minValue, maxValue time.Duration) time.Duration {
	if maxValue <= minValue {
		return minValue
	}

	delta := maxValue - minValue
	return minValue + time.Duration(rng.Int63n(int64(delta)+1))
}

func autoCellSize(areaSize, riderCount int) int {
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

func printBottomRiders(riders []*model.Rider, n int) {
	sort.Slice(riders, func(i, j int) bool {
		left := atomic.LoadInt64(&riders[i].Count)
		right := atomic.LoadInt64(&riders[j].Count)
		if left == right {
			return riders[i].UID < riders[j].UID
		}
		return left < right
	})

	fmt.Println("bottom riders:")
	for i := 0; i < n && i < len(riders); i++ {
		fmt.Printf("uid=%d count=%d\n", riders[i].UID, atomic.LoadInt64(&riders[i].Count))
	}
}
