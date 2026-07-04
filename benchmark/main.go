package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"testP/internal/engine"
	"testP/internal/model"
	"time"
)

const (
	areaSize   = 100000
	shardCount = 64
	bufferSize = 128
	loadWeight = 10000
	batchSize  = 5000
)

type Scenario struct {
	Name           string
	RiderCount     int
	OrderCount     int
	TargetDuration time.Duration
}

type Result struct {
	Scenario     Scenario
	Dynamic      bool
	Elapsed      time.Duration
	Matched      int64
	Missed       int64
	OnlineRiders int
	Throughput   float64
	TargetRate   float64
	WithinTarget bool
}

func main() {
	workers := flag.Int("workers", 2, "worker count")
	profile := flag.String("profile", "default", "default, full, or examples")
	seed := flag.Int64("seed", 1, "random seed")
	eventsPerBatch := flag.Int("events-per-batch", 3, "rider events applied per order batch in dynamic mode")
	showExamples := flag.Bool("show-examples", false, "print example benchmark scenarios and exit")
	flag.Parse()

	if *showExamples {
		printExampleScenarios(exampleScenarios())
		return
	}

	if *workers <= 0 {
		*workers = 1
	}
	if *eventsPerBatch < 0 {
		*eventsPerBatch = 0
	}

	runtime.GOMAXPROCS(*workers)

	scenarios := defaultScenarios()
	switch *profile {
	case "examples":
		scenarios = exampleScenarios()
	case "full":
		scenarios = fullScenarios()
	}

	resultFile, err := createResultFile(*profile, *eventsPerBatch)
	if err != nil {
		panic(err)
	}
	defer resultFile.Close()

	header := fmt.Sprintf("workers=%d profile=%s batch=%d events_per_batch=%d engine=sharded", *workers, *profile, batchSize, *eventsPerBatch)
	columns := "scenario,mode,riders,online_riders,orders,matched,missed,elapsed,throughput,target_elapsed,target_throughput,within_target"

	writeLine(resultFile, header)
	writeLine(resultFile, columns)
	fmt.Println(header)
	fmt.Println(columns)

	for index, scenario := range scenarios {
		baseSeed := *seed + int64(index)*1000

		fixed := runScenario(scenario, false, *eventsPerBatch, *workers, baseSeed)
		printResult(resultFile, fixed)

		dynamic := runScenario(scenario, true, *eventsPerBatch, *workers, baseSeed)
		printResult(resultFile, dynamic)
	}

	fmt.Printf("result_file=%s\n", resultFile.Name())
}

func defaultScenarios() []Scenario {
	return []Scenario{
		{Name: "100r_1w_orders", RiderCount: 100, OrderCount: 10000},
		{Name: "1000r_10w_orders", RiderCount: 1000, OrderCount: 100000},
		{Name: "1w_r_100w_orders", RiderCount: 10000, OrderCount: 1000000},
	}
}

func fullScenarios() []Scenario {
	return []Scenario{
		{Name: "100r_1w_orders", RiderCount: 100, OrderCount: 10000},
		{Name: "1000r_10w_orders", RiderCount: 1000, OrderCount: 100000},
		{Name: "1w_r_100w_orders", RiderCount: 10000, OrderCount: 1000000},
		{Name: "10w_r_1000w_orders", RiderCount: 100000, OrderCount: 10000000},
	}
}

func exampleScenarios() []Scenario {
	return []Scenario{
		{Name: "1m_1000r_10w_orders", RiderCount: 1000, OrderCount: 100000, TargetDuration: time.Minute},
		{Name: "3m_1w_r_100w_orders", RiderCount: 10000, OrderCount: 1000000, TargetDuration: 3 * time.Minute},
		{Name: "10m_10w_r_1000w_orders", RiderCount: 100000, OrderCount: 10000000, TargetDuration: 10 * time.Minute},
	}
}

func printExampleScenarios(scenarios []Scenario) {
	fmt.Println("example benchmark scenarios:")
	fmt.Println("scenario,riders,orders,target_elapsed,target_throughput")
	for _, scenario := range scenarios {
		fmt.Printf(
			"%s,%d,%d,%s,%.2f orders/s\n",
			scenario.Name,
			scenario.RiderCount,
			scenario.OrderCount,
			scenario.TargetDuration,
			float64(scenario.OrderCount)/scenario.TargetDuration.Seconds(),
		)
	}
	fmt.Println("run with: go run ./benchmark -profile examples")
}

func runScenario(scenario Scenario, dynamic bool, eventsPerBatch int, workers int, seed int64) Result {
	riderRNG := rand.New(rand.NewSource(seed))
	orderRNG := rand.New(rand.NewSource(seed + 1))
	eventRNG := rand.New(rand.NewSource(seed + 2))

	riders := generateRiders(riderRNG, scenario.RiderCount, areaSize)
	cellSize := autoCellSize(areaSize, scenario.RiderCount)
	e := newEngine(riders, cellSize)

	e.Start(workers)

	start := time.Now()
	produceOrders(context.Background(), orderRNG, eventRNG, e, riders, scenario.OrderCount, scenario.TargetDuration, dynamic, eventsPerBatch)
	e.Close()
	e.Wait()
	elapsed := time.Since(start)

	result := Result{
		Scenario:     scenario,
		Dynamic:      dynamic,
		Elapsed:      elapsed,
		Matched:      e.Matched(),
		Missed:       e.Missed(),
		OnlineRiders: e.OnlineRiders(),
		Throughput:   float64(scenario.OrderCount) / elapsed.Seconds(),
	}

	if scenario.TargetDuration > 0 {
		result.TargetRate = float64(scenario.OrderCount) / scenario.TargetDuration.Seconds()
		result.WithinTarget = elapsed <= scenario.TargetDuration
	}

	return result
}

func newEngine(riders []*model.Rider, cellSize int) engine.Engine {
	return engine.NewShardedEngine(
		riders,
		shardCount,
		bufferSize,
		cellSize,
		areaSize,
		loadWeight,
	)
}

func produceOrders(
	ctx context.Context,
	orderRNG *rand.Rand,
	eventRNG *rand.Rand,
	e engine.Engine,
	riders []*model.Rider,
	totalOrders int,
	targetDuration time.Duration,
	dynamic bool,
	eventsPerBatch int,
) {
	online := make([]bool, len(riders))
	for i := range online {
		online[i] = true
	}

	start := time.Now()
	for generated := 0; generated < totalOrders; {
		size := batchSize
		if remaining := totalOrders - generated; remaining < size {
			size = remaining
		}

		batch := makeOrderBatch(orderRNG, generated+1, size, areaSize)
		err := e.SubmitBatch(ctx, batch)
		if err != nil {
			return
		}

		if dynamic {
			applyRiderEvents(eventRNG, e, riders, online, eventsPerBatch, areaSize)
		}

		generated += size
		waitForTargetRate(ctx, start, generated, totalOrders, targetDuration)
	}
}

func waitForTargetRate(ctx context.Context, start time.Time, generated int, totalOrders int, targetDuration time.Duration) {
	if targetDuration <= 0 || totalOrders <= 0 {
		return
	}

	progress := float64(generated) / float64(totalOrders)
	targetElapsed := time.Duration(progress * float64(targetDuration))
	targetTime := start.Add(targetElapsed)
	wait := time.Until(targetTime)
	if wait <= 0 {
		return
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func makeOrderBatch(rng *rand.Rand, startID int, size int, areaSize int) model.OrderBatch {
	orders := make([]model.Order, 0, size)

	for i := 0; i < size; i++ {
		orders = append(orders, model.Order{
			ID: int64(startID + i),
			X:  rng.Intn(areaSize),
			Y:  rng.Intn(areaSize),
		})
	}

	return model.OrderBatch{Orders: orders}
}

func applyRiderEvents(
	rng *rand.Rand,
	e engine.Engine,
	riders []*model.Rider,
	online []bool,
	count int,
	areaSize int,
) {
	for i := 0; i < count; i++ {
		event := randomRiderEvent(rng, riders, online, areaSize)
		e.ApplyRiderEvent(event)
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

func createResultFile(profile string, eventsPerBatch int) (*os.File, error) {
	resultDir := filepath.Join("benchmark", "result")
	if err := os.MkdirAll(resultDir, 0755); err != nil {
		return nil, err
	}

	fileName := fmt.Sprintf(
		"benchmark_%s_%s_events%d_%s.csv",
		profile,
		"sharded",
		eventsPerBatch,
		time.Now().Format("20060102_150405"),
	)

	return os.Create(filepath.Join(resultDir, fileName))
}

func writeLine(file *os.File, line string) {
	fmt.Fprintln(file, line)
}

func printResult(file *os.File, result Result) {
	mode := "fixed"
	if result.Dynamic {
		mode = "dynamic"
	}

	line := fmt.Sprintf(
		"%s,%s,%d,%d,%d,%d,%d,%s,%.2f,%s,%s,%s\n",
		result.Scenario.Name,
		mode,
		result.Scenario.RiderCount,
		result.OnlineRiders,
		result.Scenario.OrderCount,
		result.Matched,
		result.Missed,
		result.Elapsed,
		result.Throughput,
		formatTargetDuration(result.Scenario.TargetDuration),
		formatTargetRate(result.TargetRate),
		formatWithinTarget(result.Scenario.TargetDuration, result.WithinTarget),
	)

	fmt.Print(line)
	fmt.Fprint(file, line)
}

func formatTargetDuration(duration time.Duration) string {
	if duration <= 0 {
		return ""
	}
	return duration.String()
}

func formatTargetRate(rate float64) string {
	if rate <= 0 {
		return ""
	}
	return fmt.Sprintf("%.2f", rate)
}

func formatWithinTarget(targetDuration time.Duration, withinTarget bool) string {
	if targetDuration <= 0 {
		return ""
	}
	if withinTarget {
		return "true"
	}
	return "false"
}
