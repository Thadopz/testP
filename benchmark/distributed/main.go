package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"testP/internal/checkpoint"
	clusterownership "testP/internal/cluster/ownership"
	"testP/internal/eventlog"
	"testP/internal/model"
	"testP/internal/nodeapp"
	"testP/internal/orderstate"
	"testP/internal/producerapp"
	"testP/internal/tools"

	"github.com/segmentio/kafka-go"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	defaultShardCount = 64
)

type Scenario struct {
	Name   string
	Nodes  int
	Riders int
	Orders int
}

type Result struct {
	Scenario       Scenario
	ProduceElapsed time.Duration
	ReplayElapsed  time.Duration
	TotalElapsed   time.Duration
	Submitted      int64
	Matched        int64
	Missed         int64
	Throughput     float64
	MaxLag         int64
	Stats          statsSnapshot
}

func main() {
	profile := flag.String("profile", "smoke", "smoke or default")
	backend := flag.String("backend", "local", "local or middleware")
	nodes := flag.Int("nodes", 0, "override node count; 0 uses scenario value")
	riders := flag.Int("riders", 0, "override rider count; 0 uses scenario value")
	orders := flag.Int("orders", 0, "override order count; 0 uses scenario value")
	workers := flag.Int("workers", 2, "workers per node")
	shards := flag.Int("shards", defaultShardCount, "shard count")
	seed := flag.Int64("seed", 1, "random seed")
	dataDir := flag.String("data-dir", "", "data directory; empty uses a temp directory")
	keepData := flag.Bool("keep-data", false, "keep generated data directory")
	timeout := flag.Duration("timeout", 30*time.Second, "benchmark timeout")
	pollInterval := flag.Duration("poll-interval", 10*time.Millisecond, "file event log tail poll interval")
	catchupInterval := flag.Duration("catchup-interval", 200*time.Millisecond, "checkpoint catch-up polling interval")
	kafkaBrokers := flag.String("kafka-brokers", "127.0.0.1:9092", "comma-separated Kafka broker addresses for middleware backend")
	kafkaTopicPrefix := flag.String("kafka-topic-prefix", "testp-bench", "Kafka topic prefix for middleware backend")
	kafkaMaxWait := flag.Duration("kafka-max-wait", 10*time.Millisecond, "Kafka reader max wait for middleware backend")
	deleteKafkaTopic := flag.Bool("delete-kafka-topic", true, "delete generated Kafka topic after each middleware scenario")
	etcdEndpoints := flag.String("etcd-endpoints", "127.0.0.1:2379", "comma-separated etcd endpoints for middleware backend")
	etcdPrefix := flag.String("etcd-prefix", "/testp-bench", "etcd prefix base for middleware backend")
	flag.Parse()

	if *workers <= 0 {
		*workers = 1
	}
	if *shards <= 0 {
		*shards = defaultShardCount
	}

	scenarios, err := scenariosForProfile(*profile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid profile: %v\n", err)
		os.Exit(2)
	}
	scenarios = applyOverrides(scenarios, *nodes, *riders, *orders)

	resultFile, err := createResultFile(*profile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create result file: %v\n", err)
		os.Exit(1)
	}
	defer resultFile.Close()

	header := fmt.Sprintf(
		"profile=%s backend=%s workers=%d shards=%d seed=%d",
		*profile,
		*backend,
		*workers,
		*shards,
		*seed,
	)
	columns := strings.Join([]string{
		"scenario",
		"nodes",
		"shards",
		"riders",
		"orders",
		"produce_elapsed",
		"replay_elapsed",
		"total_elapsed",
		"submitted",
		"matched",
		"missed",
		"throughput",
		"max_lag",
		"eventlog_append_count",
		"eventlog_append_elapsed",
		"eventlog_tail_count",
		"eventlog_tail_setup_elapsed",
		"eventlog_record_count",
		"eventlog_record_wait_elapsed",
		"eventlog_endoffset_count",
		"eventlog_endoffset_elapsed",
		"checkpoint_save_count",
		"checkpoint_save_elapsed",
		"checkpoint_load_count",
		"checkpoint_load_elapsed",
		"order_load_count",
		"order_load_elapsed",
		"order_save_count",
		"order_save_elapsed",
		"catchup_polls",
		"catchup_check_elapsed",
	}, ",")
	writeLine(resultFile, header)
	writeLine(resultFile, columns)
	fmt.Println(header)
	fmt.Println(columns)

	for index, scenario := range scenarios {
		runDir, cleanup, err := prepareDataDir(*dataDir, scenario.Name, *keepData)
		if err != nil {
			fmt.Fprintf(os.Stderr, "prepare data dir: %v\n", err)
			os.Exit(1)
		}

		result, err := runScenario(context.Background(), scenario, Config{
			Backend:          *backend,
			Workers:          *workers,
			Shards:           *shards,
			Seed:             *seed + int64(index)*1000,
			DataDir:          runDir,
			Timeout:          *timeout,
			PollInterval:     *pollInterval,
			CatchupInterval:  *catchupInterval,
			KafkaBrokers:     parseCommaSeparated(*kafkaBrokers),
			KafkaTopicPrefix: *kafkaTopicPrefix,
			KafkaMaxWait:     *kafkaMaxWait,
			DeleteKafkaTopic: *deleteKafkaTopic,
			EtcdEndpoints:    parseCommaSeparated(*etcdEndpoints),
			EtcdPrefix:       *etcdPrefix,
		})
		cleanup()
		if err != nil {
			fmt.Fprintf(os.Stderr, "scenario %s failed: %v\n", scenario.Name, err)
			os.Exit(1)
		}
		printResult(resultFile, result, *shards)
	}

	fmt.Printf("result_file=%s\n", resultFile.Name())
}

type Config struct {
	Backend          string
	Workers          int
	Shards           int
	Seed             int64
	DataDir          string
	Timeout          time.Duration
	PollInterval     time.Duration
	CatchupInterval  time.Duration
	KafkaBrokers     []string
	KafkaTopicPrefix string
	KafkaMaxWait     time.Duration
	DeleteKafkaTopic bool
	EtcdEndpoints    []string
	EtcdPrefix       string
}

type benchmarkRuntime struct {
	eventLog        eventlog.EventLog
	checkpointStore checkpoint.ShardStore
	orderStateStore orderstate.Store
	ownershipStore  clusterownership.OwnershipStore
	cleanup         func()
	description     string
	stats           *benchmarkStats
}

func runScenario(ctx context.Context, scenario Scenario, cfg Config) (Result, error) {
	if scenario.Nodes <= 0 {
		return Result{}, fmt.Errorf("nodes must be > 0")
	}
	if scenario.Riders <= 0 {
		return Result{}, fmt.Errorf("riders must be > 0")
	}
	if scenario.Orders < 0 {
		return Result{}, fmt.Errorf("orders must be >= 0")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	runtime, err := newBenchmarkRuntime(ctx, scenario, cfg)
	if err != nil {
		return Result{}, err
	}
	defer runtime.cleanup()

	start := time.Now()
	produceStart := time.Now()
	if _, err := producerapp.Run(ctx, producerapp.Config{
		DataDir:  cfg.DataDir,
		EventLog: runtime.eventLog,
		Orders:   scenario.Orders,
		Seed:     cfg.Seed,
		Shards:   cfg.Shards,
	}); err != nil {
		return Result{}, err
	}
	produceElapsed := time.Since(produceStart)

	replayStart := time.Now()
	nodeCtx, stopNodes := context.WithCancel(ctx)
	nodeResults, err := runNodesUntilCaughtUp(
		nodeCtx,
		stopNodes,
		scenario,
		cfg,
		runtime.eventLog,
		runtime.checkpointStore,
		runtime.orderStateStore,
		runtime.ownershipStore,
		runtime.stats,
		cfg.CatchupInterval,
	)
	if err != nil {
		return Result{}, err
	}
	replayElapsed := time.Since(replayStart)
	totalElapsed := time.Since(start)

	submitted, matched, missed := sumNodeResults(nodeResults)
	maxLag, err := maxShardLag(context.Background(), runtime.eventLog, runtime.checkpointStore, cfg.Shards)
	if err != nil {
		return Result{}, err
	}

	throughput := 0.0
	if replayElapsed > 0 {
		throughput = float64(scenario.Orders) / replayElapsed.Seconds()
	}

	return Result{
		Scenario:       scenario,
		ProduceElapsed: produceElapsed,
		ReplayElapsed:  replayElapsed,
		TotalElapsed:   totalElapsed,
		Submitted:      submitted,
		Matched:        matched,
		Missed:         missed,
		Throughput:     throughput,
		MaxLag:         maxLag,
		Stats:          runtime.stats.snapshot(),
	}, nil
}

func newBenchmarkRuntime(ctx context.Context, scenario Scenario, cfg Config) (benchmarkRuntime, error) {
	switch cfg.Backend {
	case "", "local":
		return newLocalRuntime(scenario, cfg)
	case "middleware":
		return newMiddlewareRuntime(ctx, scenario, cfg)
	default:
		return benchmarkRuntime{}, fmt.Errorf("unknown backend %q", cfg.Backend)
	}
}

func newLocalRuntime(scenario Scenario, cfg Config) (benchmarkRuntime, error) {
	stats := &benchmarkStats{}
	codec := &eventlog.JSONEventCodec{}
	fileEventLog := eventlog.NewFileEventLog(filepath.Join(cfg.DataDir, "events"), codec)
	fileEventLog.SetPollInterval(cfg.PollInterval)

	ownershipStore := newBenchmarkOwnershipStore()
	if err := assignModuloOwnership(ownershipStore, scenario.Nodes, cfg.Shards); err != nil {
		return benchmarkRuntime{}, err
	}

	return benchmarkRuntime{
		eventLog:        &instrumentedEventLog{inner: fileEventLog, stats: stats},
		checkpointStore: &instrumentedCheckpointStore{inner: checkpoint.NewMemoryStore(), stats: stats},
		orderStateStore: &instrumentedOrderStateStore{inner: orderstate.NewMemoryStore(), stats: stats},
		ownershipStore:  ownershipStore,
		cleanup:         func() {},
		description:     "eventlog=file checkpoint=memory orderstate=memory ownership=memory",
		stats:           stats,
	}, nil
}

func newMiddlewareRuntime(ctx context.Context, scenario Scenario, cfg Config) (benchmarkRuntime, error) {
	stats := &benchmarkStats{}
	if len(cfg.KafkaBrokers) == 0 {
		return benchmarkRuntime{}, fmt.Errorf("kafka brokers must not be empty")
	}
	if len(cfg.EtcdEndpoints) == 0 {
		return benchmarkRuntime{}, fmt.Errorf("etcd endpoints must not be empty")
	}

	topic := benchmarkTopicName(cfg.KafkaTopicPrefix, scenario.Name)
	prefix := benchmarkEtcdPrefix(cfg.EtcdPrefix, scenario.Name)

	if err := createKafkaTopic(ctx, cfg.KafkaBrokers[0], topic, cfg.Shards); err != nil {
		return benchmarkRuntime{}, err
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		if cfg.DeleteKafkaTopic {
			_ = deleteKafkaTopicByName(context.Background(), cfg.KafkaBrokers[0], topic)
		}
		return benchmarkRuntime{}, fmt.Errorf("connect etcd: %w", err)
	}

	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.Delete(cleanupCtx, prefix, clientv3.WithPrefix())
		_ = client.Close()
		if cfg.DeleteKafkaTopic {
			_ = deleteKafkaTopicByName(cleanupCtx, cfg.KafkaBrokers[0], topic)
		}
	}

	if _, err := client.Delete(ctx, prefix, clientv3.WithPrefix()); err != nil {
		cleanup()
		return benchmarkRuntime{}, fmt.Errorf("clean etcd prefix: %w", err)
	}

	ownershipStore := clusterownership.NewEtcdOwnershipStore(client, prefix)
	if err := assignModuloOwnership(ownershipStore, scenario.Nodes, cfg.Shards); err != nil {
		cleanup()
		return benchmarkRuntime{}, err
	}

	activeEventLog, err := eventlog.NewKafkaEventLog(eventlog.KafkaConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   topic,
		Codec:   &eventlog.JSONEventCodec{},
		MaxWait: cfg.KafkaMaxWait,
	})
	if err != nil {
		cleanup()
		return benchmarkRuntime{}, err
	}
	cleanupWithEventLog := func() {
		_ = activeEventLog.Close()
		cleanup()
	}

	return benchmarkRuntime{
		eventLog:        &instrumentedEventLog{inner: activeEventLog, stats: stats},
		checkpointStore: &instrumentedCheckpointStore{inner: checkpoint.NewEtcdStore(client, prefix), stats: stats},
		orderStateStore: &instrumentedOrderStateStore{inner: orderstate.NewEtcdStore(client, prefix), stats: stats},
		ownershipStore:  ownershipStore,
		cleanup:         cleanupWithEventLog,
		description:     fmt.Sprintf("eventlog=kafka topic=%s checkpoint=etcd orderstate=etcd ownership=etcd prefix=%s", topic, prefix),
		stats:           stats,
	}, nil
}

func runNodesUntilCaughtUp(
	ctx context.Context,
	stopNodes context.CancelFunc,
	scenario Scenario,
	cfg Config,
	eventLog eventlog.EventLog,
	checkpointStore checkpoint.ShardStore,
	orderStateStore orderstate.Store,
	ownershipStore clusterownership.ShardProvider,
	stats *benchmarkStats,
	catchupInterval time.Duration,
) ([]nodeapp.Result, error) {
	defer stopNodes()

	resultCh := make(chan nodeRunResult, scenario.Nodes)
	var wg sync.WaitGroup
	for nodeID := 1; nodeID <= scenario.Nodes; nodeID++ {
		nodeID := nodeID
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := nodeapp.RunWithResult(ctx, nodeapp.Config{
				NodeID:          nodeID,
				ShardProvider:   ownershipStore,
				DataDir:         cfg.DataDir,
				EventLog:        eventLog,
				CheckpointStore: checkpointStore,
				OrderStateStore: orderStateStore,
				Riders:          scenario.Riders,
				Workers:         cfg.Workers,
				Seed:            cfg.Seed,
				RefreshInterval: time.Hour,
			})
			resultCh <- nodeRunResult{result: result, err: err}
		}()
	}

	caughtUpCh := make(chan error, 1)
	go func() {
		caughtUpCh <- waitUntilCaughtUp(ctx, eventLog, checkpointStore, cfg.Shards, stats, catchupInterval)
	}()

	select {
	case err := <-caughtUpCh:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	stopNodes()
	wg.Wait()
	close(resultCh)

	results := make([]nodeapp.Result, 0, scenario.Nodes)
	for nodeResult := range resultCh {
		if nodeResult.err != nil && !errors.Is(nodeResult.err, context.Canceled) {
			return nil, nodeResult.err
		}
		results = append(results, nodeResult.result)
	}
	return results, nil
}

type nodeRunResult struct {
	result nodeapp.Result
	err    error
}

func waitUntilCaughtUp(
	ctx context.Context,
	eventLog eventlog.OffsetReader,
	checkpointStore checkpoint.ShardStore,
	shardCount int,
	stats *benchmarkStats,
	catchupInterval time.Duration,
) error {
	if catchupInterval <= 0 {
		catchupInterval = 200 * time.Millisecond
	}
	if stats != nil {
		return waitUntilAppendedEventsApplied(ctx, checkpointStore, shardCount, stats, catchupInterval)
	}

	ticker := time.NewTicker(catchupInterval)
	defer ticker.Stop()

	const requiredStablePolls = 3
	lastEndOffset := int64(-1)
	stablePolls := 0

	for {
		checkStart := time.Now()
		progress, err := shardProgress(ctx, eventLog, checkpointStore, shardCount)
		if stats != nil {
			stats.recordCatchupCheck(time.Since(checkStart))
		}
		if err != nil {
			return err
		}
		if progress.CaughtUp && progress.EndOffset == lastEndOffset {
			stablePolls++
		} else {
			stablePolls = 0
			lastEndOffset = progress.EndOffset
		}
		if stablePolls >= requiredStablePolls {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitUntilAppendedEventsApplied(
	ctx context.Context,
	checkpointStore checkpoint.ShardStore,
	shardCount int,
	stats *benchmarkStats,
	catchupInterval time.Duration,
) error {
	if catchupInterval <= 0 {
		catchupInterval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(catchupInterval)
	defer ticker.Stop()

	const requiredStablePolls = 3
	lastAppendCount := int64(-1)
	stablePolls := 0

	for {
		checkStart := time.Now()
		appendTargets, appendCount := stats.appendTargets()
		targetsReached, err := checkpointTargetsReached(ctx, checkpointStore, appendTargets)
		stats.recordCatchupCheck(time.Since(checkStart))
		if err != nil {
			return err
		}

		if targetsReached && appendCount == lastAppendCount {
			stablePolls++
		} else {
			stablePolls = 0
			lastAppendCount = appendCount
		}
		if stablePolls >= requiredStablePolls {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func checkpointTargetsReached(
	ctx context.Context,
	checkpointStore checkpoint.ShardStore,
	appendTargets map[int]int64,
) (bool, error) {
	for shardID, targetOffset := range appendTargets {
		loaded, found, err := checkpointStore.LoadShardCheckpoint(ctx, shardID)
		if err != nil {
			return false, err
		}
		if !found || loaded.Offset < targetOffset {
			return false, nil
		}
	}
	return true, nil
}

type progressSnapshot struct {
	EndOffset        int64
	CheckpointOffset int64
	CaughtUp         bool
}

func shardProgress(
	ctx context.Context,
	eventLog eventlog.OffsetReader,
	checkpointStore checkpoint.ShardStore,
	shardCount int,
) (progressSnapshot, error) {
	progress := progressSnapshot{
		CaughtUp: true,
	}

	for shardID := 0; shardID < shardCount; shardID++ {
		endOffset, err := eventLog.EndOffset(ctx, shardID)
		if err != nil {
			return progressSnapshot{}, err
		}
		progress.EndOffset += endOffset

		checkpointOffset := int64(0)
		loaded, found, err := checkpointStore.LoadShardCheckpoint(ctx, shardID)
		if err != nil {
			return progressSnapshot{}, err
		}
		if found {
			checkpointOffset = loaded.Offset
		}
		progress.CheckpointOffset += checkpointOffset
		if checkpointOffset < endOffset {
			progress.CaughtUp = false
		}
	}
	return progress, nil
}

func sumNodeResults(results []nodeapp.Result) (int64, int64, int64) {
	var submitted int64
	var matched int64
	var missed int64
	for _, result := range results {
		submitted += result.Submitted
		matched += result.Matched
		missed += result.Missed
	}
	return submitted, matched, missed
}

func maxShardLag(
	ctx context.Context,
	eventLog eventlog.OffsetReader,
	checkpointStore checkpoint.ShardStore,
	shardCount int,
) (int64, error) {
	var maxLag int64
	for shardID := 0; shardID < shardCount; shardID++ {
		endOffset, err := eventLog.EndOffset(ctx, shardID)
		if err != nil {
			return 0, err
		}

		checkpointOffset := int64(0)
		loaded, found, err := checkpointStore.LoadShardCheckpoint(ctx, shardID)
		if err != nil {
			return 0, err
		}
		if found {
			checkpointOffset = loaded.Offset
		}

		lag := endOffset - checkpointOffset
		if lag > maxLag {
			maxLag = lag
		}
	}
	return maxLag, nil
}

func scenariosForProfile(profile string) ([]Scenario, error) {
	switch profile {
	case "smoke":
		return []Scenario{
			{Name: "2n_100r_1k_orders", Nodes: 2, Riders: 100, Orders: 1000},
		}, nil
	case "default":
		return []Scenario{
			{Name: "1n_100r_1k_orders", Nodes: 1, Riders: 100, Orders: 1000},
			{Name: "2n_100r_1k_orders", Nodes: 2, Riders: 100, Orders: 1000},
			{Name: "4n_1000r_1w_orders", Nodes: 4, Riders: 1000, Orders: 10000},
		}, nil
	default:
		return nil, fmt.Errorf("unknown profile %q", profile)
	}
}

func applyOverrides(scenarios []Scenario, nodes int, riders int, orders int) []Scenario {
	overridden := make([]Scenario, len(scenarios))
	copy(overridden, scenarios)

	for i := range overridden {
		if nodes > 0 {
			overridden[i].Nodes = nodes
		}
		if riders > 0 {
			overridden[i].Riders = riders
		}
		if orders > 0 {
			overridden[i].Orders = orders
		}
	}
	return overridden
}

func createResultFile(profile string) (*os.File, error) {
	resultDir := filepath.Join("benchmark", "result")
	if err := os.MkdirAll(resultDir, 0755); err != nil {
		return nil, err
	}

	fileName := fmt.Sprintf(
		"distributed_%s_%s.csv",
		profile,
		time.Now().Format("20060102_150405"),
	)
	return os.Create(filepath.Join(resultDir, fileName))
}

func prepareDataDir(baseDir string, scenarioName string, keep bool) (string, func(), error) {
	if baseDir == "" {
		dir, err := os.MkdirTemp("", "testp-distributed-benchmark-*")
		if err != nil {
			return "", func() {}, err
		}
		return dir, func() {
			if !keep {
				_ = os.RemoveAll(dir)
			}
		}, nil
	}

	dir := filepath.Join(baseDir, scenarioName)
	if err := os.RemoveAll(dir); err != nil {
		return "", func() {}, err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", func() {}, err
	}
	return dir, func() {}, nil
}

func printResult(file *os.File, result Result, shardCount int) {
	stats := result.Stats
	line := fmt.Sprintf(
		"%s,%d,%d,%d,%d,%s,%s,%s,%d,%d,%d,%.2f,%d,%d,%s,%d,%s,%d,%s,%d,%s,%d,%s,%d,%s,%d,%s,%d,%s,%d,%s",
		result.Scenario.Name,
		result.Scenario.Nodes,
		shardCount,
		result.Scenario.Riders,
		result.Scenario.Orders,
		result.ProduceElapsed,
		result.ReplayElapsed,
		result.TotalElapsed,
		result.Submitted,
		result.Matched,
		result.Missed,
		result.Throughput,
		result.MaxLag,
		stats.EventLogAppend.Count,
		stats.EventLogAppend.Elapsed,
		stats.EventLogTail.Count,
		stats.EventLogTail.Elapsed,
		stats.EventLogRecord.Count,
		stats.EventLogRecord.Elapsed,
		stats.EventLogEndOffset.Count,
		stats.EventLogEndOffset.Elapsed,
		stats.CheckpointSave.Count,
		stats.CheckpointSave.Elapsed,
		stats.CheckpointLoad.Count,
		stats.CheckpointLoad.Elapsed,
		stats.OrderLoad.Count,
		stats.OrderLoad.Elapsed,
		stats.OrderSave.Count,
		stats.OrderSave.Elapsed,
		stats.CatchupPolls,
		stats.CatchupCheckElapsed,
	)
	fmt.Println(line)
	writeLine(file, line)
}

func writeLine(file *os.File, line string) {
	fmt.Fprintln(file, line)
}

type opStats struct {
	count        atomic.Int64
	elapsedNanos atomic.Int64
}

func (s *opStats) record(elapsed time.Duration) {
	s.count.Add(1)
	s.elapsedNanos.Add(int64(elapsed))
}

func (s *opStats) snapshot() opStatsSnapshot {
	return opStatsSnapshot{
		Count:   s.count.Load(),
		Elapsed: time.Duration(s.elapsedNanos.Load()),
	}
}

type opStatsSnapshot struct {
	Count   int64
	Elapsed time.Duration
}

type benchmarkStats struct {
	eventLogAppend           opStats
	eventLogTail             opStats
	eventLogRecord           opStats
	eventLogEndOffset        opStats
	checkpointSave           opStats
	checkpointLoad           opStats
	orderLoad                opStats
	orderSave                opStats
	catchupPolls             atomic.Int64
	catchupCheckElapsedNanos atomic.Int64
	appendMu                 sync.Mutex
	appendCountByShard       map[int]int64
}

func (s *benchmarkStats) recordCatchupCheck(elapsed time.Duration) {
	s.catchupPolls.Add(1)
	s.catchupCheckElapsedNanos.Add(int64(elapsed))
}

func (s *benchmarkStats) recordEventLogAppend(shardID int, elapsed time.Duration, err error) {
	s.eventLogAppend.record(elapsed)
	if err != nil {
		return
	}

	s.appendMu.Lock()
	defer s.appendMu.Unlock()

	if s.appendCountByShard == nil {
		s.appendCountByShard = make(map[int]int64)
	}
	s.appendCountByShard[shardID]++
}

func (s *benchmarkStats) appendTargets() (map[int]int64, int64) {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()

	targets := make(map[int]int64, len(s.appendCountByShard))
	total := int64(0)
	for shardID, count := range s.appendCountByShard {
		targets[shardID] = count
		total += count
	}
	return targets, total
}

func (s *benchmarkStats) snapshot() statsSnapshot {
	if s == nil {
		return statsSnapshot{}
	}
	return statsSnapshot{
		EventLogAppend:      s.eventLogAppend.snapshot(),
		EventLogTail:        s.eventLogTail.snapshot(),
		EventLogRecord:      s.eventLogRecord.snapshot(),
		EventLogEndOffset:   s.eventLogEndOffset.snapshot(),
		CheckpointSave:      s.checkpointSave.snapshot(),
		CheckpointLoad:      s.checkpointLoad.snapshot(),
		OrderLoad:           s.orderLoad.snapshot(),
		OrderSave:           s.orderSave.snapshot(),
		CatchupPolls:        s.catchupPolls.Load(),
		CatchupCheckElapsed: time.Duration(s.catchupCheckElapsedNanos.Load()),
	}
}

type statsSnapshot struct {
	EventLogAppend      opStatsSnapshot
	EventLogTail        opStatsSnapshot
	EventLogRecord      opStatsSnapshot
	EventLogEndOffset   opStatsSnapshot
	CheckpointSave      opStatsSnapshot
	CheckpointLoad      opStatsSnapshot
	OrderLoad           opStatsSnapshot
	OrderSave           opStatsSnapshot
	CatchupPolls        int64
	CatchupCheckElapsed time.Duration
}

type instrumentedEventLog struct {
	inner eventlog.EventLog
	stats *benchmarkStats
}

func (l *instrumentedEventLog) Append(ctx context.Context, event model.Event) (eventlog.Position, error) {
	start := time.Now()
	position, err := l.inner.Append(ctx, event)
	l.stats.recordEventLogAppend(event.ShardID, time.Since(start), err)
	return position, err
}

func (l *instrumentedEventLog) TailFrom(ctx context.Context, position eventlog.Position) (<-chan eventlog.Record, error) {
	start := time.Now()
	records, err := l.inner.TailFrom(ctx, position)
	l.stats.eventLogTail.record(time.Since(start))
	if err != nil {
		return nil, err
	}

	instrumentedRecords := make(chan eventlog.Record)
	go func() {
		defer close(instrumentedRecords)
		for {
			waitStart := time.Now()
			select {
			case <-ctx.Done():
				return
			case record, ok := <-records:
				if !ok {
					return
				}
				l.stats.eventLogRecord.record(time.Since(waitStart))
				select {
				case <-ctx.Done():
					return
				case instrumentedRecords <- record:
				}
			}
		}
	}()
	return instrumentedRecords, nil
}

func (l *instrumentedEventLog) EndOffset(ctx context.Context, shardID int) (int64, error) {
	start := time.Now()
	offset, err := l.inner.EndOffset(ctx, shardID)
	l.stats.eventLogEndOffset.record(time.Since(start))
	return offset, err
}

type instrumentedCheckpointStore struct {
	inner checkpoint.ShardStore
	stats *benchmarkStats
}

func (s *instrumentedCheckpointStore) SaveShardCheckpoint(ctx context.Context, value checkpoint.ShardCheckpoint) error {
	start := time.Now()
	err := s.inner.SaveShardCheckpoint(ctx, value)
	s.stats.checkpointSave.record(time.Since(start))
	return err
}

func (s *instrumentedCheckpointStore) LoadShardCheckpoint(ctx context.Context, shardID int) (checkpoint.ShardCheckpoint, bool, error) {
	start := time.Now()
	value, found, err := s.inner.LoadShardCheckpoint(ctx, shardID)
	s.stats.checkpointLoad.record(time.Since(start))
	return value, found, err
}

type instrumentedOrderStateStore struct {
	inner orderstate.Store
	stats *benchmarkStats
}

func (s *instrumentedOrderStateStore) Load(ctx context.Context, orderID int64) (orderstate.State, bool, error) {
	start := time.Now()
	value, found, err := s.inner.Load(ctx, orderID)
	s.stats.orderLoad.record(time.Since(start))
	return value, found, err
}

func (s *instrumentedOrderStateStore) Save(ctx context.Context, value orderstate.State) error {
	start := time.Now()
	err := s.inner.Save(ctx, value)
	s.stats.orderSave.record(time.Since(start))
	return err
}

type benchmarkOwnershipStore struct {
	mu     sync.Mutex
	owners map[int]clusterownership.Ownership
}

func newBenchmarkOwnershipStore() *benchmarkOwnershipStore {
	return &benchmarkOwnershipStore{
		owners: make(map[int]clusterownership.Ownership),
	}
}

func (s *benchmarkOwnershipStore) OwnerOf(shardID int) (clusterownership.Ownership, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	currentOwnership, ok := s.owners[shardID]
	return currentOwnership, ok, nil
}

func (s *benchmarkOwnershipStore) ShardsForNode(nodeID int) ([]clusterownership.Ownership, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ownerships := make([]clusterownership.Ownership, 0)
	for _, currentOwnership := range s.owners {
		if currentOwnership.NodeID == nodeID {
			ownerships = append(ownerships, currentOwnership)
		}
	}
	sort.Slice(ownerships, func(i int, j int) bool {
		return ownerships[i].ShardID < ownerships[j].ShardID
	})
	return ownerships, nil
}

func (s *benchmarkOwnershipStore) Assign(shardID int, nodeID int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	currentOwnership := s.owners[shardID]
	currentOwnership.ShardID = shardID
	currentOwnership.NodeID = nodeID
	currentOwnership.Epoch++
	if currentOwnership.Epoch == 0 {
		currentOwnership.Epoch = 1
	}
	s.owners[shardID] = currentOwnership
	return nil
}

func assignModuloOwnership(store clusterownership.OwnershipStore, nodeCount int, shardCount int) error {
	if nodeCount <= 0 {
		return fmt.Errorf("node count must be > 0")
	}
	if shardCount <= 0 {
		return fmt.Errorf("shard count must be > 0")
	}

	for shardID := 0; shardID < shardCount; shardID++ {
		nodeID := shardID%nodeCount + 1
		if err := store.Assign(shardID, nodeID); err != nil {
			return err
		}
	}
	return nil
}

func parseCommaSeparated(text string) []string {
	parts := strings.Split(text, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value != "" {
			values = append(values, value)
		}
	}
	return values
}

func benchmarkTopicName(prefix string, scenarioName string) string {
	prefix = sanitizeNamePart(prefix)
	if prefix == "" {
		prefix = "testp-bench"
	}
	scenarioName = sanitizeNamePart(scenarioName)
	if scenarioName == "" {
		scenarioName = "scenario"
	}
	return fmt.Sprintf("%s-%s-%d", prefix, scenarioName, time.Now().UnixNano())
}

func benchmarkEtcdPrefix(basePrefix string, scenarioName string) string {
	basePrefix = tools.CleanEtcdPrefix(basePrefix)
	scenarioName = sanitizeNamePart(scenarioName)
	if scenarioName == "" {
		scenarioName = "scenario"
	}
	return tools.CleanEtcdPrefix(fmt.Sprintf("%s/%s-%d", basePrefix, scenarioName, time.Now().UnixNano()))
}

func sanitizeNamePart(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	var builder strings.Builder
	lastDash := false
	for _, r := range value {
		allowed := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if allowed {
			builder.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			builder.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(builder.String(), "-")
}

func createKafkaTopic(ctx context.Context, broker string, topic string, partitions int) error {
	if partitions <= 0 {
		return fmt.Errorf("kafka topic partitions must be > 0: %d", partitions)
	}

	controllerConn, err := dialKafkaController(ctx, broker)
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	err = controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
	if err != nil {
		return fmt.Errorf("create kafka topic %s: %w", topic, err)
	}
	return waitKafkaTopicReady(ctx, broker, topic, partitions)
}

func waitKafkaTopicReady(ctx context.Context, broker string, topic string, partitions int) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()

	for {
		ready := true
		dialer := localKafkaDialer()
		for partition := 0; partition < partitions; partition++ {
			conn, err := dialer.DialLeader(ctx, "tcp", broker, topic, partition)
			if err != nil {
				ready = false
				break
			}
			_, err = conn.ReadLastOffset()
			_ = conn.Close()
			if err != nil {
				ready = false
				break
			}
		}
		if ready {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("timed out waiting for kafka topic %s leaders", topic)
		case <-ticker.C:
		}
	}
}

func deleteKafkaTopicByName(ctx context.Context, broker string, topic string) error {
	controllerConn, err := dialKafkaController(ctx, broker)
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	if err := controllerConn.DeleteTopics(topic); err != nil {
		return fmt.Errorf("delete kafka topic %s: %w", topic, err)
	}
	return nil
}

func dialKafkaController(ctx context.Context, broker string) (*kafka.Conn, error) {
	dialer := localKafkaDialer()
	conn, err := dialer.DialContext(ctx, "tcp", broker)
	if err != nil {
		return nil, fmt.Errorf("dial kafka broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return nil, fmt.Errorf("get kafka controller: %w", err)
	}

	controllerAddress := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := dialer.DialContext(ctx, "tcp", controllerAddress)
	if err != nil {
		return nil, fmt.Errorf("dial kafka controller: %w", err)
	}
	return controllerConn, nil
}

func localKafkaDialer() *kafka.Dialer {
	return &kafka.Dialer{
		Resolver: benchmarkLocalhostResolver{},
	}
}

type benchmarkLocalhostResolver struct{}

func (benchmarkLocalhostResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	if host == "localhost" {
		return []string{"127.0.0.1"}, nil
	}
	return net.DefaultResolver.LookupHost(ctx, host)
}
