package engine

import (
	"context"
	"testP/internal/matcher"
	"testP/internal/model"
	"testP/internal/scheduler"
)

type GlobalEngine struct {
	matcher   *matcher.Matcher
	scheduler *scheduler.Scheduler
	worker    *scheduler.WorkerPool
}

func NewGlobalEngine(riders []*model.Rider, shardCount int, bufferSize int, cellSize int, areaSize int, loadWeight int64) *GlobalEngine {
	m := matcher.NewMatcher(riders, cellSize, loadWeight)
	s := scheduler.NewScheduler(shardCount, bufferSize, cellSize, areaSize)

	return &GlobalEngine{
		matcher:   m,
		scheduler: s,
		worker:    scheduler.NewWorkerPool(s.Shards(), m),
	}
}

func (e *GlobalEngine) Start(workerCount int) {
	e.scheduler.Start()
	e.worker.Start(workerCount)
}

func (e *GlobalEngine) SubmitBatch(ctx context.Context, batch model.OrderBatch) error {
	return e.scheduler.SubmitBatchContext(ctx, batch)
}

func (e *GlobalEngine) ApplyRiderEvent(event model.RiderEvent) {
	e.matcher.ApplyRiderEvent(event)
}

func (e *GlobalEngine) Close() {
	e.scheduler.Close()
}

func (e *GlobalEngine) Wait() {
	e.worker.Wait()
}

func (e *GlobalEngine) Submitted() int64 {
	return e.scheduler.Submitted()
}

func (e *GlobalEngine) Matched() int64 {
	return e.matcher.Matched()
}

func (e *GlobalEngine) Missed() int64 {
	return e.matcher.Missed()
}

func (e *GlobalEngine) OnlineRiders() int {
	return e.matcher.OnlineRiders()
}
