package scheduler

import (
	"runtime"
	"sync"
	"testP/internal/matcher"
)

type WorkerPool struct {
	shards  []*Shard
	matcher *matcher.Matcher
	wg      sync.WaitGroup
}

func NewWorkerPool(shards []*Shard, matcher *matcher.Matcher) *WorkerPool {
	return &WorkerPool{
		shards:  shards,
		matcher: matcher,
	}
}

func (p *WorkerPool) Start(workerCount int) {
	if workerCount <= 0 {
		workerCount = 1
	}

	for i := 0; i < workerCount; i++ {
		p.wg.Add(1)
		go p.workerLoop(i)
	}
}

func (p *WorkerPool) Wait() {
	p.wg.Wait()
}

func (p *WorkerPool) workerLoop(workerID int) {
	defer p.wg.Done()

	closed := make([]bool, len(p.shards))
	closedCount := 0

	for closedCount < len(p.shards) {
		hasWork := false

		for i, shard := range p.shards {
			if closed[i] {
				continue
			}

			select {
			case batch, ok := <-shard.orderCh:
				if !ok {
					closed[i] = true
					closedCount++
					continue
				}

				hasWork = true
				p.matcher.MatchBatch(batch)

			default:
			}
		}

		if !hasWork {
			runtime.Gosched()
		}
	}
}
