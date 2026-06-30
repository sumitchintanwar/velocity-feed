// Package workerpool provides a bounded-concurrency worker pool that
// decouples the feed pipeline from the TopicManager publish path.
//
// Architecture:
//
//	Producer (non-blocking enqueue) → Bounded Queue → N Workers → Publisher.Publish()
//
// The pool bounds goroutine count, absorbs burst traffic, and provides
// graceful shutdown with queue drain.
package workerpool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sumit/rtmds/internal/log"
)

// Publisher is the interface for event delivery. Implemented by
// topicmanager.MemoryManager and pubsub.MemoryBus.
type Publisher interface {
	Publish(ctx context.Context, event any)
}

// Config holds worker pool parameters.
type Config struct {
	// Workers is the number of concurrent worker goroutines.
	// Default: 8 (matches typical 4C/8T CPU).
	Workers int

	// QueueCapacity is the bounded channel capacity.
	// Default: 4096 (absorbs 400ms burst at 10k events/sec).
	QueueCapacity int

	// ShutdownTimeout is the maximum time to wait for workers to drain.
	// Default: 5s.
	ShutdownTimeout time.Duration
}

// DefaultConfig returns production-ready defaults.
func DefaultConfig() Config {
	return Config{
		Workers:         8,
		QueueCapacity:   4096,
		ShutdownTimeout: 5 * time.Second,
	}
}

// PoolStats exposes runtime metrics.
type PoolStats struct {
	Enqueued   atomic.Int64
	Dropped    atomic.Int64
	Processed  atomic.Int64
	Panics     atomic.Int64
	QueueDepth atomic.Int64
}

// Pool is a bounded-concurrency worker pool. It manages N worker
// goroutines that drain a shared bounded queue and call Publisher.Publish.
type Pool struct {
	cfg       Config
	publisher Publisher
	log       *log.Logger
	stats     PoolStats
	metrics   *Metrics

	queue       chan any
	wg          sync.WaitGroup
	cancel      context.CancelFunc // cancels worker context on shutdown
	stopped     atomic.Bool        // SS1: prevents Enqueue after Shutdown
	closeOnce   sync.Once          // SS2: only close queue once
	shutdownOnce sync.Once         // SS2: only run Shutdown logic once
}

// New creates a Pool with the given config. Call Start to begin processing.
// metrics can be nil — in that case, only atomic PoolStats are updated.
func New(cfg Config, publisher Publisher, l *log.Logger, metrics *Metrics) *Pool {
	if cfg.Workers <= 0 {
		cfg.Workers = 8
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 4096
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 5 * time.Second
	}

	return &Pool{
		cfg:       cfg,
		publisher: publisher,
		log:       l,
		metrics:   metrics,
		queue:     make(chan any, cfg.QueueCapacity),
	}
}

// Stats returns a pointer to the pool's stats. The atomic fields are
// safe to read concurrently.
func (p *Pool) Stats() *PoolStats {
	return &p.stats
}

// Enqueue attempts a non-blocking send to the queue. Returns true if
// the event was accepted, false if the queue is full or pool is stopped.
//
// SS1: The stopped guard prevents send-on-closed-channel panics.
// A deferred recover handles the TOCTOU race where the channel closes
// between the stopped check and the send.
func (p *Pool) Enqueue(event any) bool {
	// SS1: reject if shutdown has started.
	if p.stopped.Load() {
		p.stats.Dropped.Add(1)
		if p.metrics != nil {
			p.metrics.TasksDropped.Inc()
		}
		return false
	}

	// SS1: recover from send-on-closed-channel panic (TOCTOU race).
	defer func() {
		if r := recover(); r != nil {
			p.stats.Dropped.Add(1)
			if p.metrics != nil {
				p.metrics.TasksDropped.Inc()
			}
		}
	}()

	p.stats.QueueDepth.Add(1)
	if p.metrics != nil {
		p.metrics.QueueDepth.Inc()
	}
	select {
	case p.queue <- event:
		// QS2: track total enqueued attempts.
		p.stats.Enqueued.Add(1)
		if p.metrics != nil {
			p.metrics.TasksReceived.Inc()
		}
		return true
	default:
		p.stats.Dropped.Add(1)
		p.stats.QueueDepth.Add(-1)
		if p.metrics != nil {
			p.metrics.TasksDropped.Inc()
			p.metrics.QueueDepth.Dec()
		}
		return false
	}
}

// Start launches N worker goroutines. Each worker reads from the queue
// and calls Publisher.Publish until the queue is closed and drained,
// or the context is cancelled.
func (p *Pool) Start(ctx context.Context) {
	// D1/GL1: derive a cancellable context so Shutdown can signal workers.
	ctx, p.cancel = context.WithCancel(ctx)

	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}
	p.log.Underlying().Info().
		Int("workers", p.cfg.Workers).
		Int("queue_capacity", p.cfg.QueueCapacity).
		Str("event", "worker_pool_started").
		Msg("worker pool started")
}

// worker is the per-goroutine loop. It uses select to receive from the
// queue or respond to context cancellation. Panics are recovered per-
// iteration so the worker continues processing.
//
// D1/GL1: Workers select on ctx.Done() so they exit promptly when
// Shutdown cancels the context, even if blocked in Publish.
func (p *Pool) worker(ctx context.Context, id int) {
	defer p.wg.Done()

	for {
		select {
		case event, ok := <-p.queue:
			if !ok {
				// Channel closed — drain complete.
				return
			}
			// Process with per-iteration panic recovery.
			p.processEvent(ctx, id, event)
		case <-ctx.Done():
			// D1/GL1: context cancelled (Shutdown or parent cancel).
			return
		}
	}
}

// processEvent handles a single event with panic recovery.
// QS1: Processed is incremented in a defer so it's counted even
// when Publish panics, preventing QueueDepth counter drift.
func (p *Pool) processEvent(ctx context.Context, id int, event any) {
	p.stats.QueueDepth.Add(-1)
	if p.metrics != nil {
		p.metrics.QueueDepth.Dec()
		p.metrics.ActiveWorkers.Inc()
	}
	start := time.Now()
	func() {
		defer func() {
			if r := recover(); r != nil {
				p.stats.Panics.Add(1)
				if p.metrics != nil {
					p.metrics.TasksFailed.Inc()
				}
				p.log.Underlying().Error().
					Int("worker", id).
					Interface("panic", r).
					Str("event", "worker_panic_recovered").
					Msg("worker panic recovered")
			}
			// QS1: always count as processed (recovered or not).
			p.stats.Processed.Add(1)
			if p.metrics != nil {
				p.metrics.TasksCompleted.Inc()
				p.metrics.TaskDurationSeconds.Observe(time.Since(start).Seconds())
				p.metrics.ActiveWorkers.Dec()
			}
		}()

		p.publisher.Publish(ctx, event)
	}()
}

// Shutdown signals the queue to close and waits for all workers to
// finish draining. Returns nil on clean shutdown, or an error if
// the timeout is exceeded.
//
// SS2: The entire Shutdown body runs inside shutdownOnce so concurrent
// callers don't spawn duplicate bridge goroutines.
func (p *Pool) Shutdown(ctx context.Context) error {
	var err error
	p.shutdownOnce.Do(func() {
		// SS1: stop accepting new events before closing the channel.
		p.stopped.Store(true)

		// Close the queue exactly once — workers will exit the range
		// after draining remaining items.
		p.closeOnce.Do(func() {
			close(p.queue)
		})

		// Wait for all workers to finish draining, with a timeout.
		shutdownCtx, cancel := context.WithTimeout(ctx, p.cfg.ShutdownTimeout)
		defer cancel()

		done := make(chan struct{})
		go func() {
			p.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		p.log.Underlying().Info().
			Int64("processed", p.stats.Processed.Load()).
			Int64("dropped", p.stats.Dropped.Load()).
			Int64("panics", p.stats.Panics.Load()).
			Str("event", "worker_pool_shut_down").
			Msg("worker pool shut down")
		case <-shutdownCtx.Done():
			// D1/GL1: workers stuck in blocking Publish — cancel
			// context to force-exit them.
			if p.cancel != nil {
				p.cancel()
			}
			p.log.Underlying().Warn().Str("event", "worker_pool_shutdown_timeout").Msg("worker pool shutdown timed out")
			err = fmt.Errorf("worker pool shutdown: %w", shutdownCtx.Err())
		}
	})
	return err
}
