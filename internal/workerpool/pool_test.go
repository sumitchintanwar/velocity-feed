package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/log"
)

// testPublisher counts published events.
type testPublisher struct {
	count atomic.Int64
}

func (p *testPublisher) Publish(_ context.Context, _ any) {
	p.count.Add(1)
}

func nopLogger() *log.Logger {
	return log.New(nil, "test")
}

// ---------- Basic Functionality ----------

func TestPool_ProcessesAllEvents(t *testing.T) {
	pub := &testPublisher{}
	cfg := Config{Workers: 4, QueueCapacity: 256, ShutdownTimeout: 5 * time.Second}
	p := New(cfg, pub, nopLogger(), nil)

	ctx := context.Background()
	p.Start(ctx)

	for i := 0; i < 100; i++ {
		if !p.Enqueue(i) {
			t.Fatal("enqueue should succeed")
		}
	}

	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if got := pub.count.Load(); got != 100 {
		t.Fatalf("processed = %d, want 100", got)
	}
}

func TestPool_ZeroEvents(t *testing.T) {
	pub := &testPublisher{}
	p := New(DefaultConfig(), pub, nopLogger(), nil)

	ctx := context.Background()
	p.Start(ctx)

	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if got := pub.count.Load(); got != 0 {
		t.Fatalf("processed = %d, want 0", got)
	}
}

// ---------- Bounded Queue ----------

func TestPool_DropsWhenQueueFull(t *testing.T) {
	// 1 worker, queue of 2. Enqueue 4 events without starting workers.
	pub := &testPublisher{}
	cfg := Config{Workers: 1, QueueCapacity: 2, ShutdownTimeout: 5 * time.Second}
	p := New(cfg, pub, nopLogger(), nil)

	accepted := 0
	dropped := 0
	for i := 0; i < 4; i++ {
		if p.Enqueue(i) {
			accepted++
		} else {
			dropped++
		}
	}

	if accepted != 2 {
		t.Fatalf("accepted = %d, want 2", accepted)
	}
	if dropped != 2 {
		t.Fatalf("dropped = %d, want 2", dropped)
	}
	if got := p.Stats().Dropped.Load(); got != 2 {
		t.Fatalf("stats.Dropped = %d, want 2", got)
	}
}

// ---------- Concurrent Enqueue ----------

func TestPool_ConcurrentEnqueue(t *testing.T) {
	pub := &testPublisher{}
	cfg := Config{Workers: 4, QueueCapacity: 1024, ShutdownTimeout: 5 * time.Second}
	p := New(cfg, pub, nopLogger(), nil)

	ctx := context.Background()
	p.Start(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			p.Enqueue(n)
		}(i)
	}
	wg.Wait()

	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if got := pub.count.Load(); got != 100 {
		t.Fatalf("processed = %d, want 100", got)
	}
}

// ---------- Graceful Shutdown ----------

func TestPool_ShutdownDrainsQueue(t *testing.T) {
	pub := &testPublisher{}
	cfg := Config{Workers: 2, QueueCapacity: 64, ShutdownTimeout: 5 * time.Second}
	p := New(cfg, pub, nopLogger(), nil)

	ctx := context.Background()
	p.Start(ctx)

	for i := 0; i < 50; i++ {
		p.Enqueue(i)
	}

	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if got := pub.count.Load(); got != 50 {
		t.Fatalf("processed = %d, want 50", got)
	}
}

func TestPool_ShutdownTimeout(t *testing.T) {
	// Publisher that blocks without respecting context — simulates
	// a real-world publisher stuck on I/O or a lock.
	slowPub := &sleepPublisher{duration: 10 * time.Second}
	cfg := Config{Workers: 1, QueueCapacity: 1, ShutdownTimeout: 100 * time.Millisecond}
	p := New(cfg, slowPub, nopLogger(), nil)

	ctx := context.Background()
	p.Start(ctx)

	p.Enqueue("block")

	err := p.Shutdown(ctx)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

// sleepPublisher blocks for a fixed duration, ignoring context.
type sleepPublisher struct {
	duration time.Duration
}

func (p *sleepPublisher) Publish(_ context.Context, _ any) {
	time.Sleep(p.duration)
}

// ---------- SS1: Enqueue After Shutdown ----------

func TestPool_EnqueueAfterShutdown(t *testing.T) {
	pub := &testPublisher{}
	p := New(DefaultConfig(), pub, nopLogger(), nil)

	ctx := context.Background()
	p.Start(ctx)

	// Shut down first.
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// Enqueue after shutdown must not panic.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("enqueue after shutdown panicked: %v", r)
		}
	}()

	ok := p.Enqueue("after-shutdown")
	if ok {
		t.Fatal("enqueue after shutdown should return false")
	}

	if got := p.Stats().Dropped.Load(); got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
}

// ---------- Panic Recovery ----------

func TestPool_WorkerRecoversFromPanic(t *testing.T) {
	pub := &panicOncePublisher{}
	cfg := Config{Workers: 1, QueueCapacity: 64, ShutdownTimeout: 5 * time.Second}
	p := New(cfg, pub, nopLogger(), nil)

	ctx := context.Background()
	p.Start(ctx)

	// First event causes panic, second should succeed.
	p.Enqueue("panic")
	p.Enqueue("ok")

	time.Sleep(50 * time.Millisecond) // let worker process

	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// panicOncePublisher panics on the first call, so only the second
	// event increments pub.count. Both are counted as processed (QS1 fix).
	if got := pub.count.Load(); got != 1 {
		t.Fatalf("processed = %d, want 1", got)
	}
	if got := p.Stats().Panics.Load(); got != 1 {
		t.Fatalf("panics = %d, want 1", got)
	}
	if got := p.Stats().Processed.Load(); got != 2 {
		t.Fatalf("processed counter = %d, want 2 (includes recovered panics)", got)
	}
}

type panicOncePublisher struct {
	once  sync.Once
	count atomic.Int64
}

func (p *panicOncePublisher) Publish(_ context.Context, _ any) {
	p.once.Do(func() {
		panic("test panic")
	})
	p.count.Add(1)
}

// ---------- Stats ----------

func TestPool_StatsAccurate(t *testing.T) {
	pub := &testPublisher{}
	cfg := Config{Workers: 2, QueueCapacity: 256, ShutdownTimeout: 5 * time.Second}
	p := New(cfg, pub, nopLogger(), nil)

	ctx := context.Background()
	p.Start(ctx)

	for i := 0; i < 200; i++ {
		p.Enqueue(i)
	}

	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	stats := p.Stats()
	if got := stats.Enqueued.Load(); got != 200 {
		t.Fatalf("enqueued = %d, want 200", got)
	}
	if got := stats.Processed.Load(); got != 200 {
		t.Fatalf("processed = %d, want 200", got)
	}
	if got := stats.Dropped.Load(); got != 0 {
		t.Fatalf("dropped = %d, want 0", got)
	}
}

// ---------- Default Config ----------

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Workers != 8 {
		t.Fatalf("workers = %d, want 8", cfg.Workers)
	}
	if cfg.QueueCapacity != 4096 {
		t.Fatalf("queue_capacity = %d, want 4096", cfg.QueueCapacity)
	}
}

// ---------- SS2: Double Shutdown ----------

func TestPool_DoubleShutdown(t *testing.T) {
	pub := &testPublisher{}
	p := New(DefaultConfig(), pub, nopLogger(), nil)

	ctx := context.Background()
	p.Start(ctx)

	p.Enqueue(1)

	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("first shutdown: %v", err)
	}
	// Second shutdown is a no-op — returns nil, no goroutine leak.
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}
