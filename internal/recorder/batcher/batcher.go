package batcher

import (
	"context"
	"sync"
	"time"

	"github.com/sumit/rtmds/internal/models"
)

// BatchProcessor defines the callback for when a micro-batch is ready.
// It returns the slice so the batcher can recycle it. Returning nil indicates the processor kept it.
type BatchProcessor func(ctx context.Context, batch []models.StoredEvent) ([]models.StoredEvent, error)

type Batcher struct {
	maxSize   int
	flushRate time.Duration
	eventCh   chan models.StoredEvent
	process   BatchProcessor
	pool      sync.Pool
	sem       chan struct{}
	wg        sync.WaitGroup
}

func NewBatcher(maxSize int, flushRate time.Duration, process BatchProcessor) *Batcher {
	b := &Batcher{
		maxSize:   maxSize,
		flushRate: flushRate,
		eventCh:   make(chan models.StoredEvent, maxSize*2),
		process:   process,
		sem:       make(chan struct{}, 10), // Bounded concurrency (Max 10 async writes)
	}
	b.pool.New = func() interface{} {
		s := make([]models.StoredEvent, 0, maxSize)
		return &s
	}
	return b
}

// Add appends an event to the batcher queue in a non-blocking way.
func (b *Batcher) Add(ev models.StoredEvent) {
	// If channel is completely full, we could drop or block. 
	// For recording, blocking applies backpressure to the ingestion pipeline.
	b.eventCh <- ev
}

// Start begins the background batching loop.
func (b *Batcher) Start(ctx context.Context) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		bufferPtr := b.pool.Get().(*[]models.StoredEvent)
		buffer := *bufferPtr
		ticker := time.NewTicker(b.flushRate)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				// Flush remaining events in buffer + drain channel
				close(b.eventCh)
				for ev := range b.eventCh {
					buffer = append(buffer, ev)
				}
				if len(buffer) > 0 {
					b.dispatchProcess(context.Background(), buffer)
				}
				return

			case ev := <-b.eventCh:
				buffer = append(buffer, ev)
				if len(buffer) >= b.maxSize {
					b.dispatchProcess(ctx, buffer)
					
					// Acquire new buffer for next batch
					bufferPtr = b.pool.Get().(*[]models.StoredEvent)
					buffer = *bufferPtr
				}

			case <-ticker.C:
				if len(buffer) > 0 {
					b.dispatchProcess(ctx, buffer)
					
					bufferPtr = b.pool.Get().(*[]models.StoredEvent)
					buffer = *bufferPtr
				}
			}
		}
	}()
}

func (b *Batcher) dispatchProcess(ctx context.Context, buffer []models.StoredEvent) {
	// Block if we have too many concurrent writes (backpressure)
	b.sem <- struct{}{}
	b.wg.Add(1)
	go func(buf []models.StoredEvent) {
		defer func() {
			<-b.sem
			b.wg.Done()
		}()
		ret, _ := b.process(ctx, buf)
		if ret != nil {
			ret = ret[:0]
			b.pool.Put(&ret)
		}
	}(buffer)
}

// Wait blocks until the batcher loop and all active flushes have completed.
func (b *Batcher) Wait() {
	b.wg.Wait()
}
