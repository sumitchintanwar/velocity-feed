package engine

import (
	"context"
	"time"

	"github.com/sumit/rtmds/internal/models"
	"github.com/sumit/rtmds/internal/recorder"
	"github.com/sumit/rtmds/internal/recorder/batcher"
	"github.com/sumit/rtmds/internal/recorder/storage"
)

// Recorder orchestrates the ingestion of live events into the storage layer.
type Recorder struct {
	store   storage.EventStore
	batcher *batcher.Batcher
}

// NewRecorder constructs a recorder utilizing micro-batching.
func NewRecorder(store storage.EventStore, batchSize int, flushRate time.Duration) *Recorder {
	r := &Recorder{
		store: store,
	}

	processor := func(ctx context.Context, batch []models.StoredEvent) ([]models.StoredEvent, error) {
		// Set recording time right before hitting the disk/store
		now := time.Now()
		for i := range batch {
			batch[i].RecordingTime = now
			recorder.EventsRecorded.WithLabelValues(batch[i].Symbol).Inc()
		}
		
		recorder.BatchSize.Observe(float64(len(batch)))
		err := r.store.WriteBatch(ctx, batch)
		
		// Return buffer to batcher for pooling
		return batch, err
	}

	r.batcher = batcher.NewBatcher(batchSize, flushRate, processor)
	return r
}

// Start begins the recording process.
func (r *Recorder) Start(ctx context.Context) {
	r.batcher.Start(ctx)
}

// Wait blocks until the underlying batcher completes all active writes.
func (r *Recorder) Wait() {
	r.batcher.Wait()
}

// RecordEvent accepts a normalized event and buffers it for storage.
func (r *Recorder) RecordEvent(ev models.StoredEvent) {
	r.batcher.Add(ev)
}
