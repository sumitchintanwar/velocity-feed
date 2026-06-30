package engine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/models"
	"github.com/sumit/rtmds/internal/recorder/batcher"
	"github.com/sumit/rtmds/internal/recorder/storage"
)

// BenchmarkRecorderThroughput measures the channel-push hot path only.
// It does NOT include batch flush or store write time.
func BenchmarkRecorderThroughput(b *testing.B) {
	store := storage.NewInMemoryStore()
	rec := NewRecorder(store, 10000, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec.Start(ctx)

	event := models.StoredEvent{
		Symbol:         "BTC-USD",
		Timestamp:      time.Now(),
		SequenceNumber: 1,
		Payload:        []byte(`{"price":50000.0,"volume":1.0}`),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rec.RecordEvent(event)
	}
}

// BenchmarkBatcherProcessor isolates the raw cost of the batch processor
// callback (RecordingTime injection + store.WriteBatch). No channels,
// no goroutines — pure function call overhead.
func BenchmarkBatcherProcessor(b *testing.B) {
	store := storage.NewInMemoryStore()
	processor := func(ctx context.Context, batch []models.StoredEvent) ([]models.StoredEvent, error) {
		now := time.Now()
		for i := range batch {
			batch[i].RecordingTime = now
		}
		err := store.WriteBatch(ctx, batch)
		return batch, err
	}

	const batchSize = 100
	batch := make([]models.StoredEvent, batchSize)
	for i := range batch {
		batch[i] = models.StoredEvent{
			EventID:        "bench-ev",
			Symbol:         "ETH-USD",
			Timestamp:      time.Now(),
			SequenceNumber: uint64(i),
			Payload:        []byte(`{"price":3000,"volume":0.5}`),
		}
	}

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		processor(ctx, batch)
	}
}

// BenchmarkRecorderEndToEnd measures the full pipeline: channel push →
// batcher accumulation → processor callback → store write. Uses a fast
// flush rate (1 ms) so partial batches drain quickly, and waits at the
// end for all writes to complete.
func BenchmarkRecorderEndToEnd(b *testing.B) {
	store := storage.NewInMemoryStore()
	var written atomic.Int64

	processor := func(ctx context.Context, batch []models.StoredEvent) ([]models.StoredEvent, error) {
		now := time.Now()
		for i := range batch {
			batch[i].RecordingTime = now
		}
		err := store.WriteBatch(ctx, batch)
		written.Add(int64(len(batch)))
		return batch, err
	}

	const batchSize = 1000
	bat := batcher.NewBatcher(batchSize, time.Millisecond, processor)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bat.Start(ctx)

	event := models.StoredEvent{
		EventID:        "e2e-ev",
		Symbol:         "BTC-USD",
		Timestamp:      time.Now(),
		SequenceNumber: 1,
		Payload:        []byte(`{"price":50000,"volume":1}`),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bat.Add(event)
	}

	// Wait for all enqueued events to be flushed to the store.
	deadline := time.After(30 * time.Second)
	for written.Load() < int64(b.N) {
		select {
		case <-deadline:
			b.Fatalf("drain timeout: wrote %d / %d", written.Load(), b.N)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
