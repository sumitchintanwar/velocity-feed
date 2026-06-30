package engine

import (
	"context"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/models"
	"github.com/sumit/rtmds/internal/recorder/storage"
)

func TestRecorderBatching(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewInMemoryStore()
	// Set batch size to 10 so we can reliably trigger a flush
	recorder := NewRecorder(store, 10, time.Second)
	recorder.Start(ctx)

	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Send exactly 10 events
	for i := 0; i < 10; i++ {
		recorder.RecordEvent(models.StoredEvent{
			EventID:        "ev1",
			Symbol:         "BTC-USD",
			Timestamp:      baseTime.Add(time.Duration(i) * time.Millisecond),
			SequenceNumber: uint64(i),
		})
	}

	// Trigger graceful shutdown and wait for batches to complete
	cancel()
	recorder.Wait()

	iterator, err := store.ReadStream(context.Background(), "BTC-USD", baseTime, baseTime.Add(time.Second), 100)
	if err != nil {
		t.Fatalf("ReadStream error: %v", err)
	}
	defer iterator.Close()

	var events []models.StoredEvent
	for {
		batch, err := iterator.Next()
		if err != nil {
			t.Fatalf("Iterator next error: %v", err)
		}
		if batch == nil {
			break
		}
		events = append(events, batch...)
	}

	if len(events) != 10 {
		t.Fatalf("Expected 10 events stored, got %d", len(events))
	}
	
	// Verify RecordingTime was injected
	if events[0].RecordingTime.IsZero() {
		t.Errorf("RecordingTime was not set by the recorder engine")
	}
}

func TestRecordingValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := storage.NewInMemoryStore()
	// High batch size for throughput
	recorder := NewRecorder(store, 1000, 100*time.Millisecond)
	recorder.Start(ctx)

	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	totalEvents := 100000

	// Write 100,000 events rapidly
	for i := 0; i < totalEvents; i++ {
		recorder.RecordEvent(models.StoredEvent{
			EventID:        "batch-ev",
			Symbol:         "BTC-USD",
			Timestamp:      baseTime.Add(time.Duration(i) * time.Millisecond),
			SequenceNumber: uint64(i),
		})
	}

	// Trigger graceful shutdown and wait for batches to complete
	cancel()
	recorder.Wait()

	iterator, err := store.ReadStream(context.Background(), "BTC-USD", baseTime, baseTime.Add(time.Duration(totalEvents)*time.Millisecond), 10000)
	if err != nil {
		t.Fatalf("ReadStream error: %v", err)
	}
	defer iterator.Close()

	var events []models.StoredEvent
	for {
		batch, err := iterator.Next()
		if err != nil {
			t.Fatalf("Iterator next error: %v", err)
		}
		if batch == nil {
			break
		}
		events = append(events, batch...)
	}

	if len(events) != totalEvents {
		t.Fatalf("Expected %d events stored, got %d", totalEvents, len(events))
	}
}


