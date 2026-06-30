package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/models"
)

type noopPublisher struct{}

func (n *noopPublisher) PublishEvent(ev models.StoredEvent) {}

// BenchmarkClockMaxSpeed measures the per-event overhead of the scheduling loop
// at max speed (speedMultiplier = 0). allocs/op must be 0.
func BenchmarkClockMaxSpeed(b *testing.B) {
	clock := NewClock(0, &noopPublisher{})

	events := make([]models.StoredEvent, 1000)
	base := time.Now()
	for i := range events {
		events[i] = models.StoredEvent{
			Timestamp: base.Add(time.Duration(i) * time.Millisecond),
		}
	}

	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Reset the clock between iterations so lastTimestamp is always zero
		// and we measure a deterministic code path every time.
		clock = NewClock(0, &noopPublisher{})
		clock.Schedule(ctx, events)
	}
}

// BenchmarkClockPauseResumeOverhead measures the overhead introduced by acquiring
// pauseMu on every event in the hot path (F-1 fix cost).
func BenchmarkClockPauseResumeOverhead(b *testing.B) {
	ctx := context.Background()

	events := make([]models.StoredEvent, 1000)
	base := time.Now()
	for i := range events {
		events[i] = models.StoredEvent{Timestamp: base.Add(time.Duration(i) * time.Millisecond)}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		clock := NewClock(0, &noopPublisher{})
		clock.Schedule(ctx, events)
	}
}
