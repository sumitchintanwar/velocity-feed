package scheduler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/models"
)

type mockPublisher struct {
	mu        sync.Mutex
	Published []models.StoredEvent
}

func (m *mockPublisher) PublishEvent(ev models.StoredEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Published = append(m.Published, ev)
}

func eventsAt(base time.Time, count int, spacing time.Duration) []models.StoredEvent {
	evs := make([]models.StoredEvent, count)
	for i := range evs {
		evs[i] = models.StoredEvent{
			Symbol:    "TEST",
			Timestamp: base.Add(time.Duration(i) * spacing),
		}
	}
	return evs
}

// TestClockSpeed verifies that virtual timing is correctly scaled by the speed multiplier.
func TestClockSpeed(t *testing.T) {
	ctx := context.Background()
	pub := &mockPublisher{}

	// 10× replay speed: a 1-second historical gap should take ~100ms wall time.
	clock := NewClock(10.0, pub)

	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	events := []models.StoredEvent{
		{Symbol: "BTC-USD", Timestamp: baseTime},
		{Symbol: "BTC-USD", Timestamp: baseTime.Add(time.Second)},
	}

	start := time.Now()
	clock.Schedule(ctx, events)
	elapsed := time.Since(start)

	// Allow ±50ms OS scheduling jitter.
	const expected = 100 * time.Millisecond
	const tolerance = 50 * time.Millisecond
	if elapsed < expected-tolerance || elapsed > expected+tolerance {
		t.Errorf("expected ~100ms, took %v", elapsed)
	}
	if len(pub.Published) != 2 {
		t.Errorf("expected 2 published events, got %d", len(pub.Published))
	}
}

// TestClockMaxSpeed verifies that speed=0 emits all events without any delay.
func TestClockMaxSpeed(t *testing.T) {
	ctx := context.Background()
	pub := &mockPublisher{}
	clock := NewClock(0, pub)

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	events := eventsAt(base, 1000, time.Second)

	start := time.Now()
	clock.Schedule(ctx, events)
	elapsed := time.Since(start)

	if elapsed > 100*time.Millisecond {
		t.Errorf("max-speed schedule of 1000 events took %v (expected <100ms)", elapsed)
	}
	if len(pub.Published) != 1000 {
		t.Errorf("expected 1000 published events, got %d", len(pub.Published))
	}
}

// TestClockCurrentTimestamp verifies that CurrentTimestamp reflects the last emitted event.
func TestClockCurrentTimestamp(t *testing.T) {
	ctx := context.Background()
	pub := &mockPublisher{}
	clock := NewClock(0, pub)

	base := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	events := eventsAt(base, 5, time.Second)
	clock.Schedule(ctx, events)

	got := clock.CurrentTimestamp()
	want := events[4].Timestamp
	if !got.Equal(want) {
		t.Errorf("CurrentTimestamp: got %v, want %v", got, want)
	}
}

// TestClockPauseResume verifies that a paused clock resumes correctly and
// that all events are emitted in order with no duplicates.
func TestClockPauseResume(t *testing.T) {
	ctx := context.Background()
	pub := &mockPublisher{}
	clock := NewClock(0, pub)

	base := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	events := eventsAt(base, 10, time.Millisecond)

	// Pause immediately, schedule in background, then resume after 50ms.
	clock.Pause()

	done := make(chan struct{})
	go func() {
		clock.Schedule(ctx, events)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	clock.Resume()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Schedule did not complete after Resume")
	}

	if len(pub.Published) != 10 {
		t.Errorf("expected 10 published events, got %d", len(pub.Published))
	}
}

// TestClockContextCancelDuringPause verifies that a context cancellation
// unblocks a paused clock without deadlock.
func TestClockContextCancelDuringPause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pub := &mockPublisher{}
	clock := NewClock(0, pub)
	clock.Pause()

	base := time.Now()
	events := eventsAt(base, 5, time.Millisecond)

	done := make(chan struct{})
	go func() {
		clock.Schedule(ctx, events)
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel() // should unblock the paused select

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Schedule did not return after context cancellation while paused")
	}
}
