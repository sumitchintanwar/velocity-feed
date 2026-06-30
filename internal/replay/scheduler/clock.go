package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/sumit/rtmds/internal/models"
)

// Publisher defines where replayed events are pushed.
type Publisher interface {
	PublishEvent(ev models.StoredEvent)
}

// Clock controls the virtual timing of historical events.
type Clock struct {
	speedMultiplier float64
	publisher       Publisher

	// pauseMu guards paused, resume, and lastTimestamp.
	// All three fields are always read and written under this single lock to
	// eliminate the data race on lastTimestamp (F-1) and the TOCTOU window
	// in the pause-check path (F-2).
	pauseMu       sync.Mutex
	paused        bool
	resume        chan struct{}
	lastTimestamp time.Time
}

// NewClock initializes the virtual scheduler. Speed <= 0 signifies "Max Speed" (no delay).
func NewClock(speed float64, publisher Publisher) *Clock {
	return &Clock{
		speedMultiplier: speed,
		publisher:       publisher,
		resume:          make(chan struct{}),
	}
}

// Pause suspends the clock. Idempotent — calling Pause while already paused is a no-op.
func (c *Clock) Pause() {
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	c.paused = true
}

// Resume unpauses the clock. Idempotent — calling Resume while running is a no-op.
func (c *Clock) Resume() {
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	if c.paused {
		c.paused = false
		close(c.resume)
		c.resume = make(chan struct{})
	}
}

// waitIfPaused blocks until the clock is resumed or ctx is cancelled.
// Returns the channel to wait on while already holding pauseMu so the
// snapshot and the select use the same channel generation (F-2 fix).
func (c *Clock) waitIfPaused(ctx context.Context) bool {
	c.pauseMu.Lock()
	if !c.paused {
		c.pauseMu.Unlock()
		return true
	}
	// Capture resumeCh while holding the lock so we always wait on the
	// correct generation — no TOCTOU window between snapshot and select.
	resumeCh := c.resume
	c.pauseMu.Unlock()

	select {
	case <-ctx.Done():
		return false
	case <-resumeCh:
		return true
	}
}

// Schedule iterates through a strictly ordered slice of events, sleeping between them
// relative to their historical timestamps divided by the speed multiplier.
func (c *Clock) Schedule(ctx context.Context, events []models.StoredEvent) {
	if len(events) == 0 {
		return
	}

	for i := 0; i < len(events); i++ {
		ev := events[i]

		// Check for cancellation before every event.
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Block if paused. Returns false when ctx is cancelled.
		if !c.waitIfPaused(ctx) {
			return
		}

		c.pauseMu.Lock()
		last := c.lastTimestamp
		c.pauseMu.Unlock()

		// Apply virtual timing delay between events.
		if c.speedMultiplier > 0 && !last.IsZero() {
			historicalDelay := ev.Timestamp.Sub(last)
			if historicalDelay > 0 {
				virtualDelay := time.Duration(float64(historicalDelay) / c.speedMultiplier)
				if virtualDelay > time.Millisecond {
					// Context-aware sleep: wakes immediately on cancellation.
					// This ensures Stop() / ctx timeout propagates even during
					// long virtual delays without leaking goroutines.
					timer := time.NewTimer(virtualDelay)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				} else if virtualDelay > 0 {
					// Busy-wait spin for sub-millisecond precision,
					// bypassing OS scheduler jitter.
					waitStart := time.Now()
					for time.Since(waitStart) < virtualDelay {
					}
				}
			}
		}

		c.publisher.PublishEvent(ev)

		// Update lastTimestamp under the lock (F-1 fix — uniform locking).
		c.pauseMu.Lock()
		c.lastTimestamp = ev.Timestamp
		c.pauseMu.Unlock()
	}
}

// CurrentTimestamp returns the last published virtual timestamp.
func (c *Clock) CurrentTimestamp() time.Time {
	c.pauseMu.Lock()
	defer c.pauseMu.Unlock()
	return c.lastTimestamp
}
