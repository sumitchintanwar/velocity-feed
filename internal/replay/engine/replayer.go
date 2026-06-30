package engine

import (
	"context"
	"time"

	"github.com/sumit/rtmds/internal/models"
	"github.com/sumit/rtmds/internal/recorder/storage"
	"github.com/sumit/rtmds/internal/replay"
	"github.com/sumit/rtmds/internal/replay/scheduler"
)

// Replayer is the public entry point for creating replay sessions.
// All sessions are tracked by the internal manager.
type Replayer struct {
	manager *SessionManager // F-5: unexported — callers must use Replayer's public API
}

func NewReplayer(store storage.EventStore) *Replayer {
	return &Replayer{
		manager: NewSessionManager(store),
	}
}

// ReplayTimeRange fetches events for a symbol within a time window and pushes them
// through the virtual clock. Returns a Session that can be used to Pause, Resume,
// Seek, or Stop the replay.
func (r *Replayer) ReplayTimeRange(ctx context.Context, symbol string, start, end time.Time, speed float64, publisher scheduler.Publisher) (*Session, error) {
	metricPub := &metricPublisher{inner: publisher}

	replay.ActiveSessions.Inc()

	session, _ := r.manager.CreateSession(ctx, symbol, start, end, speed, metricPub)

	// Decrement the active-sessions gauge when the session goroutine exits.
	go func() {
		session.Wait()
		replay.ActiveSessions.Dec()
	}()

	return session, nil
}

// GetSession retrieves an active session by its ID.
// Returns ErrSessionNotFound if the session has already completed or never existed.
func (r *Replayer) GetSession(id string) (*Session, error) {
	return r.manager.GetSession(id)
}

type metricPublisher struct {
	inner scheduler.Publisher
}

func (m *metricPublisher) PublishEvent(ev models.StoredEvent) {
	replay.EventsPublished.WithLabelValues(ev.Symbol).Inc()
	m.inner.PublishEvent(ev)
}
