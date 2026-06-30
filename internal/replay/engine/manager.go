package engine

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sumit/rtmds/internal/recorder/storage"
	"github.com/sumit/rtmds/internal/replay/scheduler"
)

var ErrSessionNotFound = errors.New("replay session not found")

// SessionManager manages concurrent replay sessions and isolates their states.
type SessionManager struct {
	mu        sync.RWMutex
	sessions  map[string]*Session
	store     storage.EventStore
	chunkSize int
}

func NewSessionManager(store storage.EventStore) *SessionManager {
	return &SessionManager{
		sessions:  make(map[string]*Session),
		store:     store,
		chunkSize: defaultChunkSize,
	}
}

// CreateSession initializes a new replay session and assigns a unique ID.
//
// F-4 fix: The context passed by the caller is used directly as the parent for the
// session's run loop. Stop() signals via a separate cancel function that is derived
// here — it does NOT wrap the caller's context in a second WithCancel. This preserves
// the standard Go context contract: a WithTimeout passed by the caller is respected,
// and a server-wide root context cancellation propagates normally.
func (m *SessionManager) CreateSession(ctx context.Context, symbol string, start, end time.Time, speed float64, publisher scheduler.Publisher) (*Session, string) {
	sessionID := uuid.New().String()

	// Derive a cancel that Stop() can call independently of the caller's context.
	sessionCtx, cancel := context.WithCancel(ctx)

	session := newSession(sessionID, m.store, symbol, speed, m.chunkSize, publisher, cancel)

	m.mu.Lock()
	m.sessions[sessionID] = session
	m.mu.Unlock()

	// Run the session in a goroutine and remove it from the map when done.
	go func() {
		defer func() {
			m.mu.Lock()
			delete(m.sessions, sessionID)
			m.mu.Unlock()
		}()
		session.run(sessionCtx, start, end)
	}()

	return session, sessionID
}

// GetSession retrieves an active session by ID.
func (m *SessionManager) GetSession(id string) (*Session, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	session, exists := m.sessions[id]
	if !exists {
		return nil, ErrSessionNotFound
	}
	return session, nil
}

// StopAll gracefully terminates all active sessions.
func (m *SessionManager) StopAll() {
	m.mu.Lock()
	sessions := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		sessions = append(sessions, s)
	}
	m.mu.Unlock()

	for _, s := range sessions {
		s.Stop() //nolint:errcheck — Stop on already-Destroyed is safely ignored here
	}
}
