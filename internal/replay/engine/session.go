package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sumit/rtmds/internal/models"
	"github.com/sumit/rtmds/internal/recorder/storage"
	"github.com/sumit/rtmds/internal/replay"
	"github.com/sumit/rtmds/internal/replay/scheduler"
)

const defaultChunkSize = 10_000

// SessionStatus reports the current progress and state of the replay session.
type SessionStatus struct {
	ID               string
	Symbol           string
	State            SessionState
	Speed            float64
	StartTimestamp   time.Time
	EndTimestamp     time.Time
	CurrentTimestamp time.Time
}

// Session represents an active replay process.
type Session struct {
	mu        sync.Mutex
	id        string
	symbol    string
	speed     float64
	chunkSize int
	publisher scheduler.Publisher
	store     storage.EventStore

	state SessionState

	clock       *scheduler.Clock
	cancel      context.CancelFunc
	innerCancel context.CancelFunc

	startTime  time.Time
	endTime    time.Time
	seekTarget *time.Time
	done       chan struct{}
}

func newSession(id string, store storage.EventStore, symbol string, speed float64, chunkSize int, publisher scheduler.Publisher, cancel context.CancelFunc) *Session {
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	return &Session{
		id:        id,
		symbol:    symbol,
		speed:     speed,
		chunkSize: chunkSize,
		publisher: publisher,
		store:     store,
		state:     StateCreated,
		clock:     scheduler.NewClock(speed, publisher),
		cancel:    cancel,
		done:      make(chan struct{}),
	}
}

// transitionState attempts to change the session state and returns an error if invalid.
// Must be called with s.mu held.
func (s *Session) transitionState(to SessionState) error {
	if !IsValidTransition(s.state, to) {
		return &InvalidStateTransitionError{From: s.state, To: to}
	}
	s.state = to
	return nil
}

// mustTransition is used by the internal run loop for transitions that must always
// succeed given the current architecture. It panics on violation so that any future
// change that breaks the internal sequencing surfaces immediately in tests rather
// than silently corrupting state (F-3 fix).
func (s *Session) mustTransition(to SessionState) {
	if err := s.transitionState(to); err != nil {
		panic(fmt.Sprintf("replay engine: invariant violation — %v", err))
	}
}

// Pause halts the current replay session.
func (s *Session) Pause() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.transitionState(StatePaused); err != nil {
		return err
	}

	replay.PausesTotal.Inc()
	s.clock.Pause()
	return nil
}

// Resume unpauses the current replay session.
func (s *Session) Resume() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.transitionState(StateRunning); err != nil {
		return err
	}

	s.clock.Resume()
	return nil
}

// Stop terminates the replay session permanently.
func (s *Session) Stop() error {
	s.mu.Lock()
	if err := s.transitionState(StateDestroyed); err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	s.cancel()
	return nil
}

// Seek repositions the replay session to target and restarts emission from there.
func (s *Session) Seek(target time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.transitionState(StateSeeking); err != nil {
		return err
	}

	replay.SeeksTotal.Inc()
	s.seekTarget = &target
	if s.innerCancel != nil {
		s.innerCancel()
	}

	return nil
}

// Status returns the current progress of the replay.
func (s *Session) Status() SessionStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	return SessionStatus{
		ID:               s.id,
		Symbol:           s.symbol,
		State:            s.state,
		Speed:            s.speed,
		StartTimestamp:   s.startTime,
		EndTimestamp:     s.endTime,
		CurrentTimestamp: s.clock.CurrentTimestamp(),
	}
}

// Wait blocks until the replay session has completely finished.
func (s *Session) Wait() {
	<-s.done
}

// run executes the replay loop, handling Seek interruptions.
// parentCtx is the caller's context and is never replaced — Stop() uses s.cancel
// which is a separate signal derived from the caller's context at CreateSession time.
func (s *Session) run(parentCtx context.Context, start, end time.Time) {
	defer close(s.done)

	s.mu.Lock()
	s.startTime = start
	s.endTime = end
	s.mustTransition(StateInitializing) // F-3: panic on invalid internal transition
	s.mu.Unlock()

	currentStart := start
	for {
		s.mu.Lock()
		if s.seekTarget != nil {
			currentStart = *s.seekTarget
			s.seekTarget = nil
			// Reset clock so the seek target is treated as a new origin,
			// preventing a 30-minute virtual sleep when jumping forward.
			s.clock = scheduler.NewClock(s.speed, s.publisher)
		}
		s.mustTransition(StateRunning) // F-3: panic on invalid internal transition
		s.mu.Unlock()

		innerCtx, innerCancel := context.WithCancel(parentCtx)
		s.mu.Lock()
		s.innerCancel = innerCancel
		s.mu.Unlock()

		err := s.streamEvents(innerCtx, currentStart, end)

		// If inner context was cancelled but parent is still alive, it was a Seek().
		if err == context.Canceled && parentCtx.Err() == nil {
			continue
		}

		// EOF or parent cancellation — determine final state.
		s.mu.Lock()
		if s.state != StateDestroyed {
			if parentCtx.Err() == nil {
				s.mustTransition(StateCompleted) // F-3: panic on invalid internal transition
			} else {
				s.mustTransition(StateDestroyed) // F-3: panic on invalid internal transition
			}
		}
		s.mu.Unlock()
		return
	}
}

func (s *Session) streamEvents(ctx context.Context, start, end time.Time) error {
	iterator, err := s.store.ReadStream(ctx, s.symbol, start, end, s.chunkSize) // F-7: configurable chunk size
	if err != nil {
		return err
	}
	var prefetchWg sync.WaitGroup
	prefetchWg.Add(1)
	
	defer func() {
		// Close iterator to unblock any pending Next() calls on real storage backends.
		iterator.Close()
		// Wait for the prefetch goroutine to exit so we don't leak it or race with its execution.
		prefetchWg.Wait()
	}()

	// Prefetch channel buffers exactly one chunk ahead of the clock.
	type fetchResult struct {
		batch []models.StoredEvent
		err   error
	}
	prefetchCh := make(chan fetchResult, 1)

	go func() {
		defer prefetchWg.Done()
		defer close(prefetchCh)
		for {
			batch, err := iterator.Next()
			select {
			case prefetchCh <- fetchResult{batch: batch, err: err}:
				if err != nil || batch == nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case res := <-prefetchCh:
			if res.err != nil {
				return res.err
			}
			if res.batch == nil {
				return nil // EOF
			}

			s.mu.Lock()
			clk := s.clock
			s.mu.Unlock()

			clk.Schedule(ctx, res.batch)

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
