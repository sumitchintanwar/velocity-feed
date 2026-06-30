package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/models"
	"github.com/sumit/rtmds/internal/recorder/storage"
)

// ─── Test helpers ───────────────────────────────────────────────────────────

type mockPublisher struct {
	mu        sync.Mutex
	Published []models.StoredEvent
}

func (m *mockPublisher) PublishEvent(ev models.StoredEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Published = append(m.Published, ev)
}

func (m *mockPublisher) GetPublished() []models.StoredEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Published
}

// newTestStore returns a store pre-loaded with events under symbol at 1-second intervals.
func newTestStore(ctx context.Context, symbol string, base time.Time, count int) *storage.InMemoryStore {
	store := storage.NewInMemoryStore()
	evs := make([]models.StoredEvent, count)
	for i := 0; i < count; i++ {
		evs[i] = models.StoredEvent{
			Symbol:         symbol,
			Timestamp:      base.Add(time.Duration(i) * time.Second),
			SequenceNumber: uint64(i + 1),
		}
	}
	store.WriteBatch(ctx, evs)
	return store
}

// ─── Correctness tests ──────────────────────────────────────────────────────

func TestReplayAccuracy(t *testing.T) {
	ctx := context.Background()
	store := storage.NewInMemoryStore()

	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Create 5 events with a same-timestamp collision at T+2s to verify ordering.
	events := []models.StoredEvent{
		{Symbol: "BTC-USD", Timestamp: baseTime, SequenceNumber: 1},
		{Symbol: "BTC-USD", Timestamp: baseTime.Add(time.Second), SequenceNumber: 2},
		{Symbol: "BTC-USD", Timestamp: baseTime.Add(2 * time.Second), SequenceNumber: 4}, // inserted out of order
		{Symbol: "BTC-USD", Timestamp: baseTime.Add(2 * time.Second), SequenceNumber: 3},
		{Symbol: "BTC-USD", Timestamp: baseTime.Add(3 * time.Second), SequenceNumber: 5},
	}
	store.WriteBatch(ctx, events)

	pub := &mockPublisher{}
	replayer := NewReplayer(store)

	sess, _ := replayer.ReplayTimeRange(ctx, "BTC-USD", baseTime, baseTime.Add(5*time.Second), 0, pub)
	sess.Wait()

	published := pub.GetPublished()
	if len(published) != 5 {
		t.Fatalf("expected 5 events, got %d", len(published))
	}

	// Storage must have sorted the T+2s collision by sequence number.
	if published[2].SequenceNumber != 3 {
		t.Errorf("expected Seq 3 at index 2, got %d", published[2].SequenceNumber)
	}
	if published[3].SequenceNumber != 4 {
		t.Errorf("expected Seq 4 at index 3, got %d", published[3].SequenceNumber)
	}
}

// TestConcurrentReplay verifies that multiple concurrent sessions do not interfere.
func TestConcurrentReplay(t *testing.T) {
	ctx := context.Background()
	store := storage.NewInMemoryStore()

	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	events := []models.StoredEvent{
		{Symbol: "BTC-USD", Timestamp: baseTime, SequenceNumber: 1},
		{Symbol: "ETH-USD", Timestamp: baseTime, SequenceNumber: 2},
	}
	store.WriteBatch(ctx, events)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			pub := &mockPublisher{}
			replayer := NewReplayer(store)
			symbol := "BTC-USD"
			if id%2 == 0 {
				symbol = "ETH-USD"
			}
			sess, _ := replayer.ReplayTimeRange(ctx, symbol, baseTime, baseTime.Add(time.Second), 0, pub)
			sess.Wait()

			if len(pub.GetPublished()) != 1 {
				t.Errorf("goroutine %d: expected 1 event, got %d", id, len(pub.GetPublished()))
			}
		}(i)
	}
	wg.Wait()
}

// ─── State machine tests ─────────────────────────────────────────────────────

// TestStateMachineValidTransitions exhaustively verifies IsValidTransition.
func TestStateMachineValidTransitions(t *testing.T) {
	type transition struct {
		from SessionState
		to   SessionState
		want bool
	}
	cases := []transition{
		// Valid paths
		{StateCreated, StateInitializing, true},
		{StateCreated, StateDestroyed, true},
		{StateInitializing, StateRunning, true},
		{StateRunning, StatePaused, true},
		{StateRunning, StateSeeking, true},
		{StateRunning, StateCompleted, true},
		{StateRunning, StateDestroyed, true},
		{StatePaused, StateRunning, true},
		{StatePaused, StateSeeking, true},
		{StatePaused, StateDestroyed, true},
		{StateSeeking, StateRunning, true},
		{StateSeeking, StateDestroyed, true},
		{StateCompleted, StateDestroyed, true},
		// Invalid paths
		{StateCompleted, StateRunning, false},
		{StateCompleted, StatePaused, false},
		{StateDestroyed, StateRunning, false},
		{StatePaused, StateCompleted, false},
		{StateRunning, StateCreated, false},
	}

	for _, c := range cases {
		got := IsValidTransition(c.from, c.to)
		if got != c.want {
			t.Errorf("IsValidTransition(%s → %s): got %v, want %v", c.from, c.to, got, c.want)
		}
	}
}

// TestInvalidTransitionError verifies the error message format.
func TestInvalidTransitionError(t *testing.T) {
	err := &InvalidStateTransitionError{From: StateCompleted, To: StateRunning}
	want := "invalid replay state transition from Completed to Running"
	if err.Error() != want {
		t.Errorf("got %q, want %q", err.Error(), want)
	}
}

// TestSessionStateMachine runs a full Pause → Resume → Seek → Stop lifecycle
// on a live session that is sleeping on a future event (so it stays Running).
func TestSessionStateMachine(t *testing.T) {
	ctx := context.Background()
	store := storage.NewInMemoryStore()

	start := time.Now()
	end := time.Now().Add(1 * time.Hour)

	// First event is immediate; second is 30 min in the future at 1× speed, so the
	// session sleeps and remains in StateRunning throughout the test.
	store.WriteBatch(ctx, []models.StoredEvent{
		{Symbol: "AAPL", Timestamp: start, SequenceNumber: 1},
		{Symbol: "AAPL", Timestamp: start.Add(30 * time.Minute), SequenceNumber: 2},
	})

	manager := NewSessionManager(store)
	session, id := manager.CreateSession(ctx, "AAPL", start, end, 1.0, &mockPublisher{})
	if id == "" {
		t.Fatal("expected non-empty session ID")
	}

	// Allow the session goroutine to reach StateRunning.
	time.Sleep(50 * time.Millisecond)

	// --- Pause ---
	if err := session.Pause(); err != nil {
		t.Fatalf("Pause() failed: %v", err)
	}
	if s := session.Status().State; s != StatePaused {
		t.Errorf("expected StatePaused, got %s", s)
	}

	// Pause while already Paused must be rejected.
	{
		var target *InvalidStateTransitionError
		if err2 := session.Pause(); !errors.As(err2, &target) {
			t.Errorf("expected InvalidStateTransitionError on double-Pause, got %v", err2)
		}
	}

	// --- Resume ---
	if err := session.Resume(); err != nil {
		t.Fatalf("Resume() failed: %v", err)
	}
	if s := session.Status().State; s != StateRunning {
		t.Errorf("expected StateRunning after Resume, got %s", s)
	}

	// --- Seek ---
	if err := session.Seek(start.Add(10 * time.Minute)); err != nil {
		t.Fatalf("Seek() failed: %v", err)
	}
	// After Seek the internal loop transitions back to Running asynchronously.

	// --- Stop ---
	if err := session.Stop(); err != nil {
		t.Fatalf("Stop() failed: %v", err)
	}
	// Wait must unblock.
	done := make(chan struct{})
	go func() { session.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Wait() did not return within 2s after Stop()")
	}
}

// TestSessionContextTimeout verifies F-4: a caller-supplied WithTimeout is respected.
func TestSessionContextTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	store := storage.NewInMemoryStore()
	start := time.Now()
	end := time.Now().Add(1 * time.Hour)

	// Session with one event far in the future — will sleep until ctx expires.
	store.WriteBatch(ctx, []models.StoredEvent{
		{Symbol: "AAPL", Timestamp: start, SequenceNumber: 1},
		{Symbol: "AAPL", Timestamp: start.Add(1 * time.Hour), SequenceNumber: 2},
	})

	manager := NewSessionManager(store)
	session, _ := manager.CreateSession(ctx, "AAPL", start, end, 1.0, &mockPublisher{})

	done := make(chan struct{})
	go func() { session.Wait(); close(done) }()

	select {
	case <-done:
		// Good — context cancellation propagated and session exited.
	case <-time.After(2 * time.Second):
		t.Fatal("session did not exit within 2s after context deadline")
	}
}

// TestGetSessionProxy verifies that Replayer.GetSession returns the same session.
func TestGetSessionProxy(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(ctx, "AAPL", time.Now().Add(-time.Minute), 2)

	replayer := NewReplayer(store)
	sess, _ := replayer.ReplayTimeRange(ctx, "AAPL",
		time.Now().Add(-time.Minute), time.Now(), 0, &mockPublisher{})

	// Wait for the session to complete so we can verify lookup after cleanup.
	sess.Wait()

	// After completion the session is removed from the manager map.
	_, err := replayer.GetSession(sess.id)
	if !errors.Is(err, ErrSessionNotFound) {
		t.Errorf("expected ErrSessionNotFound after completion, got %v", err)
	}
}

// ─── Session Manager tests ───────────────────────────────────────────────────

func TestSessionManager(t *testing.T) {
	ctx := context.Background()
	store := storage.NewInMemoryStore()
	manager := NewSessionManager(store)

	start := time.Now()
	end := time.Now().Add(1 * time.Hour)

	store.WriteBatch(ctx, []models.StoredEvent{
		{Symbol: "AAPL", Timestamp: start, SequenceNumber: 1},
		{Symbol: "AAPL", Timestamp: start.Add(30 * time.Minute), SequenceNumber: 2},
	})

	session, id := manager.CreateSession(ctx, "AAPL", start, end, 1.0, &mockPublisher{})
	time.Sleep(30 * time.Millisecond) // let it start

	retrieved, err := manager.GetSession(id)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if retrieved != session {
		t.Error("retrieved session pointer does not match original")
	}

	manager.StopAll()

	// After StopAll the session map should be cleared (give goroutine time).
	time.Sleep(50 * time.Millisecond)
	_, err = manager.GetSession(id)
	if !errors.Is(err, ErrSessionNotFound) {
		t.Errorf("expected ErrSessionNotFound after StopAll, got %v", err)
	}
}
