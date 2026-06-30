package transport

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/eventlog"
)

// mockRepository implements eventlog.Repository for testing the replay handler.
type mockRepository struct {
	queryEventsFunc func(ctx context.Context, q eventlog.ReplayQuery) (*eventlog.ReplayResult, error)
}

func (m *mockRepository) Append(_ context.Context, _ *eventlog.StoredEvent) (int64, error) {
	return 0, nil
}
func (m *mockRepository) AppendBatch(_ context.Context, _ []*eventlog.StoredEvent) ([]int64, error) {
	return nil, nil
}
func (m *mockRepository) QueryBySymbol(_ context.Context, _ string, _, _ time.Time) ([]*eventlog.StoredEvent, error) {
	return nil, nil
}
func (m *mockRepository) QueryLatest(_ context.Context, _ string, _ int) ([]*eventlog.StoredEvent, error) {
	return nil, nil
}
func (m *mockRepository) QueryEvents(ctx context.Context, q eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
	if m.queryEventsFunc != nil {
		return m.queryEventsFunc(ctx, q)
	}
	return &eventlog.ReplayResult{Events: []*eventlog.StoredEvent{}}, nil
}
func (m *mockRepository) Count(_ context.Context) (int64, error) {
	return 0, nil
}
func (m *mockRepository) CountBySymbol(_ context.Context, _ string) (int64, error) {
	return 0, nil
}
func (m *mockRepository) Close() error {
	return nil
}

// testLimiter returns a fresh concurrency limiter for tests.
func testLimiter() *concurrencyLimiter {
	return newConcurrencyLimiter(maxConcurrentQueriesPerClient)
}

// ── Replay Handler Tests ────────────────────────────────────────────────────

func TestHandleReplay_BasicQuery(t *testing.T) {
	repo := &mockRepository{
		queryEventsFunc: func(_ context.Context, q eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
			if q.Symbol != "AAPL" {
				t.Errorf("expected symbol AAPL, got %s", q.Symbol)
			}
			return &eventlog.ReplayResult{
				Events: []*eventlog.StoredEvent{
					{EventID: 1, Symbol: "AAPL", Price: 250.0},
					{EventID: 2, Symbol: "AAPL", Price: 251.0},
				},
				HasMore: false,
			}, nil
		},
	}

	handler := handleReplay(repo, testLimiter())
	req := httptest.NewRequest("GET", "/replay?symbol=AAPL", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result eventlog.ReplayResult
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(result.Events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(result.Events))
	}
	if result.Events[0].Symbol != "AAPL" {
		t.Errorf("expected symbol AAPL, got %s", result.Events[0].Symbol)
	}
	if result.HasMore {
		t.Error("expected has_more=false")
	}
}

func TestHandleReplay_TimeRange(t *testing.T) {
	repo := &mockRepository{
		queryEventsFunc: func(_ context.Context, q eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
			if q.From.IsZero() {
				t.Error("expected from to be set")
			}
			if q.To.IsZero() {
				t.Error("expected to to be set")
			}
			expectedFrom := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)
			if !q.From.Equal(expectedFrom) {
				t.Errorf("expected from %v, got %v", expectedFrom, q.From)
			}
			return &eventlog.ReplayResult{
				Events:  []*eventlog.StoredEvent{},
				HasMore: false,
			}, nil
		},
	}

	handler := handleReplay(repo, testLimiter())
	url := "/replay?from=2026-06-24T10:00:00Z&to=2026-06-24T10:05:00Z"
	req := httptest.NewRequest("GET", url, nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleReplay_CursorPagination(t *testing.T) {
	now := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)
	repo := &mockRepository{
		queryEventsFunc: func(_ context.Context, q eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
			if q.Cursor.Timestamp != now || q.Cursor.EventID != 100 {
				t.Errorf("expected cursor (2026-06-24T10:00:00Z, 100), got (%v, %d)", q.Cursor.Timestamp, q.Cursor.EventID)
			}
			if q.Limit != 50 {
				t.Errorf("expected limit 50, got %d", q.Limit)
			}
			next := eventlog.Cursor{Timestamp: now.Add(time.Second), EventID: 101}
			return &eventlog.ReplayResult{
				Events: []*eventlog.StoredEvent{
					{EventID: 101, Symbol: "AAPL", Timestamp: now.Add(time.Second)},
				},
				NextCursor: &next,
				HasMore:    true,
			}, nil
		},
	}

	handler := handleReplay(repo, testLimiter())
	cursorParam := now.Format(time.RFC3339Nano) + ",100"
	req := httptest.NewRequest("GET", "/replay?cursor="+cursorParam+"&limit=50", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result eventlog.ReplayResult
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if !result.HasMore {
		t.Error("expected has_more=true")
	}
	if result.NextCursor == nil {
		t.Fatal("expected next_cursor, got nil")
	}
	if result.NextCursor.EventID != 101 {
		t.Errorf("expected next_cursor event_id 101, got %d", result.NextCursor.EventID)
	}
}

func TestHandleReplay_InvalidFromTime(t *testing.T) {
	handler := handleReplay(&mockRepository{}, testLimiter())
	req := httptest.NewRequest("GET", "/replay?from=not-a-time", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleReplay_InvalidToTime(t *testing.T) {
	handler := handleReplay(&mockRepository{}, testLimiter())
	req := httptest.NewRequest("GET", "/replay?to=bad", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleReplay_InvalidCursor(t *testing.T) {
	handler := handleReplay(&mockRepository{}, testLimiter())
	req := httptest.NewRequest("GET", "/replay?cursor=not-a-cursor", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleReplay_InvalidCursorMissingEventID(t *testing.T) {
	handler := handleReplay(&mockRepository{}, testLimiter())
	// Only timestamp, no event_id
	req := httptest.NewRequest("GET", "/replay?cursor=2026-06-24T10:00:00Z", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleReplay_InvalidCursorBadEventID(t *testing.T) {
	handler := handleReplay(&mockRepository{}, testLimiter())
	req := httptest.NewRequest("GET", "/replay?cursor=2026-06-24T10:00:00Z,abc", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleReplay_InvalidLimit(t *testing.T) {
	handler := handleReplay(&mockRepository{}, testLimiter())
	req := httptest.NewRequest("GET", "/replay?limit=0", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleReplay_EmptyQuery(t *testing.T) {
	repo := &mockRepository{
		queryEventsFunc: func(_ context.Context, q eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
			if q.Symbol != "" {
				t.Errorf("expected empty symbol, got %s", q.Symbol)
			}
			if !q.From.IsZero() {
				t.Error("expected zero from")
			}
			if !q.To.IsZero() {
				t.Error("expected zero to")
			}
			if !q.Cursor.Timestamp.IsZero() || q.Cursor.EventID != 0 {
				t.Error("expected zero cursor")
			}
			return &eventlog.ReplayResult{
				Events:  []*eventlog.StoredEvent{},
				HasMore: false,
			}, nil
		},
	}

	handler := handleReplay(repo, testLimiter())
	req := httptest.NewRequest("GET", "/replay", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestHandleReplay_RepositoryError(t *testing.T) {
	repo := &mockRepository{
		queryEventsFunc: func(_ context.Context, _ eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
			return nil, &testError{msg: "db connection failed"}
		},
	}

	handler := handleReplay(repo, testLimiter())
	req := httptest.NewRequest("GET", "/replay?symbol=AAPL", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}

func TestHandleReplay_ConcurrencyLimiting(t *testing.T) {
	block := make(chan struct{})
	repo := &mockRepository{
		queryEventsFunc: func(_ context.Context, _ eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
			// Block until test releases
			<-block
			return &eventlog.ReplayResult{Events: []*eventlog.StoredEvent{}}, nil
		},
	}

	limiter := newConcurrencyLimiter(2)
	handler := handleReplay(repo, limiter)

	var wg sync.WaitGroup

	// Fill up the limit (same client IP)
	req1 := httptest.NewRequest("GET", "/replay?symbol=A", nil)
	req1.RemoteAddr = "10.0.0.1:12345"
	w1 := httptest.NewRecorder()
	wg.Add(1)
	go func() {
		defer wg.Done()
		handler.ServeHTTP(w1, req1)
	}()

	req2 := httptest.NewRequest("GET", "/replay?symbol=A", nil)
	req2.RemoteAddr = "10.0.0.1:12346"
	w2 := httptest.NewRecorder()
	wg.Add(1)
	go func() {
		defer wg.Done()
		handler.ServeHTTP(w2, req2)
	}()

	// Give goroutines time to acquire slots
	time.Sleep(10 * time.Millisecond)

	// Third from same IP should be rejected
	req3 := httptest.NewRequest("GET", "/replay?symbol=A", nil)
	req3.RemoteAddr = "10.0.0.1:12347"
	w3 := httptest.NewRecorder()
	handler.ServeHTTP(w3, req3)

	if w3.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", w3.Code)
	}

	// Release blocked queries
	close(block)
	wg.Wait()
}

func TestHandleReplay_FullExample(t *testing.T) {
	now := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)
	repo := &mockRepository{
		queryEventsFunc: func(_ context.Context, q eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
			events := []*eventlog.StoredEvent{
				{EventID: 1001, Timestamp: now, Symbol: "AAPL", EventType: "trade", Price: 250.25, Volume: 100},
				{EventID: 1002, Timestamp: now.Add(time.Second), Symbol: "AAPL", EventType: "trade", Price: 250.50, Volume: 200},
				{EventID: 1003, Timestamp: now.Add(2 * time.Second), Symbol: "AAPL", EventType: "quote", Price: 250.75, Bid: 250.50, Ask: 251.00},
			}
			next := eventlog.Cursor{Timestamp: now.Add(2 * time.Second), EventID: 1003}
			return &eventlog.ReplayResult{
				Events:     events,
				NextCursor: &next,
				HasMore:    true,
			}, nil
		},
	}

	handler := handleReplay(repo, testLimiter())
	req := httptest.NewRequest("GET", "/replay?symbol=AAPL&from=2026-06-24T10:00:00Z&to=2026-06-24T10:05:00Z&limit=3", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result eventlog.ReplayResult
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if len(result.Events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(result.Events))
	}
	if result.Events[0].Price != 250.25 {
		t.Errorf("expected first price 250.25, got %f", result.Events[0].Price)
	}
	if result.Events[2].EventType != "quote" {
		t.Errorf("expected third event type quote, got %s", result.Events[2].EventType)
	}
	if result.NextCursor == nil || result.NextCursor.EventID != 1003 {
		t.Errorf("expected next_cursor event_id 1003, got %v", result.NextCursor)
	}
	if !result.HasMore {
		t.Error("expected has_more=true")
	}
}

// ── ParseCursor Tests ──────────────────────────────────────────────────────

func TestParseCursor_Valid(t *testing.T) {
	now := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)
	s := now.Format(time.RFC3339Nano) + ",12345"
	cursor, err := parseCursor(s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cursor.Timestamp.Equal(now) {
		t.Errorf("expected timestamp %v, got %v", now, cursor.Timestamp)
	}
	if cursor.EventID != 12345 {
		t.Errorf("expected event_id 12345, got %d", cursor.EventID)
	}
}

func TestParseCursor_InvalidFormat(t *testing.T) {
	_, err := parseCursor("no-comma-here")
	if err == nil {
		t.Error("expected error for cursor without comma")
	}
}

func TestParseCursor_InvalidTimestamp(t *testing.T) {
	_, err := parseCursor("not-a-time,123")
	if err == nil {
		t.Error("expected error for invalid timestamp")
	}
}

func TestParseCursor_InvalidEventID(t *testing.T) {
	_, err := parseCursor("2026-06-24T10:00:00Z,abc")
	if err == nil {
		t.Error("expected error for invalid event_id")
	}
}

// ── Export Handler Tests ────────────────────────────────────────────────────

func TestHandleReplayExport_CSV(t *testing.T) {
	now := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)
	repo := &mockRepository{
		queryEventsFunc: func(_ context.Context, q eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
			return &eventlog.ReplayResult{
				Events: []*eventlog.StoredEvent{
					{EventID: 1, Timestamp: now, Symbol: "AAPL", EventType: "trade", Price: 250.25, Volume: 100},
					{EventID: 2, Timestamp: now.Add(time.Second), Symbol: "MSFT", EventType: "quote", Price: 300.00, Bid: 299.50, Ask: 300.50},
				},
				HasMore: false,
			}, nil
		},
	}

	handler := handleReplayExport(repo, testLimiter())
	req := httptest.NewRequest("GET", "/replay/export?symbol=AAPL&format=csv", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/csv") {
		t.Errorf("expected CSV content type, got %s", ct)
	}

	cd := w.Header().Get("Content-Disposition")
	if !strings.Contains(cd, "replay_export.csv") {
		t.Errorf("expected CSV filename in disposition, got %s", cd)
	}

	body := w.Body.String()
	if !strings.Contains(body, "event_id,timestamp,symbol") {
		t.Error("expected CSV header row")
	}
	if !strings.Contains(body, "AAPL") {
		t.Error("expected AAPL in CSV data")
	}
	if !strings.Contains(body, "MSFT") {
		t.Error("expected MSFT in CSV data")
	}
}

func TestHandleReplayExport_JSON(t *testing.T) {
	now := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)
	repo := &mockRepository{
		queryEventsFunc: func(_ context.Context, q eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
			return &eventlog.ReplayResult{
				Events: []*eventlog.StoredEvent{
					{EventID: 1, Timestamp: now, Symbol: "AAPL", EventType: "trade", Price: 250.25},
				},
				HasMore: false,
			}, nil
		},
	}

	handler := handleReplayExport(repo, testLimiter())
	req := httptest.NewRequest("GET", "/replay/export?format=json", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "application/x-ndjson") {
		t.Errorf("expected NDJSON content type, got %s", ct)
	}

	// Each line should be valid JSON
	lines := strings.Split(strings.TrimSpace(w.Body.String()), "\n")
	for i, line := range lines {
		var e eventlog.StoredEvent
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Errorf("line %d: invalid JSON: %v", i, err)
		}
	}
}

func TestHandleReplayExport_InvalidFormat(t *testing.T) {
	handler := handleReplayExport(&mockRepository{}, testLimiter())
	req := httptest.NewRequest("GET", "/replay/export?format=xml", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandleReplayExport_DefaultFormat(t *testing.T) {
	repo := &mockRepository{
		queryEventsFunc: func(_ context.Context, _ eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
			return &eventlog.ReplayResult{Events: []*eventlog.StoredEvent{}}, nil
		},
	}

	handler := handleReplayExport(repo, testLimiter())
	req := httptest.NewRequest("GET", "/replay/export", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// Default should be CSV
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/csv") {
		t.Errorf("expected CSV content type by default, got %s", ct)
	}
}

func TestHandleReplayExport_ConcurrencyLimiting(t *testing.T) {
	block := make(chan struct{})
	repo := &mockRepository{
		queryEventsFunc: func(_ context.Context, _ eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
			<-block
			return &eventlog.ReplayResult{Events: []*eventlog.StoredEvent{}}, nil
		},
	}

	limiter := newConcurrencyLimiter(2)
	handler := handleReplayExport(repo, limiter)

	var wg sync.WaitGroup

	req1 := httptest.NewRequest("GET", "/replay/export", nil)
	req1.RemoteAddr = "10.0.0.1:12345"
	w1 := httptest.NewRecorder()
	wg.Add(1)
	go func() {
		defer wg.Done()
		handler.ServeHTTP(w1, req1)
	}()

	req2 := httptest.NewRequest("GET", "/replay/export", nil)
	req2.RemoteAddr = "10.0.0.1:12346"
	w2 := httptest.NewRecorder()
	wg.Add(1)
	go func() {
		defer wg.Done()
		handler.ServeHTTP(w2, req2)
	}()

	time.Sleep(10 * time.Millisecond)

	req3 := httptest.NewRequest("GET", "/replay/export", nil)
	req3.RemoteAddr = "10.0.0.1:12347"
	w3 := httptest.NewRecorder()
	handler.ServeHTTP(w3, req3)

	if w3.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", w3.Code)
	}

	close(block)
	wg.Wait()
}

func TestHandleReplayExport_Pagination(t *testing.T) {
	now := time.Date(2026, 6, 24, 10, 0, 0, 0, time.UTC)
	callCount := 0
	repo := &mockRepository{
		queryEventsFunc: func(_ context.Context, q eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
			callCount++
			if callCount == 1 {
				next := eventlog.Cursor{Timestamp: now.Add(time.Second), EventID: 2}
				return &eventlog.ReplayResult{
					Events: []*eventlog.StoredEvent{
						{EventID: 1, Timestamp: now, Symbol: "AAPL"},
						{EventID: 2, Timestamp: now.Add(time.Second), Symbol: "AAPL"},
					},
					NextCursor: &next,
					HasMore:    true,
				}, nil
			}
			return &eventlog.ReplayResult{
				Events: []*eventlog.StoredEvent{
					{EventID: 3, Timestamp: now.Add(2 * time.Second), Symbol: "AAPL"},
				},
				HasMore: false,
			}, nil
		},
	}

	handler := handleReplayExport(repo, testLimiter())
	req := httptest.NewRequest("GET", "/replay/export?format=csv", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	if callCount != 2 {
		t.Errorf("expected 2 page fetches, got %d", callCount)
	}

	lines := strings.Split(strings.TrimSpace(w.Body.String()), "\n")
	// header + 3 data rows
	if len(lines) != 4 {
		t.Errorf("expected 4 lines (header + 3 rows), got %d", len(lines))
	}
}

// ── Concurrency Limiter Tests ──────────────────────────────────────────────

func TestConcurrencyLimiter_AcquireRelease(t *testing.T) {
	cl := newConcurrencyLimiter(2)

	if !cl.acquire("client1") {
		t.Error("first acquire should succeed")
	}
	if !cl.acquire("client1") {
		t.Error("second acquire should succeed")
	}
	if cl.acquire("client1") {
		t.Error("third acquire should fail (limit=2)")
	}

	cl.release("client1")
	if !cl.acquire("client1") {
		t.Error("acquire after release should succeed")
	}
}

func TestConcurrencyLimiter_DifferentClients(t *testing.T) {
	cl := newConcurrencyLimiter(1)

	if !cl.acquire("client1") {
		t.Error("client1 acquire should succeed")
	}
	if !cl.acquire("client2") {
		t.Error("client2 acquire should succeed (different client)")
	}
}

// testError is a simple error type for testing.
type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}
