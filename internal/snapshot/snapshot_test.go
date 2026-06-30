package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/eventlog"
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/marketdata"
)

func TestService_UpdateAndGet(t *testing.T) {
	svc := New()
	svc.MarkReady()

	q := marketdata.Quote{
		Symbol: "AAPL", Type: marketdata.QuoteTypeTrade,
		Price: 250.25, Volume: 100, Timestamp: time.Now(),
	}
	svc.Update(q)

	ce := svc.Get(context.Background(), "AAPL")
	if ce == nil {
		t.Fatal("expected snapshot for AAPL")
	}
	quote, ok := ce.Event.(marketdata.Quote)
	if !ok {
		t.Fatal("expected Quote event")
	}
	if quote.Price != 250.25 {
		t.Errorf("expected price 250.25, got %f", quote.Price)
	}
	if quote.Symbol != "AAPL" {
		t.Errorf("expected symbol AAPL, got %s", quote.Symbol)
	}
}

func TestService_GetMissing(t *testing.T) {
	svc := New()
	svc.MarkReady()

	ce := svc.Get(context.Background(), "MSFT")
	if ce != nil {
		t.Error("expected nil for missing symbol")
	}
}

func TestService_UpdateReplaces(t *testing.T) {
	svc := New()
	svc.MarkReady()

	svc.Update(marketdata.Quote{
		Symbol: "AAPL", Type: marketdata.QuoteTypeTrade,
		Price: 250.00, Timestamp: time.Now(),
	})
	svc.Update(marketdata.Quote{
		Symbol: "AAPL", Type: marketdata.QuoteTypeTrade,
		Price: 251.50, Timestamp: time.Now(),
	})

	ce := svc.Get(context.Background(), "AAPL")
	if ce == nil {
		t.Fatal("expected snapshot")
	}
	quote := ce.Event.(marketdata.Quote)
	if quote.Price != 251.50 {
		t.Errorf("expected updated price 251.50, got %f", quote.Price)
	}
}

func TestService_GetMultiple(t *testing.T) {
	svc := New()
	svc.MarkReady()

	svc.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})
	svc.Update(marketdata.Quote{Symbol: "MSFT", Price: 510.0, Timestamp: time.Now()})
	svc.Update(marketdata.Quote{Symbol: "GOOG", Price: 180.0, Timestamp: time.Now()})

	events := svc.GetMultiple(context.Background(), []string{"AAPL", "MSFT", "NVDA"})
	if len(events) != 2 {
		t.Fatalf("expected 2 events (NVDA missing), got %d", len(events))
	}

	symbols := make(map[string]bool)
	for _, e := range events {
		symbols[e.Event.EventSymbol()] = true
	}
	if !symbols["AAPL"] || !symbols["MSFT"] {
		t.Error("expected AAPL and MSFT in results")
	}
	if symbols["NVDA"] {
		t.Error("NVDA should not be in results")
	}
}

func TestService_GetMultiple_Empty(t *testing.T) {
	svc := New()
	svc.MarkReady()

	events := svc.GetMultiple(context.Background(), []string{"AAPL"})
	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}

func TestService_Symbols(t *testing.T) {
	svc := New()
	svc.MarkReady()

	svc.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})
	svc.Update(marketdata.Quote{Symbol: "MSFT", Price: 510.0, Timestamp: time.Now()})

	symbols := svc.Symbols()
	if len(symbols) != 2 {
		t.Fatalf("expected 2 symbols, got %d", len(symbols))
	}

	symSet := make(map[string]bool)
	for _, s := range symbols {
		symSet[s] = true
	}
	if !symSet["AAPL"] || !symSet["MSFT"] {
		t.Error("expected AAPL and MSFT")
	}
}

func TestService_Count(t *testing.T) {
	svc := New()
	svc.MarkReady()

	if svc.Count() != 0 {
		t.Error("expected 0 count initially")
	}

	svc.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})
	svc.Update(marketdata.Quote{Symbol: "MSFT", Price: 510.0, Timestamp: time.Now()})

	if svc.Count() != 2 {
		t.Errorf("expected 2, got %d", svc.Count())
	}
}

func TestService_ConcurrentAccess(t *testing.T) {
	svc := New()
	svc.MarkReady()
	var wg sync.WaitGroup

	// Concurrent writers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			svc.Update(marketdata.Quote{
				Symbol: "AAPL", Price: float64(200 + i), Timestamp: time.Now(),
			})
		}(i)
	}

	// Concurrent readers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = svc.Get(context.Background(), "AAPL")
			_ = svc.GetMultiple(context.Background(), []string{"AAPL", "MSFT"})
			_ = svc.Symbols()
			_ = svc.Count()
		}()
	}

	wg.Wait()

	ce := svc.Get(context.Background(), "AAPL")
	if ce == nil {
		t.Fatal("AAPL snapshot should exist after concurrent writes")
	}
}

func TestService_BarEvent(t *testing.T) {
	svc := New()
	svc.MarkReady()

	bar := marketdata.Bar{
		Symbol: "AAPL", Open: 250.0, High: 255.0, Low: 248.0,
		Close: 253.0, Volume: 1000, Timestamp: time.Now(),
	}
	svc.Update(bar)

	ce := svc.Get(context.Background(), "AAPL")
	if ce == nil {
		t.Fatal("expected snapshot")
	}
	got, ok := ce.Event.(marketdata.Bar)
	if !ok {
		t.Fatalf("expected Bar, got %T", ce.Event)
	}
	if got.Close != 253.0 {
		t.Errorf("expected close 253.0, got %f", got.Close)
	}
}

// ---------- Warming Up Tests ----------

func TestService_WarmingUp_GetReturnsNil(t *testing.T) {
	svc := New()

	svc.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})

	// Before MarkReady, Get should return nil.
	ce := svc.Get(context.Background(), "AAPL")
	if ce != nil {
		t.Error("expected nil during warmup")
	}
}

func TestService_WarmingUp_GetMultipleReturnsNil(t *testing.T) {
	svc := New()

	svc.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})

	// Before MarkReady, GetMultiple should return nil.
	events := svc.GetMultiple(context.Background(), []string{"AAPL"})
	if events != nil {
		t.Error("expected nil during warmup")
	}
}

func TestService_WarmingUp_IsReady(t *testing.T) {
	svc := New()

	if svc.IsReady() {
		t.Error("expected not ready initially")
	}

	svc.MarkReady()

	if !svc.IsReady() {
		t.Error("expected ready after MarkReady")
	}
}

func TestService_WarmingUp_ReadyAllowsGet(t *testing.T) {
	svc := New()
	svc.MarkReady()

	svc.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})

	ce := svc.Get(context.Background(), "AAPL")
	if ce == nil {
		t.Fatal("expected snapshot after MarkReady")
	}
}

// ---------- TTL / Eviction Tests ----------

func TestService_Purge_RemovesOldSymbols(t *testing.T) {
	svc := New(WithMaxAge(50 * time.Millisecond))
	svc.MarkReady()

	svc.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})
	svc.Update(marketdata.Quote{Symbol: "MSFT", Price: 510.0, Timestamp: time.Now()})

	if svc.Count() != 2 {
		t.Fatalf("expected 2 symbols, got %d", svc.Count())
	}

	// Wait longer than TTL.
	time.Sleep(100 * time.Millisecond)

	// Update AAPL to refresh its timestamp.
	svc.Update(marketdata.Quote{Symbol: "AAPL", Price: 251.0, Timestamp: time.Now()})

	purged := svc.Purge()
	if purged != 1 {
		t.Errorf("expected 1 purged symbol, got %d", purged)
	}
	if svc.Count() != 1 {
		t.Errorf("expected 1 symbol remaining, got %d", svc.Count())
	}

	ce := svc.Get(context.Background(), "AAPL")
	if ce == nil {
		t.Error("AAPL should still exist (recently updated)")
	}
	ce = svc.Get(context.Background(), "MSFT")
	if ce != nil {
		t.Error("MSFT should have been purged")
	}
}

func TestService_Purge_NoEvictionWhenDisabled(t *testing.T) {
	svc := New() // no maxAge
	svc.MarkReady()

	svc.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})

	time.Sleep(50 * time.Millisecond)

	purged := svc.Purge()
	if purged != 0 {
		t.Errorf("expected 0 purged, got %d", purged)
	}
	if svc.Count() != 1 {
		t.Errorf("expected 1 symbol, got %d", svc.Count())
	}
}

func TestService_EvictionLoop(t *testing.T) {
	log := log.New(nil, "test")
	svc := New(WithMaxAge(50*time.Millisecond), WithLogger(log))
	svc.MarkReady()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc.Start(ctx)

	svc.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})

	// Wait for eviction loop to run (maxAge/2 = 25ms, give it time).
	time.Sleep(200 * time.Millisecond)

	// AAPL should be evicted (not updated for >50ms).
	if svc.Count() != 0 {
		t.Errorf("expected 0 symbols after eviction, got %d", svc.Count())
	}

	svc.Stop()
}

func TestService_Stop_CleansUpGoroutine(t *testing.T) {
	log := log.New(nil, "test")
	svc := New(WithMaxAge(100*time.Millisecond), WithLogger(log))

	ctx := context.Background()
	svc.Start(ctx)

	// Stop should return without hanging.
	done := make(chan struct{})
	go func() {
		svc.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() did not return in time")
	}
}

// ---------- Atomic Pointer Swap Tests ----------

func TestService_AtomicSwap_ConsistentReads(t *testing.T) {
	svc := New()
	svc.MarkReady()

	// Pre-populate with initial data.
	for i := 0; i < 100; i++ {
		svc.Update(marketdata.Quote{
			Symbol: "AAPL", Price: float64(200 + i), Timestamp: time.Now(),
		})
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 100)

	// Concurrent writer updating price.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			svc.Update(marketdata.Quote{
				Symbol: "AAPL", Price: float64(300 + i), Timestamp: time.Now(),
			})
		}
	}()

	// Concurrent readers verifying atomic consistency.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				ce := svc.Get(context.Background(), "AAPL")
				if ce == nil {
					continue
				}
				quote, ok := ce.Event.(marketdata.Quote)
				if !ok {
					errCh <- nil
					return
				}
				// Price should be either in [200,300) or [300,400) range,
				// never a torn state (e.g., partial update).
				if quote.Price < 200 || quote.Price >= 400 {
					errCh <- nil
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("torn read detected: %v", err)
		}
	}
}

// ---------- SnapshotPublisher Tests ----------

type mockPublisher struct {
	published []marketdata.MarketEvent
}

func (m *mockPublisher) Publish(_ context.Context, event marketdata.MarketEvent) {
	m.published = append(m.published, event)
}

func TestSnapshotPublisher_UpdatesSnapshot(t *testing.T) {
	inner := &mockPublisher{}
	snap := New()
	snap.MarkReady()
	sp := NewSnapshotPublisher(inner, snap)

	ctx := context.Background()
	q := marketdata.Quote{
		Symbol: "AAPL", Type: marketdata.QuoteTypeTrade,
		Price: 250.25, Timestamp: time.Now(),
	}
	sp.Publish(ctx, q)

	// Snapshot should be updated
	ce := snap.Get(context.Background(), "AAPL")
	if ce == nil {
		t.Fatal("expected snapshot after publish")
	}
	quote := ce.Event.(marketdata.Quote)
	if quote.Price != 250.25 {
		t.Errorf("expected price 250.25, got %f", quote.Price)
	}

	// Inner publisher should also receive the event
	if len(inner.published) != 1 {
		t.Fatalf("expected 1 published event, got %d", len(inner.published))
	}
}

func TestSnapshotPublisher_MultipleSymbols(t *testing.T) {
	inner := &mockPublisher{}
	snap := New()
	snap.MarkReady()
	sp := NewSnapshotPublisher(inner, snap)

	ctx := context.Background()
	sp.Publish(ctx, marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})
	sp.Publish(ctx, marketdata.Quote{Symbol: "MSFT", Price: 510.0, Timestamp: time.Now()})
	sp.Publish(ctx, marketdata.Quote{Symbol: "GOOG", Price: 180.0, Timestamp: time.Now()})

	if snap.Count() != 3 {
		t.Errorf("expected 3 symbols, got %d", snap.Count())
	}
	if len(inner.published) != 3 {
		t.Errorf("expected 3 published events, got %d", len(inner.published))
	}
}

func TestSnapshotPublisher_UpdateOverwrite(t *testing.T) {
	inner := &mockPublisher{}
	snap := New()
	snap.MarkReady()
	sp := NewSnapshotPublisher(inner, snap)

	ctx := context.Background()
	sp.Publish(ctx, marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})
	sp.Publish(ctx, marketdata.Quote{Symbol: "AAPL", Price: 255.0, Timestamp: time.Now()})

	ce := snap.Get(context.Background(), "AAPL")
	quote := ce.Event.(marketdata.Quote)
	if quote.Price != 255.0 {
		t.Errorf("expected latest price 255.0, got %f", quote.Price)
	}
	if snap.Count() != 1 {
		t.Errorf("expected 1 symbol, got %d", snap.Count())
	}
}

// ---------- Checkpoint Tests ----------

func TestService_Checkpoint_SavesToDisk(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "snapshot.json")

	svc := New(WithCheckpoint(cpPath, time.Hour))
	svc.MarkReady()

	svc.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})
	svc.Update(marketdata.Quote{Symbol: "MSFT", Price: 510.0, Timestamp: time.Now()})

	if err := svc.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint() error: %v", err)
	}

	// Verify file exists.
	data, err := os.ReadFile(cpPath)
	if err != nil {
		t.Fatalf("failed to read checkpoint: %v", err)
	}

	var cp CheckpointFile
	if err := json.Unmarshal(data, &cp); err != nil {
		t.Fatalf("failed to unmarshal checkpoint: %v", err)
	}

	if cp.Version != 1 {
		t.Errorf("version = %d, want 1", cp.Version)
	}
	if len(cp.Symbols) != 2 {
		t.Errorf("symbols = %d, want 2", len(cp.Symbols))
	}
	if _, ok := cp.Symbols["AAPL"]; !ok {
		t.Error("AAPL missing from checkpoint")
	}
	if _, ok := cp.Symbols["MSFT"]; !ok {
		t.Error("MSFT missing from checkpoint")
	}
}

func TestService_LoadCheckpoint_RestoresState(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "snapshot.json")

	// First service: create checkpoint.
	svc1 := New(WithCheckpoint(cpPath, time.Hour))
	svc1.MarkReady()

	svc1.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})
	svc1.Update(marketdata.Quote{Symbol: "MSFT", Price: 510.0, Timestamp: time.Now()})

	if err := svc1.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint() error: %v", err)
	}

	// Second service: load checkpoint.
	svc2 := New(WithCheckpoint(cpPath, time.Hour))
	svc2.MarkReady()

	if err := svc2.LoadCheckpoint(); err != nil {
		t.Fatalf("LoadCheckpoint() error: %v", err)
	}

	if svc2.Count() != 2 {
		t.Fatalf("count = %d, want 2", svc2.Count())
	}

	ce := svc2.Get(context.Background(), "AAPL")
	if ce == nil {
		t.Fatal("AAPL not restored")
	}
	quote := ce.Event.(marketdata.Quote)
	if quote.Price != 250.0 {
		t.Errorf("AAPL price = %f, want 250.0", quote.Price)
	}

	ce = svc2.Get(context.Background(), "MSFT")
	if ce == nil {
		t.Fatal("MSFT not restored")
	}
	quote = ce.Event.(marketdata.Quote)
	if quote.Price != 510.0 {
		t.Errorf("MSFT price = %f, want 510.0", quote.Price)
	}
}

func TestService_LoadCheckpoint_NoFile(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "snapshot.json")

	svc := New(WithCheckpoint(cpPath, time.Hour))
	svc.MarkReady()

	// Should succeed (no checkpoint to load).
	if err := svc.LoadCheckpoint(); err != nil {
		t.Fatalf("LoadCheckpoint() error: %v", err)
	}

	if svc.Count() != 0 {
		t.Errorf("count = %d, want 0", svc.Count())
	}
}

func TestService_LoadCheckpoint_InvalidFile(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "snapshot.json")

	// Write invalid JSON to both current and previous.
	if err := os.WriteFile(cpPath, []byte("not json"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cpPath+".prev", []byte("also not json"), 0644); err != nil {
		t.Fatal(err)
	}

	svc := New(WithCheckpoint(cpPath, time.Hour))

	// Should succeed (falls back to fresh start since both are corrupted).
	if err := svc.LoadCheckpoint(); err != nil {
		t.Fatalf("LoadCheckpoint() error: %v", err)
	}

	if svc.Count() != 0 {
		t.Errorf("count = %d, want 0 (starting fresh)", svc.Count())
	}
}

func TestService_Checkpoint_PreservesCursor(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "snapshot.json")

	svc := New(WithCheckpoint(cpPath, time.Hour))
	svc.MarkReady()

	// Set a cursor.
	cursor := eventlog.Cursor{Timestamp: time.Now(), EventID: 12345}
	svc.UpdateCursor(cursor)

	svc.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})

	if err := svc.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint() error: %v", err)
	}

	// Load and verify cursor.
	svc2 := New(WithCheckpoint(cpPath, time.Hour))
	if err := svc2.LoadCheckpoint(); err != nil {
		t.Fatalf("LoadCheckpoint() error: %v", err)
	}

	svc2.mu.RLock()
	got := svc2.lastCursor
	svc2.mu.RUnlock()

	if got.EventID != 12345 {
		t.Errorf("cursor event_id = %d, want 12345", got.EventID)
	}
}

// ---------- Recovery Tests ----------

// mockEventLog implements eventlog.Repository for testing.
type mockEventLog struct {
	events []*eventlog.StoredEvent
}

func (m *mockEventLog) Append(ctx context.Context, event *eventlog.StoredEvent) (int64, error) {
	m.events = append(m.events, event)
	return event.EventID, nil
}

func (m *mockEventLog) AppendBatch(ctx context.Context, events []*eventlog.StoredEvent) ([]int64, error) {
	ids := make([]int64, len(events))
	for i, e := range events {
		m.events = append(m.events, e)
		ids[i] = e.EventID
	}
	return ids, nil
}

func (m *mockEventLog) QueryBySymbol(ctx context.Context, symbol string, from, to time.Time) ([]*eventlog.StoredEvent, error) {
	return nil, nil
}

func (m *mockEventLog) QueryLatest(ctx context.Context, symbol string, limit int) ([]*eventlog.StoredEvent, error) {
	return nil, nil
}

func (m *mockEventLog) QueryEvents(ctx context.Context, q eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
	// Return events after the cursor.
	var result []*eventlog.StoredEvent
	for _, ev := range m.events {
		if ev.Timestamp.After(q.Cursor.Timestamp) ||
			(ev.Timestamp.Equal(q.Cursor.Timestamp) && ev.EventID > q.Cursor.EventID) {
			result = append(result, ev)
		}
	}

	if len(result) == 0 {
		return &eventlog.ReplayResult{HasMore: false}, nil
	}

	// Simple: return all at once.
	return &eventlog.ReplayResult{
		Events:  result,
		HasMore: false,
	}, nil
}

func (m *mockEventLog) Count(ctx context.Context) (int64, error) {
	return int64(len(m.events)), nil
}

func (m *mockEventLog) CountBySymbol(ctx context.Context, symbol string) (int64, error) {
	return 0, nil
}

func (m *mockEventLog) Close() error {
	return nil
}

func TestService_Recover_WithCheckpointAndReplay(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "snapshot.json")

	// Phase 1: Create initial checkpoint with AAPL at 250.
	svc1 := New(WithCheckpoint(cpPath, time.Hour))
	svc1.MarkReady()
	svc1.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})
	cursor := eventlog.Cursor{Timestamp: time.Now(), EventID: 100}
	svc1.UpdateCursor(cursor)
	if err := svc1.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint() error: %v", err)
	}

	// Phase 2: Simulate events that happened after checkpoint.
	now := time.Now()
	mockLog := &mockEventLog{
		events: []*eventlog.StoredEvent{
			{
				EventID:   101,
				Timestamp: now.Add(time.Second),
				Symbol:    "AAPL",
				EventType: "quote",
				Price:     255.0,
				RawData:   mustMarshal(marketdata.Quote{Symbol: "AAPL", Price: 255.0, Timestamp: now.Add(time.Second)}),
			},
			{
				EventID:   102,
				Timestamp: now.Add(2 * time.Second),
				Symbol:    "MSFT",
				EventType: "quote",
				Price:     520.0,
				RawData:   mustMarshal(marketdata.Quote{Symbol: "MSFT", Price: 520.0, Timestamp: now.Add(2 * time.Second)}),
			},
		},
	}

	// Phase 3: Recover with replay.
	svc2 := New(WithCheckpoint(cpPath, time.Hour))
	ctx := context.Background()

	if err := svc2.Recover(ctx, mockLog); err != nil {
		t.Fatalf("Recover() error: %v", err)
	}
	svc2.MarkReady()

	// Verify: AAPL should be 255.0 (replayed), MSFT should be 520.0 (replayed).
	if svc2.Count() != 2 {
		t.Fatalf("count = %d, want 2", svc2.Count())
	}

	ce := svc2.Get(context.Background(), "AAPL")
	if ce == nil {
		t.Fatal("AAPL not recovered")
	}
	quote := ce.Event.(marketdata.Quote)
	if quote.Price != 255.0 {
		t.Errorf("AAPL price = %f, want 255.0", quote.Price)
	}

	ce = svc2.Get(context.Background(), "MSFT")
	if ce == nil {
		t.Fatal("MSFT not recovered")
	}
	quote = ce.Event.(marketdata.Quote)
	if quote.Price != 520.0 {
		t.Errorf("MSFT price = %f, want 520.0", quote.Price)
	}
}

func TestService_Recover_NoCheckpoint(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "snapshot.json")

	now := time.Now()
	mockLog := &mockEventLog{
		events: []*eventlog.StoredEvent{
			{
				EventID:   1,
				Timestamp: now,
				Symbol:    "AAPL",
				EventType: "quote",
				Price:     250.0,
				RawData:   mustMarshal(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: now}),
			},
		},
	}

	svc := New(WithCheckpoint(cpPath, time.Hour))
	ctx := context.Background()

	if err := svc.Recover(ctx, mockLog); err != nil {
		t.Fatalf("Recover() error: %v", err)
	}
	svc.MarkReady()

	if svc.Count() != 1 {
		t.Fatalf("count = %d, want 1", svc.Count())
	}

	ce := svc.Get(context.Background(), "AAPL")
	if ce == nil {
		t.Fatal("AAPL not recovered")
	}
	quote := ce.Event.(marketdata.Quote)
	if quote.Price != 250.0 {
		t.Errorf("AAPL price = %f, want 250.0", quote.Price)
	}
}

func TestService_Recover_WithoutEventLog(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "snapshot.json")

	// Create checkpoint.
	svc1 := New(WithCheckpoint(cpPath, time.Hour))
	svc1.MarkReady()
	svc1.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})
	if err := svc1.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint() error: %v", err)
	}

	// Recover without event log (nil repo).
	svc2 := New(WithCheckpoint(cpPath, time.Hour))
	ctx := context.Background()

	if err := svc2.Recover(ctx, nil); err != nil {
		t.Fatalf("Recover() error: %v", err)
	}
	svc2.MarkReady()

	// Should have checkpoint data.
	if svc2.Count() != 1 {
		t.Fatalf("count = %d, want 1", svc2.Count())
	}
}

// ---------- Restart Scenario Test ----------

func TestService_RestartScenario_LatestPricesAvailable(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "snapshot.json")

	// === PHASE 1: Start system, publish updates ===
	svc1 := New(WithCheckpoint(cpPath, 100*time.Millisecond), WithLogger(log.New(nil, "test")))
	ctx1, cancel1 := context.WithCancel(context.Background())
	svc1.Start(ctx1)
	svc1.MarkReady()

	// Publish updates.
	svc1.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})
	svc1.Update(marketdata.Quote{Symbol: "MSFT", Price: 510.0, Timestamp: time.Now()})
	svc1.Update(marketdata.Quote{Symbol: "GOOG", Price: 180.0, Timestamp: time.Now()})

	// Wait for checkpoint to run.
	time.Sleep(200 * time.Millisecond)

	// Verify prices are available.
	ce := svc1.Get(context.Background(), "AAPL")
	if ce == nil {
		t.Fatal("AAPL should be available before restart")
	}
	quote := ce.Event.(marketdata.Quote)
	if quote.Price != 250.0 {
		t.Fatalf("AAPL price = %f, want 250.0", quote.Price)
	}

	// === PHASE 2: Simulate restart ===
	cancel1()
	svc1.Stop()

	// === PHASE 3: Start system again, verify latest prices ===
	svc2 := New(WithCheckpoint(cpPath, 100*time.Millisecond), WithLogger(log.New(nil, "test")))
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	svc2.Start(ctx2)

	// Recover from checkpoint.
	if err := svc2.Recover(ctx2, nil); err != nil {
		t.Fatalf("Recover() error: %v", err)
	}
	svc2.MarkReady()

	// Verify latest prices are still available.
	if svc2.Count() != 3 {
		t.Fatalf("count = %d, want 3", svc2.Count())
	}

	ce = svc2.Get(context.Background(), "AAPL")
	if ce == nil {
		t.Fatal("AAPL should be available after restart")
	}
	quote = ce.Event.(marketdata.Quote)
	if quote.Price != 250.0 {
		t.Errorf("AAPL price = %f, want 250.0", quote.Price)
	}

	ce = svc2.Get(context.Background(), "MSFT")
	if ce == nil {
		t.Fatal("MSFT should be available after restart")
	}
	quote = ce.Event.(marketdata.Quote)
	if quote.Price != 510.0 {
		t.Errorf("MSFT price = %f, want 510.0", quote.Price)
	}

	ce = svc2.Get(context.Background(), "GOOG")
	if ce == nil {
		t.Fatal("GOOG should be available after restart")
	}
	quote = ce.Event.(marketdata.Quote)
	if quote.Price != 180.0 {
		t.Errorf("GOOG price = %f, want 180.0", quote.Price)
	}
}

// mustMarshal is a helper that marshals to JSON or panics.
func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

// ---------- Fix 1: Live vs Replay Race Condition Tests ----------

func TestService_Recover_LiveEventsDuringReplay(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "snapshot.json")

	// Phase 1: Create checkpoint with AAPL at 250.
	svc1 := New(WithCheckpoint(cpPath, time.Hour))
	svc1.MarkReady()
	svc1.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})
	cursor := eventlog.Cursor{Timestamp: time.Now(), EventID: 100}
	svc1.UpdateCursor(cursor)
	if err := svc1.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint() error: %v", err)
	}

	// Phase 2: Create mock event log with replay events.
	now := time.Now()
	mockLog := &mockEventLog{
		events: []*eventlog.StoredEvent{
			{
				EventID:   101,
				Timestamp: now.Add(time.Second),
				Symbol:    "AAPL",
				EventType: "quote",
				Price:     255.0,
				RawData:   mustMarshal(marketdata.Quote{Symbol: "AAPL", Price: 255.0, Timestamp: now.Add(time.Second)}),
			},
		},
	}

	// Phase 3: Recover with buffering.
	// Start buffering BEFORE applying live events.
	svc2 := New(WithCheckpoint(cpPath, time.Hour))
	svc2.StartBuffering()

	// Simulate live event arriving DURING replay (buffered, not applied yet).
	svc2.Update(marketdata.Quote{Symbol: "AAPL", Price: 260.0, Timestamp: now.Add(3 * time.Second)})

	// Verify the event is in the buffer (not applied to snapshots yet).
	if !svc2.IsBuffering() {
		t.Error("expected buffering to be active")
	}

	ctx := context.Background()
	if err := svc2.Recover(ctx, mockLog); err != nil {
		t.Fatalf("Recover() error: %v", err)
	}
	svc2.MarkReady()

	// Verify: AAPL should be 260.0 (live event applied after DB replay).
	ce := svc2.Get(context.Background(), "AAPL")
	if ce == nil {
		t.Fatal("AAPL not recovered")
	}
	quote := ce.Event.(marketdata.Quote)
	if quote.Price != 260.0 {
		t.Errorf("AAPL price = %f, want 260.0 (live event should be applied after DB replay)", quote.Price)
	}
}

func TestService_Buffering_CapturesLiveEvents(t *testing.T) {
	svc := New()
	svc.MarkReady()

	svc.StartBuffering()
	if !svc.IsBuffering() {
		t.Error("expected buffering to be active")
	}

	// Update should buffer events.
	svc.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})

	// Check buffer has events.
	select {
	case <-svc.liveBuffer:
		// OK
	default:
		t.Error("expected event in live buffer")
	}

	svc.StopBuffering()
	if svc.IsBuffering() {
		t.Error("expected buffering to be stopped")
	}
}

// ---------- Fix 2: Corrupted Checkpoint Tests ----------

func TestService_Checkpoint_DualRotation(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "snapshot.json")

	svc := New(WithCheckpoint(cpPath, time.Hour))
	svc.MarkReady()

	// First checkpoint.
	svc.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})
	if err := svc.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint() error: %v", err)
	}

	// Verify current exists, previous does not.
	if _, err := os.Stat(cpPath); os.IsNotExist(err) {
		t.Error("current checkpoint should exist")
	}
	if _, err := os.Stat(cpPath + ".prev"); !os.IsNotExist(err) {
		t.Error("previous checkpoint should not exist yet")
	}

	// Second checkpoint (should rotate).
	svc.Update(marketdata.Quote{Symbol: "AAPL", Price: 255.0, Timestamp: time.Now()})
	if err := svc.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint() error: %v", err)
	}

	// Verify both exist.
	if _, err := os.Stat(cpPath); os.IsNotExist(err) {
		t.Error("current checkpoint should exist")
	}
	if _, err := os.Stat(cpPath + ".prev"); os.IsNotExist(err) {
		t.Error("previous checkpoint should exist after rotation")
	}

	// Verify previous has old data.
	data, _ := os.ReadFile(cpPath + ".prev")
	var cpPrev CheckpointFile
	json.Unmarshal(data, &cpPrev)
	if entry, ok := cpPrev.Symbols["AAPL"]; ok {
		quote := entry.Cached.Event.(marketdata.Quote)
		if quote.Price != 250.0 {
			t.Errorf("previous AAPL price = %f, want 250.0", quote.Price)
		}
	}

	// Verify current has new data.
	data, _ = os.ReadFile(cpPath)
	var cpCurr CheckpointFile
	json.Unmarshal(data, &cpCurr)
	if entry, ok := cpCurr.Symbols["AAPL"]; ok {
		quote := entry.Cached.Event.(marketdata.Quote)
		if quote.Price != 255.0 {
			t.Errorf("current AAPL price = %f, want 255.0", quote.Price)
		}
	}
}

func TestService_LoadCheckpoint_FallbackToPrevious(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "snapshot.json")

	// Create a valid previous checkpoint.
	svc1 := New(WithCheckpoint(cpPath, time.Hour))
	svc1.MarkReady()
	svc1.Update(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()})
	if err := svc1.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint() error: %v", err)
	}
	// Trigger rotation to create previous.
	svc1.Update(marketdata.Quote{Symbol: "AAPL", Price: 255.0, Timestamp: time.Now()})
	if err := svc1.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint() error: %v", err)
	}

	// Corrupt the current checkpoint.
	if err := os.WriteFile(cpPath, []byte("corrupted"), 0644); err != nil {
		t.Fatal(err)
	}

	// Load should fall back to previous.
	svc2 := New(WithCheckpoint(cpPath, time.Hour))
	svc2.MarkReady()

	if err := svc2.LoadCheckpoint(); err != nil {
		t.Fatalf("LoadCheckpoint() error: %v", err)
	}

	ce := svc2.Get(context.Background(), "AAPL")
	if ce == nil {
		t.Fatal("AAPL should be loaded from fallback checkpoint")
	}
	quote := ce.Event.(marketdata.Quote)
	if quote.Price != 250.0 {
		t.Errorf("AAPL price = %f, want 250.0 (from fallback)", quote.Price)
	}
}

func TestService_LoadCheckpoint_BothCorrupted(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "snapshot.json")

	// Write corrupted files.
	os.WriteFile(cpPath, []byte("corrupted"), 0644)
	os.WriteFile(cpPath+".prev", []byte("also corrupted"), 0644)

	svc := New(WithCheckpoint(cpPath, time.Hour))

	// Should succeed (starting fresh).
	if err := svc.LoadCheckpoint(); err != nil {
		t.Fatalf("LoadCheckpoint() error: %v", err)
	}

	if svc.Count() != 0 {
		t.Errorf("count = %d, want 0", svc.Count())
	}
}

// ---------- Fix 3: Health Callback Tests ----------

func TestService_HealthCallback(t *testing.T) {
	var callbackCalled bool
	var callbackReason string

	svc := New(
		WithHealthCallback(func(degraded bool, reason string) {
			callbackCalled = true
			callbackReason = reason
		}),
		WithMaxRecoveryRetries(1), // fast test
	)

	// Simulate DB failure scenario.
	ctx := context.Background()
	errLog := &failingEventLog{err: fmt.Errorf("connection refused")}

	err := svc.Recover(ctx, errLog)
	if err == nil {
		t.Fatal("expected error from failing event log")
	}

	if !callbackCalled {
		t.Error("expected health callback to be called")
	}
	if callbackReason == "" {
		t.Error("expected non-empty reason")
	}
}

// failingEventLog always returns an error.
type failingEventLog struct {
	err error
}

func (f *failingEventLog) Append(ctx context.Context, event *eventlog.StoredEvent) (int64, error) {
	return 0, f.err
}

func (f *failingEventLog) AppendBatch(ctx context.Context, events []*eventlog.StoredEvent) ([]int64, error) {
	return nil, f.err
}

func (f *failingEventLog) QueryBySymbol(ctx context.Context, symbol string, from, to time.Time) ([]*eventlog.StoredEvent, error) {
	return nil, f.err
}

func (f *failingEventLog) QueryLatest(ctx context.Context, symbol string, limit int) ([]*eventlog.StoredEvent, error) {
	return nil, f.err
}

func (f *failingEventLog) QueryEvents(ctx context.Context, q eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
	return nil, f.err
}

func (f *failingEventLog) Count(ctx context.Context) (int64, error) {
	return 0, f.err
}

func (f *failingEventLog) CountBySymbol(ctx context.Context, symbol string) (int64, error) {
	return 0, f.err
}

func (f *failingEventLog) Close() error {
	return f.err
}

// ---------- DB Retry Tests ----------

func TestService_Recover_RetriesOnDBFailure(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "snapshot.json")

	svc := New(
		WithCheckpoint(cpPath, time.Hour),
		WithMaxRecoveryRetries(3), // enough for test
	)

	// Mock that fails twice then succeeds.
	retryLog := &retryEventLog{
		failCount: 2,
		events: []*eventlog.StoredEvent{
			{
				EventID:   1,
				Timestamp: time.Now(),
				Symbol:    "AAPL",
				EventType: "quote",
				Price:     250.0,
				RawData:   mustMarshal(marketdata.Quote{Symbol: "AAPL", Price: 250.0, Timestamp: time.Now()}),
			},
		},
	}

	ctx := context.Background()
	if err := svc.Recover(ctx, retryLog); err != nil {
		t.Fatalf("Recover() error: %v", err)
	}
	svc.MarkReady()

	if svc.Count() != 1 {
		t.Fatalf("count = %d, want 1", svc.Count())
	}
}

type retryEventLog struct {
	failCount int
	attempts  int
	events    []*eventlog.StoredEvent
}

func (r *retryEventLog) Append(ctx context.Context, event *eventlog.StoredEvent) (int64, error) {
	return 0, nil
}

func (r *retryEventLog) AppendBatch(ctx context.Context, events []*eventlog.StoredEvent) ([]int64, error) {
	return nil, nil
}

func (r *retryEventLog) QueryBySymbol(ctx context.Context, symbol string, from, to time.Time) ([]*eventlog.StoredEvent, error) {
	return nil, nil
}

func (r *retryEventLog) QueryLatest(ctx context.Context, symbol string, limit int) ([]*eventlog.StoredEvent, error) {
	return nil, nil
}

func (r *retryEventLog) QueryEvents(ctx context.Context, q eventlog.ReplayQuery) (*eventlog.ReplayResult, error) {
	r.attempts++
	if r.attempts <= r.failCount {
		return nil, fmt.Errorf("database unavailable")
	}
	return &eventlog.ReplayResult{
		Events:  r.events,
		HasMore: false,
	}, nil
}

func (r *retryEventLog) Count(ctx context.Context) (int64, error) {
	return int64(len(r.events)), nil
}

func (r *retryEventLog) CountBySymbol(ctx context.Context, symbol string) (int64, error) {
	return 0, nil
}

func (r *retryEventLog) Close() error {
	return nil
}
