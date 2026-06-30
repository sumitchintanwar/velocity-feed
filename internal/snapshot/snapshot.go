// Package snapshot provides an in-memory service that maintains the latest
// market state for all symbols. It enables instant catch-up for newly
// connected clients by sending current prices before live streaming begins.
//
// Design:
//
//	Symbol → Latest CachedEvent (O(1) lookup)
//	Protected by sync.RWMutex for concurrent access.
//	Atomic pointer swaps prevent torn reads on snapshot updates.
//	Memory: O(number of symbols), not O(number of updates).
//	TTL-based eviction removes inactive symbols to prevent memory leaks.
//	Checkpoint persistence enables fast restart recovery.
//	Dual-checkpoint rotation prevents corruption on crash.
//	Live-event buffering prevents the replay race condition.
package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sumit/rtmds/internal/eventlog"
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/pubsub"
	"github.com/sumit/rtmds/internal/tracing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// snapshotEntry wraps a CachedEvent with metadata for concurrency safety
// and TTL tracking.
type snapshotEntry struct {
	Cached      *marketdata.CachedEvent `json:"-"`
	LastUpdated time.Time               `json:"last_updated"`
	// Raw event data for checkpoint serialization.
	RawData   []byte `json:"raw_data,omitempty"`
	EventType string `json:"event_type,omitempty"`
}

// MarshalJSON implements custom JSON marshaling for snapshotEntry.
func (e *snapshotEntry) MarshalJSON() ([]byte, error) {
	type Alias struct {
		LastUpdated time.Time `json:"last_updated"`
		RawData     []byte    `json:"raw_data,omitempty"`
		EventType   string    `json:"event_type,omitempty"`
	}
	return json.Marshal(&Alias{
		LastUpdated: e.LastUpdated,
		RawData:     e.RawData,
		EventType:   e.EventType,
	})
}

// UnmarshalJSON implements custom JSON unmarshaling for snapshotEntry.
func (e *snapshotEntry) UnmarshalJSON(data []byte) error {
	type Alias struct {
		LastUpdated time.Time `json:"last_updated"`
		RawData     []byte    `json:"raw_data,omitempty"`
		EventType   string    `json:"event_type,omitempty"`
	}
	var a Alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	e.LastUpdated = a.LastUpdated
	e.RawData = a.RawData
	e.EventType = a.EventType
	if len(a.RawData) > 0 {
		event, err := reconstructEvent(a.EventType, a.RawData)
		if err == nil {
			e.Cached = marketdata.NewCachedEvent(event)
		}
	}
	return nil
}

// CheckpointFile is the on-disk format for snapshot persistence.
type CheckpointFile struct {
	Version   int                       `json:"version"`
	Timestamp time.Time                 `json:"timestamp"`
	Cursor    eventlog.Cursor           `json:"cursor"`
	Symbols   map[string]*snapshotEntry `json:"symbols"`
}

// Service maintains the latest market state for all symbols.
// It is safe for concurrent use.
//
// Concurrency model:
//   - The map is protected by sync.RWMutex to prevent concurrent map panics.
//   - Each snapshot value is an immutable *CachedEvent replaced via pointer swap.
//   - Readers holding RLock see a consistent snapshot (no torn reads).
//   - Writers holding Lock replace the pointer atomically.
//
// Recovery model (fixes the live-vs-replay race):
//   - StartBuffering() begins capturing live events into a buffered channel.
//   - Recover() replays DB events, then drains the buffer, then marks ready.
//   - StopBuffering() is called after replay to drain remaining buffered events.
type Service struct {
	mu        sync.RWMutex
	snapshots map[string]*snapshotEntry

	// ready indicates whether the service is ready to serve traffic.
	// During warmup (e.g., crash recovery rebuild), the service
	// rejects read requests to prevent serving stale data.
	ready atomic.Bool

	// TTL for inactive symbols. If a symbol is not updated within
	// maxAge, it is evicted. Zero disables eviction.
	maxAge time.Duration

	// Checkpoint persistence (dual-checkpoint rotation).
	checkpointPath     string
	checkpointPrevPath string // previous checkpoint (fallback)
	checkpointInterval time.Duration
	lastCursor         eventlog.Cursor

	// Live-event buffering (fixes replay race condition).
	// During recovery, live events are captured here so they are
	// not lost between "DB replay finished" and "live stream starts".
	buffering  atomic.Bool
	liveBuffer chan marketdata.MarketEvent
	bufferOnce sync.Once

	// Health callback for dynamic downgrade when upstream is lost.
	healthCallback func(degraded bool, reason string)

	// Max retries for DB queries during recovery.
	maxRecoveryRetries int

	// Background goroutine lifecycle.
	log  *log.Logger
	done chan struct{}
	wg   sync.WaitGroup
}

// Option configures the Snapshot Service.
type Option func(*Service)

// WithMaxAge sets the TTL for inactive symbols. Symbols not updated
// within this duration are evicted. Zero (default) disables eviction.
func WithMaxAge(d time.Duration) Option {
	return func(s *Service) { s.maxAge = d }
}

// WithLogger sets the logger for the snapshot service.
func WithLogger(l *log.Logger) Option {
	return func(s *Service) { s.log = l }
}

// WithCheckpoint configures snapshot persistence to disk.
// Two checkpoint files are maintained: current and previous.
// If the current checkpoint is corrupted, the previous is used as fallback.
func WithCheckpoint(path string, interval time.Duration) Option {
	return func(s *Service) {
		s.checkpointPath = path
		s.checkpointPrevPath = path + ".prev"
		s.checkpointInterval = interval
	}
}

// WithHealthCallback sets a callback invoked when the service detects
// upstream issues (e.g., DB unavailable). The gateway can use this to
// dynamically downgrade health checks and disconnect clients.
func WithHealthCallback(fn func(degraded bool, reason string)) Option {
	return func(s *Service) { s.healthCallback = fn }
}

// WithMaxRecoveryRetries sets the maximum number of retries for DB
// queries during recovery. Default is 5. Used primarily for testing.
func WithMaxRecoveryRetries(n int) Option {
	return func(s *Service) { s.maxRecoveryRetries = n }
}

// New creates a new Snapshot Service.
func New(opts ...Option) *Service {
	s := &Service{
		snapshots:          make(map[string]*snapshotEntry),
		log:                log.New(nil, "snapshot"),
		done:               make(chan struct{}),
		liveBuffer:         make(chan marketdata.MarketEvent, 10000),
		maxRecoveryRetries: 5,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Start begins background goroutines (eviction and checkpointing).
// Must be called before serving traffic. Returns immediately.
func (s *Service) Start(ctx context.Context) {
	if s.maxAge > 0 {
		s.wg.Add(1)
		go s.evictionLoop(ctx)
	}
	if s.checkpointPath != "" && s.checkpointInterval > 0 {
		s.wg.Add(1)
		go s.checkpointLoop(ctx)
	}
}

// Stop signals background goroutines to exit and waits for them.
func (s *Service) Stop() {
	close(s.done)
	s.wg.Wait()
	// Final checkpoint on shutdown.
	if s.checkpointPath != "" {
		if err := s.Checkpoint(); err != nil {
			s.log.Underlying().Warn().Err(err).Str("event", "checkpoint_final_failed").Msg("snapshot-service: final checkpoint failed")
		}
	}
}

// MarkReady signals that the service is ready to serve traffic.
// Until this is called, Get and GetMultiple return nil/empty.
func (s *Service) MarkReady() {
	s.ready.Store(true)
}

// IsReady returns true if the service is ready to serve traffic.
func (s *Service) IsReady() bool {
	return s.ready.Load()
}

// ---------- Live-Event Buffering (Fix 1: Replay Race) ----------

// StartBuffering begins capturing live events into an internal buffer.
// Call this BEFORE starting DB replay so that events arriving during
// replay are not lost. Thread-safe.
func (s *Service) StartBuffering() {
	s.buffering.Store(true)
	s.log.Underlying().Info().Str("event", "buffering_started").Msg("snapshot-service: live-event buffering started")
}

// StopBuffering drains the buffer and applies all buffered events,
// then stops capturing. Must be called AFTER DB replay completes.
// Thread-safe.
func (s *Service) StopBuffering() {
	s.buffering.Store(false)
	s.drainBuffer()
	s.log.Underlying().Info().Str("event", "buffering_stopped").Msg("snapshot-service: live-event buffering stopped")
}

// IsBuffering returns true if live events are being buffered.
func (s *Service) IsBuffering() bool {
	return s.buffering.Load()
}

// drainBuffer applies all buffered live events to the snapshot state.
func (s *Service) drainBuffer() {
	drained := 0
	for {
		select {
		case event := <-s.liveBuffer:
			s.Update(event)
			drained++
		default:
			if drained > 0 {
				s.log.Underlying().Info().Int("events_drained", drained).
					Str("event", "buffer_drained").
					Msg("snapshot-service: buffered live events applied")
			}
			return
		}
	}
}

// ---------- Core Operations ----------

// Update stores the latest event for a symbol. If the symbol already has
// a snapshot, it is replaced via atomic pointer swap. Thread-safe.
//
// If buffering is active (during recovery), the event is ONLY placed
// into the live buffer. It will be applied to the snapshot state when
// StopBuffering() is called, AFTER DB replay completes.
func (s *Service) Update(event marketdata.MarketEvent) {
	// During recovery buffering, only buffer the event — don't touch the map.
	// This ensures live events are not lost when LoadCheckpoint overwrites the map.
	if s.buffering.Load() {
		s.bufferEvent(event)
		return
	}

	s.applyEvent(event)
}

// bufferEvent puts an event into the live buffer without touching the map.
func (s *Service) bufferEvent(event marketdata.MarketEvent) {
	select {
	case s.liveBuffer <- event:
	default:
		s.log.Underlying().Warn().Str("event", "buffer_full").Msg("snapshot-service: live buffer full, dropping oldest event")
		select {
		case <-s.liveBuffer:
		default:
		}
		s.liveBuffer <- event
	}
}

// applyEvent directly applies an event to the snapshot map (bypasses buffer).
// Used during DB replay when buffering is active.
func (s *Service) applyEvent(event marketdata.MarketEvent) {
	now := time.Now()
	rawData, _ := json.Marshal(event)
	entry := &snapshotEntry{
		Cached:      marketdata.NewCachedEvent(event),
		LastUpdated: now,
		RawData:     rawData,
		EventType:   event.EventType(),
	}
	s.mu.Lock()
	s.snapshots[event.EventSymbol()] = entry
	s.mu.Unlock()
}

// UpdateCursor sets the last processed event cursor for checkpoint persistence.
func (s *Service) UpdateCursor(cursor eventlog.Cursor) {
	s.mu.Lock()
	s.lastCursor = cursor
	s.mu.Unlock()
}

// Get returns the latest cached event for a symbol, or nil if no snapshot
// exists or the service is warming up. Thread-safe.
//
// The ctx parameter links the snapshot.lookup span to the caller's trace
// (e.g., subscription_request), enabling end-to-end latency diagnosis.
//
// Trace boundary: "snapshot.lookup" — covers the cache lookup for a single
// symbol. Useful when snapshot latency spikes for specific symbols.
func (s *Service) Get(ctx context.Context, symbol string) *marketdata.CachedEvent {
	_, span := tracing.TracerForComponent("snapshot").Start(ctx, "snapshot.lookup",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("snapshot.operation", "get"),
		),
	)
	defer span.End()

	if !s.ready.Load() {
		span.SetAttributes(attribute.Bool("snapshot.warming_up", true))
		return nil
	}
	s.mu.RLock()
	entry := s.snapshots[symbol]
	s.mu.RUnlock()
	if entry == nil {
		span.SetAttributes(attribute.Bool("snapshot.hit", false))
		return nil
	}
	span.SetAttributes(attribute.Bool("snapshot.hit", true))
	return entry.Cached
}

// GetMultiple returns the latest cached events for the given symbols.
// Missing symbols are omitted from the result (no nil entries).
// Returns nil if the service is warming up. Thread-safe.
//
// The ctx parameter links the snapshot.request span to the caller's trace
// (e.g., subscription_request), enabling end-to-end latency diagnosis.
//
// Trace boundary: "snapshot.request" — covers the batch snapshot lookup.
// Useful when snapshot latency spikes (per design spec).
func (s *Service) GetMultiple(ctx context.Context, symbols []string) []*marketdata.CachedEvent {
	_, span := tracing.TracerForComponent("snapshot").Start(ctx, "snapshot.request",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int("snapshot.symbol_count", len(symbols)),
		),
	)
	defer span.End()

	if !s.ready.Load() {
		span.SetAttributes(attribute.Bool("snapshot.warming_up", true))
		return nil
	}
	s.mu.RLock()
	result := make([]*marketdata.CachedEvent, 0, len(symbols))
	for _, sym := range symbols {
		if entry, ok := s.snapshots[sym]; ok {
			result = append(result, entry.Cached)
		}
	}
	s.mu.RUnlock()

	span.SetAttributes(attribute.Int("snapshot.hit_count", len(result)))
	return result
}

// Symbols returns a snapshot of all symbols with stored snapshots.
// Thread-safe.
func (s *Service) Symbols() []string {
	s.mu.RLock()
	syms := make([]string, 0, len(s.snapshots))
	for sym := range s.snapshots {
		syms = append(syms, sym)
	}
	s.mu.RUnlock()
	return syms
}

// Count returns the number of symbols with stored snapshots.
// Thread-safe.
func (s *Service) Count() int {
	s.mu.RLock()
	n := len(s.snapshots)
	s.mu.RUnlock()
	return n
}

// Purge removes symbols that haven't been updated within the maxAge.
// Returns the number of symbols purged. Thread-safe.
func (s *Service) Purge() int {
	if s.maxAge <= 0 {
		return 0
	}
	cutoff := time.Now().Add(-s.maxAge)
	purged := 0
	s.mu.Lock()
	for sym, entry := range s.snapshots {
		if entry.LastUpdated.Before(cutoff) {
			delete(s.snapshots, sym)
			purged++
		}
	}
	s.mu.Unlock()
	if purged > 0 {
		s.log.Underlying().Info().Int("purged", purged).Dur("max_age", s.maxAge).
			Str("event", "symbols_purged").
			Msg("snapshot-service: purged inactive symbols")
	}
	return purged
}

// ---------- Checkpoint Persistence (Fix 2: Dual Checkpoint) ----------

// checkpointPathForIndex returns the file path for a checkpoint index.
// index 0 = current, index 1 = previous.
func (s *Service) checkpointPathForIndex(index int) string {
	if index == 0 {
		return s.checkpointPath
	}
	return s.checkpointPrevPath
}

// Checkpoint saves the current snapshot state to disk.
// Implements dual-checkpoint rotation: current → previous, new → current.
// This ensures that if the process crashes mid-write, the previous
// checkpoint is still valid for recovery.
// Thread-safe. Returns an error if the write fails.
func (s *Service) Checkpoint() error {
	s.mu.RLock()
	cp := CheckpointFile{
		Version:   1,
		Timestamp: time.Now(),
		Cursor:    s.lastCursor,
		Symbols:   make(map[string]*snapshotEntry, len(s.snapshots)),
	}
	for sym, entry := range s.snapshots {
		cp.Symbols[sym] = entry
	}
	s.mu.RUnlock()

	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("checkpoint marshal: %w", err)
	}

	dir := filepath.Dir(s.checkpointPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("checkpoint mkdir: %w", err)
	}

	// Step 1: Rotate current → previous (if current exists).
	if _, err := os.Stat(s.checkpointPath); err == nil {
		// Remove old previous.
		os.Remove(s.checkpointPrevPath)
		// Rename current → previous.
		if err := os.Rename(s.checkpointPath, s.checkpointPrevPath); err != nil {
			s.log.Underlying().Warn().Err(err).Str("event", "checkpoint_rotate_failed").Msg("snapshot-service: failed to rotate checkpoint")
		}
	}

	// Step 2: Write new checkpoint to temp file.
	tmpPath := s.checkpointPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("checkpoint write: %w", err)
	}

	// Step 3: Atomic rename temp → current.
	if err := os.Rename(tmpPath, s.checkpointPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("checkpoint rename: %w", err)
	}

	s.log.Underlying().Debug().
		Int("symbols", len(cp.Symbols)).
		Str("path", s.checkpointPath).
		Str("event", "checkpoint_saved").
		Msg("snapshot-service: checkpoint saved")
	return nil
}

// LoadCheckpoint loads snapshot state from a checkpoint file on disk.
// If the current checkpoint is corrupted, falls back to the previous one.
// Returns nil if no checkpoint exists (first start).
func (s *Service) LoadCheckpoint() error {
	// Try current checkpoint first, then previous.
	for i, path := range []string{s.checkpointPath, s.checkpointPrevPath} {
		if path == "" {
			continue
		}
		err := s.loadCheckpointFrom(path)
		if err == nil {
			if i == 1 {
			s.log.Underlying().Warn().Str("path", path).
				Str("event", "checkpoint_fallback_loaded").
				Msg("snapshot-service: loaded fallback checkpoint (current was corrupted)")
			}
			return nil
		}
		if !os.IsNotExist(err) {
			s.log.Underlying().Warn().Err(err).Str("path", path).
				Str("event", "checkpoint_unreadable").
				Msg("snapshot-service: checkpoint unreadable, trying fallback")
		}
	}

	s.log.Underlying().Info().Str("event", "no_checkpoint").Msg("snapshot-service: no valid checkpoint found, starting fresh")
	return nil
}

// loadCheckpointFrom loads snapshot state from a specific file path.
func (s *Service) loadCheckpointFrom(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var cp CheckpointFile
	if err := json.Unmarshal(data, &cp); err != nil {
		return fmt.Errorf("checkpoint unmarshal: %w", err)
	}

	if cp.Version != 1 {
		return fmt.Errorf("checkpoint: unsupported version %d", cp.Version)
	}

	s.mu.Lock()
	s.snapshots = cp.Symbols
	s.lastCursor = cp.Cursor
	s.mu.Unlock()

	s.log.Underlying().Info().
		Int("symbols", len(cp.Symbols)).
		Time("checkpoint_time", cp.Timestamp).
		Str("path", path).
		Str("event", "checkpoint_loaded").
		Msg("snapshot-service: checkpoint loaded")
	return nil
}

// ---------- Recovery (Fix 1 + Fix 2) ----------

// Recover loads the checkpoint and replays missing events from the event log.
// Implements the "Subscribe first, buffer, then Replay" pattern:
//
//	1. Start buffering live events
//	2. Load checkpoint
//	3. Replay DB events up to cursor
//	4. Stop buffering (drains live events)
//	5. Mark Ready
//
// This prevents the race where events arrive via live stream during
// DB replay and are silently dropped.
//
// The repo parameter is optional. If nil, only checkpoint loading is performed.
// If the DB is unavailable, retries with exponential backoff (max 5 attempts).
func (s *Service) Recover(ctx context.Context, repo eventlog.Repository) error {
	// Step 1: Start buffering live events BEFORE any replay.
	// This ensures events arriving during DB replay are captured.
	s.StartBuffering()
	defer s.StopBuffering()

	// Step 2: Load checkpoint.
	if err := s.LoadCheckpoint(); err != nil {
		return fmt.Errorf("recovery: load checkpoint: %w", err)
	}

	// Step 3: Replay missing events if event log is available.
	if repo != nil {
		s.mu.RLock()
		cursor := s.lastCursor
		s.mu.RUnlock()

		s.log.Underlying().Info().
			Time("from_cursor", cursor.Timestamp).
			Int64("cursor_event_id", cursor.EventID).
			Str("event", "replay_started").
			Msg("snapshot-service: replaying missing events")

		replayed := 0
		retryCount := 0

		for {
			select {
			case <-ctx.Done():
				return fmt.Errorf("recovery: context cancelled after replaying %d events", replayed)
			default:
			}

			result, err := repo.QueryEvents(ctx, eventlog.ReplayQuery{
				Cursor: cursor,
				Limit:  1000,
			})
			if err != nil {
				retryCount++
				if retryCount > s.maxRecoveryRetries {
					if s.healthCallback != nil {
						s.healthCallback(true, "event log unreachable after retries")
					}
					return fmt.Errorf("recovery: event log unreachable after %d retries: %w", s.maxRecoveryRetries, err)
				}
				backoff := time.Duration(math.Min(float64(time.Second)*math.Pow(2, float64(retryCount)), float64(30*time.Second)))
				s.log.Underlying().Warn().Err(err).Int("retry", retryCount).Dur("backoff", backoff).
					Str("event", "replay_retry").
					Msg("snapshot-service: event log query failed, retrying")
				select {
				case <-time.After(backoff):
					continue
				case <-ctx.Done():
					return fmt.Errorf("recovery: context cancelled during retry backoff")
				}
			}
			retryCount = 0 // reset on success

			for _, ev := range result.Events {
				event, err := reconstructEvent(ev.EventType, ev.RawData)
				if err != nil {
					s.log.Underlying().Warn().Err(err).Int64("event_id", ev.EventID).
						Str("event", "replay_event_skipped").
						Msg("snapshot-service: failed to reconstruct event, skipping")
					continue
				}
				// Apply directly to map — bypasses buffer (buffering is active for live events).
				s.applyEvent(event)
				replayed++
			}

			if result.NextCursor != nil {
				cursor = *result.NextCursor
				s.UpdateCursor(cursor)
			}

			if !result.HasMore {
				break
			}
		}

		s.log.Underlying().Info().Int("events_replayed", replayed).
			Str("event", "replay_completed").
			Msg("snapshot-service: replay complete")
	}

	// Step 4: StopBuffering is called by defer — drains any live events
	// that arrived during replay, ensuring zero event loss.

	return nil
}

// reconstructEvent rebuilds a MarketEvent from raw data.
func reconstructEvent(eventType string, rawData []byte) (marketdata.MarketEvent, error) {
	if len(rawData) == 0 {
		return nil, fmt.Errorf("no raw data")
	}

	switch eventType {
	case "quote", "trade", "":
		var q marketdata.Quote
		if err := json.Unmarshal(rawData, &q); err != nil {
			return nil, fmt.Errorf("unmarshal quote: %w", err)
		}
		return q, nil
	case "bar":
		var b marketdata.Bar
		if err := json.Unmarshal(rawData, &b); err != nil {
			return nil, fmt.Errorf("unmarshal bar: %w", err)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("unknown event type: %s", eventType)
	}
}

// ---------- Background Loops ----------

// evictionLoop runs periodic purge cycles.
func (s *Service) evictionLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.maxAge / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.Purge()
		case <-s.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

// checkpointLoop runs periodic checkpoint cycles.
func (s *Service) checkpointLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.checkpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.Checkpoint(); err != nil {
				s.log.Underlying().Warn().Err(err).Str("event", "checkpoint_periodic_failed").Msg("snapshot-service: periodic checkpoint failed")
			}
		case <-s.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

// ---------- SnapshotPublisher ----------

// SnapshotPublisher wraps a pubsub.Publisher and updates the Snapshot
// Service on every publish. This decorator sits in the pipeline:
//
//	Feed → SnapshotPublisher → TopicManager
//
// The snapshot update is a lightweight side-effect (one map write per event).
type SnapshotPublisher struct {
	inner pubsub.Publisher
	snap  *Service
}

// NewSnapshotPublisher creates a decorator that updates the snapshot service
// on every Publish call.
func NewSnapshotPublisher(inner pubsub.Publisher, snap *Service) *SnapshotPublisher {
	return &SnapshotPublisher{inner: inner, snap: snap}
}

// Publish delivers the event to the inner publisher and updates the snapshot.
func (sp *SnapshotPublisher) Publish(ctx context.Context, event marketdata.MarketEvent) {
	sp.snap.Update(event)
	sp.inner.Publish(ctx, event)
}
