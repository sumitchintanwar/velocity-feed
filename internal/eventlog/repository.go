// Package eventlog provides an append-only persistent event log for
// market data events. It uses the repository pattern to abstract
// storage, allowing the implementation to be swapped (PostgreSQL,
// BadgerDB, etc.) without changing callers.
package eventlog

import (
	"context"
	"time"
)

// StoredEvent represents a persisted market event.
type StoredEvent struct {
	EventID   int64     `json:"event_id"`
	Timestamp time.Time `json:"timestamp"`
	Symbol    string    `json:"symbol"`
	EventType string    `json:"event_type"`
	Price     float64   `json:"price,omitempty"`
	Bid       float64   `json:"bid,omitempty"`
	Ask       float64   `json:"ask,omitempty"`
	Volume    int64     `json:"volume,omitempty"`
	Exchange  string    `json:"exchange,omitempty"`
	Provider  string    `json:"provider,omitempty"`
	RawData   []byte    `json:"raw_data,omitempty"`
}

// Cursor encodes a position in the event stream using (Timestamp, EventID).
// This composite cursor ensures strict chronological ordering even when
// distributed systems produce non-monotonic event IDs.
type Cursor struct {
	Timestamp time.Time `json:"t"`
	EventID   int64     `json:"e"`
}

// ReplayQuery defines the parameters for a paginated replay query.
type ReplayQuery struct {
	// Symbol filters events by symbol. Empty means all symbols.
	Symbol string
	// From is the start of the time range (inclusive). Zero value means no lower bound.
	From time.Time
	// To is the end of the time range (inclusive). Zero value means no upper bound.
	To time.Time
	// Cursor is the position to start after (exclusive). Zero values mean start from the beginning.
	Cursor Cursor
	// Limit is the maximum number of events to return. Default 100, max 1000.
	Limit int
}

// ReplayResult holds a page of replay events with cursor pagination metadata.
type ReplayResult struct {
	Events     []*StoredEvent `json:"events"`
	NextCursor *Cursor        `json:"next_cursor,omitempty"`
	HasMore    bool           `json:"has_more"`
}

// Repository defines the interface for persisting market events.
// Implementations must be safe for concurrent use.
type Repository interface {
	// Append persists a single market event. Returns the assigned event ID.
	Append(ctx context.Context, event *StoredEvent) (int64, error)

	// AppendBatch persists multiple events in a single transaction.
	// More efficient than calling Append in a loop.
	AppendBatch(ctx context.Context, events []*StoredEvent) ([]int64, error)

	// QueryBySymbol returns all events for a symbol within a time range,
	// ordered by timestamp ascending. Used for replay.
	QueryBySymbol(ctx context.Context, symbol string, from, to time.Time) ([]*StoredEvent, error)

	// QueryLatest returns the N most recent events for a symbol.
	QueryLatest(ctx context.Context, symbol string, limit int) ([]*StoredEvent, error)

	// QueryEvents returns a paginated page of events matching the given filters.
	// Supports optional symbol filter, time range, and cursor-based pagination.
	QueryEvents(ctx context.Context, q ReplayQuery) (*ReplayResult, error)

	// Count returns the total number of persisted events.
	Count(ctx context.Context) (int64, error)

	// CountBySymbol returns the number of events for a specific symbol.
	CountBySymbol(ctx context.Context, symbol string) (int64, error)

	// Close closes the underlying connection pool.
	Close() error
}
