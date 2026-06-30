package storage

import (
	"context"
	"time"

	"github.com/sumit/rtmds/internal/models"
)

// EventStore defines the interface for durable market data storage.
type EventStore interface {
	// WriteBatch persists a slice of events efficiently.
	WriteBatch(ctx context.Context, events []models.StoredEvent) error
	
	// ReadStream returns an iterator that streams events matching a symbol within a specific time range.
	// Events should be returned strictly ordered by (Timestamp, SequenceNumber).
	ReadStream(ctx context.Context, symbol string, start, end time.Time, chunkSize int) (EventIterator, error)
}

// EventIterator streams historical events from storage without loading the entire dataset into memory.
type EventIterator interface {
	// Next returns the next batch of events. Returns (nil, nil) when the stream is exhausted.
	Next() ([]models.StoredEvent, error)
	// Close releases any underlying resources (e.g., database cursors).
	Close() error
}
