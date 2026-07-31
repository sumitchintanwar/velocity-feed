package eventlog

import (
	"context"
	"strconv"
	"time"

	"github.com/sumit/rtmds/internal/models"
	"github.com/sumit/rtmds/internal/recorder/storage"
)

// StoreAdapter bridges eventlog.Repository to storage.EventStore.
// This allows the replay engine to read from the persistent PostgreSQL
// event log instead of requiring a separate storage backend.
//
// The adapter converts between the two data models:
//   - eventlog.StoredEvent (price-centric, int64 EventID) →
//   - models.StoredEvent (payload-centric, string EventID)
//
// ReadStream uses cursor-based pagination internally to stream large
// result sets without loading them all into memory.
type StoreAdapter struct {
	repo Repository
}

// NewStoreAdapter creates a StoreAdapter backed by the given Repository.
func NewStoreAdapter(repo Repository) *StoreAdapter {
	return &StoreAdapter{repo: repo}
}

// WriteBatch persists a slice of models.StoredEvent by converting them
// to eventlog.StoredEvent and delegating to AppendBatch.
func (a *StoreAdapter) WriteBatch(ctx context.Context, events []models.StoredEvent) error {
	if len(events) == 0 {
		return nil
	}

	stored := make([]*StoredEvent, len(events))
	for i, e := range events {
		stored[i] = &StoredEvent{
			Timestamp: e.Timestamp,
			Symbol:    e.Symbol,
			EventType: e.EventType,
			Exchange:  e.Exchange,
			RawData:   e.Payload,
		}
	}

	_, err := a.repo.AppendBatch(ctx, stored)
	return err
}

// ReadStream returns an iterator that streams events for the given symbol
// within the specified time range. Events are fetched from PostgreSQL using
// cursor-based pagination, loading at most chunkSize events per query.
func (a *StoreAdapter) ReadStream(ctx context.Context, symbol string, start, end time.Time, chunkSize int) (storage.EventIterator, error) {
	if chunkSize <= 0 {
		chunkSize = 1000
	}
	return &storeIterator{
		repo:      a.repo,
		symbol:    symbol,
		from:      start,
		to:        end,
		chunkSize: chunkSize,
		ctx:       ctx,
	}, nil
}

// storeIterator implements storage.EventIterator by paginating through
// eventlog.Repository.QueryEvents using a composite cursor.
type storeIterator struct {
	repo      Repository
	symbol    string
	from, to  time.Time
	chunkSize int
	ctx       context.Context

	cursor      Cursor
	hasMore     bool
	initialized bool
}

// Next returns the next batch of events. Returns (nil, nil) when the
// stream is exhausted. Each call issues one QueryEvents query.
func (it *storeIterator) Next() ([]models.StoredEvent, error) {
	if !it.initialized {
		it.initialized = true
		it.hasMore = true
	}

	if !it.hasMore {
		return nil, nil
	}

	q := ReplayQuery{
		Symbol: it.symbol,
		From:   it.from,
		To:     it.to,
		Cursor: it.cursor,
		Limit:  it.chunkSize,
	}

	result, err := it.repo.QueryEvents(it.ctx, q)
	if err != nil {
		return nil, err
	}

	if len(result.Events) == 0 {
		return nil, nil
	}

	it.hasMore = result.HasMore
	if result.NextCursor != nil {
		it.cursor = *result.NextCursor
	}

	events := make([]models.StoredEvent, len(result.Events))
	for i, e := range result.Events {
		events[i] = eventlogToModel(e)
	}

	return events, nil
}

// Close releases resources held by the iterator.
func (it *storeIterator) Close() error {
	return nil
}

// eventlogToModel converts an eventlog.StoredEvent to a models.StoredEvent.
// The RawData field (containing the original market event JSON) is mapped
// to Payload so the replay engine can reconstruct MarketEvents from it.
func eventlogToModel(e *StoredEvent) models.StoredEvent {
	return models.StoredEvent{
		EventID:        strconv.FormatInt(e.EventID, 10),
		SchemaVersion:  1,
		Exchange:       e.Exchange,
		Symbol:         e.Symbol,
		EventType:      e.EventType,
		Timestamp:      e.Timestamp,
		SequenceNumber: uint64(e.EventID),
		Payload:        e.RawData,
	}
}
