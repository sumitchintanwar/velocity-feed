package eventlog

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/pubsub"
)

// PersistingPublisher wraps a pubsub.Publisher and persists every event
// to the event log before forwarding to the underlying publisher.
// Events are buffered and flushed in batches for throughput.
// Persistence failures are logged but do not block live delivery
// (availability over strict durability).
type PersistingPublisher struct {
	repo          Repository
	inner         pubsub.Publisher
	log           *log.Logger
	dropped       uint64
	batchSize     int
	flushInterval time.Duration

	mu     sync.Mutex
	buffer []bufferedEvent
	closed bool
	wg     sync.WaitGroup
}

// PersistingPublisherOption configures the PersistingPublisher.
type PersistingPublisherOption func(*PersistingPublisher)

// WithBatchSize sets the batch size for batch inserts (default: 1).
func WithBatchSize(n int) PersistingPublisherOption {
	return func(p *PersistingPublisher) { p.batchSize = n }
}

type bufferedEvent struct {
	event     marketdata.MarketEvent
	arrivedAt time.Time
}

// WithFlushInterval sets the maximum time between flushes (default: 1s).
func WithFlushInterval(d time.Duration) PersistingPublisherOption {
	return func(p *PersistingPublisher) { p.flushInterval = d }
}

// NewPersistingPublisher creates a publisher that persists events before
// forwarding them to the inner publisher.
func NewPersistingPublisher(repo Repository, inner pubsub.Publisher, l *log.Logger, opts ...PersistingPublisherOption) *PersistingPublisher {
	p := &PersistingPublisher{
		repo:          repo,
		inner:         inner,
		log:           l,
		batchSize:     1,
		flushInterval: time.Second,
		buffer:        make([]bufferedEvent, 0, 64),
	}
	for _, opt := range opts {
		opt(p)
	}
	p.wg.Add(1)
	go p.flushLoop()
	return p
}

func (p *PersistingPublisher) flushLoop() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.flushInterval)
	defer ticker.Stop()
	for range ticker.C {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}
		if len(p.buffer) >= p.batchSize {
			batch := p.buffer
			p.buffer = make([]bufferedEvent, 0, cap(batch))
			p.mu.Unlock()
			p.flush(batch)
		} else {
			p.mu.Unlock()
		}
	}
}

// flush persists a batch of events to the repository.
func (p *PersistingPublisher) flush(batch []bufferedEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	storedBatch := make([]*StoredEvent, 0, len(batch))
	for _, be := range batch {
		se := p.toStoredEvent(be.event, be.arrivedAt)
		if se != nil {
			storedBatch = append(storedBatch, se)
		}
	}

	if len(storedBatch) == 0 {
		return
	}

	_, err := p.repo.AppendBatch(ctx, storedBatch)
	if err != nil {
		p.log.Underlying().Warn().Err(err).
			Int("batch_size", len(batch)).
			Str("event", "batch_persist_failed").
			Msg("persisting-publisher: failed to persist batch")
		p.dropped += uint64(len(batch))
	}
}

// Publish persists the event to the event log, then forwards to the inner publisher.
// When batchSize > 1, events are buffered and flushed in batches.
func (p *PersistingPublisher) Publish(ctx context.Context, event marketdata.MarketEvent) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.dropped++
		p.inner.Publish(ctx, event)
		return
	}

	p.buffer = append(p.buffer, bufferedEvent{
		event:     event,
		arrivedAt: time.Now(),
	})
	var batch []bufferedEvent
	if len(p.buffer) >= p.batchSize {
		batch = p.buffer
		p.buffer = make([]bufferedEvent, 0, cap(batch))
	}
	p.mu.Unlock()

	if batch != nil {
		p.flush(batch)
	}

	p.inner.Publish(ctx, event)
}

// Close flushes remaining buffered events and stops the flush goroutine.
func (p *PersistingPublisher) Close() {
	p.mu.Lock()
	p.closed = true
	remaining := p.buffer
	p.buffer = nil
	p.mu.Unlock()

	if len(remaining) > 0 {
		p.flush(remaining)
	}
	p.wg.Wait()
}

// Dropped returns the total number of events that failed to persist.
func (p *PersistingPublisher) Dropped() uint64 {
	return p.dropped
}

// toStoredEvent converts a MarketEvent to a StoredEvent for persistence.
// Returns nil if serialization fails (logged as warning).
func (p *PersistingPublisher) toStoredEvent(event marketdata.MarketEvent, arrivedAt time.Time) *StoredEvent {
	se := &StoredEvent{
		Timestamp: arrivedAt,
		Symbol:    event.EventSymbol(),
		EventType: event.EventType(),
	}

	switch e := event.(type) {
	case marketdata.Quote:
		se.Price = e.Price
		se.Bid = e.Bid
		se.Ask = e.Ask
		se.Volume = e.Volume
		se.Timestamp = e.Timestamp
		se.Provider = e.Provider
		raw, err := json.Marshal(e)
		if err != nil {
			p.log.Underlying().Warn().Err(err).
				Str("symbol", event.EventSymbol()).
				Str("event", "quote_marshal_failed").
				Msg("persisting-publisher: failed to marshal quote to raw_data")
		} else {
			se.RawData = raw
		}
	case marketdata.Bar:
		se.Price = e.Close
		se.Volume = e.Volume
		se.Timestamp = e.Timestamp
		se.Provider = e.Provider
		raw, err := json.Marshal(e)
		if err != nil {
			p.log.Underlying().Warn().Err(err).
				Str("symbol", event.EventSymbol()).
				Str("event", "bar_marshal_failed").
				Msg("persisting-publisher: failed to marshal bar to raw_data")
		} else {
			se.RawData = raw
		}
	default:
		raw, err := json.Marshal(event)
		if err != nil {
			p.log.Underlying().Warn().Err(err).
				Str("symbol", event.EventSymbol()).
				Str("event", "unknown_marshal_failed").
				Msg("persisting-publisher: failed to marshal unknown event to raw_data")
		} else {
			se.RawData = raw
		}
	}

	return se
}
