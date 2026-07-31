package wal

import (
	"context"
	"encoding/json"

	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/pubsub"
	"github.com/sumit/rtmds/pkg/marketdata"
)

// Publisher wraps an underlying pubsub.Publisher and appends
// every event to the WAL before forwarding it.
type Publisher struct {
	walLog Log
	next   pubsub.Publisher
	log    *log.Logger
}

// NewPublisher creates a new WAL-persisting publisher.
func NewPublisher(walLog Log, next pubsub.Publisher, log *log.Logger) *Publisher {
	return &Publisher{
		walLog: walLog,
		next:   next,
		log:    log,
	}
}

// Publish implements pubsub.Publisher.
func (p *Publisher) Publish(ctx context.Context, ev marketdata.MarketEvent) {
	// 1. We must sequence and serialize the event
	seqEv, isSeq := ev.(marketdata.SequencedEvent)
	if !isSeq {
		p.next.Publish(ctx, ev)
		return
	}

	tsEv, isTs := ev.(marketdata.TimestampedEvent)
	if !isTs {
		p.next.Publish(ctx, ev)
		return
	}

	payload, err := json.Marshal(ev)
	if err != nil {
		p.log.Underlying().Error().Err(err).Str("event", "wal_marshal_error").Msg("failed to marshal event for WAL")
		p.next.Publish(ctx, ev)
		return
	}

	msg := &Message{
		Sequence:  uint64(seqEv.GetSeq()),
		Timestamp: tsEv.GetTimestamp().UnixNano(),
		Topic:     ev.EventSymbol(),
		Type:      ev.EventType(),
		Payload:   payload,
	}

	// 2. Append to WAL (buffered write)
	if _, _, err := p.walLog.Append(msg); err != nil {
		p.log.Underlying().Error().Err(err).Str("event", "wal_append_error").Msg("failed to append to WAL")
	}

	// 3. Forward to the next publisher (e.g. Redis or TopicManager)
	p.next.Publish(ctx, ev)
}
