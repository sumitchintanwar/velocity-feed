// Package topicmanager provides the core routing layer for the pub/sub
// system. It manages topic registrations, subscriber bookkeeping, and
// efficient fan-out with minimal lock contention.
//
// Architecture:
//
//	Topic Registry:    Topic    → Subscriber Set   (used by Publish)
//	Subscriber Index:  Subscriber → Topic Set       (used by Disconnect)
//
// Locking: Sharded RWMutex per topic for publish hot-path.
// Delivery: Non-blocking, drop-on-full per subscriber queue.
package topicmanager

import (
	"context"

	"github.com/sumit/rtmds/internal/marketdata"
)

// ID is an opaque subscriber identifier (typically a client UUID).
type ID = string

// Topic is a routing key (e.g. a stock symbol like "AAPL").
type Topic = string

// Handle is returned by Subscribe. It exposes the delivery channel and
// provides a Cancel method to terminate the subscription.
type Handle interface {
	// C returns the channel on which events are delivered. It is NEVER
	// closed. Use Done() to detect cancellation.
	// Events are *CachedEvent with pre-encoded JSON bytes for zero-copy writes.
	C() <-chan *marketdata.CachedEvent

	// Done returns a channel that is closed when the subscription is
	// cancelled. Select on this alongside C() to detect termination.
	Done() <-chan struct{}

	// Cancel terminates the subscription. Idempotent.
	Cancel()

	// ID returns the subscriber's opaque identifier.
	ID() ID
}

// Manager defines the contract for topic-based routing.
type Manager interface {
	// Subscribe registers the subscriber for the given topics. Returns
	// a Handle whose C channel will receive matching events. If the
	// subscriber already exists, the new topics are merged.
	Subscribe(id ID, topics ...Topic) Handle

	// Unsubscribe removes the subscriber from all topics and closes
	// its delivery channel. Idempotent and safe for concurrent use.
	Unsubscribe(id ID)

	// Publish delivers an event to all subscribers of event.Topic().
	// The contract is non-blocking: if a subscriber's buffer is full
	// the event is dropped for that subscriber only.
	Publish(ctx context.Context, event marketdata.MarketEvent)

	// SubscriberCount returns the number of subscribers for a topic.
	SubscriberCount(topic Topic) int

	// TopicCount returns the number of topics with ≥1 subscriber.
	TopicCount() int

	// Topics returns a snapshot of all active topic names.
	Topics() []Topic
}
