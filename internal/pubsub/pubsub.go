// Package pubsub defines the interfaces for the Publisher Service — the
// central event distribution layer. Consumers depend on these interfaces,
// not on concrete implementations. This allows swapping the in-memory bus
// for Redis, Kafka, or NATS without changing any caller.
//
// Dependency direction:
//
//	Domain (marketdata.MarketEvent)
//	      ↑
//	Publisher interface          ← this package
//	      ↑
//	Application / Transport     ← depend on interfaces
//	      ↑
//	Infrastructure              ← implements interfaces
package pubsub

import (
	"context"

	"github.com/sumit/rtmds/internal/marketdata"
)

// Publisher accepts market events and distributes them to all active
// subscribers of the matching symbols. Implementations must be safe
// for concurrent use.
type Publisher interface {
	// Publish fans out a MarketEvent to every subscriber registered for
	// the event's symbol. The contract is non-blocking: if a subscriber's
	// buffer is full the event is dropped for that subscriber. The hot
	// path must never allocate, log, or block.
	Publish(ctx context.Context, event marketdata.MarketEvent)
}

// Subscription is a handle to an active symbol subscription. It provides
// a channel of events and a way to terminate the subscription.
type Subscription interface {
	// C returns the channel on which events are delivered. The channel
	// is closed when the subscription is cancelled.
	C() <-chan marketdata.MarketEvent

	// Cancel terminates the subscription. After Cancel returns the
	// channel returned by C is closed and no more events will be
	// delivered. Cancel is idempotent.
	Cancel()
}

// Bus combines publishing and subscription management into a single
// abstraction. It is the primary entry point for the Publisher Service.
type Bus interface {
	Publisher

	// Subscribe registers interest in the given symbols and returns a
	// Subscription whose C channel will receive matching events. The
	// subscription is active until Cancel is called. id is an opaque
	// identifier used for logging and metrics (typically a client UUID).
	Subscribe(id string, symbols ...string) Subscription
}
