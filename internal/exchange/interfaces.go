package exchange

import (
	"context"

	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/normalization"
)

// Purpose: Defines the contract between the Exchange Adapter Framework and specific exchange implementations.
// Architecture: Strict interface boundaries prevent exchange-specific details from leaking into the core application.
// Design Decisions: The ExchangeAdapter embeds marketdata.Feed (Name, Subscribe, Unsubscribe, Run) and adds a Connect method.

// ExchangeAdapter represents a pluggable market data feed from an exchange.
type ExchangeAdapter interface {
	Name() string
	Subscribe(symbols ...string) error
	Unsubscribe(symbols ...string) error

	// Connect establishes the underlying transport connection to the exchange.
	// It should block until the connection is established or the context is cancelled.
	Connect(ctx context.Context) error
	
	// Disconnect cleanly tears down the underlying connection.
	Disconnect(ctx context.Context) error

	// Run starts the adapter's read loop, returning a channel of raw, un-normalized messages.
	Run(ctx context.Context) (<-chan marketdata.RawMessage, error)

	// Mapper returns the adapter-specific normalization mapper.
	Mapper() normalization.Mapper
}

// AdapterFactory represents a constructor for a specific ExchangeAdapter.
type AdapterFactory func(cfg AdapterConfig) (ExchangeAdapter, error)
