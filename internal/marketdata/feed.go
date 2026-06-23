package marketdata

import "context"

// Feed is the interface every upstream market-data provider must satisfy.
// Implementations live in internal/marketdata/feed/ subdirectories (e.g.
// alpaca/, polygon/, simulator/).
type Feed interface {
	// Name returns a human-readable identifier for this provider.
	Name() string

	// Subscribe tells the feed to begin delivering quotes for the given symbols.
	// Symbols already being tracked are silently ignored.
	Subscribe(symbols ...string) error

	// Unsubscribe removes symbols from the active subscription set.
	Unsubscribe(symbols ...string) error

	// Run starts the feed's read loop. It blocks until ctx is cancelled or an
	// unrecoverable error occurs. All received quotes are sent on the returned
	// channel. The channel is closed when Run returns.
	Run(ctx context.Context) (<-chan Quote, error)
}

// BarFeed extends Feed with OHLCV bar delivery.
type BarFeed interface {
	Feed

	// Bars returns a channel of OHLCV bars. Valid only while Run is active.
	Bars() <-chan Bar
}
