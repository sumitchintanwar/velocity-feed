package marketdata

// MarketEvent is the base abstraction for all events flowing through the
// Publisher Service. The distribution layer routes events by Symbol and
// does not need to know the concrete type. Specific event implementations
// (Quote, Bar, OrderBookUpdate, etc.) satisfy this interface.
//
// This decoupling allows the Publisher to distribute new event types
// without modification — only the consumer needs to type-assert.
type MarketEvent interface {
	// EventSymbol returns the symbol this event relates to.
	// Used by the Publisher for topic-based routing.
	EventSymbol() string

	// EventType returns a discriminator string (e.g. "trade", "bar",
	// "order_book"). Consumers use this to decide how to handle the event.
	EventType() string
}
