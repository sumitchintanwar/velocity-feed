package marketdata

import "time"

// RawMessage represents an unprocessed, un-normalized message from an upstream exchange.
// This is the output of the ExchangeAdapter before it passes through the Normalization Layer.
type RawMessage struct {
	// Provider is the identifier of the exchange (e.g. "nasdaq", "nyse", "binance").
	Provider string
	
	// Payload contains the exchange-specific data structure.
	// We use 'any' to allow adapters to pass already-unmarshaled structs (e.g. NasdaqTick)
	// to avoid JSON serialization overhead during normalization.
	Payload any
	
	// ReceivedAt is the local monotonic timestamp when the adapter ingested this message.
	ReceivedAt time.Time
}
