// Package marketdata defines the core domain types for the real-time market
// data system. All other packages depend on these types; this package must
// not import any other internal package (no circular deps).
package marketdata

import "time"

// QuoteType distinguishes the kind of market event.
type QuoteType string

const (
	QuoteTypeTrade QuoteType = "trade"
	QuoteTypeQuote QuoteType = "quote"
	QuoteTypeBar   QuoteType = "bar"
)

// Quote represents a single normalised market-data event regardless of the
// upstream provider that emitted it.
type Quote struct {
	Symbol    string    `json:"symbol"`
	Type      QuoteType `json:"type"`
	Seq       int64     `json:"seq"`               // Per-symbol sequence number for ordering guarantees.
	Price     float64   `json:"price"`
	Bid       float64   `json:"bid,omitempty"`
	Ask       float64   `json:"ask,omitempty"`
	Volume    int64     `json:"volume,omitempty"`
	Timestamp time.Time `json:"timestamp"`
	// Provider is the feed source identifier (e.g. "alpaca", "polygon").
	Provider string `json:"provider,omitempty"`
	// Extensions holds arbitrary exchange-specific metadata.
	Extensions map[string]any `json:"extensions,omitempty"`
}

// EventSymbol implements MarketEvent.
func (q Quote) EventSymbol() string { return q.Symbol }

// EventType implements MarketEvent.
func (q Quote) EventType() string { return string(q.Type) }

// GetSeq returns the sequence number.
func (q Quote) GetSeq() int64 { return q.Seq }

// GetTimestamp returns the quote timestamp for max-age filtering.
func (q Quote) GetTimestamp() time.Time { return q.Timestamp }

// Bar represents an OHLCV bar for a symbol.
type Bar struct {
	Symbol    string    `json:"symbol"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    int64     `json:"volume"`
	Timestamp time.Time `json:"timestamp"`
	Provider  string    `json:"provider,omitempty"`
	// Extensions holds arbitrary exchange-specific metadata.
	Extensions map[string]any `json:"extensions,omitempty"`
}

// EventSymbol implements MarketEvent.
func (b Bar) EventSymbol() string { return b.Symbol }

// EventType implements MarketEvent.
func (b Bar) EventType() string { return "bar" }

// GetTimestamp returns the bar timestamp for max-age filtering.
func (b Bar) GetTimestamp() time.Time { return b.Timestamp }

// SubscribeRequest is the message a client sends to subscribe to symbols.
type SubscribeRequest struct {
	Action  string   `json:"action"`  // "subscribe" | "unsubscribe"
	Symbols []string `json:"symbols"` // e.g. ["AAPL", "TSLA"]
}

// ServerMessage is the envelope the server pushes to WebSocket clients.
type ServerMessage struct {
	Type    string `json:"type"`    // "quote" | "bar" | "error" | "subscribed"
	Payload any    `json:"payload"` // Quote | Bar | string
}
