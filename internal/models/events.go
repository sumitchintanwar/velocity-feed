package models

import "time"

// StoredEvent represents a single normalized market event recorded in the database.
type StoredEvent struct {
	EventID        string    // Unique identifier
	SchemaVersion  int       // Schema version for future compatibility
	Exchange       string    // Source exchange (e.g., "COINBASE")
	Symbol         string    // Canonical symbol (e.g., "BTC-USD")
	EventType      string    // Event type (e.g., "trade", "l2update")
	Timestamp      time.Time // Original market event timestamp
	SequenceNumber uint64    // Ordering sequence from the exchange
	Payload        []byte    // Serialized event payload (JSON or binary)
	RecordingTime  time.Time // Time the event was flushed to storage
}
