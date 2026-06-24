package marketdata

import "encoding/json"

// CachedEvent wraps a MarketEvent with its pre-encoded JSON bytes.
// This eliminates redundant JSON serialization in the fan-out path:
// the TopicManager encodes once, and all Gateway clients share the
// same pre-encoded bytes via a single pointer.
//
// Flow:
//
//	Publisher → TopicManager.Publish() → encode once → CachedEvent
//	                                                ↓
//	                              chan *CachedEvent → N subscribers
//	                                                ↓
//	                              Gateway.writePump → write raw bytes
type CachedEvent struct {
	Event      MarketEvent
	EncodedMsg []byte // pre-encoded ServerMessage JSON
}

// NewCachedEvent encodes a MarketEvent into a CachedEvent.
// The ServerMessage wrapper is applied here so the Gateway
// can write the bytes directly without re-encoding.
func NewCachedEvent(ev MarketEvent) *CachedEvent {
	env := struct {
		Type    string `json:"type"`
		Payload any    `json:"payload"`
	}{
		Type:    ev.EventType(),
		Payload: ev,
	}
	encoded, err := json.Marshal(env)
	if err != nil {
		// Fallback: encode without wrapper (should never happen for Quote/Bar).
		encoded, _ = json.Marshal(ev)
	}
	return &CachedEvent{
		Event:      ev,
		EncodedMsg: encoded,
	}
}

// EventSymbol returns the symbol of the wrapped event.
func (ce *CachedEvent) EventSymbol() string {
	return ce.Event.EventSymbol()
}

// EventType returns the type of the wrapped event.
func (ce *CachedEvent) EventType() string {
	return ce.Event.EventType()
}
