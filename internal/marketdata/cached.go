package marketdata

import (
	"bytes"
	"encoding/json"
	"sync"
	"time"
)

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

// PreEncodedEvent wraps a pre-encoded JSON payload along with routing metadata.
// It implements MarketEvent, SequencedEvent, and TimestampedEvent.
type PreEncodedEvent struct {
	Symbol     string
	Typ        string
	SeqNum     int64
	Time       time.Time
	EncodedMsg []byte
}

func (p PreEncodedEvent) EventSymbol() string { return p.Symbol }
func (p PreEncodedEvent) EventType() string { return p.Typ }
func (p PreEncodedEvent) GetSeq() int64 { return p.SeqNum }
func (p PreEncodedEvent) GetTimestamp() time.Time { return p.Time }

var bufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

// NewCachedEvent encodes a MarketEvent into a CachedEvent.
// The ServerMessage wrapper is applied here so the Gateway
// can write the bytes directly without re-encoding.
func NewCachedEvent(ev MarketEvent) *CachedEvent {
	// If the event is already a CachedEvent, return it directly.
	if ce, ok := ev.(*CachedEvent); ok {
		return ce
	}
	// If the event is already pre-encoded, wrap it in a CachedEvent without re-encoding.
	if pe, ok := ev.(PreEncodedEvent); ok {
		return &CachedEvent{
			Event:      pe,
			EncodedMsg: pe.EncodedMsg,
		}
	}
	if pe, ok := ev.(*PreEncodedEvent); ok {
		return &CachedEvent{
			Event:      pe,
			EncodedMsg: pe.EncodedMsg,
		}
	}

	buf := bufferPool.Get().(*bytes.Buffer)
	defer bufferPool.Put(buf)
	buf.Reset()

	buf.WriteString(`{"type":"`)
	buf.WriteString(ev.EventType())
	buf.WriteString(`","payload":`)
	
	err := json.NewEncoder(buf).Encode(ev)
	if err != nil {
		return &CachedEvent{Event: ev, EncodedMsg: nil}
	}

	b := buf.Bytes()
	if len(b) > 0 && b[len(b)-1] == '\n' {
		b = b[:len(b)-1] // trim newline added by json.Encoder
	}
	b = append(b, '}')

	// Copy to a precisely sized, un-pooled slice so the buffer can be reused.
	encoded := make([]byte, len(b))
	copy(encoded, b)

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
