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
	Event       MarketEvent
	JSON        []byte // pre-encoded ServerMessage JSON
	Protobuf    []byte // pre-encoded Protobuf payload
	FlatBuffers []byte // pre-encoded FlatBuffers payload
}

// Encoder defines a serialization interface that can be injected from the top level
// (e.g., app root) to avoid import cycles between marketdata and protocol packages.
type Encoder interface {
	Serialize(ev MarketEvent) ([]byte, error)
}

var (
	// ProtobufEncoder, if set, is used to pre-encode events into Protobuf.
	ProtobufEncoder Encoder
	// FlatBuffersEncoder, if set, is used to pre-encode events into FlatBuffers.
	FlatBuffersEncoder Encoder
)

// PreEncodedEvent wraps a pre-encoded JSON payload along with routing metadata.
// It implements MarketEvent, SequencedEvent, and TimestampedEvent.
type PreEncodedEvent struct {
	Symbol      string
	Typ         string
	SeqNum      int64
	Time        time.Time
	JSON        []byte
	Protobuf    []byte
	FlatBuffers []byte
}

func (p PreEncodedEvent) EventSymbol() string     { return p.Symbol }
func (p PreEncodedEvent) EventType() string       { return p.Typ }
func (p PreEncodedEvent) GetSeq() int64           { return p.SeqNum }
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
			Event:       pe,
			JSON:        pe.JSON,
			Protobuf:    pe.Protobuf,
			FlatBuffers: pe.FlatBuffers,
		}
	}
	if pe, ok := ev.(*PreEncodedEvent); ok {
		return &CachedEvent{
			Event:       pe,
			JSON:        pe.JSON,
			Protobuf:    pe.Protobuf,
			FlatBuffers: pe.FlatBuffers,
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
		return &CachedEvent{Event: ev}
	}

	b := buf.Bytes()
	if len(b) > 0 && b[len(b)-1] == '\n' {
		b = b[:len(b)-1] // trim newline added by json.Encoder
	}
	b = append(b, '}')

	// Copy to a precisely sized, un-pooled slice so the buffer can be reused.
	encoded := make([]byte, len(b))
	copy(encoded, b)

	ce := &CachedEvent{
		Event: ev,
		JSON:  encoded,
	}

	if ProtobufEncoder != nil {
		if b, err := ProtobufEncoder.Serialize(ev); err == nil {
			ce.Protobuf = b
		}
	}
	if FlatBuffersEncoder != nil {
		if b, err := FlatBuffersEncoder.Serialize(ev); err == nil {
			ce.FlatBuffers = b
		}
	}

	return ce
}

// EventSymbol returns the symbol of the wrapped event.
func (ce *CachedEvent) EventSymbol() string {
	return ce.Event.EventSymbol()
}

// EventType returns the type of the wrapped event.
func (ce *CachedEvent) EventType() string {
	return ce.Event.EventType()
}
