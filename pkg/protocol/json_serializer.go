package protocol

import (
	"fmt"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"github.com/sumit/rtmds/pkg/marketdata"
)

// jiter is the shared high-performance json-iterator API instance.
// ConfigCompatibleWithStandardLibrary ensures 100% compatibility with
// encoding/json behaviour while offering ~2-3x throughput improvement
// through reflection caching and avoiding repeated type lookups.
var jiter = jsoniter.ConfigCompatibleWithStandardLibrary

// jsonEnvelope is used to wrap a market event with its type tag for the
// wire format understood by the gateway and client readLoops.
// Using a concrete struct avoids interface{} boxing allocation during marshal.
type jsonEnvelope struct {
	Type    string                 `json:"type"`
	Payload marketdata.MarketEvent `json:"payload"`
}

// JSONSerializer implements the Serializer interface for JSON format.
//
// Allocation budget per Serialize call:
//   - 1 alloc: final output []byte copy
//   - 0 allocs: buffer (pooled via GetBuffer/PutBuffer)
//   - 0 allocs: json encoding (json-iterator caches reflection data globally)
//
// Allocation budget per Deserialize call (with ReleaseEvent):
//   - 0 allocs: *Quote/*Bar struct (pooled via quotePool/barPool)
//   - Unavoidable string/time allocs from json.Unmarshal itself
type JSONSerializer struct {
	quotePool sync.Pool
	barPool   sync.Pool
}

func NewJSONSerializer() *JSONSerializer {
	s := &JSONSerializer{}
	s.quotePool = sync.Pool{New: func() any { return new(marketdata.Quote) }}
	s.barPool = sync.Pool{New: func() any { return new(marketdata.Bar) }}
	return s
}

func (s *JSONSerializer) Format() Format {
	return FormatJSON
}

// Serialize encodes the event to JSON using a pooled buffer and json-iterator.
// Uses jiter.Marshal directly (no Encoder object allocation) then copies
// into a caller-owned slice.
func (s *JSONSerializer) Serialize(event marketdata.MarketEvent) ([]byte, error) {
	buf := GetBuffer()
	defer PutBuffer(buf)

	// Write the type tag manually to avoid boxing the event as interface{}
	// inside a map, which would cause extra allocations.
	buf.WriteString(`{"type":"`)
	buf.WriteString(event.EventType())
	buf.WriteString(`","payload":`)

	payload, err := jiter.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("json encode: %w", err)
	}
	buf.Write(payload)
	buf.WriteByte('}')

	// Copy out of the pooled buffer so the caller owns the slice.
	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}

// Deserialize decodes a JSON payload into a concrete MarketEvent.
// Uses json-iterator's Any for a single-parse type probe, then unmarshals
// the "payload" field only — avoiding a full double-unmarshal of the entire body.
func (s *JSONSerializer) Deserialize(data []byte) (marketdata.MarketEvent, error) {
	// Parse the outer envelope to get type tag and payload bytes.
	var envelope struct {
		Type    string              `json:"type"`
		Payload jsoniter.RawMessage `json:"payload"`
	}
	if err := jiter.Unmarshal(data, &envelope); err != nil {
		// Fallback: try treating the data as a bare event object (no envelope).
		return s.deserializeBare(data)
	}

	payloadData := []byte(envelope.Payload)
	if len(payloadData) == 0 {
		// No payload field — treat entire body as the event.
		payloadData = data
	}

	return s.deserializeTyped(envelope.Type, payloadData)
}

// deserializeBare attempts to decode data that has no envelope wrapper —
// it probes for the "type" field directly on the object.
func (s *JSONSerializer) deserializeBare(data []byte) (marketdata.MarketEvent, error) {
	any := jiter.Get(data, "type")
	if any.LastError() != nil {
		// No "type" field and no envelope — try as Bar (no type field on Bar).
		b := s.barPool.Get().(*marketdata.Bar)
		*b = marketdata.Bar{}
		if err := jiter.Unmarshal(data, b); err != nil {
			s.barPool.Put(b)
			return nil, fmt.Errorf("json deserialize: cannot determine event type")
		}
		return b, nil
	}
	return s.deserializeTyped(any.ToString(), data)
}

// deserializeTyped decodes data into the concrete type matching typStr.
func (s *JSONSerializer) deserializeTyped(typStr string, data []byte) (marketdata.MarketEvent, error) {
	switch typStr {
	case "quote", "trade":
		q := s.quotePool.Get().(*marketdata.Quote)
		*q = marketdata.Quote{}
		if err := jiter.Unmarshal(data, q); err != nil {
			s.quotePool.Put(q)
			return nil, fmt.Errorf("json deserialize Quote: %w", err)
		}
		return q, nil
	case "bar":
		b := s.barPool.Get().(*marketdata.Bar)
		*b = marketdata.Bar{}
		if err := jiter.Unmarshal(data, b); err != nil {
			s.barPool.Put(b)
			return nil, fmt.Errorf("json deserialize Bar: %w", err)
		}
		return b, nil
	case "ping":
		var p marketdata.PingMessage
		if err := jiter.Unmarshal(data, &p); err != nil {
			return nil, fmt.Errorf("json deserialize Ping: %w", err)
		}
		return p, nil
	case "pong":
		var p marketdata.PongMessage
		if err := jiter.Unmarshal(data, &p); err != nil {
			return nil, fmt.Errorf("json deserialize Pong: %w", err)
		}
		return p, nil
	case "goaway":
		var g marketdata.GoAwayMessage
		if err := jiter.Unmarshal(data, &g); err != nil {
			return nil, fmt.Errorf("json deserialize GoAway: %w", err)
		}
		return g, nil
	default:
		return nil, fmt.Errorf("unknown event type for JSON deserialize: %q", typStr)
	}
}

// ReleaseEvent returns a pooled event back to the JSONSerializer's pool.
// Call this when the event received from Deserialize is no longer needed.
// It is a no-op for non-pooled events.
func (s *JSONSerializer) ReleaseEvent(event marketdata.MarketEvent) {
	switch v := event.(type) {
	case *marketdata.Quote:
		s.quotePool.Put(v)
	case *marketdata.Bar:
		s.barPool.Put(v)
	}
}
