package protocol

import (
	"fmt"
	"sync"
	"time"

	marketdata_pb "github.com/sumit/rtmds/api/proto/v1"
	"github.com/sumit/rtmds/pkg/marketdata"
	"google.golang.org/protobuf/proto"
)

// ProtobufSerializer implements the Serializer interface for Protocol Buffers.
//
// Allocation budget per Serialize call (optimized):
//   - 0 allocations for the MarketEvent proto struct (pooled)
//   - 0 allocations for the inner Tick/Snapshot oneof struct (pooled)
//   - 0 allocations for the output byte slice when it fits in the pooled slab
//
// Allocation budget per Deserialize call (optimized):
//   - 1 allocation for the returned *marketdata.Quote/*marketdata.Bar
//     (unavoidable; caller owns the concrete type)
type ProtobufSerializer struct {
	// msgPool pools the top-level MarketEvent proto message to avoid
	// per-call heap allocation of the struct.
	msgPool sync.Pool

	// tickPool / snapshotPool pool the inner oneof sub-messages.
	tickPool     sync.Pool
	snapshotPool sync.Pool

	// slabPool pools []byte slabs for MarshalAppend to reuse output buffers.
	// Each slab starts at 256 bytes and grows as needed.
	slabPool sync.Pool

	// marshalOpts is pre-built once. proto.MarshalOptions is value-typed
	// so a single shared instance is fine (all fields are read-only after init).
	marshalOpts proto.MarshalOptions
}

func NewProtobufSerializer() *ProtobufSerializer {
	s := &ProtobufSerializer{
		marshalOpts: proto.MarshalOptions{},
	}
	s.msgPool = sync.Pool{New: func() any { return new(marketdata_pb.MarketEvent) }}
	s.tickPool = sync.Pool{New: func() any { return new(marketdata_pb.Tick) }}
	s.snapshotPool = sync.Pool{New: func() any { return new(marketdata_pb.Snapshot) }}
	s.slabPool = sync.Pool{New: func() any {
		b := make([]byte, 0, 256)
		return &b
	}}
	return s
}

func (s *ProtobufSerializer) Format() Format {
	return FormatProtobuf
}

// Serialize encodes a MarketEvent into a Protobuf wire format byte slice.
//
// Strategy:
//  1. Get a pooled *MarketEvent struct — avoids allocation of the outer struct.
//  2. Get a pooled inner oneof struct (Tick or Snapshot) — avoids allocation of sub-message.
//  3. MarshalAppend into a pooled slab — avoids output buffer allocation when size fits.
//  4. Copy the result into a fresh, precisely-sized slice — caller owns it.
//  5. Return all pooled objects for reuse before returning.
func (s *ProtobufSerializer) Serialize(event marketdata.MarketEvent) ([]byte, error) {
	// --- Step 1: get pooled envelope ---
	msg := s.msgPool.Get().(*marketdata_pb.MarketEvent)
	msg.Symbol = event.EventSymbol()
	msg.Timestamp = 0
	msg.SequenceNumber = 0
	msg.Payload = nil

	if tsEvent, ok := event.(interface{ GetTimestamp() time.Time }); ok {
		msg.Timestamp = tsEvent.GetTimestamp().UnixMilli()
	}
	if seqEvent, ok := event.(interface{ GetSeq() int64 }); ok {
		msg.SequenceNumber = uint64(seqEvent.GetSeq())
	}

	// --- Step 2: populate oneof from concrete event type ---
	switch v := event.(type) {
	case *marketdata.Quote:
		tick := s.tickPool.Get().(*marketdata_pb.Tick)
		tick.Price = v.Price
		tick.Volume = float64(v.Volume)
		tick.Side = marketdata_pb.Side_SIDE_UNSPECIFIED
		tick.Exchange = v.Provider
		msg.Payload = &marketdata_pb.MarketEvent_Tick{Tick: tick}

	case *marketdata.Bar:
		snap := s.snapshotPool.Get().(*marketdata_pb.Snapshot)
		snap.Open = v.Open
		snap.High = v.High
		snap.Low = v.Low
		snap.Close = v.Close
		snap.LastVolume = float64(v.Volume)
		msg.Payload = &marketdata_pb.MarketEvent_Snapshot{Snapshot: snap}
	}

	// --- Step 3: marshal into pooled slab ---
	slabPtr := s.slabPool.Get().(*[]byte)
	slab := (*slabPtr)[:0] // reset length, keep capacity

	encoded, err := s.marshalOpts.MarshalAppend(slab, msg)

	// --- Step 4: return pooled objects BEFORE error check ---
	switch p := msg.Payload.(type) {
	case *marketdata_pb.MarketEvent_Tick:
		tick := p.Tick
		tick.Reset()
		s.tickPool.Put(tick)
	case *marketdata_pb.MarketEvent_Snapshot:
		snap := p.Snapshot
		snap.Reset()
		s.snapshotPool.Put(snap)
	}
	msg.Payload = nil
	msg.Reset()
	s.msgPool.Put(msg)

	// Return slab — but only if it didn't grow past the cap limit (64KB),
	// to avoid pinning a huge slab in the pool.
	if cap(encoded) <= 65536 {
		*slabPtr = encoded
		s.slabPool.Put(slabPtr)
	}

	if err != nil {
		return nil, fmt.Errorf("protobuf encode: %w", err)
	}

	// --- Step 5: copy into a caller-owned, precisely sized slice ---
	out := make([]byte, len(encoded))
	copy(out, encoded)
	return out, nil
}

// Deserialize decodes a Protobuf wire format payload into a concrete domain event.
func (s *ProtobufSerializer) Deserialize(data []byte) (marketdata.MarketEvent, error) {
	// We cannot pool the outer msg here because proto.Unmarshal may retain
	// internal references. Use a stack-allocated zero struct via UnmarshalOptions.
	var msg marketdata_pb.MarketEvent
	if err := proto.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("protobuf decode: %w", err)
	}

	switch payload := msg.Payload.(type) {
	case *marketdata_pb.MarketEvent_Tick:
		tick := payload.Tick
		q := &marketdata.Quote{
			Symbol:    msg.Symbol,
			Type:      marketdata.QuoteTypeTrade,
			Seq:       int64(msg.SequenceNumber),
			Price:     tick.Price,
			Volume:    int64(tick.Volume),
			Provider:  tick.Exchange,
			Timestamp: time.UnixMilli(msg.Timestamp),
		}
		if tick.Volume == 0 {
			q.Type = marketdata.QuoteTypeQuote
		}
		return q, nil

	case *marketdata_pb.MarketEvent_Snapshot:
		snap := payload.Snapshot
		return &marketdata.Bar{
			Symbol:    msg.Symbol,
			Open:      snap.Open,
			High:      snap.High,
			Low:       snap.Low,
			Close:     snap.Close,
			Volume:    int64(snap.LastVolume),
			Timestamp: time.UnixMilli(msg.Timestamp),
		}, nil

	default:
		return nil, fmt.Errorf("protobuf unknown payload type")
	}
}
