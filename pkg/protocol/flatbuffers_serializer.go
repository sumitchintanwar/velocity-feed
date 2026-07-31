package protocol

import (
	"fmt"
	"sync"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	fbv1 "github.com/sumit/rtmds/api/flatbuffers/v1/rtmds/marketdata/v1"
	"github.com/sumit/rtmds/pkg/marketdata"
)

// FlatBuffersSerializer implements the Serializer interface for FlatBuffers.
//
// FlatBuffers key properties:
//   - Zero-copy reads: Deserialize returns a view into the original []byte,
//     no unmarshalling step needed.
//   - Builders must be reset between uses; we pool them via builderPool.
//   - Output bytes are extracted from the builder's finished buffer and copied
//     so the pooled builder can be safely Reset and reused.
//
// Allocation budget per Serialize call:
//   - 0 allocs: Builder (pooled)
//   - 1 alloc:  final output []byte copy (caller-owned)
//
// Allocation budget per Deserialize call:
//   - 0 allocs: FlatBuffers read is a view into the input slice (true zero-copy)
//   - 1 alloc:  *marketdata.Quote or *marketdata.Bar (unavoidable; caller needs ownership)
type FlatBuffersSerializer struct {
	builderPool sync.Pool
}

func NewFlatBuffersSerializer() *FlatBuffersSerializer {
	s := &FlatBuffersSerializer{}
	s.builderPool = sync.Pool{
		New: func() any {
			// 512 bytes covers most market events without builder growth.
			return flatbuffers.NewBuilder(512)
		},
	}
	return s
}

func (s *FlatBuffersSerializer) Format() Format {
	return FormatFlatBuffers
}

// Serialize encodes a MarketEvent into FlatBuffers wire format.
// The Builder is retrieved from the pool, used, then returned.
func (s *FlatBuffersSerializer) Serialize(event marketdata.MarketEvent) ([]byte, error) {
	b := s.builderPool.Get().(*flatbuffers.Builder)
	b.Reset()

	var ts int64
	var seq uint64

	if tsEvent, ok := event.(interface{ GetTimestamp() time.Time }); ok {
		ts = tsEvent.GetTimestamp().UnixMilli()
	}
	if seqEvent, ok := event.(interface{ GetSeq() int64 }); ok {
		seq = uint64(seqEvent.GetSeq())
	}

	symbol := b.CreateString(event.EventSymbol())

	var payloadOffset flatbuffers.UOffsetT
	var payloadType fbv1.EventPayload

	switch v := event.(type) {
	case *marketdata.Quote:
		// Build inner Tick table — strings must be created BEFORE StartObject.
		exchange := b.CreateString(v.Provider)

		fbv1.TickStart(b)
		fbv1.TickAddPrice(b, v.Price)
		fbv1.TickAddVolume(b, float64(v.Volume))
		fbv1.TickAddSide(b, fbv1.Side(0)) // SIDE_UNSPECIFIED
		fbv1.TickAddExchange(b, exchange)
		payloadOffset = fbv1.TickEnd(b)
		payloadType = fbv1.EventPayloadTick

	case *marketdata.Bar:
		// No string fields inside Snapshot.
		fbv1.SnapshotStart(b)
		fbv1.SnapshotAddOpen(b, v.Open)
		fbv1.SnapshotAddHigh(b, v.High)
		fbv1.SnapshotAddLow(b, v.Low)
		fbv1.SnapshotAddClose(b, v.Close)
		fbv1.SnapshotAddLastVolume(b, float64(v.Volume))
		payloadOffset = fbv1.SnapshotEnd(b)
		payloadType = fbv1.EventPayloadSnapshot

	default:
		s.builderPool.Put(b)
		return nil, fmt.Errorf("flatbuffers: unsupported event type %T", event)
	}

	// Build the outer MarketEvent table.
	fbv1.MarketEventStart(b)
	fbv1.MarketEventAddSymbol(b, symbol)
	fbv1.MarketEventAddTimestamp(b, ts)
	fbv1.MarketEventAddSequenceNumber(b, seq)
	fbv1.MarketEventAddPayloadType(b, payloadType)
	fbv1.MarketEventAddPayload(b, payloadOffset)
	root := fbv1.MarketEventEnd(b)
	fbv1.FinishMarketEventBuffer(b, root)

	// Copy out of the builder's internal buffer before returning it to the pool.
	finished := b.FinishedBytes()
	out := make([]byte, len(finished))
	copy(out, finished)

	s.builderPool.Put(b)
	return out, nil
}

// Deserialize decodes a FlatBuffers payload. FlatBuffers decoding is
// zero-copy — the returned event fields are views into `data`.
// We materialise the fields into a *Quote/*Bar for a consistent interface.
func (s *FlatBuffersSerializer) Deserialize(data []byte) (marketdata.MarketEvent, error) {
	ev := fbv1.GetRootAsMarketEvent(data, 0)
	symbol := string(ev.Symbol())
	ts := time.UnixMilli(ev.Timestamp())

	switch ev.PayloadType() {
	case fbv1.EventPayloadTick:
		var t flatbuffers.Table
		if !ev.Payload(&t) {
			return nil, fmt.Errorf("flatbuffers: missing Tick payload")
		}
		tick := new(fbv1.Tick)
		tick.Init(t.Bytes, t.Pos)

		return &marketdata.Quote{
			Symbol:    symbol,
			Type:      marketdata.QuoteTypeTrade,
			Seq:       int64(ev.SequenceNumber()),
			Price:     tick.Price(),
			Volume:    int64(tick.Volume()),
			Provider:  string(tick.Exchange()),
			Timestamp: ts,
		}, nil

	case fbv1.EventPayloadSnapshot:
		var t flatbuffers.Table
		if !ev.Payload(&t) {
			return nil, fmt.Errorf("flatbuffers: missing Snapshot payload")
		}
		snap := new(fbv1.Snapshot)
		snap.Init(t.Bytes, t.Pos)

		return &marketdata.Bar{
			Symbol:    symbol,
			Open:      snap.Open(),
			High:      snap.High(),
			Low:       snap.Low(),
			Close:     snap.Close(),
			Volume:    int64(snap.LastVolume()),
			Timestamp: ts,
		}, nil

	default:
		return nil, fmt.Errorf("flatbuffers: unknown payload type %v", ev.PayloadType())
	}
}
