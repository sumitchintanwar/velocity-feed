// serializer_bench_test.go — comprehensive serialization benchmarks comparing
// JSON, Protocol Buffers, and FlatBuffers across latency, allocs, and message size.
//
// Run all benchmarks:
//
//	go test -bench=. -benchmem -count=3 ./pkg/protocol/
//
// Run only the comparison suite:
//
//	go test -bench=BenchmarkCompare -benchmem -count=3 ./pkg/protocol/
//
// Run correctness tests only:
//
//	go test -v -run Test ./pkg/protocol/
package protocol

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	marketdata_pb "github.com/sumit/rtmds/api/proto/v1"
	"github.com/sumit/rtmds/pkg/marketdata"
)

// ---------------------------------------------------------------------------
// Shared test fixtures
// ---------------------------------------------------------------------------

var benchQuote = &marketdata.Quote{
	Symbol:    "AAPL",
	Type:      marketdata.QuoteTypeTrade,
	Seq:       1234567890,
	Price:     182.34,
	Bid:       182.33,
	Ask:       182.35,
	Volume:    10000,
	Timestamp: time.Date(2026, 1, 1, 9, 30, 0, 0, time.UTC),
	Provider:  "alpaca",
}

var benchBar = &marketdata.Bar{
	Symbol:    "TSLA",
	Open:      250.10,
	High:      255.80,
	Low:       249.00,
	Close:     253.45,
	Volume:    450000,
	Timestamp: time.Date(2026, 1, 1, 16, 0, 0, 0, time.UTC),
	Provider:  "polygon",
}

// ---------------------------------------------------------------------------
// BASELINE: raw stdlib — establishes "before" numbers
// ---------------------------------------------------------------------------

// BenchmarkBaseline_JSONMarshal_Quote measures encoding/json.Marshal.
// This is what the old JSONSerializer used.
func BenchmarkBaseline_JSONMarshal_Quote(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(benchQuote)
	}
}

// BenchmarkBaseline_JSONUnmarshal_Quote measures the naive double-unmarshal approach:
// peek type first, then unmarshal body.
func BenchmarkBaseline_JSONUnmarshal_Quote(b *testing.B) {
	data, _ := json.Marshal(benchQuote)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var peek struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(data, &peek)
		q := &marketdata.Quote{}
		_ = json.Unmarshal(data, q)
	}
}

// BenchmarkBaseline_ProtoMarshal_Quote measures proto.Marshal without pooling.
func BenchmarkBaseline_ProtoMarshal_Quote(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		msg := &marketdata_pb.MarketEvent{
			Symbol:         benchQuote.Symbol,
			Timestamp:      benchQuote.Timestamp.UnixMilli(),
			SequenceNumber: uint64(benchQuote.Seq),
			Payload: &marketdata_pb.MarketEvent_Tick{
				Tick: &marketdata_pb.Tick{
					Price:    benchQuote.Price,
					Volume:   float64(benchQuote.Volume),
					Exchange: benchQuote.Provider,
				},
			},
		}
		_, _ = proto.Marshal(msg)
	}
}

// BenchmarkBaseline_ProtoUnmarshal_Quote measures plain proto.Unmarshal.
func BenchmarkBaseline_ProtoUnmarshal_Quote(b *testing.B) {
	msg := &marketdata_pb.MarketEvent{
		Symbol: benchQuote.Symbol, Timestamp: benchQuote.Timestamp.UnixMilli(),
		Payload: &marketdata_pb.MarketEvent_Tick{Tick: &marketdata_pb.Tick{
			Price: benchQuote.Price, Volume: float64(benchQuote.Volume),
		}},
	}
	data, _ := proto.Marshal(msg)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out marketdata_pb.MarketEvent
		_ = proto.Unmarshal(data, &out)
	}
}

// ---------------------------------------------------------------------------
// JSON Serializer — Serialize
// ---------------------------------------------------------------------------

func BenchmarkCompare_JSON_Serialize_Quote(b *testing.B) {
	s := NewJSONSerializer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := s.Serialize(benchQuote)
		if err != nil {
			b.Fatal(err)
		}
		_ = data
	}
}

func BenchmarkCompare_JSON_Serialize_Bar(b *testing.B) {
	s := NewJSONSerializer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := s.Serialize(benchBar)
		if err != nil {
			b.Fatal(err)
		}
		_ = data
	}
}

// ---------------------------------------------------------------------------
// JSON Serializer — Deserialize
// ---------------------------------------------------------------------------

func BenchmarkCompare_JSON_Deserialize_Quote(b *testing.B) {
	s := NewJSONSerializer()
	data, _ := s.Serialize(benchQuote)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev, err := s.Deserialize(data)
		if err != nil {
			b.Fatal(err)
		}
		s.ReleaseEvent(ev)
	}
}

func BenchmarkCompare_JSON_Deserialize_Bar(b *testing.B) {
	s := NewJSONSerializer()
	data, _ := s.Serialize(benchBar)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev, err := s.Deserialize(data)
		if err != nil {
			b.Fatal(err)
		}
		s.ReleaseEvent(ev)
	}
}

// BenchmarkCompare_JSON_RoundTrip_Quote is the hot path: serialize then deserialize.
func BenchmarkCompare_JSON_RoundTrip_Quote(b *testing.B) {
	s := NewJSONSerializer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := s.Serialize(benchQuote)
		if err != nil {
			b.Fatal(err)
		}
		ev, err := s.Deserialize(data)
		if err != nil {
			b.Fatal(err)
		}
		s.ReleaseEvent(ev)
	}
}

// ---------------------------------------------------------------------------
// Protobuf Serializer — Serialize
// ---------------------------------------------------------------------------

func BenchmarkCompare_Protobuf_Serialize_Quote(b *testing.B) {
	s := NewProtobufSerializer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := s.Serialize(benchQuote)
		if err != nil {
			b.Fatal(err)
		}
		_ = data
	}
}

func BenchmarkCompare_Protobuf_Serialize_Bar(b *testing.B) {
	s := NewProtobufSerializer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := s.Serialize(benchBar)
		if err != nil {
			b.Fatal(err)
		}
		_ = data
	}
}

// ---------------------------------------------------------------------------
// Protobuf Serializer — Deserialize
// ---------------------------------------------------------------------------

func BenchmarkCompare_Protobuf_Deserialize_Quote(b *testing.B) {
	s := NewProtobufSerializer()
	data, _ := s.Serialize(benchQuote)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev, err := s.Deserialize(data)
		if err != nil {
			b.Fatal(err)
		}
		_ = ev
	}
}

func BenchmarkCompare_Protobuf_Deserialize_Bar(b *testing.B) {
	s := NewProtobufSerializer()
	data, _ := s.Serialize(benchBar)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev, err := s.Deserialize(data)
		if err != nil {
			b.Fatal(err)
		}
		_ = ev
	}
}

func BenchmarkCompare_Protobuf_RoundTrip_Quote(b *testing.B) {
	s := NewProtobufSerializer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := s.Serialize(benchQuote)
		if err != nil {
			b.Fatal(err)
		}
		ev, err := s.Deserialize(data)
		if err != nil {
			b.Fatal(err)
		}
		_ = ev
	}
}

// ---------------------------------------------------------------------------
// FlatBuffers Serializer — Serialize
// ---------------------------------------------------------------------------

func BenchmarkCompare_FlatBuffers_Serialize_Quote(b *testing.B) {
	s := NewFlatBuffersSerializer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := s.Serialize(benchQuote)
		if err != nil {
			b.Fatal(err)
		}
		_ = data
	}
}

func BenchmarkCompare_FlatBuffers_Serialize_Bar(b *testing.B) {
	s := NewFlatBuffersSerializer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := s.Serialize(benchBar)
		if err != nil {
			b.Fatal(err)
		}
		_ = data
	}
}

// ---------------------------------------------------------------------------
// FlatBuffers Serializer — Deserialize
// ---------------------------------------------------------------------------

func BenchmarkCompare_FlatBuffers_Deserialize_Quote(b *testing.B) {
	s := NewFlatBuffersSerializer()
	data, _ := s.Serialize(benchQuote)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev, err := s.Deserialize(data)
		if err != nil {
			b.Fatal(err)
		}
		_ = ev
	}
}

func BenchmarkCompare_FlatBuffers_Deserialize_Bar(b *testing.B) {
	s := NewFlatBuffersSerializer()
	data, _ := s.Serialize(benchBar)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev, err := s.Deserialize(data)
		if err != nil {
			b.Fatal(err)
		}
		_ = ev
	}
}

func BenchmarkCompare_FlatBuffers_RoundTrip_Quote(b *testing.B) {
	s := NewFlatBuffersSerializer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := s.Serialize(benchQuote)
		if err != nil {
			b.Fatal(err)
		}
		ev, err := s.Deserialize(data)
		if err != nil {
			b.Fatal(err)
		}
		_ = ev
	}
}

// ---------------------------------------------------------------------------
// Parallel / concurrent throughput (simulates N gateway clients)
// ---------------------------------------------------------------------------

func BenchmarkCompare_JSON_Serialize_Parallel(b *testing.B) {
	s := NewJSONSerializer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			data, err := s.Serialize(benchQuote)
			if err != nil {
				b.Error(err)
				return
			}
			_ = data
		}
	})
}

func BenchmarkCompare_Protobuf_Serialize_Parallel(b *testing.B) {
	s := NewProtobufSerializer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			data, err := s.Serialize(benchQuote)
			if err != nil {
				b.Error(err)
				return
			}
			_ = data
		}
	})
}

func BenchmarkCompare_FlatBuffers_Serialize_Parallel(b *testing.B) {
	s := NewFlatBuffersSerializer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			data, err := s.Serialize(benchQuote)
			if err != nil {
				b.Error(err)
				return
			}
			_ = data
		}
	})
}

// ---------------------------------------------------------------------------
// Message size comparison (not a timing benchmark — prints byte sizes)
// ---------------------------------------------------------------------------

func TestMessageSizeComparison(t *testing.T) {
	jsons := NewJSONSerializer()
	protos := NewProtobufSerializer()
	fbs := NewFlatBuffersSerializer()

	events := []struct {
		name  string
		event marketdata.MarketEvent
	}{
		{"Quote/AAPL", benchQuote},
		{"Bar/TSLA", benchBar},
	}

	formats := []struct {
		name string
		fn   func(marketdata.MarketEvent) ([]byte, error)
	}{
		{"JSON     ", jsons.Serialize},
		{"Protobuf ", protos.Serialize},
		{"FlatBufs ", fbs.Serialize},
	}

	t.Log("\n=== Message Size Comparison ===")
	t.Log(fmt.Sprintf("%-20s %-12s %s", "Event", "Format", "Bytes"))
	t.Log("--------------------------------------------")
	for _, ev := range events {
		for _, f := range formats {
			data, err := f.fn(ev.event)
			if err != nil {
				t.Errorf("%s/%s: %v", ev.name, f.name, err)
				continue
			}
			t.Log(fmt.Sprintf("%-20s %-12s %d bytes", ev.name, f.name, len(data)))
		}
	}
}

// ---------------------------------------------------------------------------
// Correctness tests — ensure pooling/optimisations don't corrupt data
// ---------------------------------------------------------------------------

func TestJSONSerializer_SerializeDeserialize_Quote(t *testing.T) {
	s := NewJSONSerializer()
	data, err := s.Serialize(benchQuote)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	ev, err := s.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	defer s.ReleaseEvent(ev)
	q, ok := ev.(*marketdata.Quote)
	if !ok {
		t.Fatalf("expected *Quote, got %T", ev)
	}
	assertQuoteEqual(t, benchQuote, q)
}

func TestJSONSerializer_SerializeDeserialize_Bar(t *testing.T) {
	s := NewJSONSerializer()
	data, err := s.Serialize(benchBar)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	ev, err := s.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	defer s.ReleaseEvent(ev)
	bar, ok := ev.(*marketdata.Bar)
	if !ok {
		t.Fatalf("expected *Bar, got %T", ev)
	}
	assertBarEqual(t, benchBar, bar)
}

func TestProtobufSerializer_SerializeDeserialize_Quote(t *testing.T) {
	s := NewProtobufSerializer()
	data, err := s.Serialize(benchQuote)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	ev, err := s.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	q, ok := ev.(*marketdata.Quote)
	if !ok {
		t.Fatalf("expected *Quote, got %T", ev)
	}
	assertQuoteEqual(t, benchQuote, q)
}

func TestProtobufSerializer_SerializeDeserialize_Bar(t *testing.T) {
	s := NewProtobufSerializer()
	data, err := s.Serialize(benchBar)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	ev, err := s.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	bar, ok := ev.(*marketdata.Bar)
	if !ok {
		t.Fatalf("expected *Bar, got %T", ev)
	}
	assertBarEqual(t, benchBar, bar)
}

func TestFlatBuffersSerializer_SerializeDeserialize_Quote(t *testing.T) {
	s := NewFlatBuffersSerializer()
	data, err := s.Serialize(benchQuote)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	ev, err := s.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	q, ok := ev.(*marketdata.Quote)
	if !ok {
		t.Fatalf("expected *Quote, got %T", ev)
	}
	assertQuoteEqual(t, benchQuote, q)
}

func TestFlatBuffersSerializer_SerializeDeserialize_Bar(t *testing.T) {
	s := NewFlatBuffersSerializer()
	data, err := s.Serialize(benchBar)
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	ev, err := s.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	bar, ok := ev.(*marketdata.Bar)
	if !ok {
		t.Fatalf("expected *Bar, got %T", ev)
	}
	assertBarEqual(t, benchBar, bar)
}

// TestPoolSafety checks that pooled serializers return clean data across
// multiple sequential calls (validates zero-out of pooled structs).
func TestPoolSafety_JSON_Reuse(t *testing.T) {
	s := NewJSONSerializer()
	for i := 0; i < 5; i++ {
		data, _ := s.Serialize(benchQuote)
		ev, err := s.Deserialize(data)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		q := ev.(*marketdata.Quote)
		if q.Symbol != "AAPL" {
			t.Errorf("iteration %d: symbol corrupted: %q", i, q.Symbol)
		}
		if q.Price != 182.34 {
			t.Errorf("iteration %d: price corrupted: %v", i, q.Price)
		}
		s.ReleaseEvent(ev)
	}
}

func TestPoolSafety_Protobuf_Reuse(t *testing.T) {
	s := NewProtobufSerializer()
	for i := 0; i < 5; i++ {
		data, _ := s.Serialize(benchQuote)
		ev, err := s.Deserialize(data)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		q := ev.(*marketdata.Quote)
		if q.Symbol != "AAPL" {
			t.Errorf("iteration %d: symbol corrupted: %q", i, q.Symbol)
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func assertQuoteEqual(t *testing.T, want, got *marketdata.Quote) {
	t.Helper()
	if got.Symbol != want.Symbol {
		t.Errorf("Symbol: want %q got %q", want.Symbol, got.Symbol)
	}
	if got.Price != want.Price {
		t.Errorf("Price: want %v got %v", want.Price, got.Price)
	}
	if got.Volume != want.Volume {
		t.Errorf("Volume: want %v got %v", want.Volume, got.Volume)
	}
}

func assertBarEqual(t *testing.T, want, got *marketdata.Bar) {
	t.Helper()
	if got.Symbol != want.Symbol {
		t.Errorf("Symbol: want %q got %q", want.Symbol, got.Symbol)
	}
	if got.Open != want.Open {
		t.Errorf("Open: want %v got %v", want.Open, got.Open)
	}
	if got.Close != want.Close {
		t.Errorf("Close: want %v got %v", want.Close, got.Close)
	}
	if got.Volume != want.Volume {
		t.Errorf("Volume: want %v got %v", want.Volume, got.Volume)
	}
}
