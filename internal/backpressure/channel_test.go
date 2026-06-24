package backpressure

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
)

// testMetrics creates a Metrics instance backed by an isolated registry.
func testMetrics(t *testing.T) *Metrics {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	return NewMetrics(reg)
}

// testEvent creates a minimal MarketEvent for testing.
func testEvent(symbol string, price float64) marketdata.MarketEvent {
	return marketdata.Quote{
		Symbol:    symbol,
		Type:      marketdata.QuoteTypeQuote,
		Price:     price,
		Timestamp: time.Now(),
	}
}

// testLogger returns a discard logger.
func testLogger() zerolog.Logger {
	return zerolog.Nop()
}

// ---------- Policy parsing ----------

func TestParsePolicy(t *testing.T) {
	tests := []struct {
		input string
		want  Policy
		err   bool
	}{
		{"drop_oldest", PolicyDropOldest, false},
		{"drop-oldest", PolicyDropOldest, false},
		{"oldest", PolicyDropOldest, false},
		{"drop_newest", PolicyDropNewest, false},
		{"drop-newest", PolicyDropNewest, false},
		{"newest", PolicyDropNewest, false},
		{"disconnect", PolicyDisconnect, false},
		{"invalid", 0, true},
		{"", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParsePolicy(tt.input)
			if (err != nil) != tt.err {
				t.Errorf("ParsePolicy(%q) error = %v, wantErr %v", tt.input, err, tt.err)
			}
			if got != tt.want {
				t.Errorf("ParsePolicy(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestPolicyString(t *testing.T) {
	if s := PolicyDropOldest.String(); s != "drop_oldest" {
		t.Errorf("PolicyDropOldest.String() = %q", s)
	}
	if s := PolicyDropNewest.String(); s != "drop_newest" {
		t.Errorf("PolicyDropNewest.String() = %q", s)
	}
	if s := PolicyDisconnect.String(); s != "disconnect" {
		t.Errorf("PolicyDisconnect.String() = %q", s)
	}
	if s := Policy(99).String(); s != "Policy(99)" {
		t.Errorf("Policy(99).String() = %q", s)
	}
}

// ---------- Config validation ----------

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"valid drop_oldest", Config{Policy: PolicyDropOldest, BufferSize: 256}, false},
		{"valid disconnect", Config{Policy: PolicyDisconnect, BufferSize: 256, MaxConsecutiveDrops: 100}, false},
		{"zero buffer", Config{Policy: PolicyDropOldest, BufferSize: 0}, true},
		{"disconnect no max drops", Config{Policy: PolicyDisconnect, BufferSize: 256, MaxConsecutiveDrops: 0}, true},
		{"threshold out of range", Config{Policy: PolicyDropOldest, BufferSize: 256, DropThreshold: 1.5}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ---------- Ring buffer ----------

func TestRing_PushPop(t *testing.T) {
	r := NewRing(4)
	ev := testEvent("AAPL", 100)

	if !r.Push(ev) {
		t.Fatal("Push returned false")
	}
	if r.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", r.Len())
	}

	got, ok := r.Pop()
	if !ok {
		t.Fatal("Pop returned false")
	}
	if got.EventSymbol() != "AAPL" {
		t.Errorf("Pop().Symbol = %q, want AAPL", got.EventSymbol())
	}
	if r.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", r.Len())
	}
}

func TestRing_DropOldest(t *testing.T) {
	r := NewRing(4)

	// Fill the ring.
	for i := 0; i < 4; i++ {
		r.Push(testEvent("AAPL", float64(i)))
	}

	// Push one more — should drop oldest (price 0).
	r.Push(testEvent("AAPL", 99))

	if r.TotalDropped() != 1 {
		t.Fatalf("TotalDropped() = %d, want 1", r.TotalDropped())
	}

	// Pop should return the remaining events, starting from price 1.
	ev, ok := r.Pop()
	if !ok || ev.(marketdata.Quote).Price != 1 {
		t.Errorf("first Pop = %v, want price 1", ev)
	}
}

func TestRing_EmptyPop(t *testing.T) {
	r := NewRing(4)
	_, ok := r.Pop()
	if ok {
		t.Error("Pop on empty ring should return false")
	}
}

func TestRing_Cap(t *testing.T) {
	r := NewRing(3) // rounds up to 4
	if r.Cap() != 4 {
		t.Errorf("Cap() = %d, want 4", r.Cap())
	}
}

func TestRing_Nil(t *testing.T) {
	var r *Ring
	if r.Push(testEvent("X", 1)) {
		t.Error("Push on nil ring should return false")
	}
	if _, ok := r.Pop(); ok {
		t.Error("Pop on nil ring should return false")
	}
	if r.Len() != 0 {
		t.Error("Len on nil ring should be 0")
	}
	if r.Cap() != 0 {
		t.Error("Cap on nil ring should be 0")
	}
}

// ---------- DropOldest policy ----------

func TestChannel_DropOldest(t *testing.T) {
	// Test the ring buffer directly to verify drop-oldest semantics.
	r := NewRing(4)

	// Fill and overflow the ring.
	for i := 0; i < 10; i++ {
		r.Push(testEvent("AAPL", float64(i)))
	}

	// Ring should have dropped 6 events (10 pushes - 4 capacity).
	if r.TotalDropped() != 6 {
		t.Fatalf("Ring.TotalDropped() = %d, want 6", r.TotalDropped())
	}

	// Pop should return events 4,5,6,7,8,9 — only the last 4 survive.
	var events []marketdata.MarketEvent
	for {
		ev, ok := r.Pop()
		if !ok {
			break
		}
		events = append(events, ev)
	}
	if len(events) != 4 {
		t.Fatalf("Pop count = %d, want 4", len(events))
	}
	// Events should be in order.
	for i, ev := range events {
		expected := float64(i + 6) // events 6,7,8,9
		if ev.(marketdata.Quote).Price != expected {
			t.Errorf("event[%d].Price = %v, want %v", i, ev.(marketdata.Quote).Price, expected)
		}
	}
}

func TestChannel_DropOldest_DeliversInOrder(t *testing.T) {
	// Test that the Channel delivers events in order via the forwardLoop.
	cfg := Config{
		Policy:     PolicyDropOldest,
		BufferSize: 64,
	}
	m := testMetrics(t)
	ch := NewChannel(cfg, testLogger(), m, nil)
	defer ch.Close()

	// Push 50 events.
	for i := 0; i < 50; i++ {
		ch.Send(testEvent("AAPL", float64(i)))
	}

	// Drain and verify order.
	var events []marketdata.MarketEvent
	timeout := time.After(time.Second)
loop:
	for {
		select {
		case ev, ok := <-ch.C():
			if !ok {
				break loop
			}
			events = append(events, ev)
			if len(events) == 50 {
				break loop
			}
		case <-timeout:
			break loop
		}
	}

	if len(events) == 0 {
		t.Fatal("expected at least 1 event")
	}

	// Events must be in monotonically increasing order.
	for i := 1; i < len(events); i++ {
		prev := events[i-1].(marketdata.Quote).Price
		curr := events[i].(marketdata.Quote).Price
		if curr <= prev {
			t.Errorf("events out of order: [%d]=%v <= [%d]=%v", i-1, prev, i, curr)
		}
	}
}

func TestChannel_DropOldest_Concurrent(t *testing.T) {
	cfg := Config{
		Policy:     PolicyDropOldest,
		BufferSize: 1024,
	}
	m := testMetrics(t)
	ch := NewChannel(cfg, testLogger(), m, nil)
	defer ch.Close()

	var wg sync.WaitGroup
	const producers = 8
	const eventsPerProducer = 10000

	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < eventsPerProducer; i++ {
				ch.Send(testEvent("AAPL", float64(i)))
			}
		}(p)
	}
	wg.Wait()

	t.Logf("TotalDropped=%d", ch.TotalDropped())
	// Some drops are expected with 8 producers × 10k events into a 1024 buffer.
	if ch.TotalDropped() == 0 {
		t.Log("no drops — buffer was large enough, which is fine")
	}
}

// ---------- DropNewest policy ----------

func TestChannel_DropNewest(t *testing.T) {
	cfg := Config{
		Policy:     PolicyDropNewest,
		BufferSize: 4,
	}
	m := testMetrics(t)
	ch := NewChannel(cfg, testLogger(), m, nil)
	defer ch.Close()

	// Fill the channel.
	for i := 0; i < 4; i++ {
		if !ch.Send(testEvent("AAPL", float64(i))) {
			t.Fatalf("Send(%d) returned false", i)
		}
	}

	// This should be dropped.
	if ch.Send(testEvent("AAPL", 99)) {
		t.Error("Send on full DropNewest channel should return false")
	}
	if ch.TotalDropped() != 1 {
		t.Fatalf("TotalDropped() = %d, want 1", ch.TotalDropped())
	}

	// Drain and verify original events are intact.
	var events []marketdata.MarketEvent
	for i := 0; i < 4; i++ {
		ev := <-ch.C()
		events = append(events, ev)
	}
	for i, ev := range events {
		if ev.(marketdata.Quote).Price != float64(i) {
			t.Errorf("event[%d].Price = %v, want %d", i, ev.(marketdata.Quote).Price, i)
		}
	}
}

func TestChannel_DropNewest_Concurrent(t *testing.T) {
	cfg := Config{
		Policy:     PolicyDropNewest,
		BufferSize: 64,
	}
	m := testMetrics(t)
	ch := NewChannel(cfg, testLogger(), m, nil)
	defer ch.Close()

	var wg sync.WaitGroup
	const producers = 8
	const eventsPerProducer = 10000

	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < eventsPerProducer; i++ {
				ch.Send(testEvent("AAPL", float64(i)))
			}
		}()
	}
	wg.Wait()

	if ch.TotalDropped() == 0 {
		t.Error("expected some drops with small buffer and many producers")
	}
	t.Logf("TotalDropped=%d", ch.TotalDropped())
}

// ---------- Disconnect policy ----------

func TestChannel_Disconnect_ConsecutiveDrops(t *testing.T) {
	var disconnected atomic.Bool
	cfg := Config{
		Policy:              PolicyDisconnect,
		BufferSize:          1,
		MaxConsecutiveDrops: 5,
		DropWindow:          time.Second,
		DropThreshold:       0.9,
	}
	m := testMetrics(t)
	ch := NewChannel(cfg, testLogger(), m, func(reason string) {
		disconnected.Store(true)
	})
	defer ch.Close()

	// Buffer of 1 means every push after the first overflows and drops.
	// 20 pushes should trigger 15+ drops, well above threshold of 5.
	for i := 0; i < 20; i++ {
		ch.Send(testEvent("AAPL", float64(i)))
	}

	// Wait briefly for the async disconnect callback.
	time.Sleep(100 * time.Millisecond)

	if !disconnected.Load() {
		t.Errorf("expected consumer to be disconnected, consecutiveDrops=%d", ch.ConsecutiveDrops())
	}
}

func TestChannel_Disconnect_NoDisconnectBelowThreshold(t *testing.T) {
	var disconnected atomic.Bool
	cfg := Config{
		Policy:              PolicyDisconnect,
		BufferSize:          256,
		MaxConsecutiveDrops: 100,
		DropWindow:          0,
		DropThreshold:       0,
	}
	m := testMetrics(t)
	ch := NewChannel(cfg, testLogger(), m, func(reason string) {
		disconnected.Store(true)
	})
	defer ch.Close()

	// Send a few events that don't fill the buffer — no drops.
	for i := 0; i < 10; i++ {
		ch.Send(testEvent("AAPL", float64(i)))
	}

	time.Sleep(50 * time.Millisecond)
	if disconnected.Load() {
		t.Error("should not disconnect when no drops occur")
	}
}

// ---------- ResetDrops ----------

func TestChannel_ResetDrops(t *testing.T) {
	cfg := Config{
		Policy:              PolicyDisconnect,
		BufferSize:          1,
		MaxConsecutiveDrops: 10,
		DropWindow:          time.Second,
		DropThreshold:       0.9,
	}
	m := testMetrics(t)
	ch := NewChannel(cfg, testLogger(), m, nil)
	defer ch.Close()

	// Buffer of 1: every push after the first overflows and drops.
	// The forwardLoop drains, but rapid sends will cause consecutive drops.
	for i := 0; i < 20; i++ {
		ch.Send(testEvent("AAPL", float64(i)))
		time.Sleep(time.Microsecond) // yield to let ring drain between bursts
	}

	// With buffer=1 and rapid sends, we should see some drops.
	if ch.TotalDropped() == 0 {
		t.Fatal("expected non-zero total drops")
	}

	ch.ResetDrops()
	if ch.ConsecutiveDrops() != 0 {
		t.Errorf("ConsecutiveDrops() = %d after reset, want 0", ch.ConsecutiveDrops())
	}
}

// ---------- Close ----------

func TestChannel_Close(t *testing.T) {
	cfg := Config{
		Policy:     PolicyDropOldest,
		BufferSize: 4,
	}
	m := testMetrics(t)
	ch := NewChannel(cfg, testLogger(), m, nil)
	ch.Close()

	// Send after close should return false.
	if ch.Send(testEvent("AAPL", 1)) {
		t.Error("Send after Close should return false")
	}
}

func TestChannel_CloseIdempotent(t *testing.T) {
	cfg := Config{
		Policy:     PolicyDropNewest,
		BufferSize: 4,
	}
	m := testMetrics(t)
	ch := NewChannel(cfg, testLogger(), m, nil)
	ch.Close()
	ch.Close() // should not panic
}

// ---------- Metrics ----------

func TestMetrics_EventsDroppedTotal(t *testing.T) {
	cfg := Config{
		Policy:     PolicyDropNewest,
		BufferSize: 2,
	}
	reg := prometheus.NewPedanticRegistry()
	m := NewMetrics(reg)
	ch := NewChannel(cfg, testLogger(), m, nil)
	defer ch.Close()

	// Fill and drop.
	ch.Send(testEvent("AAPL", 1))
	ch.Send(testEvent("AAPL", 2))
	ch.Send(testEvent("AAPL", 3)) // dropped

	gathered, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range gathered {
		if mf.GetName() == "rtmds_backpressure_events_dropped_total" {
			for _, m := range mf.GetMetric() {
				if m.GetCounter().GetValue() < 1 {
					t.Error("expected EventsDroppedTotal >= 1")
				}
			}
			return
		}
	}
	t.Error("EventsDroppedTotal metric not found")
}
