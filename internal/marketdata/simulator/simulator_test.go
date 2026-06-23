package simulator

import (
	"context"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/marketdata"
)

// testClock is a deterministic clock for testing.
type testClock struct {
	now time.Time
}

func (c *testClock) Now() time.Time { return c.now }

// --- Config validation tests ---

func TestDefaultConfigIsValid(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatalf("DefaultConfig should be valid: %v", err)
	}
}

func TestConfig_Validate_TickIntervalZero(t *testing.T) {
	cfg := Config{TickInterval: 0, BasePrice: 100, Volatility: 0.02}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for TickInterval=0")
	}
}

func TestConfig_Validate_TickIntervalNegative(t *testing.T) {
	cfg := Config{TickInterval: -1 * time.Second, BasePrice: 100, Volatility: 0.02}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for negative TickInterval")
	}
}

func TestConfig_Validate_BasePriceZero(t *testing.T) {
	cfg := Config{TickInterval: time.Second, BasePrice: 0, Volatility: 0.02}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for BasePrice=0")
	}
}

func TestConfig_Validate_BasePriceNegative(t *testing.T) {
	cfg := Config{TickInterval: time.Second, BasePrice: -50, Volatility: 0.02}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for negative BasePrice")
	}
}

func TestConfig_Validate_VolatilityNegative(t *testing.T) {
	cfg := Config{TickInterval: time.Second, BasePrice: 100, Volatility: -0.1}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for Volatility < 0")
	}
}

func TestConfig_Validate_VolatilityTooHigh(t *testing.T) {
	cfg := Config{TickInterval: time.Second, BasePrice: 100, Volatility: 1.5}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for Volatility > 1")
	}
}

func TestConfig_Validate_VolatilityBoundary(t *testing.T) {
	// Volatility = 0 and 1 should both be valid.
	for _, v := range []float64{0, 1} {
		cfg := Config{TickInterval: time.Second, BasePrice: 100, Volatility: v}
		if err := cfg.Validate(); err != nil {
			t.Errorf("Volatility=%f should be valid: %v", v, err)
		}
	}
}

// --- Constructor tests ---

func TestNew_InvalidConfig(t *testing.T) {
	cfg := Config{TickInterval: 0, BasePrice: 0, Volatility: 0}
	_, err := New(cfg, nil)
	if err == nil {
		t.Fatal("expected error for invalid config")
	}
}

func TestNew_NilClockDefaultsToWallClock(t *testing.T) {
	cfg := DefaultConfig()
	s, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.clock == nil {
		t.Fatal("clock should not be nil after construction")
	}
}

func TestNew_WithSymbols(t *testing.T) {
	cfg := DefaultConfig()
	s, err := New(cfg, nil, "AAPL", "TSLA")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(s.symbols) != 2 {
		t.Fatalf("expected 2 symbols, got %d", len(s.symbols))
	}
}

// --- Subscribe / Unsubscribe tests ---

func TestSubscribe_AddsSymbols(t *testing.T) {
	s, _ := New(DefaultConfig(), nil)
	_ = s.Subscribe("AAPL", "TSLA")
	if len(s.symbols) != 2 {
		t.Fatalf("expected 2 symbols, got %d", len(s.symbols))
	}
	if _, ok := s.prices["AAPL"]; !ok {
		t.Fatal("AAPL should have an initial price")
	}
}

func TestSubscribe_DuplicatesIgnored(t *testing.T) {
	s, _ := New(DefaultConfig(), nil, "AAPL")
	_ = s.Subscribe("AAPL")
	if len(s.symbols) != 1 {
		t.Fatalf("expected 1 symbol (deduped), got %d", len(s.symbols))
	}
}

func TestUnsubscribe_RemovesSymbol(t *testing.T) {
	s, _ := New(DefaultConfig(), nil, "AAPL", "TSLA")
	_ = s.Unsubscribe("AAPL")
	if len(s.symbols) != 1 {
		t.Fatalf("expected 1 symbol after unsubscribe, got %d", len(s.symbols))
	}
	if _, ok := s.prices["AAPL"]; ok {
		t.Fatal("AAPL price should be removed")
	}
	if s.symbols[0] != "TSLA" {
		t.Fatalf("expected remaining symbol TSLA, got %s", s.symbols[0])
	}
}

func TestUnsubscribe_NonexistentSymbol(t *testing.T) {
	s, _ := New(DefaultConfig(), nil, "AAPL")
	_ = s.Unsubscribe("MSFT") // should not panic
	if len(s.symbols) != 1 {
		t.Fatal("unsubscribing nonexistent symbol should not affect others")
	}
}

// --- Name ---

func TestName(t *testing.T) {
	s, _ := New(DefaultConfig(), nil)
	if s.Name() != "simulator" {
		t.Fatalf("expected 'simulator', got %q", s.Name())
	}
}

// --- Price generation tests ---

func TestNextPrice_PositiveBase(t *testing.T) {
	cfg := Config{TickInterval: time.Second, BasePrice: 100, Volatility: 0.02}
	s, _ := New(cfg, nil, "SYM")

	// Run nextPrice many times — price should never go negative or zero.
	for i := 0; i < 1000; i++ {
		p := s.nextPrice("SYM")
		if p <= 0 {
			t.Fatalf("price went non-positive: %f at iteration %d", p, i)
		}
	}
}

func TestNextPrice_VolatilityZero(t *testing.T) {
	cfg := Config{TickInterval: time.Second, BasePrice: 100, Volatility: 0}
	s, _ := New(cfg, nil, "SYM")

	// With zero volatility, price should stay at base.
	for i := 0; i < 100; i++ {
		p := s.nextPrice("SYM")
		if p != 100.0 {
			t.Fatalf("expected price 100.0 with zero volatility, got %f", p)
		}
	}
}

// --- Run tests ---

func TestRun_EmitsQuotesForAllSymbols(t *testing.T) {
	cfg := Config{TickInterval: 10 * time.Millisecond, BasePrice: 50, Volatility: 0.01}
	clk := &testClock{now: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
	s, _ := New(cfg, clk, "AAPL", "TSLA")

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	quotes, err := s.Run(ctx)
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	seen := make(map[string]bool)
	for q := range quotes {
		seen[q.Symbol] = true
		if q.Provider != "simulator" {
			t.Errorf("expected provider 'simulator', got %q", q.Provider)
		}
		if q.Type != marketdata.QuoteTypeTrade {
			t.Errorf("expected type 'trade', got %q", q.Type)
		}
	}

	if !seen["AAPL"] {
		t.Error("never received AAPL quote")
	}
	if !seen["TSLA"] {
		t.Error("never received TSLA quote")
	}
}

func TestRun_UsesClockTimestamp(t *testing.T) {
	fixed := time.Date(2025, 6, 15, 12, 0, 0, 0, time.UTC)
	clk := &testClock{now: fixed}

	cfg := Config{TickInterval: 10 * time.Millisecond, BasePrice: 100, Volatility: 0}
	s, _ := New(cfg, clk, "SYM")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	quotes, err := s.Run(ctx)
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	q := <-quotes
	if !q.Timestamp.Equal(fixed) {
		t.Errorf("expected timestamp %v, got %v", fixed, q.Timestamp)
	}
}

func TestRun_StopOnContext(t *testing.T) {
	cfg := Config{TickInterval: time.Second, BasePrice: 100, Volatility: 0.02}
	s, _ := New(cfg, nil, "AAPL")

	ctx, cancel := context.WithCancel(context.Background())
	quotes, err := s.Run(ctx)
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	// Cancel immediately — Run should return and channel should close.
	cancel()

	// Drain any buffered quotes, then expect channel closed.
	for range quotes {
	}
	// If we get here without deadlock, the test passes.
}

func TestRun_DoubleCallReturnsError(t *testing.T) {
	cfg := Config{TickInterval: time.Second, BasePrice: 100, Volatility: 0.02}
	s, _ := New(cfg, nil, "AAPL")

	ctx := context.Background()
	_, err := s.Run(ctx)
	if err != nil {
		t.Fatalf("first Run should succeed: %v", err)
	}

	_, err = s.Run(ctx)
	if err == nil {
		t.Fatal("second Run should return error")
	}
}

func TestRun_BidAskSpread(t *testing.T) {
	cfg := Config{TickInterval: 10 * time.Millisecond, BasePrice: 100, Volatility: 0}
	s, _ := New(cfg, nil, "SYM")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	quotes, err := s.Run(ctx)
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	q := <-quotes
	if q.Bid >= q.Price {
		t.Errorf("bid (%f) should be less than price (%f)", q.Bid, q.Price)
	}
	if q.Ask <= q.Price {
		t.Errorf("ask (%f) should be greater than price (%f)", q.Ask, q.Price)
	}
}

func TestRun_ConcurrentSubscribeNoRace(t *testing.T) {
	cfg := Config{TickInterval: 5 * time.Millisecond, BasePrice: 100, Volatility: 0.01}
	s, _ := New(cfg, nil, "AAPL")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	quotes, err := s.Run(ctx)
	if err != nil {
		t.Fatalf("Run error: %v", err)
	}

	// Subscribe while Run is active — exercises the mutex.
	go func() {
		for i := 0; i < 10; i++ {
			_ = s.Subscribe("TSLA")
			_ = s.Unsubscribe("TSLA")
		}
	}()

	// Drain until context expires.
	for range quotes {
	}
	// No race detector abort = success.
}
