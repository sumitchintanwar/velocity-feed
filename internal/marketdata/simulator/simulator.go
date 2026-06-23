// Package simulator provides a fake Feed implementation that emits random
// market quotes at a configurable rate. Useful for local development and
// integration tests without needing real API credentials.
package simulator

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sumit/rtmds/internal/marketdata"
)

// Config controls simulator behaviour.
type Config struct {
	// TickInterval is how often a new quote is generated per symbol.
	TickInterval time.Duration
	// BasePrice is the starting price for all simulated symbols.
	BasePrice float64
	// Volatility controls the +/- price jitter (as a fraction of BasePrice).
	// 0.02 means ±2% per tick.
	Volatility float64
}

// DefaultConfig returns sensible simulator defaults.
func DefaultConfig() Config {
	return Config{
		TickInterval: 500 * time.Millisecond,
		BasePrice:    100.0,
		Volatility:   0.02,
	}
}

// Validate checks that the config values are within acceptable ranges.
func (c Config) Validate() error {
	if c.TickInterval <= 0 {
		return fmt.Errorf("simulator: TickInterval must be positive, got %v", c.TickInterval)
	}
	if c.BasePrice <= 0 {
		return fmt.Errorf("simulator: BasePrice must be positive, got %f", c.BasePrice)
	}
	if c.Volatility < 0 || c.Volatility > 1 {
		return fmt.Errorf("simulator: Volatility must be in [0, 1], got %f", c.Volatility)
	}
	return nil
}

// Simulator implements marketdata.Feed with synthetic data.
type Simulator struct {
	cfg   Config
	clock marketdata.Clock

	mu      sync.RWMutex // protects symbols and prices
	symbols []string
	prices  map[string]float64

	started atomic.Bool // guards against multiple Run calls
}

// New returns a new Simulator. symbols may be empty and added later via
// Subscribe. If clock is nil, the real system clock is used.
// Returns an error if the config is invalid.
func New(cfg Config, clock marketdata.Clock, symbols ...string) (*Simulator, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if clock == nil {
		clock = marketdata.WallClock{}
	}
	s := &Simulator{
		cfg:    cfg,
		clock:  clock,
		prices: make(map[string]float64),
	}
	// Subscribe does not need the lock here — Run has not started yet.
	_ = s.subscribeUnlocked(symbols...)
	return s, nil
}

// Name implements marketdata.Feed.
func (s *Simulator) Name() string { return "simulator" }

// Subscribe implements marketdata.Feed.
func (s *Simulator) Subscribe(symbols ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subscribeUnlocked(symbols...)
}

func (s *Simulator) subscribeUnlocked(symbols ...string) error {
	for _, sym := range symbols {
		if _, ok := s.prices[sym]; !ok {
			s.prices[sym] = s.cfg.BasePrice
			s.symbols = append(s.symbols, sym)
		}
	}
	return nil
}

// Unsubscribe implements marketdata.Feed.
func (s *Simulator) Unsubscribe(symbols ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rm := make(map[string]struct{}, len(symbols))
	for _, sym := range symbols {
		rm[sym] = struct{}{}
		delete(s.prices, sym)
	}
	filtered := s.symbols[:0]
	for _, sym := range s.symbols {
		if _, skip := rm[sym]; !skip {
			filtered = append(filtered, sym)
		}
	}
	s.symbols = filtered
	return nil
}

// Run implements marketdata.Feed. It ticks at cfg.TickInterval and emits a
// randomly-walked quote for each subscribed symbol.
//
// Run may only be called once. A second call returns ErrAlreadyStarted.
func (s *Simulator) Run(ctx context.Context) (<-chan marketdata.Quote, error) {
	if !s.started.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("simulator: Run already started")
	}

	out := make(chan marketdata.Quote, 64)

	go func() {
		defer close(out)
		ticker := time.NewTicker(s.cfg.TickInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := s.clock.Now()

				// Snapshot symbols under read lock to avoid racing
				// with Subscribe/Unsubscribe.
				s.mu.RLock()
				syms := make([]string, len(s.symbols))
				copy(syms, s.symbols)
				s.mu.RUnlock()

				for _, sym := range syms {
					// Take write lock briefly to update price.
					s.mu.Lock()
					price := s.nextPrice(sym)
					s.mu.Unlock()

					q := marketdata.Quote{
						Symbol:    sym,
						Type:      marketdata.QuoteTypeTrade,
						Price:     price,
						Bid:       price * 0.9995,
						Ask:       price * 1.0005,
						Volume:    int64(rand.Intn(10000) + 100), //nolint:gosec
						Timestamp: now,
						Provider:  s.Name(),
					}

					// Non-blocking send: drop if consumer is too slow.
					select {
					case out <- q:
					case <-ctx.Done():
						return
					default:
						// Channel full — drop rather than stall.
					}
				}
			}
		}
	}()

	return out, nil
}

func (s *Simulator) nextPrice(symbol string) float64 {
	prev := s.prices[symbol]
	delta := prev * s.cfg.Volatility * (rand.Float64()*2 - 1) //nolint:gosec
	next := prev + delta
	if next <= 0 {
		next = prev
	}
	s.prices[symbol] = next
	return next
}
