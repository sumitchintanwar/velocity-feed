// Package simulator provides a fake Feed implementation that emits random
// market quotes at a configurable rate. Useful for local development and
// integration tests without needing real API credentials.
//
// Three pre-built configurations are available:
//
//   - DefaultConfig()        — 500 ms tick interval, suitable for development.
//   - BenchmarkConfig()      — 100 µs tick interval, suitable for unit benchmarks.
//   - MaxThroughputConfig()  — unlimited mode, one goroutine per symbol,
//     designed to saturate downstream components during stress testing.
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

// BurstConfig defines an optional burst phase injected into fixed-rate generation.
// When Multiplier > 0 the simulator periodically emits a burst of messages at
// Multiplier × the base rate for DurationMs milliseconds, then returns to the
// base rate.  This simulates opening-bell surges, flash-crash events, or
// exchange-recovery floods.
type BurstConfig struct {
	// Multiplier is the burst rate relative to the base TickInterval.
	// 5.0 means tick 5× faster during the burst window.
	// 0 disables bursting.
	Multiplier float64
	// DurationMs is how long (in milliseconds) each burst lasts.
	DurationMs int
	// PeriodMs is how often (in milliseconds) a burst occurs.
	// e.g. PeriodMs=60000, DurationMs=500 simulates a burst every minute.
	PeriodMs int
}

// Config controls simulator behaviour.
type Config struct {
	// TickInterval is how often a new quote is generated per symbol.
	// Ignored when Unlimited is true.
	TickInterval time.Duration

	// Unlimited disables rate-limiting entirely.  One goroutine per symbol
	// runs a tight hot loop, making the simulator capable of producing
	// millions of messages per second.  Use this mode to find the true
	// saturation point of downstream components.
	//
	// NOTE: In Unlimited mode, subscriptions are statically snapshotted when
	// Run() is called. Dynamic Subscribe() / Unsubscribe() calls are ignored.
	Unlimited bool

	// Seed controls the random number generator.  When non-zero, each
	// per-symbol goroutine is initialised with a deterministic seed derived
	// from this value, enabling perfectly reproducible benchmark runs.
	// When 0, the global (non-deterministic) rand source is used.
	Seed int64

	// Burst configures optional burst traffic injection.
	// Only active in fixed-rate mode (Unlimited == false).
	Burst BurstConfig

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

// BenchmarkConfig returns a high-throughput config for unit benchmarks.
// Produces ~50,000 msg/sec with 5 symbols (100 µs tick interval).
// For true saturation testing, prefer MaxThroughputConfig.
func BenchmarkConfig() Config {
	return Config{
		TickInterval: 100 * time.Microsecond,
		BasePrice:    100.0,
		Volatility:   0.02,
	}
}

// MaxThroughputConfig returns an unlimited, deterministically-seeded config
// for stress testing.  Each symbol runs its own hot-loop goroutine with no
// artificial rate limiting, allowing the generator to saturate the downstream
// publisher, Redis, and gateway components.
//
// Use this config to find the true saturation point of the system under test.
func MaxThroughputConfig() Config {
	return Config{
		Unlimited:  true,
		Seed:       42, // deterministic; change per run to vary sequences
		BasePrice:  100.0,
		Volatility: 0.02,
	}
}

// Validate checks that the config values are within acceptable ranges.
func (c Config) Validate() error {
	if !c.Unlimited && c.TickInterval <= 0 {
		return fmt.Errorf("simulator: TickInterval must be positive, got %v", c.TickInterval)
	}
	if c.BasePrice <= 0 {
		return fmt.Errorf("simulator: BasePrice must be positive, got %f", c.BasePrice)
	}
	if c.Volatility < 0 || c.Volatility > 1 {
		return fmt.Errorf("simulator: Volatility must be in [0, 1], got %f", c.Volatility)
	}
	if c.Burst.Multiplier < 0 {
		return fmt.Errorf("simulator: Burst.Multiplier must be >= 0, got %f", c.Burst.Multiplier)
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

	started      atomic.Bool  // guards against multiple Run calls
	droppedCount atomic.Int64 // total messages dropped due to a full output channel
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

// DroppedCount returns the total number of messages silently dropped because
// the output channel was full.  Non-zero values indicate the consumer is
// slower than the generator — useful for diagnosing backpressure.
func (s *Simulator) DroppedCount() int64 { return s.droppedCount.Load() }

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

// Run implements marketdata.Feed.
//
// When cfg.Unlimited is false, Run ticks at cfg.TickInterval and emits a
// randomly-walked quote for each subscribed symbol — identical to the original
// behaviour.
//
// When cfg.Unlimited is true, Run spawns one goroutine per symbol, each
// running a tight hot loop with no intentional rate limiting.  This is designed
// to saturate downstream components (publisher, Redis, gateway) so engineers
// can identify the true system bottleneck.
//
// Run may only be called once. A second call returns ErrAlreadyStarted.
func (s *Simulator) Run(ctx context.Context) (<-chan marketdata.Quote, error) {
	if !s.started.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("simulator: Run already started")
	}

	// Snapshot the symbol list once at startup.
	s.mu.RLock()
	syms := make([]string, len(s.symbols))
	copy(syms, s.symbols)
	s.mu.RUnlock()

	// Mitigate channel lock contention in Unlimited mode by vastly increasing the
	// buffer size, as multiple hot-loop goroutines will be firing concurrently.
	bufSize := 4096
	if s.cfg.Unlimited {
		bufSize = 65536
	}
	out := make(chan marketdata.Quote, bufSize)

	if s.cfg.Unlimited {
		s.runUnlimited(ctx, syms, out)
	} else {
		s.runFixedRate(ctx, syms, out)
	}

	return out, nil
}

// runUnlimited launches one goroutine per symbol.  Each goroutine owns its
// own price state and rand.Rand, eliminating all lock contention on the hot
// path.  The channel closer is handled by a coordinator goroutine that waits
// for all symbol goroutines to finish.
func (s *Simulator) runUnlimited(ctx context.Context, syms []string, out chan<- marketdata.Quote) {
	var wg sync.WaitGroup

	for i, sym := range syms {
		wg.Add(1)
		sym := sym // capture
		// Each symbol gets its own seeded rand to ensure deterministic output
		// when cfg.Seed != 0.  When cfg.Seed == 0 we fall back to the global
		// source (non-deterministic), consistent with existing behaviour.
		var rng *rand.Rand
		if s.cfg.Seed != 0 {
			// Derive a unique seed per symbol by mixing the global seed with
			// the symbol index.  This guarantees different price sequences per
			// symbol while remaining fully reproducible across runs.
			rng = rand.New(rand.NewSource(s.cfg.Seed + int64(i))) //nolint:gosec
		}

		price := s.cfg.BasePrice

		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				var delta float64
				if rng != nil {
					delta = price * s.cfg.Volatility * (rng.Float64()*2 - 1)
				} else {
					delta = price * s.cfg.Volatility * (rand.Float64()*2 - 1) //nolint:gosec
				}
				price += delta
				if price <= 0 {
					price = s.cfg.BasePrice
				}

				var vol int64
				if rng != nil {
					vol = int64(rng.Intn(10000) + 100) //nolint:gosec
				} else {
					vol = int64(rand.Intn(10000) + 100) //nolint:gosec
				}

				q := marketdata.Quote{
					Symbol:    sym,
					Type:      marketdata.QuoteTypeTrade,
					Price:     price,
					Bid:       price * 0.9995,
					Ask:       price * 1.0005,
					Volume:    vol,
					Timestamp: s.clock.Now(),
					Provider:  s.Name(),
				}

				select {
				case out <- q:
				case <-ctx.Done():
					return
				default:
					// Channel full — record the drop and continue.
					// Dropping is intentional in unlimited mode: the goal is
					// to saturate downstream, not to guarantee delivery.
					s.droppedCount.Add(1)
				}
			}
		}()
	}

	// Coordinator: close out when all symbol goroutines have exited.
	go func() {
		wg.Wait()
		close(out)
	}()
}

// runFixedRate is the original single-goroutine, ticker-driven generation
// loop.  All existing behaviour is preserved exactly.
func (s *Simulator) runFixedRate(ctx context.Context, syms []string, out chan<- marketdata.Quote) {
	go func() {
		defer close(out)
		ticker := time.NewTicker(s.cfg.TickInterval)
		defer ticker.Stop()

		// Burst state.
		inBurst := false
		var burstEnd time.Time
		var nextBurst time.Time
		if s.cfg.Burst.Multiplier > 0 && s.cfg.Burst.PeriodMs > 0 {
			nextBurst = time.Now().Add(time.Duration(s.cfg.Burst.PeriodMs) * time.Millisecond)
		}

		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				// --- Burst management ---
				if s.cfg.Burst.Multiplier > 0 && s.cfg.Burst.PeriodMs > 0 {
					if !inBurst && t.After(nextBurst) {
						inBurst = true
						burstEnd = t.Add(time.Duration(s.cfg.Burst.DurationMs) * time.Millisecond)
						ticker.Reset(time.Duration(float64(s.cfg.TickInterval) / s.cfg.Burst.Multiplier))
					} else if inBurst && t.After(burstEnd) {
						inBurst = false
						nextBurst = t.Add(time.Duration(s.cfg.Burst.PeriodMs) * time.Millisecond)
						ticker.Reset(s.cfg.TickInterval)
					}
				}

				now := s.clock.Now()

				// Snapshot symbols under read lock to avoid racing
				// with Subscribe/Unsubscribe.
				s.mu.RLock()
				currentSyms := make([]string, len(s.symbols))
				copy(currentSyms, s.symbols)
				s.mu.RUnlock()

				for _, sym := range currentSyms {
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
						s.droppedCount.Add(1)
					}
				}
			}
		}
	}()
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
