package aggregation

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Publisher interface for outputting aggregated data.
type Publisher interface {
	PublishOHLC(ohlc OHLC)
	PublishVWAP(vwap VWAP)
}

// Engine manages aggregation state across all symbols.
type Engine struct {
	symbols   sync.Map // string -> *symbolAggregator
	publisher Publisher
	publishCh chan OHLC
	windows   []WindowSize
}

// NewEngine creates a new aggregation engine.
func NewEngine(publisher Publisher, windows ...WindowSize) *Engine {
	if len(windows) == 0 {
		windows = []WindowSize{Window1Second, Window5Second, Window1Minute}
	}
	return &Engine{
		publisher: publisher,
		publishCh: make(chan OHLC, 10000), // Bounded async buffer
		windows:   windows,
	}
}

// Start spawns background workers and the stagnant window sweeper.
func (e *Engine) Start(ctx context.Context) {
	// Spawn 4 publisher workers to drain the publishCh
	for i := 0; i < 4; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case ohlc := <-e.publishCh:
					if e.publisher != nil {
						e.publisher.PublishOHLC(ohlc)
					}
				}
			}
		}()
	}

	// Spawn stagnant window sweeper
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				e.sweepStagnant(t)
			}
		}
	}()
}

func (e *Engine) sweepStagnant(currentTime time.Time) {
	e.symbols.Range(func(key, value interface{}) bool {
		agg := value.(*symbolAggregator)
		agg.mu.Lock()

		// 1. Memory Leak Fix (Evict stale symbols)
		if currentTime.Sub(agg.lastUpdate) > 24*time.Hour {
			agg.deleted = true
			e.symbols.Delete(key)
			ActiveSymbols.Dec()
			agg.mu.Unlock()
			return true
		}

		// 2. VWAP Daily Reset
		if agg.lastVWAPReset.Day() != currentTime.Day() || agg.lastVWAPReset.Month() != currentTime.Month() || agg.lastVWAPReset.Year() != currentTime.Year() {
			agg.vwap.Reset()
			agg.lastVWAPReset = currentTime
		}

		// 3. Flush stagnant OHLC windows
		for _, ohlcAgg := range agg.ohlcs {
			if ohlcAgg.IsStagnant(currentTime) {
				if ohlc := ohlcAgg.ForceFlush(); ohlc != nil {
					agg.publishOHLC(*ohlc)
				}
			}
		}

		agg.mu.Unlock()
		return true // continue iteration
	})
}

// ProcessTick routes a tick to the appropriate symbol aggregators.
func (e *Engine) ProcessTick(tick Tick) {
	start := time.Now()
	
	TicksProcessed.WithLabelValues(tick.Symbol).Inc()

	for {
		v, ok := e.symbols.Load(tick.Symbol)
		if !ok {
			var loaded bool
			v, loaded = e.symbols.LoadOrStore(tick.Symbol, newSymbolAggregator(tick.Symbol, e.publishCh, e.publisher, e.windows))
			if !loaded {
				ActiveSymbols.Inc()
			}
		}
		agg := v.(*symbolAggregator)

		// AddTick returns false if the aggregator was just evicted by the sweeper race condition.
		if agg.AddTick(tick) {
			break // Success
		}
		
		// If false, it was deleted. Loop around to LoadOrStore a fresh aggregator.
	}

	TickProcessingLatency.Observe(time.Since(start).Seconds())
}

// symbolAggregator encapsulates all aggregation state for a single symbol.
type symbolAggregator struct {
	mu        sync.Mutex
	symbol    string
	publishCh chan OHLC
	publisher Publisher
	
	ohlcs         []*OHLCAggregator
	vwap          *VWAPAggregator
	lastUpdate      time.Time
	lastVWAPReset   time.Time
	lastVWAPPublish time.Time
	deleted         bool
}

func newSymbolAggregator(symbol string, publishCh chan OHLC, publisher Publisher, windows []WindowSize) *symbolAggregator {
	agg := &symbolAggregator{
		symbol:        symbol,
		publishCh:     publishCh,
		publisher:     publisher,
		ohlcs:           make([]*OHLCAggregator, len(windows)),
		vwap:            NewVWAPAggregator(),
		lastUpdate:      time.Now(),
		lastVWAPReset:   time.Now(),
		lastVWAPPublish: time.Now(),
	}
	for i, w := range windows {
		agg.ohlcs[i] = NewOHLCAggregator(w)
	}
	return agg
}

// AddTick ingests a tick and delegates it to the window tracking logic.
// Returns true if processed, false if this aggregator has been marked as deleted (race condition).
func (s *symbolAggregator) AddTick(tick Tick) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.deleted {
		return false
	}

	s.lastUpdate = tick.Timestamp
	var wasLate bool

	// OHLCs
	for _, ohlcAgg := range s.ohlcs {
		finalized, late := ohlcAgg.AddTick(tick)
		if late {
			wasLate = true
		}
		if finalized != nil {
			s.publishOHLC(*finalized)
		}
	}

	if wasLate {
		LateEvents.WithLabelValues(tick.Symbol).Inc()
	}

	// VWAP
	currentVWAP := s.vwap.AddTick(tick)
	
	// VWAP is throttled to 1 publish per second
	if s.publisher != nil && tick.Timestamp.Sub(s.lastVWAPPublish) >= time.Second {
		s.publisher.PublishVWAP(currentVWAP)
		s.lastVWAPPublish = tick.Timestamp
	}

	return true
}

// publishOHLC safely dispatches the event without blocking the hot path
func (s *symbolAggregator) publishOHLC(ohlc OHLC) {
	AggregationsPublished.WithLabelValues(fmt.Sprintf("ohlc_%ds", int(time.Duration(ohlc.WindowSize).Seconds()))).Inc()
	
	// Non-blocking send to bounded channel.
	// If channel is full, we must drop or block. In high freq systems, dropping is better than deadlock,
	// but we must alert on it.
	select {
	case s.publishCh <- ohlc:
	default:
		DroppedCandles.WithLabelValues(s.symbol).Inc()
		// (In a real system, you'd also log a throttled Warn here)
	}
}
