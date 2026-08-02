package aggregation

import (
	"time"
)

// OHLCAggregator maintains the state for multiple active time windows.
type OHLCAggregator struct {
	windowSize WindowSize
	states     map[time.Time]*OHLC
}

// NewOHLCAggregator creates a new aggregator.
func NewOHLCAggregator(size WindowSize) *OHLCAggregator {
	return &OHLCAggregator{
		windowSize: size,
		states:     make(map[time.Time]*OHLC),
	}
}

// AddTick processes a new tick. It handles out-of-order ticks by keeping multiple active windows.
// It returns a boolean indicating if the tick was rejected for being extremely late (stagnant).
func (a *OHLCAggregator) AddTick(tick Tick, currentTime time.Time) bool {
	windowStart := a.alignTime(tick.Timestamp)

	// If the tick is for a window that is already stagnant and flushed, drop it.
	if currentTime.After(windowStart.Add(time.Duration(a.windowSize))) {
		return true
	}

	state, exists := a.states[windowStart]
	if !exists {
		state = &OHLC{
			Symbol:     tick.Symbol,
			WindowSize: a.windowSize,
			Start:      windowStart,
			End:        windowStart.Add(time.Duration(a.windowSize)),
			Open:       tick.Price, // Open is best-effort for out-of-order
			High:       tick.Price,
			Low:        tick.Price,
			Close:      tick.Price,
			Volume:     0,
			TradeCount: 0,
		}
		a.states[windowStart] = state
	}

	if tick.Price > state.High {
		state.High = tick.Price
	}
	if tick.Price < state.Low {
		state.Low = tick.Price
	}
	// Best-effort close for out of order.
	state.Close = tick.Price
	state.Volume += tick.Volume
	state.TradeCount++

	return false
}

// FlushStagnant returns all states that are stagnant and removes them from the map.
func (a *OHLCAggregator) FlushStagnant(currentTime time.Time) []*OHLC {
	var flushed []*OHLC
	for start, state := range a.states {
		if currentTime.After(state.End) {
			flushed = append(flushed, state)
			delete(a.states, start)
		}
	}
	return flushed
}

// ForceFlush returns all current states and clears the map. Useful on shutdown.
func (a *OHLCAggregator) ForceFlush() []*OHLC {
	var flushed []*OHLC
	for _, state := range a.states {
		flushed = append(flushed, state)
	}
	a.states = make(map[time.Time]*OHLC)
	return flushed
}

// alignTime truncates the timestamp to the nearest WindowSize boundary.
func (a *OHLCAggregator) alignTime(t time.Time) time.Time {
	return t.Truncate(time.Duration(a.windowSize))
}
