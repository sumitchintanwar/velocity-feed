package aggregation

import (
	"time"
)

// OHLCAggregator maintains the state for a specific time window.
type OHLCAggregator struct {
	windowSize WindowSize
	state      *OHLC
}

// NewOHLCAggregator creates a new aggregator.
func NewOHLCAggregator(size WindowSize) *OHLCAggregator {
	return &OHLCAggregator{
		windowSize: size,
	}
}

// AddTick processes a new tick. If the tick belongs to a new window (wall-clock aligned),
// it returns the finalized prior OHLC for publishing, and starts a new one.
// It also returns a boolean indicating if the tick was rejected for being late.
// The caller is responsible for publishing the returned *OHLC.
func (a *OHLCAggregator) AddTick(tick Tick) (*OHLC, bool) {
	windowStart := a.alignTime(tick.Timestamp)

	// If no state exists, initialize
	if a.state == nil {
		a.init(tick, windowStart)
		return nil, false
	}

	// If the tick belongs to a FUTURE window, we must flush the current state.
	if windowStart.After(a.state.Start) {
		finished := a.state
		a.init(tick, windowStart)
		return finished, false
	}

	// Late ticks (older than current window) are discarded in this basic implementation.
	if windowStart.Before(a.state.Start) {
		return nil, true
	}

	// Same window: Update aggregates
	if tick.Price > a.state.High {
		a.state.High = tick.Price
	}
	if tick.Price < a.state.Low {
		a.state.Low = tick.Price
	}
	a.state.Close = tick.Price
	a.state.Volume += tick.Volume
	a.state.TradeCount++

	return nil, false
}

// ForceFlush returns the current state and clears it. Useful on shutdown or periodic sweeps.
func (a *OHLCAggregator) ForceFlush() *OHLC {
	if a.state == nil {
		return nil
	}
	st := a.state
	a.state = nil
	return st
}

// IsStagnant returns true if the current time has passed the end of the window.
func (a *OHLCAggregator) IsStagnant(currentTime time.Time) bool {
	if a.state == nil {
		return false
	}
	return currentTime.After(a.state.End)
}

func (a *OHLCAggregator) init(tick Tick, start time.Time) {
	a.state = &OHLC{
		Symbol:     tick.Symbol,
		WindowSize: a.windowSize,
		Start:      start,
		End:        start.Add(time.Duration(a.windowSize)),
		Open:       tick.Price,
		High:       tick.Price,
		Low:        tick.Price,
		Close:      tick.Price,
		Volume:     tick.Volume,
		TradeCount: 1,
	}
}

// alignTime truncates the timestamp to the nearest WindowSize boundary.
func (a *OHLCAggregator) alignTime(t time.Time) time.Time {
	return t.Truncate(time.Duration(a.windowSize))
}
