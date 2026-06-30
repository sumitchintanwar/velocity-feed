package aggregation

import "time"

// WindowSize defines the duration of an aggregation window.
type WindowSize time.Duration

const (
	Window1Second WindowSize = WindowSize(time.Second)
	Window5Second WindowSize = WindowSize(5 * time.Second)
	Window1Minute WindowSize = WindowSize(time.Minute)
)

// Tick represents a single raw trade/event coming into the aggregator.
type Tick struct {
	Symbol    string
	Price     float64
	Volume    float64
	Timestamp time.Time
}

// OHLC represents the aggregated Open-High-Low-Close state.
type OHLC struct {
	Symbol     string
	WindowSize WindowSize
	Start      time.Time
	End        time.Time
	Open       float64
	High       float64
	Low        float64
	Close      float64
	Volume     float64
	TradeCount int64
}

// VWAP represents the continuously updating Volume-Weighted Average Price.
type VWAP struct {
	Symbol     string
	Start      time.Time
	End        time.Time // Set when the window is flushed
	CumulativePriceVolume float64
	CumulativeVolume      float64
	VWAP                  float64
}
