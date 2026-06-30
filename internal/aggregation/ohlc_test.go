package aggregation

import (
	"testing"
	"time"
)

func TestOHLCAggregator(t *testing.T) {
	agg := NewOHLCAggregator(Window1Second)

	// Base time exactly on the second
	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// 1st tick at 12:00:00.100
	tick1 := Tick{Price: 100.0, Volume: 1.0, Timestamp: baseTime.Add(100 * time.Millisecond)}
	res, _ := agg.AddTick(tick1)
	if res != nil {
		t.Fatalf("Expected nil, got %v", res)
	}

	// 2nd tick at 12:00:00.500 (New High)
	tick2 := Tick{Price: 105.0, Volume: 2.0, Timestamp: baseTime.Add(500 * time.Millisecond)}
	agg.AddTick(tick2)

	// 3rd tick at 12:00:00.900 (New Low, Close)
	tick3 := Tick{Price: 95.0, Volume: 3.0, Timestamp: baseTime.Add(900 * time.Millisecond)}
	agg.AddTick(tick3)

	// Verify internal state before flush
	if agg.state.Open != 100.0 { t.Errorf("Expected Open 100.0, got %f", agg.state.Open) }
	if agg.state.High != 105.0 { t.Errorf("Expected High 105.0, got %f", agg.state.High) }
	if agg.state.Low != 95.0 { t.Errorf("Expected Low 95.0, got %f", agg.state.Low) }
	if agg.state.Close != 95.0 { t.Errorf("Expected Close 95.0, got %f", agg.state.Close) }
	if agg.state.Volume != 6.0 { t.Errorf("Expected Volume 6.0, got %f", agg.state.Volume) }
	if agg.state.TradeCount != 3 { t.Errorf("Expected TradeCount 3, got %d", agg.state.TradeCount) }

	// 4th tick at 12:00:01.100 (Crosses window boundary)
	tick4 := Tick{Price: 101.0, Volume: 1.0, Timestamp: baseTime.Add(1100 * time.Millisecond)}
	res, _ = agg.AddTick(tick4)

	// Must return the flushed OHLC
	if res == nil {
		t.Fatalf("Expected flushed OHLC, got nil")
	}

	if res.Start != baseTime {
		t.Errorf("Expected Start %v, got %v", baseTime, res.Start)
	}
	if res.End != baseTime.Add(time.Second) {
		t.Errorf("Expected End %v, got %v", baseTime.Add(time.Second), res.End)
	}
	
	// Ensure new state is tracking tick4
	if agg.state.Open != 101.0 { t.Errorf("Expected new Open 101.0, got %f", agg.state.Open) }
}
