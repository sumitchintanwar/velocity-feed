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
	rejected := agg.AddTick(tick1, baseTime.Add(100*time.Millisecond))
	if rejected {
		t.Fatalf("Expected tick to be accepted")
	}

	// 2nd tick at 12:00:00.500 (New High)
	tick2 := Tick{Price: 105.0, Volume: 2.0, Timestamp: baseTime.Add(500 * time.Millisecond)}
	agg.AddTick(tick2, baseTime.Add(500*time.Millisecond))

	// 3rd tick at 12:00:00.900 (New Low, Close)
	tick3 := Tick{Price: 95.0, Volume: 3.0, Timestamp: baseTime.Add(900 * time.Millisecond)}
	agg.AddTick(tick3, baseTime.Add(900*time.Millisecond))

	// Verify internal state before flush
	state := agg.states[baseTime]
	if state == nil {
		t.Fatalf("Expected state to exist")
	}
	if state.Open != 100.0 {
		t.Errorf("Expected Open 100.0, got %f", state.Open)
	}
	if state.High != 105.0 {
		t.Errorf("Expected High 105.0, got %f", state.High)
	}
	if state.Low != 95.0 {
		t.Errorf("Expected Low 95.0, got %f", state.Low)
	}
	if state.Close != 95.0 {
		t.Errorf("Expected Close 95.0, got %f", state.Close)
	}
	if state.Volume != 6.0 {
		t.Errorf("Expected Volume 6.0, got %f", state.Volume)
	}
	if state.TradeCount != 3 {
		t.Errorf("Expected TradeCount 3, got %d", state.TradeCount)
	}

	// Flush stagnant check at 12:00:00.900 (should be empty)
	flushed := agg.FlushStagnant(baseTime.Add(900 * time.Millisecond))
	if len(flushed) != 0 {
		t.Fatalf("Expected 0 flushed windows, got %d", len(flushed))
	}

	// 4th tick at 12:00:01.100 (Crosses window boundary)
	tick4 := Tick{Price: 101.0, Volume: 1.0, Timestamp: baseTime.Add(1100 * time.Millisecond)}
	agg.AddTick(tick4, baseTime.Add(1100*time.Millisecond))

	// Flush stagnant check at 12:00:01.100
	flushed = agg.FlushStagnant(baseTime.Add(1100 * time.Millisecond))

	// Must return the flushed OHLC for 12:00:00
	if len(flushed) != 1 {
		t.Fatalf("Expected 1 flushed OHLC, got %d", len(flushed))
	}
	res := flushed[0]

	if res.Start != baseTime {
		t.Errorf("Expected Start %v, got %v", baseTime, res.Start)
	}
	if res.End != baseTime.Add(time.Second) {
		t.Errorf("Expected End %v, got %v", baseTime.Add(time.Second), res.End)
	}

	// Ensure new state is tracking tick4
	newState := agg.states[baseTime.Add(time.Second)]
	if newState.Open != 101.0 {
		t.Errorf("Expected new Open 101.0, got %f", newState.Open)
	}
}
