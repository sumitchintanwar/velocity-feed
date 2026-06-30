package aggregation

import (
	"testing"
	"time"
)

func TestVWAPAggregator(t *testing.T) {
	vwap := NewVWAPAggregator()
	baseTime := time.Now()

	// Tick 1: 100 price, 10 volume -> VWAP = 100
	res1 := vwap.AddTick(Tick{Price: 100.0, Volume: 10.0, Timestamp: baseTime})
	if res1.VWAP != 100.0 {
		t.Errorf("Expected VWAP 100.0, got %f", res1.VWAP)
	}

	// Tick 2: 110 price, 10 volume -> Sum(PV)=1000+1100=2100, Sum(V)=20 -> VWAP = 105
	res2 := vwap.AddTick(Tick{Price: 110.0, Volume: 10.0, Timestamp: baseTime.Add(time.Second)})
	if res2.VWAP != 105.0 {
		t.Errorf("Expected VWAP 105.0, got %f", res2.VWAP)
	}

	// Tick 3: 50 price, 20 volume -> Sum(PV)=2100+1000=3100, Sum(V)=40 -> VWAP = 77.5
	res3 := vwap.AddTick(Tick{Price: 50.0, Volume: 20.0, Timestamp: baseTime.Add(2 * time.Second)})
	if res3.VWAP != 77.5 {
		t.Errorf("Expected VWAP 77.5, got %f", res3.VWAP)
	}

	// Reset
	vwap.Reset()
	if vwap.state != nil {
		t.Errorf("Expected nil state after reset")
	}
}
