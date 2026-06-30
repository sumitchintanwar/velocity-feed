package aggregation

import (
	"context"
	"testing"
	"time"
)

type discardPublisher struct{}

func (d *discardPublisher) PublishOHLC(ohlc OHLC) {}
func (d *discardPublisher) PublishVWAP(vwap VWAP) {}

func BenchmarkEngineProcessTick(b *testing.B) {
	engine := NewEngine(&discardPublisher{})
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine.Start(ctx)
	
	tick := Tick{
		Symbol:    "BTC-USD",
		Price:     100.0,
		Volume:    1.0,
		Timestamp: time.Now(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Just hammering the same timestamp so it doesn't trigger flushes,
		// testing the pure incremental aggregation hot path.
		engine.ProcessTick(tick)
	}
}
