package aggregation

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

type mockPublisher struct {
	mu        sync.Mutex
	Published []OHLC
}

func (m *mockPublisher) PublishOHLC(ohlc OHLC) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Published = append(m.Published, ohlc)
}

func (m *mockPublisher) PublishVWAP(vwap VWAP) {}

func (m *mockPublisher) GetPublished() []OHLC {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Published
}

func TestConcurrentSymbols(t *testing.T) {
	engine := NewEngine(&mockPublisher{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine.Start(ctx)

	var wg sync.WaitGroup

	numSymbols := 1000
	ticksPerSymbol := 100

	wg.Add(numSymbols)
	for i := 0; i < numSymbols; i++ {
		go func(symbolID int) {
			defer wg.Done()
			sym := fmt.Sprintf("SYM-%d", symbolID)
			baseTime := time.Now()
			for j := 0; j < ticksPerSymbol; j++ {
				engine.ProcessTick(Tick{
					Symbol:    sym,
					Price:     100.0 + float64(j),
					Volume:    1.0,
					Timestamp: baseTime.Add(time.Duration(j) * 100 * time.Millisecond),
				})
			}
		}(i)
	}

	wg.Wait()
	// If it doesn't panic and go test -race passes, this is a success.
}

func TestMissingTicks(t *testing.T) {
	pub := &mockPublisher{}
	engine := NewEngine(pub)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine.Start(ctx)

	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Tick 1
	engine.ProcessTick(Tick{Symbol: "BTC-USD", Price: 100, Volume: 1, Timestamp: baseTime})
	
	// Fast forward 10 minutes (simulate missing ticks/illiquid market)
	lateTime := baseTime.Add(10 * time.Minute)
	
	// Tick 2
	engine.ProcessTick(Tick{Symbol: "BTC-USD", Price: 101, Volume: 1, Timestamp: lateTime})

	// Wait for async publisher
	time.Sleep(10 * time.Millisecond)

	published := pub.GetPublished()
	
	// We expect 3 events (the 1s, 5s, and 1m windows from 12:00:00 flushed by the late tick)
	if len(published) != 3 {
		t.Errorf("Expected exactly 3 flushes, got %d", len(published))
	}

	for _, ohlc := range published {
		if ohlc.Start != baseTime {
			t.Errorf("Expected flushed window to start at baseTime, got %v", ohlc.Start)
		}
	}
}

func TestWindowRollover(t *testing.T) {
	pub := &mockPublisher{}
	engine := NewEngine(pub)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine.Start(ctx)

	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	engine.ProcessTick(Tick{Symbol: "ETH-USD", Price: 2000, Volume: 1, Timestamp: baseTime.Add(2 * time.Second)})
	engine.ProcessTick(Tick{Symbol: "ETH-USD", Price: 2005, Volume: 1, Timestamp: baseTime.Add(4999 * time.Millisecond)})
	engine.ProcessTick(Tick{Symbol: "ETH-USD", Price: 2010, Volume: 1, Timestamp: baseTime.Add(5 * time.Second)})

	// Wait for async publisher
	time.Sleep(10 * time.Millisecond)

	published := pub.GetPublished()
	
	// Finding the 5-second window flush
	var found5s bool
	for _, ohlc := range published {
		if ohlc.WindowSize == Window5Second {
			found5s = true
			if ohlc.Start != baseTime {
				t.Errorf("Expected start 12:00:00, got %v", ohlc.Start)
			}
			if ohlc.End != baseTime.Add(5*time.Second) {
				t.Errorf("Expected end 12:00:05, got %v", ohlc.End)
			}
			if ohlc.Close != 2005 {
				t.Errorf("Expected close 2005, got %f", ohlc.Close)
			}
		}
	}

	if !found5s {
		t.Errorf("Did not find 5-second OHLC publish event")
	}
}

func TestStagnantSweeper(t *testing.T) {
	pub := &mockPublisher{}
	engine := NewEngine(pub)
	
	// Create context but do NOT run engine.Start() normally because we want to trigger sweep manually.
	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Send a tick to open a window.
	engine.ProcessTick(Tick{Symbol: "SOL-USD", Price: 150, Volume: 1, Timestamp: baseTime})
	
	// Fast forward time by 2 seconds, enough to make the 1-second window stagnant.
	sweepTime := baseTime.Add(2 * time.Second)
	
	// Manually trigger the sweep function (which normally runs on a ticker)
	engine.sweepStagnant(sweepTime)
	
	// We need to drain the publishCh manually since we didn't call Start()
	select {
	case <-engine.publishCh:
		// success
	default:
		t.Errorf("Expected sweeper to flush the stagnant 1s window into publishCh")
	}
}
