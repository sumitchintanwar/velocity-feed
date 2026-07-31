package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"time"

	"github.com/sumit/rtmds/pkg/marketdata"
	"github.com/sumit/rtmds/pkg/protocol"
)

type result struct {
	Count       int
	Format      string
	Operation   string
	Duration    time.Duration
	Throughput  float64 // msgs/sec
	MemoryMB    float64
	AllocCount  uint64
	TotalAllocs uint64
}

func main() {
	counts := []int{100_000, 500_000, 1_000_000}

	fmt.Println("===============================================================")
	fmt.Println("              Serialization Load Test Results")
	fmt.Println("===============================================================")
	fmt.Printf("%-10s | %-12s | %-15s | %-10s | %-12s | %-12s | %-10s\n",
		"Count", "Format", "Operation", "Duration", "Msgs/sec", "Mem (MB)", "Allocs/op")
	fmt.Println("--------------------------------------------------------------------------------------------------")

	for _, count := range counts {
		events := generateEvents(count)

		runTest(count, "JSON", events, protocol.NewJSONSerializer())
		runTest(count, "Protobuf", events, protocol.NewProtobufSerializer())
		runTest(count, "FlatBuffers", events, protocol.NewFlatBuffersSerializer())

		fmt.Println("--------------------------------------------------------------------------------------------------")
	}
}

func runTest(count int, format string, events []marketdata.MarketEvent, serializer protocol.Serializer) {
	// --- Serialization Test ---

	// Force GC before measuring memory
	runtime.GC()
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)

	start := time.Now()

	// Allocate slice to hold all serialized payloads (simulating memory pressure)
	payloads := make([][]byte, count)

	for i := 0; i < count; i++ {
		data, err := serializer.Serialize(events[i])
		if err != nil {
			panic(err)
		}
		payloads[i] = data
	}

	dur := time.Since(start)
	runtime.ReadMemStats(&m2)

	allocs := m2.Mallocs - m1.Mallocs
	allocsPerOp := allocs / uint64(count)
	memUsed := float64(m2.TotalAlloc-m1.TotalAlloc) / 1024 / 1024

	printResult(result{
		Count:       count,
		Format:      format,
		Operation:   "Serialize",
		Duration:    dur,
		Throughput:  float64(count) / dur.Seconds(),
		MemoryMB:    memUsed,
		TotalAllocs: allocs,
		AllocCount:  allocsPerOp,
	})

	// --- Deserialization Test ---

	// Release original events to pool if applicable (JSON) to avoid poisoning stats
	// But in this test we don't, we want to measure pure deserialize cost.

	runtime.GC()
	runtime.ReadMemStats(&m1)

	start = time.Now()

	for i := 0; i < count; i++ {
		ev, err := serializer.Deserialize(payloads[i])
		if err != nil {
			panic(err)
		}

		// For JSON we must release the event back to the pool
		// otherwise 1M structs stay on heap and kill memory
		if js, ok := serializer.(*protocol.JSONSerializer); ok {
			js.ReleaseEvent(ev)
		}
	}

	dur = time.Since(start)
	runtime.ReadMemStats(&m2)

	allocs = m2.Mallocs - m1.Mallocs
	allocsPerOp = allocs / uint64(count)
	memUsed = float64(m2.TotalAlloc-m1.TotalAlloc) / 1024 / 1024

	printResult(result{
		Count:       count,
		Format:      format,
		Operation:   "Deserialize",
		Duration:    dur,
		Throughput:  float64(count) / dur.Seconds(),
		MemoryMB:    memUsed,
		TotalAllocs: allocs,
		AllocCount:  allocsPerOp,
	})

	fmt.Println()
}

func printResult(r result) {
	fmt.Printf("%-10s | %-12s | %-15s | %-10v | %-12.0f | %-12.2f | %-10d\n",
		formatCount(r.Count), r.Format, r.Operation, r.Duration.Round(time.Millisecond), r.Throughput, r.MemoryMB, r.AllocCount)
}

func formatCount(c int) string {
	switch c {
	case 100_000:
		return "100k"
	case 500_000:
		return "500k"
	case 1_000_000:
		return "1M"
	default:
		return fmt.Sprintf("%d", c)
	}
}

func generateEvents(count int) []marketdata.MarketEvent {
	events := make([]marketdata.MarketEvent, count)
	symbols := []string{"AAPL", "MSFT", "GOOG", "TSLA", "NVDA", "AMZN"}
	now := time.Now()

	for i := 0; i < count; i++ {
		sym := symbols[i%len(symbols)]

		// 80% Quotes, 20% Bars
		if rand.Float32() < 0.8 {
			events[i] = &marketdata.Quote{
				Symbol:    sym,
				Type:      marketdata.QuoteTypeTrade,
				Seq:       int64(i),
				Price:     100.0 + rand.Float64()*100.0,
				Volume:    int64(100 + rand.Intn(1000)),
				Provider:  "testgen",
				Timestamp: now.Add(time.Duration(i) * time.Millisecond),
			}
		} else {
			p := 100.0 + rand.Float64()*100.0
			events[i] = &marketdata.Bar{
				Symbol:    sym,
				Open:      p,
				High:      p + 1.0,
				Low:       p - 1.0,
				Close:     p + 0.5,
				Volume:    int64(10000 + rand.Intn(50000)),
				Provider:  "testgen",
				Timestamp: now.Add(time.Duration(i) * time.Millisecond),
			}
		}
	}
	return events
}
