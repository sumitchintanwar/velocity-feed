package simulator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/marketdata"
)

// ---------- Quote Generation Rate ----------

func BenchmarkSimulator_Generate_1Symbol(b *testing.B) {
	benchSimulator(b, 1)
}

func BenchmarkSimulator_Generate_5Symbols(b *testing.B) {
	benchSimulator(b, 5)
}

func BenchmarkSimulator_Generate_100Symbols(b *testing.B) {
	benchSimulator(b, 100)
}

func benchSimulator(b *testing.B, numSymbols int) {
	syms := make([]string, numSymbols)
	for i := range syms {
		syms[i] = fmt.Sprintf("SYM%d", i)
	}

	s, err := New(Config{
		TickInterval: time.Nanosecond, // As fast as possible
		BasePrice:    100.0,
		Volatility:   0.02,
	}, marketdata.WallClock{}, syms...)
	if err != nil {
		b.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quotes, err := s.Run(ctx)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		<-quotes
	}

	b.StopTimer()
	quotesPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(quotesPerSec, "quotes/sec")
}

// ---------- Subscribe/Unsubscribe ----------

func BenchmarkSimulator_Subscribe_1Symbol(b *testing.B) {
	benchSimulatorSubscribe(b, 1)
}

func BenchmarkSimulator_Subscribe_10Symbols(b *testing.B) {
	benchSimulatorSubscribe(b, 10)
}

func BenchmarkSimulator_Subscribe_100Symbols(b *testing.B) {
	benchSimulatorSubscribe(b, 100)
}

func benchSimulatorSubscribe(b *testing.B, numSymbols int) {
	s, err := New(DefaultConfig(), marketdata.WallClock{})
	if err != nil {
		b.Fatal(err)
	}

	syms := make([]string, numSymbols)
	for i := range syms {
		syms[i] = fmt.Sprintf("SYM%d", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = s.Subscribe(syms...)
		_ = s.Unsubscribe(syms...)
	}
}

// ---------- Price Generation ----------

func BenchmarkSimulator_NextPrice(b *testing.B) {
	s, err := New(DefaultConfig(), marketdata.WallClock{}, "AAPL")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		s.nextPrice("AAPL")
	}
}

// ---------- Max-Throughput (Unlimited Mode) ----------

// BenchmarkMaxThroughput_1Symbol measures the maximum message generation rate
// in unlimited mode for a single symbol.  This benchmark exercises the true
// ceiling of the generator with no artificial rate limiting.
func BenchmarkMaxThroughput_1Symbol(b *testing.B) {
	benchMaxThroughput(b, 1)
}

// BenchmarkMaxThroughput_5Symbols exercises 5 concurrent symbol goroutines.
// 5 symbols matches the production benchmark scenario.
func BenchmarkMaxThroughput_5Symbols(b *testing.B) {
	benchMaxThroughput(b, 5)
}

// BenchmarkMaxThroughput_100Symbols exercises 100 concurrent symbol goroutines
// to quantify contention on the shared output channel at scale.
func BenchmarkMaxThroughput_100Symbols(b *testing.B) {
	benchMaxThroughput(b, 100)
}

func benchMaxThroughput(b *testing.B, numSymbols int) {
	b.Helper()
	syms := make([]string, numSymbols)
	for i := range syms {
		syms[i] = fmt.Sprintf("SYM%d", i)
	}

	s, err := New(MaxThroughputConfig(), marketdata.WallClock{}, syms...)
	if err != nil {
		b.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	quotes, err := s.Run(ctx)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		<-quotes
	}

	b.StopTimer()
	quotesPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(quotesPerSec, "quotes/sec")
}

