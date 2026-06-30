package metadata

import (
	"context"
	"fmt"
	"testing"

	"github.com/sumit/rtmds/internal/log"
)

func BenchmarkCacheLookup(b *testing.B) {
	// Setup 10,000 synthetic instruments
	seeds := make([]*Instrument, 10000)
	for i := 0; i < 10000; i++ {
		seeds[i] = &Instrument{
			CanonicalSymbol: fmt.Sprintf("SYM-%d", i),
			Exchange:        "NYSE",
			AssetClass:      AssetClassEquity,
		}
	}

	repo := NewInMemoryRepository(seeds)
	logger := log.New(nil, "bench")
	svc := NewService(repo, logger)
	_ = svc.LoadCache(context.Background())

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			symbol := fmt.Sprintf("SYM-%d", i%10000)
			_, err := svc.GetInstrument(symbol)
			if err != nil {
				b.Fatalf("Lookup failed for %s", symbol)
			}
			i++
		}
	})
}
