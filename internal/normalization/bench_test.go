package normalization

import (
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/marketdata"
)

type benchMapper struct{}

func (m *benchMapper) Map(raw marketdata.RawMessage) (marketdata.Quote, error) {
	// Simulate simple struct cast as done in crypto/nasdaq mappers
	q := raw.Payload.(marketdata.Quote)
	q.Provider = raw.Provider
	return q, nil
}

// BenchmarkPipeline measures the overhead of the full normalization pipeline.
// Target: Minimal allocations (ideally 0) and high throughput.
func BenchmarkPipeline(b *testing.B) {
	logger := log.New(nil, "benchmark")
	pipe := NewPipeline(&benchMapper{}, NewDefaultValidator(), logger)

	raw := marketdata.RawMessage{
		Provider: "benchmark",
		Payload: marketdata.Quote{
			Symbol:    "BTC/USD",
			Type:      marketdata.QuoteTypeTrade,
			Price:     50000.0,
			Volume:    10,
			Timestamp: time.Now(),
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := pipe.Normalize(raw)
		if err != nil {
			b.Fatal(err)
		}
	}
}
