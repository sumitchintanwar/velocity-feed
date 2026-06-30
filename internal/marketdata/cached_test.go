package marketdata_test

import (
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/marketdata"
)

// mockQuote implements marketdata.MarketEvent for testing
type mockQuote struct {
	Sym string    `json:"symbol"`
	Bid float64   `json:"bid"`
	Ask float64   `json:"ask"`
	TS  time.Time `json:"ts"`
}

func (m mockQuote) EventSymbol() string       { return m.Sym }
func (m mockQuote) EventType() string         { return "quote" }
func (m mockQuote) GetTimestamp() time.Time   { return m.TS }

func BenchmarkNewCachedEvent(b *testing.B) {
	q := mockQuote{
		Sym: "BTC-USD",
		Bid: 50000.50,
		Ask: 50001.00,
		TS:  time.Now(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = marketdata.NewCachedEvent(q)
	}
}
