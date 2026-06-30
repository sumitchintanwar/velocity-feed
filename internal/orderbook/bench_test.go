package orderbook

import (
	"testing"
)

// Generate a deep book for testing
func generateDeepBook(levels int) *OrderBook {
	book := &OrderBook{
		Symbol:   "BENCH",
		Sequence: 1,
		Bids:     make([]PriceLevel, levels),
		Asks:     make([]PriceLevel, levels),
	}
	
	// Center price around 1000
	for i := 0; i < levels; i++ {
		// Bids go down from 1000
		book.Bids[i] = PriceLevel{Price: 1000.0 - float64(i)*0.1, Quantity: 1.0}
		// Asks go up from 1000.1
		book.Asks[i] = PriceLevel{Price: 1000.1 + float64(i)*0.1, Quantity: 1.0}
	}
	return book
}

// Benchmark applying single level updates to a deep book (1000 levels each side)
func BenchmarkOrderBookApply(b *testing.B) {
	book := generateDeepBook(1000)
	
	inc := OrderBookIncrement{
		Sequence: 2,
		Updates: []LevelUpdate{
			{Action: ActionUpdate, Side: BidSide, Price: 999.0, Size: 5.0}, // Update near top
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Just bypass sequence check for pure speed test
		book.Sequence = int64(i)
		inc.Sequence = int64(i + 1)
		_ = book.Apply(inc)
	}
}

// Benchmark applying single level inserts into a deep book (worst case allocation)
func BenchmarkOrderBookInsert(b *testing.B) {
	book := generateDeepBook(1000)
	
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		
		// Every ~100 iterations, shrink the slice to simulate real depth limits
		// to prevent it growing to millions of levels and skewing results.
		if len(book.Bids) > 1100 {
			book.Bids = book.Bids[:1000]
		}
		
		book.Sequence = int64(i)
		inc := OrderBookIncrement{
			Sequence: int64(i + 1),
			Updates: []LevelUpdate{
				{Action: ActionInsert, Side: BidSide, Price: 1000.0 + float64(i)*0.01, Size: 5.0}, // Insert at top
			},
		}
		b.StartTimer()
		_ = book.Apply(inc)
	}
}
