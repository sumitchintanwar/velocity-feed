package orderbook

import (
	"testing"
	"time"
)

func TestOrderBookInsert(t *testing.T) {
	book := &OrderBook{Symbol: "BTC-USD"}

	inc := OrderBookIncrement{
		Symbol:   "BTC-USD",
		Sequence: 1,
		Updates: []LevelUpdate{
			{Action: ActionInsert, Side: BidSide, Price: 100.0, Size: 1.5},
			{Action: ActionInsert, Side: AskSide, Price: 101.0, Size: 2.0},
			{Action: ActionInsert, Side: BidSide, Price: 100.5, Size: 1.0}, // Should be at top of bids
			{Action: ActionInsert, Side: AskSide, Price: 100.8, Size: 1.0}, // Should be at top of asks
		},
		Timestamp: time.Now(),
	}

	err := book.Apply(inc)
	if err != nil {
		t.Fatalf("Failed to apply increment: %v", err)
	}

	// Verify Bids (Descending: 100.5, 100.0)
	if len(book.Bids) != 2 {
		t.Fatalf("Expected 2 bids, got %d", len(book.Bids))
	}
	if book.Bids[0].Price != 100.5 || book.Bids[1].Price != 100.0 {
		t.Errorf("Bids sorted incorrectly: %v", book.Bids)
	}

	// Verify Asks (Ascending: 100.8, 101.0)
	if len(book.Asks) != 2 {
		t.Fatalf("Expected 2 asks, got %d", len(book.Asks))
	}
	if book.Asks[0].Price != 100.8 || book.Asks[1].Price != 101.0 {
		t.Errorf("Asks sorted incorrectly: %v", book.Asks)
	}
}

func TestOrderBookUpdateAndDelete(t *testing.T) {
	book := &OrderBook{
		Symbol:   "BTC-USD",
		Sequence: 1,
		Bids: []PriceLevel{
			{Price: 100.5, Quantity: 1.0},
			{Price: 100.0, Quantity: 1.5},
		},
		Asks: []PriceLevel{
			{Price: 100.8, Quantity: 1.0},
			{Price: 101.0, Quantity: 2.0},
		},
	}

	inc := OrderBookIncrement{
		Sequence: 2,
		Updates: []LevelUpdate{
			{Action: ActionUpdate, Side: BidSide, Price: 100.5, Size: 5.0}, // Change top bid size
			{Action: ActionDelete, Side: AskSide, Price: 100.8, Size: 0.0}, // Remove top ask
		},
	}

	err := book.Apply(inc)
	if err != nil {
		t.Fatalf("Failed to apply update: %v", err)
	}

	if book.Bids[0].Quantity != 5.0 {
		t.Errorf("Expected bid quantity 5.0, got %f", book.Bids[0].Quantity)
	}
	
	if len(book.Asks) != 1 || book.Asks[0].Price != 101.0 {
		t.Errorf("Expected 1 ask at 101.0, got %v", book.Asks)
	}
}

func TestSequenceValidation(t *testing.T) {
	book := &OrderBook{Symbol: "BTC-USD", Sequence: 10}

	// Gap
	err := book.Apply(OrderBookIncrement{Sequence: 12})
	if err != ErrSequenceGap {
		t.Errorf("Expected ErrSequenceGap, got %v", err)
	}

	// Old/Duplicate
	err = book.Apply(OrderBookIncrement{Sequence: 10})
	if err != ErrOldSequence {
		t.Errorf("Expected ErrOldSequence, got %v", err)
	}

	// Valid
	err = book.Apply(OrderBookIncrement{Sequence: 11})
	if err != nil {
		t.Errorf("Expected success, got %v", err)
	}
}

func TestManager(t *testing.T) {
	mockPub := &MockPublisher{}
	mgr := NewManager(mockPub)

	// Init
	mgr.InitBook(&OrderBook{
		Symbol:   "AAPL",
		Sequence: 1,
		Bids:     []PriceLevel{{Price: 150.0, Quantity: 100}},
		Asks:     []PriceLevel{{Price: 151.0, Quantity: 100}},
	})

	// Apply valid
	err := mgr.ApplyIncrement(OrderBookIncrement{
		Symbol:   "AAPL",
		Sequence: 2,
		Updates: []LevelUpdate{
			{Action: ActionInsert, Side: BidSide, Price: 150.5, Size: 50},
		},
	})
	if err != nil {
		t.Fatalf("Failed to apply to manager: %v", err)
	}

	// Give the async publisher a moment to run
	time.Sleep(10 * time.Millisecond)

	// Check pub
	if count := mockPub.GetPublishedCount(); count != 1 {
		t.Errorf("Expected 1 publish event, got %d", count)
	}

	// Check Snapshot
	snap, err := mgr.GetSnapshot("AAPL")
	if err != nil {
		t.Fatalf("Failed to get snapshot: %v", err)
	}
	if snap.Sequence != 2 {
		t.Errorf("Expected snapshot sequence 2, got %d", snap.Sequence)
	}
	if len(snap.Bids) != 2 {
		t.Errorf("Expected 2 bids in snapshot, got %d", len(snap.Bids))
	}

	// Uninitialized symbol
	err = mgr.ApplyIncrement(OrderBookIncrement{Symbol: "UNKNOWN", Sequence: 1})
	if err == nil {
		t.Errorf("Expected error applying to unknown book")
	}
}

func TestZeroSizeDelete(t *testing.T) {
	book := &OrderBook{
		Symbol:   "BTC-USD",
		Sequence: 1,
		Bids: []PriceLevel{
			{Price: 100.0, Quantity: 1.5},
		},
	}

	inc := OrderBookIncrement{
		Sequence: 2,
		Updates: []LevelUpdate{
			{Action: ActionUpdate, Side: BidSide, Price: 100.0, Size: 0.0}, // Should trigger auto-delete
		},
	}

	err := book.Apply(inc)
	if err != nil {
		t.Fatalf("Failed to apply update: %v", err)
	}

	if len(book.Bids) != 0 {
		t.Errorf("Expected 0 bids, got %v", book.Bids)
	}
}

func TestMaxDepth(t *testing.T) {
	book := &OrderBook{Symbol: "BTC-USD"}

	// Insert MaxDepth + 5 levels
	inc := OrderBookIncrement{
		Sequence: 1,
		Updates:  make([]LevelUpdate, MaxDepth+5),
	}

	for i := 0; i < MaxDepth+5; i++ {
		inc.Updates[i] = LevelUpdate{Action: ActionInsert, Side: BidSide, Price: float64(i), Size: 1.0}
	}

	err := book.Apply(inc)
	if err != nil {
		t.Fatalf("Failed to apply increment: %v", err)
	}

	if len(book.Bids) != MaxDepth {
		t.Errorf("Expected MaxDepth (%d) bids, got %d", MaxDepth, len(book.Bids))
	}
	// The highest prices should be kept, so prices 5 to 204.
	// Since bids are descending, Bids[0] = 204.0, Bids[199] = 5.0
	if book.Bids[0].Price != float64(MaxDepth+4) {
		t.Errorf("Expected highest price %f, got %f", float64(MaxDepth+4), book.Bids[0].Price)
	}
	if book.Bids[MaxDepth-1].Price != 5.0 {
		t.Errorf("Expected lowest price 5.0, got %f", book.Bids[MaxDepth-1].Price)
	}
}

func TestConcurrentClients(t *testing.T) {
	mockPub := &MockPublisher{}
	mgr := NewManager(mockPub)

	// Init 10 symbols
	symbols := []string{"BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "DOT-USD", "XRP-USD", "DOGE-USD", "AVAX-USD", "LINK-USD", "MATIC-USD"}
	for _, sym := range symbols {
		mgr.InitBook(&OrderBook{Symbol: sym, Sequence: 1})
	}

	done := make(chan bool)
	
	// Spawn 100 writers
	for i := 0; i < 100; i++ {
		go func(writerID int) {
			sym := symbols[writerID%len(symbols)]
			for j := 0; j < 100; j++ {
				// We don't care about the sequence gap error here, we just want to stress the locks
				_ = mgr.ApplyIncrement(OrderBookIncrement{
					Symbol:   sym,
					Sequence: int64(j + 2),
					Updates: []LevelUpdate{
						{Action: ActionInsert, Side: BidSide, Price: float64(j), Size: 1.0},
					},
				})
			}
			done <- true
		}(i)
	}

	// Spawn 100 readers
	for i := 0; i < 100; i++ {
		go func(readerID int) {
			sym := symbols[readerID%len(symbols)]
			for j := 0; j < 100; j++ {
				_, _ = mgr.GetSnapshot(sym)
			}
			done <- true
		}(i)
	}

	// Wait for all 200 goroutines
	for i := 0; i < 200; i++ {
		<-done
	}
}
