package orderbook

import (
	"fmt"
	"sync"
)

// managedBook wraps an OrderBook with its own sync.RWMutex.
// This allows updates to different symbols to happen in parallel.
type managedBook struct {
	mu   sync.RWMutex
	book *OrderBook
}

// Manager orchestrates multiple L2 order books across different symbols.
type Manager struct {
	books     sync.Map // string -> *managedBook
	publisher Publisher
}

// NewManager creates a new Order Book Manager.
func NewManager(publisher Publisher) *Manager {
	return &Manager{
		publisher: publisher,
	}
}

// InitBook initializes or forcibly resets an order book from a complete snapshot.
func (m *Manager) InitBook(snapshot *OrderBook) {
	mb, _ := m.books.LoadOrStore(snapshot.Symbol, &managedBook{
		book: &OrderBook{Symbol: snapshot.Symbol},
	})
	
	wrapper := mb.(*managedBook)
	wrapper.mu.Lock()
	defer wrapper.mu.Unlock()

	// Deep copy to prevent external mutation
	newBids := make([]PriceLevel, len(snapshot.Bids))
	copy(newBids, snapshot.Bids)
	
	newAsks := make([]PriceLevel, len(snapshot.Asks))
	copy(newAsks, snapshot.Asks)

	wrapper.book.Sequence = snapshot.Sequence
	wrapper.book.Timestamp = snapshot.Timestamp
	wrapper.book.Bids = newBids
	wrapper.book.Asks = newAsks
}

// ApplyIncrement processes an incoming delta, validates sequence, applies to the book,
// and publishes the delta if successful.
func (m *Manager) ApplyIncrement(inc OrderBookIncrement) error {
	v, ok := m.books.Load(inc.Symbol)
	if !ok {
		// Cannot apply increment to a book we don't track. Caller must fetch snapshot.
		return fmt.Errorf("book not initialized for symbol: %s", inc.Symbol)
	}

	wrapper := v.(*managedBook)
	
	// Anonymous function to limit scope of lock
	err := func() error {
		wrapper.mu.Lock()
		defer wrapper.mu.Unlock()
		return wrapper.book.Apply(inc)
	}()
	
	if err != nil {
		return err
	}

	// Publish to downstream (e.g. Redis)
	// Execute asynchronously to prevent slow network I/O from blocking the next update
	if m.publisher != nil {
		go func(increment OrderBookIncrement) {
			if err := m.publisher.PublishIncrement(increment); err != nil {
				// Best-effort publish, but we should log this in production
				_ = err // fmt.Errorf("failed to publish increment: %w", err)
			}
		}(inc)
	}

	return nil
}

// GetSnapshot returns a safe, deep copy of the current order book.
func (m *Manager) GetSnapshot(symbol string) (*OrderBook, error) {
	v, ok := m.books.Load(symbol)
	if !ok {
		return nil, fmt.Errorf("book not found for symbol: %s", symbol)
	}

	wrapper := v.(*managedBook)
	wrapper.mu.RLock()
	defer wrapper.mu.RUnlock()

	// Deep copy inside the lock
	snap := &OrderBook{
		Symbol:    wrapper.book.Symbol,
		Sequence:  wrapper.book.Sequence,
		Timestamp: wrapper.book.Timestamp,
	}

	snap.Bids = make([]PriceLevel, len(wrapper.book.Bids))
	copy(snap.Bids, wrapper.book.Bids)

	snap.Asks = make([]PriceLevel, len(wrapper.book.Asks))
	copy(snap.Asks, wrapper.book.Asks)

	return snap, nil
}
