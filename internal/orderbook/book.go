package orderbook

import (
	"math"
	"sort"
)

const (
	MaxDepth = 200
	epsilon  = 1e-8
)

// Apply applies an incremental update to the OrderBook.
// It handles binary search insertions, updates, and deletions in the Bids and Asks slices.
// It also validates sequence numbers to prevent data corruption.
func (b *OrderBook) Apply(inc OrderBookIncrement) error {
	if b.Sequence == 0 {
		// Initialization case: accept any sequence if book is completely fresh
		b.Sequence = inc.Sequence
	} else if inc.Sequence <= b.Sequence {
		return ErrOldSequence
	} else if inc.Sequence != b.Sequence+1 {
		return ErrSequenceGap
	}

	// Sequence is strictly valid (b.Sequence + 1)
	for _, update := range inc.Updates {
		switch update.Side {
		case BidSide:
			b.Bids = applyLevel(b.Bids, update, true)
		case AskSide:
			b.Asks = applyLevel(b.Asks, update, false)
		}
	}

	b.Sequence = inc.Sequence
	b.Timestamp = inc.Timestamp
	return nil
}

// applyLevel handles the sorting and binary search logic for a single side.
	// Bids are sorted descending (highest price first).
// Asks are sorted ascending (lowest price first).
func applyLevel(levels []PriceLevel, update LevelUpdate, isBid bool) []PriceLevel {
	// Auto-cast update to size 0 as delete
	if update.Action == ActionUpdate && update.Size <= 0 {
		update.Action = ActionDelete
	}

	// Find insertion point
	var idx int
	if isBid {
		// Bids: descending
		idx = sort.Search(len(levels), func(i int) bool {
			return levels[i].Price <= update.Price + epsilon
		})
	} else {
		// Asks: ascending
		idx = sort.Search(len(levels), func(i int) bool {
			return levels[i].Price >= update.Price - epsilon
		})
	}

	exists := idx < len(levels) && math.Abs(levels[idx].Price-update.Price) < epsilon

	switch update.Action {
	case ActionInsert, ActionUpdate:
		if exists {
			// Update in place
			levels[idx].Quantity = update.Size
		} else {
			// Insert at idx (shifts elements right)
			// Optimize allocation by appending zero and shifting
			levels = append(levels, PriceLevel{})
			copy(levels[idx+1:], levels[idx:])
			levels[idx] = PriceLevel{Price: update.Price, Quantity: update.Size}
			
			if len(levels) > MaxDepth {
				levels = levels[:MaxDepth]
			}
		}
	case ActionDelete:
		if exists {
			// Delete at idx (shifts elements left)
			copy(levels[idx:], levels[idx+1:])
			levels = levels[:len(levels)-1]
		}
	}

	return levels
}
