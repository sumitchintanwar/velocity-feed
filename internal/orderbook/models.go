package orderbook

import (
	"errors"
	"time"
)

var (
	ErrSequenceGap   = errors.New("sequence gap detected")
	ErrOldSequence   = errors.New("sequence is older than current")
	ErrInvalidUpdate = errors.New("invalid order book update")
)

// PriceLevel represents a single price depth containing volume.
type PriceLevel struct {
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"` // Can be float for crypto
}

// Side represents the side of the order book (bid or ask).
type Side string

const (
	BidSide Side = "bid"
	AskSide Side = "ask"
)

// Action represents what to do with a LevelUpdate.
type Action string

const (
	ActionInsert Action = "insert"
	ActionUpdate Action = "update"
	ActionDelete Action = "delete"
)

// LevelUpdate is a single instruction to modify the book.
type LevelUpdate struct {
	Action Action  `json:"action"`
	Side   Side    `json:"side"`
	Price  float64 `json:"price"`
	Size   float64 `json:"size"`
}

// OrderBookIncrement represents a delta update from an exchange.
type OrderBookIncrement struct {
	Symbol    string        `json:"symbol"`
	Sequence  int64         `json:"sequence"`
	Updates   []LevelUpdate `json:"updates"`
	Timestamp time.Time     `json:"timestamp"`
}

// OrderBook Snapshot represents the full depth of the order book.
type OrderBook struct {
	Symbol    string       `json:"symbol"`
	Sequence  int64        `json:"sequence"`
	Bids      []PriceLevel `json:"bids"` // Sorted descending (highest first)
	Asks      []PriceLevel `json:"asks"` // Sorted ascending (lowest first)
	Timestamp time.Time    `json:"timestamp"`
}
