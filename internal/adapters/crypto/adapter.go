package crypto

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/sumit/rtmds/internal/exchange"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/normalization"
)

// Purpose: Mock adapter representing a WebSocket-style crypto feed (e.g. Binance).
// Architecture: Implements exchange.ExchangeAdapter. Generates fake crypto quotes.
// Design Decisions: Fast tick rate to simulate high-frequency crypto trading.

type Adapter struct {
	symbols []string
}

// RawTick represents the un-normalized structure received from the mock exchange
type RawTick struct {
	Pair   string  `json:"pair"`
	Amount float64 `json:"amount"`
	Qty    float64 `json:"qty"`
	TS     int64   `json:"ts"`
}

type Mapper struct{}

func (m *Mapper) Map(raw marketdata.RawMessage) (marketdata.Quote, error) {
	tick, ok := raw.Payload.(RawTick)
	if !ok {
		return marketdata.Quote{}, fmt.Errorf("expected crypto.RawTick")
	}
	return marketdata.Quote{
		Symbol:    tick.Pair,
		Type:      marketdata.QuoteTypeTrade,
		Price:     tick.Amount,
		Volume:    int64(tick.Qty * 1000000), // convert crypto qty to internal scaled volume
		Timestamp: time.UnixMicro(tick.TS),
		Provider:  raw.Provider,
	}, nil
}

func (a *Adapter) Mapper() normalization.Mapper {
	return &Mapper{}
}

func (a *Adapter) Name() string { return "crypto" }

func (a *Adapter) Connect(ctx context.Context) error {
	select {
	case <-time.After(15 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Disconnect implements exchange.ExchangeAdapter.
func (a *Adapter) Disconnect(ctx context.Context) error {
	return nil
}

func (a *Adapter) Subscribe(symbols ...string) error {
	a.symbols = append(a.symbols, symbols...)
	return nil
}

func (a *Adapter) Unsubscribe(symbols ...string) error {
	return fmt.Errorf("unsubscribe not implemented in crypto mock")
}

func (a *Adapter) Run(ctx context.Context) (<-chan marketdata.RawMessage, error) {
	out := make(chan marketdata.RawMessage, 100)
	go func() {
		defer close(out)
		ticker := time.NewTicker(50 * time.Millisecond) // Faster ticks for crypto
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, sym := range a.symbols {
					price := 50000.0 + (rand.Float64() * 1000) //nolint:gosec
					tick := RawTick{
						Pair:   sym,
						Amount: price,
						Qty:    0.5,
						TS:     time.Now().UnixMicro(),
					}
					msg := marketdata.RawMessage{
						Provider:   a.Name(),
						Payload:    tick,
						ReceivedAt: time.Now(),
					}
					select {
					case out <- msg:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out, nil
}

func init() {
	exchange.Register("crypto", func(cfg exchange.AdapterConfig) (exchange.ExchangeAdapter, error) {
		return &Adapter{}, nil
	})
}
