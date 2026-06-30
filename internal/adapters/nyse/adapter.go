package nyse

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/sumit/rtmds/internal/exchange"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/normalization"
)

// Purpose: Mock adapter representing a FIX-style NYSE equity feed.
// Architecture: Implements exchange.ExchangeAdapter. Generates fake NYSE quotes.
// Design Decisions: Built to test multi-feed processing in the ExchangeManager.

type Adapter struct {
	symbols []string
}

// RawTick represents the un-normalized structure received from the mock exchange
type RawTick struct {
	Symbol string  `json:"symbol"`
	Price  float64 `json:"price"`
	Size   int64   `json:"size"`
	Epoch  int64   `json:"epoch"`
}

type Mapper struct{}

func (m *Mapper) Map(raw marketdata.RawMessage) (marketdata.Quote, error) {
	tick, ok := raw.Payload.(RawTick)
	if !ok {
		return marketdata.Quote{}, fmt.Errorf("expected nyse.RawTick")
	}
	return marketdata.Quote{
		Symbol:    tick.Symbol,
		Type:      marketdata.QuoteTypeTrade,
		Price:     tick.Price,
		Volume:    tick.Size,
		Timestamp: time.UnixMilli(tick.Epoch),
		Provider:  raw.Provider,
	}, nil
}

func (a *Adapter) Mapper() normalization.Mapper {
	return &Mapper{}
}

func (a *Adapter) Name() string { return "nyse" }

func (a *Adapter) Connect(ctx context.Context) error {
	select {
	case <-time.After(5 * time.Millisecond):
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
	return fmt.Errorf("unsubscribe not implemented in nyse mock")
}

func (a *Adapter) Run(ctx context.Context) (<-chan marketdata.RawMessage, error) {
	out := make(chan marketdata.RawMessage, 100)
	go func() {
		defer close(out)
		ticker := time.NewTicker(120 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, sym := range a.symbols {
					price := 200.0 + (rand.Float64() * 15) //nolint:gosec
					tick := RawTick{
						Symbol: sym,
						Price:  price,
						Size:   50,
						Epoch:  time.Now().UnixMilli(),
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
	exchange.Register("nyse", func(cfg exchange.AdapterConfig) (exchange.ExchangeAdapter, error) {
		return &Adapter{}, nil
	})
}
