package nasdaq

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/sumit/rtmds/internal/exchange"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/normalization"
)

// Purpose: Mock adapter representing a TCP/ITCH-style NASDAQ equity feed.
// Architecture: Implements exchange.ExchangeAdapter. Generates fake NASDAQ quotes.
// Design Decisions: Built to test the ExchangeManager's ability to multiplex different adapters.

type Adapter struct {
	symbols []string
}

// RawTick represents the un-normalized structure received from the mock exchange
type RawTick struct {
	Sym  string  `json:"sym"`
	Prc  float64 `json:"prc"`
	Vol  int64   `json:"vol"`
	Time int64   `json:"time"`
}

type Mapper struct{}

func (m *Mapper) Map(raw marketdata.RawMessage) (marketdata.Quote, error) {
	tick, ok := raw.Payload.(RawTick)
	if !ok {
		return marketdata.Quote{}, fmt.Errorf("expected nasdaq.RawTick")
	}
	return marketdata.Quote{
		Symbol:    tick.Sym,
		Type:      marketdata.QuoteTypeTrade,
		Price:     tick.Prc,
		Bid:       tick.Prc - 0.05,
		Ask:       tick.Prc + 0.05,
		Volume:    tick.Vol,
		Timestamp: time.UnixMilli(tick.Time),
		Provider:  raw.Provider,
	}, nil
}

func (a *Adapter) Mapper() normalization.Mapper {
	return &Mapper{}
}

func (a *Adapter) Name() string { return "nasdaq" }

func (a *Adapter) Connect(ctx context.Context) error {
	// Simulate connection delay
	select {
	case <-time.After(10 * time.Millisecond):
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
	return fmt.Errorf("unsubscribe not implemented in mock")
}

func (a *Adapter) Run(ctx context.Context) (<-chan marketdata.RawMessage, error) {
	out := make(chan marketdata.RawMessage, 100)
	go func() {
		defer close(out)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, sym := range a.symbols {
					price := 150.0 + (rand.Float64() * 10) //nolint:gosec
					tick := RawTick{
						Sym:  sym,
						Prc:  price,
						Vol:  100,
						Time: time.Now().UnixMilli(),
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
	exchange.Register("nasdaq", func(cfg exchange.AdapterConfig) (exchange.ExchangeAdapter, error) {
		return &Adapter{}, nil
	})
}
