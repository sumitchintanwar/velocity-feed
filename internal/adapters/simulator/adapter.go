package simulator

import (
	"context"
	"fmt"
	"time"

	"github.com/sumit/rtmds/internal/exchange"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/normalization"
	sim "github.com/sumit/rtmds/internal/marketdata/simulator"
)

// Purpose: Adapts the existing marketdata simulator into the Exchange Adapter Framework.
// Architecture: Wraps the sim.Simulator with the Connect lifecycle method.
// Design Decisions: Enables testing the framework pipeline without real external dependencies.

type Adapter struct {
	*sim.Simulator
}

type Mapper struct{}

func (m *Mapper) Map(raw marketdata.RawMessage) (marketdata.Quote, error) {
	q, ok := raw.Payload.(marketdata.Quote)
	if !ok {
		return marketdata.Quote{}, fmt.Errorf("expected marketdata.Quote")
	}
	q.Provider = raw.Provider
	return q, nil
}

func (a *Adapter) Mapper() normalization.Mapper {
	return &Mapper{}
}

func (a *Adapter) Run(ctx context.Context) (<-chan marketdata.RawMessage, error) {
	quotes, err := a.Simulator.Run(ctx)
	if err != nil {
		return nil, err
	}

	out := make(chan marketdata.RawMessage, 100)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case q, ok := <-quotes:
				if !ok {
					return
				}
				select {
				case out <- marketdata.RawMessage{
					Provider:   a.Name(),
					Payload:    q,
					ReceivedAt: time.Now(),
				}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, nil
}

// Connect implements exchange.ExchangeAdapter.
func (a *Adapter) Connect(ctx context.Context) error {
	// Simulator doesn't need to actually connect anywhere.
	return nil
}

// Disconnect implements exchange.ExchangeAdapter.
func (a *Adapter) Disconnect(ctx context.Context) error {
	return nil
}

func init() {
	exchange.Register("simulator", func(cfg exchange.AdapterConfig) (exchange.ExchangeAdapter, error) {
		interval := 500 * time.Millisecond
		if val, ok := cfg.Custom["tick_interval_ms"].(float64); ok {
			interval = time.Duration(val * float64(time.Millisecond))
		}
		
		sCfg := sim.DefaultConfig()
		sCfg.TickInterval = interval
		
		s, err := sim.New(sCfg, nil)
		if err != nil {
			return nil, err
		}
		return &Adapter{Simulator: s}, nil
	})
}
