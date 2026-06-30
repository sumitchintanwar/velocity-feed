package exchange

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/normalization"
)

// Purpose: Orchestrates the lifecycle of all enabled adapters.
// Architecture: Centralized manager coordinates starting, stopping, and merging feeds from multiple adapters.
// Design Decisions: Uses wait groups for graceful shutdown and multiplexes all adapter channels into one canonical channel for the Publisher.

// Manager manages multiple exchange adapters and merges their outputs.
type Manager struct {
	configs  []AdapterConfig
	adapters []ExchangeAdapter
	logger   *log.Logger
	merged   chan marketdata.Quote
	wg       sync.WaitGroup
}

// NewManager creates a new ExchangeManager with the provided configurations.
func NewManager(configs []AdapterConfig, logger *log.Logger) (*Manager, error) {
	m := &Manager{
		configs: configs,
		logger:  logger,
		merged:  make(chan marketdata.Quote, 10000), // Buffer to handle spikes from multiple adapters
	}

	for _, cfg := range configs {
		if !cfg.Enabled {
			logger.Underlying().Info().Str("adapter", cfg.Name).Msg("Adapter disabled by configuration")
			continue
		}

		factory, err := GetFactory(cfg.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get factory for %s: %w", cfg.Name, err)
		}

		adapter, err := factory(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create adapter %s: %w", cfg.Name, err)
		}

		m.adapters = append(m.adapters, adapter)
	}

	return m, nil
}

// Adapters returns the list of active adapters.
func (m *Manager) Adapters() []ExchangeAdapter {
	return m.adapters
}

// Name implements marketdata.Feed.
func (m *Manager) Name() string {
	return "exchange_manager"
}

// Subscribe implements marketdata.Feed.
func (m *Manager) Subscribe(symbols ...string) error {
	var errs []error
	for _, adapter := range m.adapters {
		if err := adapter.Subscribe(symbols...); err != nil {
			errs = append(errs, fmt.Errorf("adapter %s: %w", adapter.Name(), err))
		}
	}
	return errors.Join(errs...)
}

// Unsubscribe implements marketdata.Feed.
func (m *Manager) Unsubscribe(symbols ...string) error {
	var errs []error
	for _, adapter := range m.adapters {
		if err := adapter.Unsubscribe(symbols...); err != nil {
			errs = append(errs, fmt.Errorf("adapter %s: %w", adapter.Name(), err))
		}
	}
	return errors.Join(errs...)
}

// Run connects all adapters, subscribes to their symbols, and begins multiplexing normalized quotes.
func (m *Manager) Run(ctx context.Context) (<-chan marketdata.Quote, error) {
	if len(m.adapters) == 0 {
		m.logger.Underlying().Warn().Msg("No exchange adapters enabled")
		close(m.merged)
		return m.merged, nil
	}

	for i, adapter := range m.adapters {
		cfg := m.configs[i]
		
		// 1. Connect
		if err := adapter.Connect(ctx); err != nil {
			m.logger.Underlying().Error().Err(err).Str("adapter", adapter.Name()).Msg("Failed to connect adapter")
			continue
		}

		// 2. Subscribe (if symbols are provided in config)
		if len(cfg.Symbols) > 0 {
			if err := adapter.Subscribe(cfg.Symbols...); err != nil {
				m.logger.Underlying().Error().Err(err).Str("adapter", adapter.Name()).Msg("Failed to subscribe symbols")
				continue
			}
		}

		// 3. Run
		rawMsgs, err := adapter.Run(ctx)
		if err != nil {
			m.logger.Underlying().Error().Err(err).Str("adapter", adapter.Name()).Msg("Failed to run adapter")
			continue
		}

		// 3.5 Construct Normalization Pipeline
		pipe := normalization.NewPipeline(
			adapter.Mapper(),
			normalization.NewDefaultValidator(),
			m.logger,
		)

		// 4. Multiplex
		m.wg.Add(1)
		go func(a ExchangeAdapter, q <-chan marketdata.RawMessage, p *normalization.Pipeline) {
			defer m.wg.Done()
			m.logger.Underlying().Info().Str("adapter", a.Name()).Msg("Adapter streaming started")
			
			for {
				select {
				case raw, ok := <-q:
					if !ok {
						m.logger.Underlying().Info().Str("adapter", a.Name()).Msg("Adapter channel closed")
						return
					}
					
					// Normalize
					quote, err := p.Normalize(raw)
					if err != nil {
						continue // Drop invalid quote and continue
					}

					select {
					case m.merged <- quote:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}(adapter, rawMsgs, pipe)
	}

	// Wait for all adapters to finish, then close merged channel
	go func() {
		m.wg.Wait()
		close(m.merged)
		m.logger.Underlying().Info().Msg("Exchange manager shutdown complete")
	}()

	return m.merged, nil
}
