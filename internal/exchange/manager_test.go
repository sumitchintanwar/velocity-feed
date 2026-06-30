package exchange_test

import (
	"context"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/exchange"
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/normalization"
)

type mockAdapter struct {
	name string
}

func (m *mockAdapter) Name() string { return m.name }
func (m *mockAdapter) Connect(ctx context.Context) error { return nil }
func (m *mockAdapter) Subscribe(symbols ...string) error { return nil }
func (m *mockAdapter) Unsubscribe(symbols ...string) error { return nil }
func (m *mockAdapter) Disconnect(ctx context.Context) error { return nil }

type mockMapper struct{}
func (m *mockMapper) Map(raw marketdata.RawMessage) (marketdata.Quote, error) {
	return raw.Payload.(marketdata.Quote), nil
}
func (m *mockAdapter) Mapper() normalization.Mapper {
	return &mockMapper{}
}

func (m *mockAdapter) Run(ctx context.Context) (<-chan marketdata.RawMessage, error) {
	out := make(chan marketdata.RawMessage, 100)
	go func() {
		defer close(out)
		ticker := time.NewTicker(10 * time.Microsecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				out <- marketdata.RawMessage{Payload: marketdata.Quote{
					Symbol:    "AAPL",
					Type:      marketdata.QuoteTypeTrade,
					Price:     150.0,
					Volume:    100,
					Timestamp: time.Now(),
				}}
			}
		}
	}()
	return out, nil
}

func TestManagerAndRegistry(t *testing.T) {
	exchange.Register("test_mock", func(cfg exchange.AdapterConfig) (exchange.ExchangeAdapter, error) {
		return &mockAdapter{name: cfg.Name}, nil
	})

	logger := log.NewFromConfig(log.Config{Level: "debug"})
	cfg := []exchange.AdapterConfig{
		{Name: "test_mock", Enabled: true},
	}
	
	m, err := exchange.NewManager(cfg, logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	
	ch, err := m.Run(ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	
	q := <-ch
	if q.Symbol != "AAPL" {
		t.Errorf("expected AAPL, got %s", q.Symbol)
	}
}

func BenchmarkManager(b *testing.B) {
	logger := log.NewFromConfig(log.Config{Level: "error"})
	cfg := []exchange.AdapterConfig{
		{Name: "test_mock", Enabled: true},
	}
	
	m, _ := exchange.NewManager(cfg, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	ch, _ := m.Run(ctx)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		<-ch
	}
}
