package crypto_test

import (
	"context"
	"testing"
	"time"

	_ "github.com/sumit/rtmds/internal/adapters/crypto" // Register adapter
	"github.com/sumit/rtmds/internal/exchange"
)

func TestAdapter(t *testing.T) {
	factory, err := exchange.GetFactory("crypto")
	if err != nil {
		t.Fatalf("failed to get factory: %v", err)
	}
	
	adapter, err := factory(exchange.AdapterConfig{})
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	
	if err := adapter.Connect(ctx); err != nil {
		t.Fatalf("connect failed: %v", err)
	}
	
	if err := adapter.Subscribe("BTC-USD"); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	
	ch, err := adapter.Run(ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	
	select {
	case q := <-ch:
		if q.Provider != "crypto" {
			t.Errorf("expected crypto provider, got %s", q.Provider)
		}
	case <-time.After(200 * time.Millisecond):
		t.Errorf("timeout waiting for quote")
	}
}

func BenchmarkAdapter(b *testing.B) {
	factory, _ := exchange.GetFactory("crypto")
	adapter, _ := factory(exchange.AdapterConfig{})
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	adapter.Connect(ctx)
	adapter.Subscribe("BTC-USD")
	ch, _ := adapter.Run(ctx)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		<-ch
	}
}
