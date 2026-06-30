package nasdaq_test

import (
	"context"
	"testing"
	"time"

	_ "github.com/sumit/rtmds/internal/adapters/nasdaq" // Register adapter
	"github.com/sumit/rtmds/internal/exchange"
)

func TestAdapter(t *testing.T) {
	factory, err := exchange.GetFactory("nasdaq")
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
	
	if err := adapter.Subscribe("AAPL"); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	
	ch, err := adapter.Run(ctx)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	
	select {
	case q := <-ch:
		if q.Provider != "nasdaq" {
			t.Errorf("expected nasdaq provider, got %s", q.Provider)
		}
	case <-time.After(200 * time.Millisecond):
		t.Errorf("timeout waiting for quote")
	}
}

func BenchmarkAdapter(b *testing.B) {
	factory, _ := exchange.GetFactory("nasdaq")
	adapter, _ := factory(exchange.AdapterConfig{})
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	adapter.Connect(ctx)
	adapter.Subscribe("AAPL")
	ch, _ := adapter.Run(ctx)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		<-ch
	}
}
