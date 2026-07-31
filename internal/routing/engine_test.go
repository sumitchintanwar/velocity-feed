package routing

import (
	"context"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/topology"
)

func setupTestEngine(t *testing.T) (*Engine, *topology.MemoryRegistry, *PartitionManager) {
	t.Helper()
	reg := topology.NewMemoryRegistry(10 * time.Second)
	ring := NewConsistentHashRing(100) // 100 vnodes
	pm := NewPartitionManager(25600, ring)
	engine := NewEngine("gateway-1", reg, pm, 10*time.Millisecond)
	return engine, reg, pm
}

func TestEngine_AutomaticRebalancing(t *testing.T) {
	ctx := context.Background()
	engine, reg, _ := setupTestEngine(t)

	// Start engine daemon
	engine.Start(ctx)
	defer engine.Stop()

	// 1. Initially empty ring (only the local node might exist, but we haven't registered it yet)
	if target := engine.RedirectTarget("AAPL"); target != "" {
		t.Fatalf("expected no redirect on empty ring, got %s", target)
	}

	// 2. Gateway-2 joins the cluster
	reg.Register(ctx, topology.GatewayMetadata{
		ID:      "gateway-2",
		Address: "10.0.0.2:8080",
		State:   topology.StateOnline,
	})

	// Wait for poll
	time.Sleep(30 * time.Millisecond)

	// Because Gateway-2 is the ONLY node in the ring, ALL traffic should redirect to it.
	target := engine.RedirectTarget("AAPL")
	if target != "ws://10.0.0.2:8080/ws" {
		t.Fatalf("expected AAPL to redirect to gateway-2, got %s", target)
	}

	// 3. Gateway-1 (Local node) joins the cluster
	reg.Register(ctx, topology.GatewayMetadata{
		ID:      "gateway-1",
		Address: "10.0.0.1:8080",
		State:   topology.StateOnline,
	})

	time.Sleep(30 * time.Millisecond)

	// Now we have a 50/50 split.
	// To test rebalancing accurately, let's register 10 nodes and verify minimal movement.
}

func TestEngine_MinimalMovementRebalancing(t *testing.T) {
	ctx := context.Background()
	engine, reg, _ := setupTestEngine(t)
	engine.Start(ctx)
	defer engine.Stop()

	// Register 5 nodes
	for i := 1; i <= 5; i++ {
		reg.Register(ctx, topology.GatewayMetadata{
			ID:      "gateway-A" + string(rune('0'+i)),
			Address: "10.0.0." + string(rune('0'+i)) + ":8080",
			State:   topology.StateOnline,
		})
	}

	time.Sleep(50 * time.Millisecond)

	// Map 10,000 symbols to their assigned gateways
	initialDistribution := make(map[string]string)
	symbols := []string{"AAPL", "MSFT", "GOOG", "AMZN", "TSLA", "META", "NFLX", "NVDA", "JPM", "V"}

	for i := 0; i < 1000; i++ {
		for _, sym := range symbols {
			s := sym + string(rune(i))
			initialDistribution[s] = engine.RedirectTarget(s)
		}
	}

	// Gateway-6 joins the cluster
	reg.Register(ctx, topology.GatewayMetadata{
		ID:      "gateway-A6",
		Address: "10.0.0.6:8080",
		State:   topology.StateOnline,
	})

	time.Sleep(50 * time.Millisecond)

	// Measure how many symbols moved
	moved := 0
	for sym, initialOwner := range initialDistribution {
		newOwner := engine.RedirectTarget(sym)
		if newOwner != initialOwner {
			moved++
			// Rule of consistent hashing: if it moved, it MUST have moved to the NEW node.
			if newOwner != "ws://10.0.0.6:8080/ws" {
				t.Fatalf("Symbol %s moved from %s to %s, but should only move to the new node gateway-A6", sym, initialOwner, newOwner)
			}
		}
	}

	// In a perfectly balanced consistent hash ring of N nodes, adding 1 node
	// should move roughly 1/(N+1) of the keys.
	// 10000 / 6 = ~1666. Accept anything under 2500 for variance.
	if moved > 2500 || moved < 800 {
		t.Fatalf("expected roughly 1666 symbols to move, but %d moved", moved)
	}
}

func TestEngine_NodeFailureRebalancing(t *testing.T) {
	ctx := context.Background()
	engine, reg, _ := setupTestEngine(t)
	engine.Start(ctx)
	defer engine.Stop()

	// Register 3 nodes
	reg.Register(ctx, topology.GatewayMetadata{ID: "gw1", Address: "a1", State: topology.StateOnline})
	reg.Register(ctx, topology.GatewayMetadata{ID: "gw2", Address: "a2", State: topology.StateOnline})
	reg.Register(ctx, topology.GatewayMetadata{ID: "gw3", Address: "a3", State: topology.StateOnline})

	time.Sleep(30 * time.Millisecond)

	// Find a symbol that gw2 owns
	var gw2Symbol string
	for i := 0; i < 1000; i++ {
		sym := "SYM" + string(rune(i))
		if engine.RedirectTarget(sym) == "ws://a2/ws" {
			gw2Symbol = sym
			break
		}
	}

	if gw2Symbol == "" {
		t.Fatalf("could not find a symbol assigned to gw2")
	}

	// gw2 fails (state goes offline)
	reg.UpdateState(ctx, "gw2", topology.StateOffline)

	time.Sleep(30 * time.Millisecond)

	// The symbol must be reassigned
	newOwner := engine.RedirectTarget(gw2Symbol)
	if newOwner == "ws://a2/ws" {
		t.Fatalf("Symbol was not reassigned after owner failure")
	}
	if newOwner == "" {
		t.Fatalf("Symbol has no owner after owner failure")
	}
}
