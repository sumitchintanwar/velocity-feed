package routing

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/topology"
)

// BenchmarkEngine_RedirectTarget tests the raw throughput of a fully populated routing engine
// doing redirect lookups (Symbol -> Partition -> Ring -> Gateway URL).
func BenchmarkEngine_RedirectTarget(b *testing.B) {
	ctx := context.Background()
	reg := topology.NewMemoryRegistry(10 * time.Second)
	ring := NewConsistentHashRing(100) // 100 vnodes per physical node
	pm := NewPartitionManager(25600, ring)
	engine := NewEngine("gateway-0", reg, pm, 10*time.Millisecond)

	// Populate cluster with 100 Gateways
	for i := 1; i <= 100; i++ {
		id := "gateway-" + strconv.Itoa(i)
		reg.Register(ctx, topology.GatewayMetadata{
			ID:      id,
			Address: "10.0.0." + strconv.Itoa(i) + ":8080",
			State:   topology.StateOnline,
		})
	}
	_ = engine.syncTopology(ctx)

	// Pre-generate strings to avoid allocation in benchmark loop
	symbols := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		symbols[i] = "SYMBOL_" + strconv.Itoa(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Cycle through symbols to prevent branch prediction from caching a single route
		sym := symbols[i%1000]
		_ = engine.RedirectTarget(sym)
	}
}

// BenchmarkEngine_SyncTopology measures how fast the engine can pull state from the Registry
// and update a 100-node consistent hash ring.
func BenchmarkEngine_SyncTopology(b *testing.B) {
	ctx := context.Background()
	reg := topology.NewMemoryRegistry(10 * time.Second)
	ring := NewConsistentHashRing(100)
	pm := NewPartitionManager(25600, ring)
	engine := NewEngine("gateway-0", reg, pm, 10*time.Millisecond)

	// Populate cluster with 100 Gateways
	for i := 1; i <= 100; i++ {
		id := "gateway-" + strconv.Itoa(i)
		reg.Register(ctx, topology.GatewayMetadata{
			ID:      id,
			Address: "10.0.0." + strconv.Itoa(i) + ":8080",
			State:   topology.StateOnline,
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = engine.syncTopology(ctx)
	}
}
