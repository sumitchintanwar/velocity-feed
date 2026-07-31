package routing

import (
	"context"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/hashing"
	"github.com/sumit/rtmds/internal/topology"
)

// Generate millions of lookups strings once
var symbols []string

func init() {
	symbols = make([]string, 1000000)
	for i := 0; i < 1000000; i++ {
		symbols[i] = "SYMBOL_LONG_NAME_" + strconv.Itoa(i)
	}
}

// 1. Hash Throughput
func BenchmarkHashThroughput(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hashing.HashString(symbols[i%1000000])
	}
}

// 2. Partition Lookup (Symbol -> Partition ID)
func BenchmarkPartitionLookup(b *testing.B) {
	pm := NewPartitionManager(25600, nil)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = pm.PartitionFor(symbols[i%1000000])
	}
}

// 3. Gateway Lookup (Partition ID -> Gateway String)
func BenchmarkGatewayLookup(b *testing.B) {
	ring := NewConsistentHashRing(100)
	for i := 0; i < 50; i++ {
		ring.AddNode("gateway-" + strconv.Itoa(i))
	}
	pm := NewPartitionManager(25600, ring)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Use i directly as a partition ID
		_ = pm.GatewayForPartition(uint32(i % 25600))
	}
}

// 4. Full Lookup Latency (Symbol -> Partition -> Gateway -> Address)
func BenchmarkFullLookupLatency(b *testing.B) {
	ctx := context.Background()
	reg := topology.NewMemoryRegistry(10 * time.Second)
	ring := NewConsistentHashRing(100)
	pm := NewPartitionManager(25600, ring)
	engine := NewEngine("gateway-0", reg, pm, 10*time.Millisecond)

	for i := 1; i <= 50; i++ {
		reg.Register(ctx, topology.GatewayMetadata{
			ID:      "gateway-" + strconv.Itoa(i),
			Address: "10.0.0." + strconv.Itoa(i) + ":8080",
			State:   topology.StateOnline,
		})
	}
	_ = engine.syncTopology(ctx)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = engine.RedirectTarget(symbols[i%1000000])
	}
}

// 5. Node Add (Time to inject into Ring)
func BenchmarkNodeAdd(b *testing.B) {
	ring := NewConsistentHashRing(100)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Reset ring every 500 nodes to keep array sizes realistic
		if i%500 == 0 {
			b.StopTimer()
			ring = NewConsistentHashRing(100)
			b.StartTimer()
		}
		ring.AddNode("gateway-" + strconv.Itoa(i))
	}
}

// 6. Node Remove (Time to remove from Ring)
func BenchmarkNodeRemove(b *testing.B) {
	ring := NewConsistentHashRing(100)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nodeID := "gateway-" + strconv.Itoa(i)
		ring.AddNode(nodeID)
		b.StartTimer()

		ring.RemoveNode(nodeID)
	}
}

// 7. Rebalance Time (Topology Sync)
func BenchmarkRebalanceTime(b *testing.B) {
	ctx := context.Background()
	reg := topology.NewMemoryRegistry(10 * time.Second)
	ring := NewConsistentHashRing(100)
	pm := NewPartitionManager(25600, ring)
	engine := NewEngine("gateway-0", reg, pm, 10*time.Millisecond)

	// Populate 100 nodes
	for i := 1; i <= 100; i++ {
		reg.Register(ctx, topology.GatewayMetadata{
			ID:      "gateway-" + strconv.Itoa(i),
			Address: "10.0.0." + strconv.Itoa(i) + ":8080",
			State:   topology.StateOnline,
		})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = engine.syncTopology(ctx)
	}
}

// TestDistributionQuality isn't a benchmark in terms of speed, but we use it to log
// standard deviation of our partitioning scheme across 10 million lookups.
func TestDistributionQuality(t *testing.T) {
	ring := NewConsistentHashRing(100) // 100 VNodes
	pm := NewPartitionManager(25600, ring)

	numGateways := 50
	for i := 0; i < numGateways; i++ {
		ring.AddNode("gateway-" + strconv.Itoa(i))
	}

	distribution := make(map[string]int)

	// Simulate 10 Million subscriptions
	r := rand.New(rand.NewSource(42))
	for i := 0; i < 10_000_000; i++ {
		// Use random string formats to represent wild symbols
		sym := "SYM_" + strconv.Itoa(r.Int())
		owner := pm.GatewayForSymbol(sym)
		distribution[owner]++
	}

	var sum float64
	for _, count := range distribution {
		sum += float64(count)
	}
	mean := sum / float64(numGateways)

	var sqDiffSum float64
	for _, count := range distribution {
		diff := float64(count) - mean
		sqDiffSum += diff * diff
	}

	variance := sqDiffSum / float64(numGateways)
	stdDev := math.Sqrt(variance)

	t.Logf("Distribution Quality across 10,000,000 lookups (50 Gateways):")
	t.Logf("Mean symbols per gateway: %.2f", mean)
	t.Logf("Standard Deviation: %.2f (%.2f%%)", stdDev, (stdDev/mean)*100)

	// Enforce 15% tolerance on distribution imbalance
	if (stdDev / mean) > 0.15 {
		t.Errorf("Distribution standard deviation too high: %.2f%%", (stdDev/mean)*100)
	}
}
