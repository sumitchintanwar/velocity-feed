package routing

import (
	"strconv"
	"testing"
)

// BenchmarkRing_Lookup measures the speed of locating a node on a heavily populated ring.
func BenchmarkRing_Lookup(b *testing.B) {
	// Simulate 100 gateways, each with 256 virtual nodes (25,600 total nodes on the ring)
	ring := NewConsistentHashRing(256)
	for i := 0; i < 100; i++ {
		ring.AddNode("gateway-" + strconv.Itoa(i))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// We use LookupPartition to simulate the hot path for millions of lookups
		_ = ring.LookupPartition(uint32(i))
	}
}
