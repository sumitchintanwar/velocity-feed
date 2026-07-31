package routing

import (
	"fmt"
	"math"
	"testing"
)

func TestRing_AddAndRemove(t *testing.T) {
	ring := NewConsistentHashRing(100)

	if ring.NodeCount() != 0 {
		t.Fatalf("expected 0 nodes, got %d", ring.NodeCount())
	}

	ring.AddNode("gateway-1")
	if ring.NodeCount() != 1 {
		t.Fatalf("expected 1 node, got %d", ring.NodeCount())
	}

	// Verify idempotency
	ring.AddNode("gateway-1")
	if ring.NodeCount() != 1 {
		t.Fatalf("expected 1 node after duplicate add, got %d", ring.NodeCount())
	}

	if len(ring.keys) != 100 {
		t.Fatalf("expected 100 virtual nodes, got %d", len(ring.keys))
	}

	ring.RemoveNode("gateway-1")
	if ring.NodeCount() != 0 {
		t.Fatalf("expected 0 nodes after remove, got %d", ring.NodeCount())
	}
	if len(ring.keys) != 0 {
		t.Fatalf("expected 0 virtual nodes after remove, got %d", len(ring.keys))
	}
}

func TestRing_Distribution(t *testing.T) {
	// Add 5 nodes with 100 vnodes each as requested
	vnodes := 100
	ring := NewConsistentHashRing(vnodes)

	nodes := []string{"gw1", "gw2", "gw3", "gw4", "gw5"}
	for _, n := range nodes {
		ring.AddNode(n)
	}

	// Map 1,000,000 keys and check the distribution
	counts := make(map[string]int)
	totalKeys := 1000000

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("partition-%d", i)
		node := ring.Lookup(key)
		counts[node]++
	}

	// Calculate statistics
	expected := float64(totalKeys) / float64(len(nodes))

	var varianceSum float64
	maxCount := 0
	minCount := totalKeys

	for _, count := range counts {
		if count > maxCount {
			maxCount = count
		}
		if count < minCount {
			minCount = count
		}

		diff := float64(count) - expected
		varianceSum += diff * diff
	}

	variance := varianceSum / float64(len(nodes))
	stdDev := math.Sqrt(variance)
	stdDevPercent := (stdDev / expected) * 100.0

	worstCaseImbalance := float64(maxCount) - expected
	worstCasePercent := (worstCaseImbalance / expected) * 100.0

	// Output the metrics for visibility
	t.Logf("--- Distribution Metrics (%d VNodes, %d Physical Nodes) ---", vnodes, len(nodes))
	t.Logf("Total Keys: %d", totalKeys)
	t.Logf("Mean (Expected): %.2f", expected)
	t.Logf("Min Keys: %d | Max Keys: %d", minCount, maxCount)
	t.Logf("Standard Deviation: %.2f (%.2f%%)", stdDev, stdDevPercent)
	t.Logf("Worst-Case Imbalance: +%.2f (%.2f%%)", worstCaseImbalance, worstCasePercent)

	// Enforce strict balancing limits
	// With 100 virtual nodes, we expect stdDev < 15% and worst-case < 25%
	if stdDevPercent > 15.0 {
		t.Errorf("Standard deviation %.2f%% is too high (expected < 15%%)", stdDevPercent)
	}
	if worstCasePercent > 25.0 {
		t.Errorf("Worst-case imbalance %.2f%% is too high (expected < 25%%)", worstCasePercent)
	}
}

func TestRing_Stability(t *testing.T) {
	ring := NewConsistentHashRing(256)
	ring.AddNode("gw1")
	ring.AddNode("gw2")
	ring.AddNode("gw3")

	// Map 10,000 partitions
	partitionMap := make(map[uint32]string)
	for i := uint32(0); i < 10000; i++ {
		partitionMap[i] = ring.LookupPartition(i)
	}

	// Add a 4th node
	ring.AddNode("gw4")

	// Count how many partitions migrated
	migrated := 0
	for i := uint32(0); i < 10000; i++ {
		newNode := ring.LookupPartition(i)
		if newNode != partitionMap[i] {
			migrated++
			// Any migrated partition MUST go to gw4
			if newNode != "gw4" {
				t.Fatalf("Partition migrated to %s instead of new node gw4", newNode)
			}
		}
	}

	// Expect roughly 25% migration
	expectedMigration := 10000.0 / 4.0
	margin := expectedMigration * 0.15
	diff := math.Abs(float64(migrated) - expectedMigration)
	if diff > margin {
		t.Errorf("Migrated %d partitions, expected ~%.0f. Disruption too high.", migrated, expectedMigration)
	}
}
