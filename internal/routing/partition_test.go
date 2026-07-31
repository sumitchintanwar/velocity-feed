package routing

import (
	"testing"
)

func TestPartitionManager_Mapping(t *testing.T) {
	// Create a ring with 2 physical nodes
	ring := NewConsistentHashRing(100)
	ring.AddNode("gateway-A")
	ring.AddNode("gateway-B")

	// Create manager with 1024 fixed partitions
	manager := NewPartitionManager(1024, ring)

	// Test Symbol to Partition hashing
	partitionID := manager.PartitionFor("AAPL.NASDAQ")

	if partitionID >= 1024 {
		t.Fatalf("Partition ID out of bounds: %d", partitionID)
	}

	// It must be strictly deterministic
	if manager.PartitionFor("AAPL.NASDAQ") != partitionID {
		t.Fatalf("Partition hash is not deterministic")
	}

	// Ensure different symbols hash to different partitions (statistically likely)
	if manager.PartitionFor("MSFT.NASDAQ") == partitionID {
		t.Fatalf("Collision detected for tiny sample, bad hash distribution")
	}
}

func TestPartitionManager_Overrides(t *testing.T) {
	ring := NewConsistentHashRing(100)
	ring.AddNode("gateway-A")

	manager := NewPartitionManager(1024, ring)

	// Without override, it should resolve to gateway-A (the only node on the ring)
	gw := manager.GatewayForPartition(42)
	if gw != "gateway-A" {
		t.Fatalf("Expected gateway-A, got %s", gw)
	}

	// Apply manual override
	manager.MovePartition(42, "gateway-Z")

	// Now it should bypass the ring and resolve to gateway-Z
	gwOverride := manager.GatewayForPartition(42)
	if gwOverride != "gateway-Z" {
		t.Fatalf("Override failed, expected gateway-Z, got %s", gwOverride)
	}

	// Clear override
	manager.ClearPartitionOverride(42)

	// Should resolve back to gateway-A
	gwRevert := manager.GatewayForPartition(42)
	if gwRevert != "gateway-A" {
		t.Fatalf("Clear override failed, expected gateway-A, got %s", gwRevert)
	}
}
