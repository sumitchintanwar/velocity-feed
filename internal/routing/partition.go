package routing

import (
	"sync"

	"github.com/sumit/rtmds/internal/hashing"
)

// PartitionManager governs the mapping of symbols to partitions, and partitions to gateways.
// It sits in front of the ConsistentHashRing to support fixed-partitioning and manual routing overrides.
type PartitionManager struct {
	mu             sync.RWMutex
	partitionCount uint32
	ring           *ConsistentHashRing
	overrides      map[uint32]string // Maps partition ID directly to a physical gateway
}

// NewPartitionManager creates a new PartitionManager with a fixed number of logical partitions.
func NewPartitionManager(partitions uint32, ring *ConsistentHashRing) *PartitionManager {
	return &PartitionManager{
		partitionCount: partitions,
		ring:           ring,
		overrides:      make(map[uint32]string),
	}
}

// PartitionFor maps a symbol string deterministically to a fixed partition ID.
// This is the first step of the routing process (Symbol -> Partition).
func (m *PartitionManager) PartitionFor(symbol string) uint32 {
	hash := hashing.HashString(symbol)
	return uint32(hash % uint64(m.partitionCount))
}

// GatewayForPartition looks up the physical gateway owner for a given partition ID.
// It checks for manual overrides first, and falls back to the math-based Consistent Hash Ring.
func (m *PartitionManager) GatewayForPartition(id uint32) string {
	m.mu.RLock()
	gateway, hasOverride := m.overrides[id]
	m.mu.RUnlock()

	if hasOverride {
		return gateway
	}

	// Fallback to the consistent hash ring mathematical placement
	return m.ring.LookupPartition(id)
}

// GatewayForSymbol is a convenience wrapper that combines PartitionFor and GatewayForPartition.
func (m *PartitionManager) GatewayForSymbol(symbol string) string {
	partitionID := m.PartitionFor(symbol)
	return m.GatewayForPartition(partitionID)
}

// MovePartition manually overrides the consistent hash ring and pins a partition to a specific gateway.
// This is useful for load balancing "hot" partitions that are skewing the natural distribution.
func (m *PartitionManager) MovePartition(id uint32, newGateway string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.overrides[id] = newGateway
}

// ClearPartitionOverride removes the manual pin, returning the partition's routing back to the Hash Ring.
func (m *PartitionManager) ClearPartitionOverride(id uint32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.overrides, id)
}
