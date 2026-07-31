package routing

import (
	"sync"
	"sync/atomic"
	"time"
)

// PartitionStats holds the raw and computed metrics for a single partition.
type PartitionStats struct {
	// Raw counters (strictly increasing)
	TotalMessages atomic.Uint64
	TotalBytes    atomic.Uint64
	ActiveClients atomic.Int64 // Can increase and decrease

	// Snapshot counters for computing deltas
	lastMessages uint64
	lastBytes    uint64

	// Computed metrics (updated periodically)
	mu          sync.RWMutex
	Throughput  float64 // Messages per second
	ByteRate    float64 // Bytes per second
	CPUEstimate float64 // Arbitrary unit (Throughput * W1 + ActiveClients * W2)
	MemEstimate float64 // Bytes (ActiveClients * ConnMem + ByteRate * BufferMem)
}

// Snapshot returns a thread-safe copy of the computed metrics.
func (p *PartitionStats) Snapshot() PartitionSnapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return PartitionSnapshot{
		Messages:    p.TotalMessages.Load(),
		Clients:     p.ActiveClients.Load(),
		Bytes:       p.TotalBytes.Load(),
		Throughput:  p.Throughput,
		ByteRate:    p.ByteRate,
		CPUEstimate: p.CPUEstimate,
		MemEstimate: p.MemEstimate,
	}
}

// PartitionSnapshot is a point-in-time read of the stats, safe for serialization.
type PartitionSnapshot struct {
	Messages    uint64  `json:"messages"`
	Clients     int64   `json:"clients"`
	Bytes       uint64  `json:"bytes"`
	Throughput  float64 `json:"throughput_mps"`
	ByteRate    float64 `json:"byte_rate_bps"`
	CPUEstimate float64 `json:"cpu_estimate"`
	MemEstimate float64 `json:"memory_estimate"`
}

// GatewayMetrics collects telemetry for all partitions owned by this physical gateway.
// It is heavily optimized for concurrent hot-path counter increments.
type GatewayMetrics struct {
	mu         sync.RWMutex
	partitions map[uint32]*PartitionStats
}

// NewGatewayMetrics creates a new telemetry collector for partitions.
func NewGatewayMetrics() *GatewayMetrics {
	return &GatewayMetrics{
		partitions: make(map[uint32]*PartitionStats),
	}
}

func (m *GatewayMetrics) getOrCreate(partition uint32) *PartitionStats {
	m.mu.RLock()
	stats, exists := m.partitions[partition]
	m.mu.RUnlock()

	if exists {
		return stats
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Double-check locking
	if stats, exists = m.partitions[partition]; exists {
		return stats
	}
	stats = &PartitionStats{}
	m.partitions[partition] = stats
	return stats
}

// RecordMessage increments the message and byte counts for a partition.
func (m *GatewayMetrics) RecordMessage(partition uint32, sizeBytes uint64) {
	stats := m.getOrCreate(partition)
	stats.TotalMessages.Add(1)
	stats.TotalBytes.Add(sizeBytes)
}

// AddClient increments the active client count for a partition.
func (m *GatewayMetrics) AddClient(partition uint32) {
	stats := m.getOrCreate(partition)
	stats.ActiveClients.Add(1)
}

// RemoveClient decrements the active client count for a partition.
func (m *GatewayMetrics) RemoveClient(partition uint32) {
	stats := m.getOrCreate(partition)
	stats.ActiveClients.Add(-1)
}

// ComputeRates calculates the throughput and resource estimates for all partitions.
// It is intended to be called periodically (e.g. every 1 second).
func (m *GatewayMetrics) ComputeRates(window time.Duration) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	secs := window.Seconds()
	if secs <= 0 {
		return
	}

	for _, stats := range m.partitions {
		currentMsgs := stats.TotalMessages.Load()
		currentBytes := stats.TotalBytes.Load()
		clients := float64(stats.ActiveClients.Load())

		// Calculate deltas
		deltaMsgs := currentMsgs - stats.lastMessages
		deltaBytes := currentBytes - stats.lastBytes

		// Update snapshots for next computation
		stats.lastMessages = currentMsgs
		stats.lastBytes = currentBytes

		throughput := float64(deltaMsgs) / secs
		byteRate := float64(deltaBytes) / secs

		// Arbitrary heuristic coefficients for load balancing
		// 1 Msg/sec ~= 0.01 CPU units. 1 Client ~= 0.05 CPU units (due to idle ping/pong)
		cpuEstimate := (throughput * 0.01) + (clients * 0.05)

		// 1 Client ~= 32KB memory overhead. Buffered bytes = byteRate * 0.5s avg buffer
		memEstimate := (clients * 32768.0) + (byteRate * 0.5)

		stats.mu.Lock()
		stats.Throughput = throughput
		stats.ByteRate = byteRate
		stats.CPUEstimate = cpuEstimate
		stats.MemEstimate = memEstimate
		stats.mu.Unlock()
	}
}

// GatewaySnapshot returns the aggregated metrics for the entire gateway.
func (m *GatewayMetrics) GatewaySnapshot() map[uint32]PartitionSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[uint32]PartitionSnapshot, len(m.partitions))
	for id, stats := range m.partitions {
		result[id] = stats.Snapshot()
	}
	return result
}
