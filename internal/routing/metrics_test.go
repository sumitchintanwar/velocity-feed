package routing

import (
	"sync"
	"testing"
	"time"
)

func TestGatewayMetrics_RecordAndSnapshot(t *testing.T) {
	gm := NewGatewayMetrics()

	partitionID := uint32(42)

	// Record initial stats
	gm.AddClient(partitionID)
	gm.AddClient(partitionID)
	gm.RecordMessage(partitionID, 1024)
	gm.RecordMessage(partitionID, 512)

	// Simulate someone leaving
	gm.RemoveClient(partitionID)

	snapshot := gm.GatewaySnapshot()
	stats, exists := snapshot[partitionID]

	if !exists {
		t.Fatalf("expected partition 42 in snapshot")
	}

	if stats.Messages != 2 {
		t.Errorf("expected 2 messages, got %d", stats.Messages)
	}

	if stats.Bytes != 1536 {
		t.Errorf("expected 1536 bytes, got %d", stats.Bytes)
	}

	if stats.Clients != 1 {
		t.Errorf("expected 1 active client, got %d", stats.Clients)
	}
}

func TestGatewayMetrics_ComputeRates(t *testing.T) {
	gm := NewGatewayMetrics()
	partitionID := uint32(100)

	// Add a client and a batch of messages
	gm.AddClient(partitionID)
	gm.AddClient(partitionID) // 2 clients

	// Simulate 1000 messages of 100 bytes over a 1-second window
	for i := 0; i < 1000; i++ {
		gm.RecordMessage(partitionID, 100)
	}

	// Compute for a 1 second window
	gm.ComputeRates(1 * time.Second)

	snapshot := gm.GatewaySnapshot()[partitionID]

	// Throughput should be exactly 1000 msgs/sec
	if snapshot.Throughput != 1000.0 {
		t.Errorf("expected throughput of 1000, got %f", snapshot.Throughput)
	}

	// ByteRate should be 1000 * 100 = 100,000 bytes/sec
	if snapshot.ByteRate != 100000.0 {
		t.Errorf("expected byte rate of 100000, got %f", snapshot.ByteRate)
	}

	// CPU Estimate: (1000 * 0.01) + (2 * 0.05) = 10.0 + 0.1 = 10.1
	if snapshot.CPUEstimate != 10.1 {
		t.Errorf("expected CPU estimate 10.1, got %f", snapshot.CPUEstimate)
	}

	// Memory Estimate: (2 * 32768) + (100000 * 0.5) = 65536 + 50000 = 115536
	if snapshot.MemEstimate != 115536.0 {
		t.Errorf("expected Mem estimate 115536.0, got %f", snapshot.MemEstimate)
	}

	// Test zeroing out throughput if traffic stops
	gm.ComputeRates(1 * time.Second) // Second window, no new messages
	snapshot = gm.GatewaySnapshot()[partitionID]

	if snapshot.Throughput != 0.0 {
		t.Errorf("expected throughput to drop to 0, got %f", snapshot.Throughput)
	}

	if snapshot.ByteRate != 0.0 {
		t.Errorf("expected byte rate to drop to 0, got %f", snapshot.ByteRate)
	}

	// CPU should still reflect the baseline cost of 2 idle clients (2 * 0.05 = 0.1)
	if snapshot.CPUEstimate != 0.1 {
		t.Errorf("expected baseline CPU estimate 0.1, got %f", snapshot.CPUEstimate)
	}
}

func TestGatewayMetrics_Concurrency(t *testing.T) {
	gm := NewGatewayMetrics()

	var wg sync.WaitGroup
	partitionID := uint32(7)

	// Simulate 100 concurrent connections writing 1000 messages each
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			gm.AddClient(partitionID)
			for j := 0; j < 1000; j++ {
				gm.RecordMessage(partitionID, 10)
			}
			gm.RemoveClient(partitionID)
		}()
	}

	// Simulate periodic rate computation concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			gm.ComputeRates(50 * time.Millisecond)
			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()

	stats := gm.GatewaySnapshot()[partitionID]

	// 100 routines * 1000 messages = 100,000
	if stats.Messages != 100000 {
		t.Errorf("expected 100,000 messages, got %d", stats.Messages)
	}

	// 100,000 * 10 bytes
	if stats.Bytes != 1000000 {
		t.Errorf("expected 1,000,000 bytes, got %d", stats.Bytes)
	}

	// All clients removed
	if stats.Clients != 0 {
		t.Errorf("expected 0 active clients, got %d", stats.Clients)
	}
}
