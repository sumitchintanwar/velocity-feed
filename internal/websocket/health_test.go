package websocket

import (
	"testing"
	"time"
)

func TestHealthMonitor_Healthy(t *testing.T) {
	// Create a dummy gateway
	g := &Gateway{lifecycle: NewLifecycle()}
	_ = g.lifecycle.Transition(LifecycleHealthy)
	g.activeCount.Store(100) // well below 90% of maxConnections (10000)

	hm := NewHealthMonitor(g)

	health := hm.Check()

	if health.State != StateHealthy {
		t.Errorf("expected state %s, got %s", StateHealthy, health.State)
	}
	if health.ActiveClients != 100 {
		t.Errorf("expected 100 active clients, got %d", health.ActiveClients)
	}
	if health.HeartbeatStatus != "Stopped" {
		t.Errorf("expected heartbeat stopped, got %s", health.HeartbeatStatus)
	}
}

func TestHealthMonitor_Warning(t *testing.T) {
	g := &Gateway{lifecycle: NewLifecycle()}
	_ = g.lifecycle.Transition(LifecycleHealthy)
	// Set active clients above 90% threshold (9000)
	g.activeCount.Store(9001)

	hm := NewHealthMonitor(g)

	health := hm.Check()

	if health.State != StateWarning {
		t.Errorf("expected state %s, got %s", StateWarning, health.State)
	}
}

func TestHealthMonitor_Offline(t *testing.T) {
	g := &Gateway{lifecycle: NewLifecycle()}
	_ = g.lifecycle.Transition(LifecycleOffline)

	hm := NewHealthMonitor(g)

	health := hm.Check()

	if health.State != StateOffline {
		t.Errorf("expected state %s, got %s", StateOffline, health.State)
	}
}

func TestHealthMonitor_HeartbeatRunning(t *testing.T) {
	g := &Gateway{
		lifecycle: NewLifecycle(),
		heartbeat: &HeartbeatManager{},
	}

	hm := NewHealthMonitor(g)

	health := hm.Check()

	if health.HeartbeatStatus != "Running" {
		t.Errorf("expected heartbeat running, got %s", health.HeartbeatStatus)
	}
}

func TestHealthMonitor_Uptime(t *testing.T) {
	g := &Gateway{lifecycle: NewLifecycle()}
	hm := NewHealthMonitor(g)

	// manually mock start time to be 5 seconds ago
	hm.startTime = time.Now().Add(-5 * time.Second)

	health := hm.Check()

	if health.Uptime != "5s" {
		t.Errorf("expected uptime 5s, got %s", health.Uptime)
	}
}
