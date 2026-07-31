package websocket

import (
	"runtime"
	"time"
)

// HealthState represents the current health state of the Gateway.
type HealthState string

const (
	StateHealthy HealthState = "Healthy"
	StateWarning HealthState = "Warning"
	StateOffline HealthState = "Offline"
)

// GatewayHealth contains the health metrics for the Gateway.
type GatewayHealth struct {
	State            HealthState `json:"state"`
	Uptime           string      `json:"uptime"`
	CPUUsage         float64     `json:"cpu_usage"`
	MemoryUsage      uint64      `json:"memory_usage"`
	ActiveClients    int64       `json:"active_clients"`
	ActivePartitions int         `json:"active_partitions"`
	HeartbeatStatus  string      `json:"heartbeat_status"`
}

// HealthMonitor tracks and reports the health of the Gateway.
type HealthMonitor struct {
	gateway   *Gateway
	startTime time.Time
}

// NewHealthMonitor creates a new HealthMonitor for the given Gateway.
func NewHealthMonitor(g *Gateway) *HealthMonitor {
	return &HealthMonitor{
		gateway:   g,
		startTime: time.Now(),
	}
}

// Check returns the current health status of the Gateway.
func (hm *HealthMonitor) Check() GatewayHealth {
	uptime := time.Since(hm.startTime).Round(time.Second).String()

	// CPU usage placeholder
	cpuUsage := 0.0

	// Memory usage placeholder (using actual MemStats for realism)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	memoryUsage := m.Alloc

	// Active clients from the Gateway
	var activeClients int64
	if hm.gateway != nil {
		activeClients = hm.gateway.activeCount.Load()
	}

	// Active partitions placeholder
	activePartitions := 0

	// Heartbeat status
	hbStatus := "Stopped"
	if hm.gateway != nil && hm.gateway.heartbeat != nil {
		hbStatus = "Running"
	}

	// Determine overall state
	state := StateHealthy

	// If draining, we consider it Offline (not accepting new connections)
	if hm.gateway != nil {
		ls := hm.gateway.State()
		if ls == LifecycleDraining || ls == LifecycleOffline {
			state = StateOffline
		} else if activeClients > maxConnections*9/10 {
			state = StateWarning
		}
	} else {
		state = StateOffline
	}

	return GatewayHealth{
		State:            state,
		Uptime:           uptime,
		CPUUsage:         cpuUsage,
		MemoryUsage:      memoryUsage,
		ActiveClients:    activeClients,
		ActivePartitions: activePartitions,
		HeartbeatStatus:  hbStatus,
	}
}
