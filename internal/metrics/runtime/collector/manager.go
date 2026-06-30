// Package collector provides a centralized background engine for polling runtime statistics.
package collector

import (
	"context"
	"runtime"
	"time"

	"github.com/sumit/rtmds/internal/metrics/runtime/gc"
	"github.com/sumit/rtmds/internal/metrics/runtime/goroutine"
	"github.com/sumit/rtmds/internal/metrics/runtime/memory"
)

// Manager synchronizes background updates to runtime metrics without blocking application hot paths.
type Manager struct {
	memory    *memory.Metrics
	gc        *gc.Metrics
	goroutine *goroutine.Metrics
	interval  time.Duration
}

// NewManager creates a runtime poller.
func NewManager(m *memory.Metrics, g *gc.Metrics, gr *goroutine.Metrics, interval time.Duration) *Manager {
	return &Manager{
		memory:    m,
		gc:        g,
		goroutine: gr,
		interval:  interval,
	}
}

// Start launches the background goroutine to poll runtime statistics.
// It stops automatically when the context is cancelled.
func (mgr *Manager) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(mgr.interval)
		defer ticker.Stop()

		var stats runtime.MemStats

		// Initial poll
		mgr.poll(&stats)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				mgr.poll(&stats)
			}
		}
	}()
}

func (mgr *Manager) poll(stats *runtime.MemStats) {
	// ReadMemStats stops the world very briefly. 
	// Doing this in a low-frequency background loop is standard and safe.
	runtime.ReadMemStats(stats)

	mgr.memory.Update(stats)
	mgr.gc.Update(stats)
	mgr.goroutine.Update()
}
