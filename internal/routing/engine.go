package routing

import (
	"context"
	"sync"
	"time"

	"github.com/sumit/rtmds/internal/topology"
)

// Engine is the control plane daemon that monitors the Gateway Registry
// and maintains the routing topology. It automatically rebalances the consistent
// hash ring as gateways join or leave the cluster.
type Engine struct {
	mu           sync.RWMutex
	localID      string
	registry     topology.Registry
	partitionMgr *PartitionManager
	ring         *ConsistentHashRing

	nodeAddrs map[string]string // Maps Gateway ID to WebSocket Address

	pollInterval time.Duration
	stopCh       chan struct{}
}

// NewEngine creates a new rebalancing Engine.
func NewEngine(localID string, registry topology.Registry, pm *PartitionManager, pollInterval time.Duration) *Engine {
	return &Engine{
		localID:      localID,
		registry:     registry,
		partitionMgr: pm,
		ring:         pm.ring,
		nodeAddrs:    make(map[string]string),
		pollInterval: pollInterval,
		stopCh:       make(chan struct{}),
	}
}

// Start begins the automatic rebalancing daemon.
func (e *Engine) Start(ctx context.Context) {
	// Perform an initial synchronous load
	_ = e.syncTopology(ctx)

	go func() {
		ticker := time.NewTicker(e.pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				_ = e.syncTopology(ctx)
			case <-e.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Stop halts the rebalancing daemon.
func (e *Engine) Stop() {
	close(e.stopCh)
}

// syncTopology fetches the current active gateways from the Registry
// and updates the local Consistent Hash Ring.
func (e *Engine) syncTopology(ctx context.Context) error {
	nodes, err := e.registry.List(ctx)
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	activeNodes := make(map[string]bool)

	for _, n := range nodes {
		// Only consider gateways that are actively participating in routing
		if n.State == topology.StateOnline || n.State == topology.StateLeaving {
			activeNodes[n.ID] = true
			e.nodeAddrs[n.ID] = n.Address
			e.ring.AddNode(n.ID)
		}
	}

	// Evict offline/dead nodes from the ring to automatically reassign their partitions
	for id := range e.nodeAddrs {
		if !activeNodes[id] {
			e.ring.RemoveNode(id)
			delete(e.nodeAddrs, id)
		}
	}

	return nil
}

// RedirectTarget implements the websocket.Redirector interface.
// It computes the partition for a symbol, looks up the assigned gateway,
// and returns its full WebSocket URI if it's not the local gateway.
func (e *Engine) RedirectTarget(symbol string) string {
	owner := e.partitionMgr.GatewayForSymbol(symbol)

	// If no owner is found (e.g. ring is empty) or the owner is the local gateway,
	// do not redirect. Process the subscription locally.
	if owner == "" || owner == e.localID {
		return ""
	}

	e.mu.RLock()
	addr := e.nodeAddrs[owner]
	e.mu.RUnlock()

	if addr == "" {
		return "" // Fallback safely to local execution if address is somehow missing
	}

	return "ws://" + addr + "/ws"
}
