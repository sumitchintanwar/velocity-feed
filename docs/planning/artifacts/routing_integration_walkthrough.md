# Distributed Routing Integration

The distributed request routing architecture, utilizing consistent hashing and partition management, has been successfully integrated into the main `rtmds` application. 

## Integration Overview

The goal was to integrate the `routing` and `discovery` components into the main application lifecycle with minimal disruption to existing code, while allowing WebSockets to intelligently redirect clients.

### Key Changes
1. **`internal/app/app.go`**:
   - Initialized `routing.ConsistentHashRing` with 100 virtual nodes per gateway.
   - Initialized `routing.PartitionManager` mapping 25,600 partitions across the ring.
   - Implemented `discoveryRegistryAdapter` to bridge `discovery.Registry` outputs into the generic `topology.Registry` expected by the routing engine.
   - Initialized `routing.Engine` and integrated it into the application's startup and shutdown lifecycle.
   - Injected the `routing.Engine` into the `websocket.Gateway` via `SetRedirector()`.

2. **`internal/websocket/client.go` & `gateway.go`**:
   - Exposed the `SetRedirector` method to allow the `Engine` to dictate symbol ownership dynamically.
   - During client subscription, the `Redirector` is checked for ownership. If the symbol belongs to a different node, a `redirect` JSON control frame is sent to the client instead of executing the subscription locally.

3. **`internal/config/config.go`**:
   - Re-used `Server.GetGatewayID()` effectively utilizing the port assignment as the gateway identity for the hash ring.

### Verification Plan Executed
We successfully verified the integration using newly authored integration and stress tests in `internal/app/app_routing_test.go`:

1. **`TestRoutingIntegration_Redirect`**:
   - Spins up **Gateway A** and **Gateway B** inside a unified test cluster.
   - Allows topology discovery via an embedded `miniredis` registry.
   - Connects a mock WebSocket client to Gateway A.
   - Issues a subscription for a symbol owned by Gateway B.
   - **Verification**: Asserts that Gateway A gracefully responds with a `redirect` frame pointing to Gateway B's explicit WebSocket URL (`ws://127.0.0.1:8082/ws`).

2. **`TestRoutingIntegration_Stress`**:
   - Establishes 100 parallel WebSocket connections to a single gateway.
   - Blasts parallel subscriptions continuously in a tight loop.
   - Dynamically adds a second gateway mid-test to force the `routing.Engine` to trigger a partition rebalance event while processing active reads/writes.
   - **Verification**: Ensures that the `websocket.Gateway` safely handles routing updates concurrently without crashing or creating race conditions, dropping connections cleanly during partition handoffs.

> [!SUCCESS]
> Both tests pass successfully, confirming that the stateless routing tier, topology discovery, and consistent hash ring behave cohesively in a full integration environment!

*Note: Minor unrelated test failures exist in `internal/replay` and `internal/sequencer` resulting from an older partial implementation of the WAL refactoring task. Since this task strictly concerned the routing integration, these were intentionally kept isolated to maintain a minimal blast radius.*
