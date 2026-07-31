# Request Routing Architecture

This document details the request routing flow for client subscriptions, based on a distributed gateway model where any gateway can receive a connection, but only a specific gateway owns the requested symbol.

## 1. The Routing Flow

```text
1. [Client]      -- Connects to Load Balancer --> [Gateway A]
2. [Client]      -- Sends: {"subscribe": "AAPL"} -> [Gateway A]
3. [Gateway A]   -- Computes PartitionFor("AAPL")
4. [Gateway A]   -- Checks topology cache: GatewayForPartition(42) == "Gateway B"
5. [Gateway A]   -- Scenario 1 (Redirection) or Scenario 2 (Proxy) -> [Gateway B]
6. [Gateway B]   -- Streams "AAPL" to Client
```

---

## 2. Redirection vs. Internal Proxying

When a client hits the "wrong" gateway, we have two distinct architectures for routing them to the correct one. 

### Approach A: Client-Side Redirection (Thick Client)
When `Gateway A` realizes it does not own `AAPL`, it refuses the subscription and sends a control message back to the client:
```json
{
  "type": "redirect",
  "symbol": "AAPL",
  "target": "wss://gateway-b.marketdata.internal"
}
```
**Pros**: 
- Completely removes `Gateway A` from the data path, reducing cluster-wide bandwidth.
- Ultimate low-latency once established (direct connection).
**Cons**: 
- Forces the client SDK to manage multiple WebSocket connections if they subscribe to symbols across different gateways.

### Approach B: Internal Gateway Proxying (Thin Client)
`Gateway A` acts as a transparent proxy. It opens an internal, high-speed connection to `Gateway B`, requests `AAPL`, and forwards the bytes back to the client.
**Pros**: 
- Client implementation remains trivial (one WebSocket handles everything).
**Cons**: 
- Double network hop (Client -> Gateway A -> Gateway B).
- Doubles bandwidth usage inside the VPC.

---

## 3. Caching (Topology)

To achieve extreme throughput, **a gateway must never query Redis/Control Plane in the hot path of a subscription request.**

1. **In-Memory Ring**: Each Gateway holds a complete `PartitionManager` and `ConsistentHashRing` in memory.
2. **Pub/Sub Updates**: The Gateway subscribes to the Control Plane (e.g., Redis Pub/Sub). When a node joins/leaves or an override is applied, the Control Plane broadcasts a `TopologyUpdate` event.
3. **Eventual Consistency**: The Gateway updates its in-memory ring asynchronously. Partition lookups (`GatewayForPartition(id)`) take < 150ns because they only hit local memory.

---

## 4. Latency Optimizations

1. **Smart Client SDK Caching**: If using Redirection, the Client SDK should cache the routing table locally. When the client subscribes to `AAPL` tomorrow, the SDK directly dials `Gateway B`, entirely bypassing the redirect hop.
2. **Internal Connection Pooling**: If using Internal Proxying, `Gateway A` maintains a persistent, multiplexed connection pool (e.g., gRPC streams or raw TCP with framed multiplexing) to all other gateways. It does not perform a TLS/TCP handshake per client subscription.
3. **Zero-Allocation Hashing**: The `PartitionManager` computes the route using the `hashing.HashBytes` function, resulting in zero heap allocations during the routing decision.

---

## 5. Failure Handling

### Scenario A: Stale Cache Misrouting
**Issue**: `Gateway A` thinks `Gateway B` owns `AAPL`, but `Gateway B` just crashed 50ms ago. `Gateway A` redirects the client to `B`.
**Resolution**: The client attempts to connect to `B` and fails. The client triggers its standard backoff-retry. By the time the client reconnects to `Gateway A`, `A`'s cache has converged via a Redis timeout, and it correctly redirects the client to `Gateway C`.

### Scenario B: Split Brain / Partition Transfer
**Issue**: `Gateway A` owns `AAPL`, but `Gateway B` joins and the ring says `Gateway B` now owns it. `A` and `B` might both try to publish temporarily.
**Resolution**: Gateways only process symbols if they hold a strict Redis lease for that partition. If `Gateway A` loses the lease, it actively drops all local client subscriptions to `AAPL` with a `reconnect` control message, forcing clients to re-resolve the route and land on `Gateway B`.
