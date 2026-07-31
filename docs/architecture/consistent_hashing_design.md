# Stateful Consistent Hashing Architecture

This document details the transition from randomized routing to a deterministic, stateful routing layer using Consistent Hashing. This architecture guarantees that specific market data symbols are strictly pinned to specific gateway instances, enabling immense horizontal scalability, high cache-hit ratios, and isolated failure domains.

---

## 1. Architectural Components

1. **Client / Edge Load Balancer**: A standard L4 load balancer (e.g., AWS NLB) that distributes incoming WebSocket connections randomly to the Routing Tier.
2. **Stateless Routing Tier (Ingress Proxies)**: A fleet of ultra-low latency proxies (or a lightweight Go layer). They hold a cached copy of the consistent hashing ring in memory.
3. **Stateful Gateway Tier**: The actual market data servers hosting the Write-Ahead Logs (WAL), snapshots, and live publishers.
4. **Topology Coordinator (Control Plane)**: A strongly consistent store (like Redis, etcd, or ZooKeeper) that tracks gateway health, leases, and the partition mapping table.

---

## 2. Partition & Virtual Node Design

To satisfy both even load distribution and future manual reassignments, we decouple the symbol from the physical node using a two-step mapping: **Symbol -> Partition -> Virtual Node -> Gateway**.

### Step 1: Fixed Partitions
Instead of hashing symbols directly onto the ring, the symbol space is divided into a fixed number of logical partitions (e.g., `1024`).
- **Hash Function**: `Murmur3` or `xxHash` (fast, non-cryptographic).
- **Mapping**: `PartitionID = hash(Symbol) % 1024`.

### Step 2: Virtual Nodes (VNodes)
To prevent "hot spots" (where one physical gateway accidentally receives a disproportionate chunk of the ring), we use Virtual Nodes.
- Each physical Gateway is assigned `N` virtual nodes (e.g., `N = 256`).
- **VNode IDs**: `hash(GatewayID + "#1")`, `hash(GatewayID + "#2")`, ..., `hash(GatewayID + "#N")`.
- These VNodes are placed onto a `0` to `2^32-1` hash ring.

### Step 3: Ring Resolution
- To find the owner of `Partition 42`, we calculate `hash("Partition-42")`.
- We walk clockwise on the ring to find the first VNode with an ID `>= hash("Partition-42")`.
- That VNode dictates the physical Gateway owner.

> [!TIP]
> This two-step process allows us to override the ring. If `Partition 42` is too hot, the Control Plane can inject a manual rule overriding the consistent hash for `Partition 42` to specifically route to `Gateway-X`.

---

## 3. Routing Flow

Because WebSockets are persistent, the routing decision only occurs at connection time.

1. **Connection Initiated**: Client connects to `Router-A`.
2. **Subscription Request**: Client sends `{"action": "subscribe", "symbols": ["AAPL", "MSFT"]}`.
3. **Partition Resolution**:
   - `AAPL` maps to `Partition 100` -> `Gateway-1`.
   - `MSFT` maps to `Partition 850` -> `Gateway-2`.
4. **Proxy Multiplexing**: 
   - `Router-A` opens internal TCP/WebSocket connections to both `Gateway-1` and `Gateway-2`.
   - It acts as a transparent multiplexer. Data flowing from `Gateway-1` and `Gateway-2` is merged into the single WebSocket connection back to the client.
5. **Zero-Copy Optimization**: To keep latency low, the Router uses `io.Copy` or `splice()` where possible to stream raw frames without decoding the JSON/Binary payload payload.

---

## 4. Gateway Lifecycle

- **Join**: 
  1. A new Gateway boots and registers with the Control Plane (e.g., setting a Redis key with a 5-second TTL).
  2. The Control Plane emits a `TopologyChanged` event.
  3. All Routers recalculate the ring. The new Gateway injects its 256 VNodes.
  4. ~1/N of the partitions naturally fall to the new Gateway.
- **Graceful Leave (Scale Down / Deploy)**: 
  1. Gateway marks itself as `Draining` in the Control Plane.
  2. The ring removes its VNodes. Partitions shift to neighboring Gateways on the ring.
  3. The leaving Gateway sends `GOAWAY` frames to connected Routers.
  4. Routers seamlessly reconnect to the new Gateway owners.
- **Crash Leave**: 
  1. The Gateway's TTL expires in the Control Plane.
  2. Routers detect the topology change, update the ring, and re-route disconnected clients upon reconnection.

---

## 5. Partition Lifecycle

A partition is not just a mathematical abstraction; it represents the actual ownership of WAL segments and Snapshots for a subset of symbols.

- `Unassigned`: No gateway currently owns the partition (startup state).
- `Assigning`: A gateway is booting up the WAL and loading snapshots from disk/S3 for this partition. Routers queue or block requests for this partition.
- `Active`: Gateway is actively serving traffic.
- `Draining`: Gateway is flushing the WAL to persistent storage and handing off ownership to another gateway.

---

## 6. Control Plane APIs

These APIs are exposed by the Control Plane / Routing Tier for cluster management:

```http
GET /v1/topology/ring
```
Returns the current VNode mapping and physical gateway addresses.

```http
GET /v1/topology/route?symbol=AAPL
```
Calculates and returns the active Gateway and Partition ID for the given symbol.

```http
POST /v1/topology/overrides
{
  "partition_id": 42,
  "target_gateway": "gateway-x-1",
  "reason": "hot_partition_isolation"
}
```
Manually pins a partition to a gateway, bypassing the consistent hash calculation.

---

## 7. Failure Scenarios

| Failure | Mitigation |
| :--- | :--- |
| **Gateway Crash** | Routers detect severed internal sockets. Clients aren't necessarily disconnected if the Router buffers. Router resolves the new Gateway for the partition based on the updated ring, requests replay from the WAL, and resumes the stream seamlessly. |
| **Router Crash** | Client WebSocket drops. Client naturally reconnects. External Load Balancer routes to a surviving Router. The new Router calculates the exact same Gateway mappings since the ring is shared. |
| **Control Plane Outage** | Routers cache the ring locally. Routing continues perfectly for existing Gateways. You just cannot add/remove Gateways until the Control Plane recovers. |
| **Network Partition (Split Brain)** | Use a consensus algorithm (Raft via etcd/ZooKeeper) or Redis Redlock to establish a cluster leader that dictates the definitive ring topology. |

---

## 8. Scalability Discussion (Billions of Messages)

1. **State Locality**: Because `AAPL` always goes to `Gateway-1`, `Gateway-1` only needs to maintain the WAL and snapshots for its assigned partitions. This means memory usage scales perfectly horizontally. A single gateway never holds the entire market state.
2. **O(1) Lookups**: Ring lookups via binary search (`sort.Search`) take `< 1 microsecond`. Calculating the target for millions of messages a second is mathematically trivial.
3. **No Thundering Herd**: When a node goes down, its partitions are distributed across *many* other nodes (thanks to VNodes), rather than dumping the entire load onto a single backup node.

---

## 9. Tradeoffs

> [!WARNING]
> **Thick Client vs. Proxy Overhead**
> The current design uses an intermediate Stateless Router. This adds one network hop (typically 100μs–1ms latency). 
> 
> *Alternative*: You can expose the Consistent Hashing Ring directly to the client SDKs (Thick Client). The client calculates the hash and connects directly to `Gateway-1` and `Gateway-2`. This removes the network hop (crucial for HFT), but forces clients to manage multiple websockets and topology updates themselves.

> [!CAUTION]
> **Cross-Partition Aggregation**
> If a client asks for "Top 10 most active symbols", a stateless router cannot easily compute this. Queries spanning multiple partitions require scatter-gather logic across the gateways, adding complexity to the Router.
