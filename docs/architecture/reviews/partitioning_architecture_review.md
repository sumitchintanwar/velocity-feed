# Partitioning Architecture Review
**Scale Assumptions:** 100 Gateways, 1,000,000 Symbols, 10,000,000 Clients

## 1. Routing & Network Traffic

**Current Design**: Client connects to a Gateway -> Requests Symbol -> Gateway computes Partition -> Sends Redirect -> Client connects to target Gateway.
**Evaluation**: Excellent for internal microservices, but extremely problematic for public clients.
**The Weakness (The N-Connection Problem)**: 
If a single user's UI dashboard displays 50 different stocks, the Consistent Hash Ring guarantees those 50 stocks will be distributed across ~40-50 different gateways. With client-side redirection, the client SDK must open **50 concurrent WebSocket connections** to 50 different IPs. 
Across 10 million clients, this results in **500 million active WebSocket connections** globally. The constant TCP keep-alives and TLS overhead alone would saturate the client's network and gateway firewalls.

> [!WARNING]
> **Production Improvement**: Switch to a **Stateless Edge Proxy / Internal Proxy** model for multi-symbol clients. A client opens exactly *one* connection to an Edge Proxy. The Proxy internally establishes persistent, multiplexed gRPC/TCP streams to all 100 Gateways and routes the subscriptions internally.

## 2. Hot Partitions (The AAPL Problem)

**Current Design**: `PartitionManager` assigns a symbol to exactly one partition, which is owned by exactly one Gateway.
**Evaluation**: This works perfectly if all 1,000,000 symbols have roughly equal popularity. They don't.
**The Weakness**: 
Suppose 5,000,000 of the 10,000,000 clients subscribe to `AAPL`. The hash ring assigns `AAPL` to Partition 42, owned by `Gateway-7`. `Gateway-7` must now maintain 5 million active connections and fan out every `AAPL` trade 5 million times. `Gateway-7`'s CPU and memory will instantly melt down, while `Gateway-8` sits idle hosting penny stocks.

> [!CAUTION]
> **Production Improvement**: **Dynamic Fan-out / Sub-Partitioning**.
> 1. The telemetry engine we built (`PartitionMetrics`) detects that `AAPL` throughput/clients exceeded a threshold.
> 2. The Control Plane promotes `AAPL` to a "Global/Broadcast" topic.
> 3. `Gateway-7` stops serving clients directly. Instead, it publishes `AAPL` to an internal message bus (e.g., Redis Pub/Sub, NATS, or Kafka).
> 4. ALL 100 Gateways subscribe to the internal bus and fan out `AAPL` to whatever clients happen to be connected to them. 

## 3. Rebalance Cost & Thundering Herds

**Current Design**: When a gateway joins/leaves, the `Engine` updates the ring. About 1/N partitions (10,000 symbols) move to the new node.
**Evaluation**: The math is highly efficient, but the physical reality of moving clients is dangerous.
**The Weakness**: 
If `Gateway-99` crashes, its 100,000 clients instantly disconnect. They all immediately hit the Load Balancer to reconnect. This is a classic **Thundering Herd**. Worse, if a *new* gateway joins, the existing gateways must forcibly drop the clients for the 10,000 symbols that moved, causing another thundering herd of reconnects to the new gateway.

> [!IMPORTANT]
> **Production Improvement**: 
> - **Graceful Draining**: When a node joins, old owners should send a "soft redirect" frame, allowing the client to connect to the new node *before* dropping the old connection.
> - **Jitter**: Gateways should forcefully disconnect moving clients over a randomized 30-60 second window to smooth out the reconnect spike.

## 4. Memory Estimates

**Current Design**: 10M clients / 100 gateways = 100,000 clients per gateway.
**Evaluation**: At ~32KB per idle WebSocket connection, 100,000 clients consume ~3.2GB of RAM. The Go runtime will easily handle this. However, if a garbage collection (GC) cycle triggers while fanning out a high-throughput event to 100k clients, Stop-The-World (STW) pauses could cause massive latency spikes.

> [!TIP]
> **Production Improvement**: 
> - Ensure all outbound JSON payloads are pre-encoded (already implemented via `CachedEvent`).
> - Tune `GOGC` (e.g., `GOGC=500` or `GOMEMLIMIT`) to delay GC cycles, trading excess RAM (which we have plenty of) for lower latency jitter.

## 5. Lookup Latency

**Current Design**: In-memory `ConsistentHashRing` backed by binary search (`sort.Search`).
**Evaluation**: At 250ns per lookup with 0 allocations, this is essentially perfect. 100 gateways * 100 vnodes = 10,000 array elements. A binary search takes ~13 operations.

> [!NOTE]
> **Production Improvement**: No architectural changes needed here. The routing engine's execution path is flawless. The focus must be shifted entirely to how we manage the network topology (Internal Proxies vs Redirection) and hot-key mitigation.
