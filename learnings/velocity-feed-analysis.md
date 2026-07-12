# Velocity Feed (RTMDS) — Project Analysis

---

## Project Name

**Velocity Feed** — Real-Time Market Data System (RTMDS)

---

## What problem does it solve?

### Who is the user?

| User | How They Use It |
|------|----------------|
| **Trading Desks** | Connect via WebSocket to receive live quotes for algorithmic/human trading |
| **Quant Researchers** | Use the Replay Engine to backtest strategies against historical data streams |
| **Platform Engineers** | Monitor via Prometheus/Grafana, operate via Admin API, run chaos experiments |
| **Developers** | Use the Go client SDK (`pkg/client/`) to build downstream applications |

### What pain point exists?

Financial market data must reach traders' screens in real-time. Existing systems either:

1. **Batch and poll** — introducing latency that costs money on every tick
2. **Use expensive licensed platforms** (Bloomberg, Refinitiv) — millions in annual fees for opaque black-box infrastructure
3. **Don't scale horizontally** — a single server becomes a bottleneck, and adding capacity means vertical upgrades (more RAM, faster CPU) rather than adding cheap commodity servers

The core pain: **sub-millisecond fan-out of normalized market quotes to thousands of concurrent WebSocket clients** is hard, expensive, and most solutions are closed-source.

### What happens without your solution?

- Traders see stale prices → bad trades → financial loss
- No crash recovery → reconnection means missing ticks → data gaps in charts
- Single point of failure → one server crash kills all clients
- No replay capability → quants can't backtest against real streaming data
- No observability → you don't know it's broken until traders complain

### Why did I build it?

- **Wanted to learn distributed systems** — Redis Pub/Sub, service discovery, horizontal scaling
- **Wanted hands-on backend experience** — Go concurrency, goroutines, channels, sync primitives
- **Wanted to understand event-driven architectures** — from adapter ingest to WebSocket delivery
- **Wanted to build something production-grade** — not a toy demo, but a system with health checks, metrics, tracing, chaos engineering, and Kubernetes deployment

Interviewers love this. It shows you didn't just follow a tutorial — you built a system that handles real failure modes.

---

## High-Level Architecture

### User → Delivery Path

```
                          ┌─────────────────────────┐
                          │     WebSocket Client     │
                          │  (Browser / Trading App) │
                          └────────────┬────────────┘
                                       │ WebSocket
                                       ▼
                          ┌─────────────────────────┐
                          │    Load Balancer (Nginx) │
                          │     least_conn routing   │
                          └────────────┬────────────┘
                                       │
                    ┌──────────────────┼──────────────────┐
                    │                  │                  │
              ┌─────▼─────┐     ┌─────▼─────┐     ┌─────▼─────┐
              │  Gateway 1 │     │  Gateway 2 │     │  Gateway N │
              │  (Primary) │     │(Subscriber)│     │(Subscriber)│
              └─────┬─────┘     └─────┬─────┘     └─────┬─────┘
                    │                  │                  │
                    │           ┌──────▼──────┐          │
                    │           │  Redis Bus  │◄─────────┘
                    │           │  (Pub/Sub)  │
                    │           └──────┬──────┘
                    │                  │
              ┌─────▼──────────────────▼─────┐
              │        API Layer (Chi)        │
              │  /ws  /health  /ready  /metrics│
              │  /replay  /gateways  /admin   │
              └─────────────┬────────────────┘
                            │
              ┌─────────────▼────────────────┐
              │     Business Logic Layer      │
              │                               │
              │  ┌─────────────┐ ┌──────────┐│
              │  │Topic Manager│ │ Feed     ││
              │  │ (RCU/lock-  │ │ Pipeline ││
              │  │  free)      │ │          ││
              │  └─────────────┘ └──────────┘│
              │  ┌─────────────┐ ┌──────────┐│
              │  │  Snapshot   │ │ Replay   ││
              │  │  Service    │ │ Engine   ││
              │  └─────────────┘ └──────────┘│
              │  ┌─────────────┐ ┌──────────┐│
              │  │  Sequencer  │ │ Worker   ││
              │  │ (per-symbol)│ │ Pool     ││
              │  └─────────────┘ └──────────┘│
              └─────────────┬────────────────┘
                            │
              ┌─────────────▼────────────────┐
              │         Storage Layer         │
              │                               │
              │  ┌──────────┐ ┌────────────┐ │
              │  │  Redis   │ │ PostgreSQL │ │
              │  │ (Pub/Sub │ │ (Event Log)│ │
              │  │  + Cache) │ │            │ │
              │  └──────────┘ └────────────┘ │
              └──────────────────────────────┘
```

### Queues

- **Redis Pub/Sub** — inter-gateway message bus (per-symbol channels: `market:AAPL`, `market:MSFT`)
- **Client Queue** — per-client bounded buffer (64 msgs, DropOldest, MaxAge 100ms)
- **Worker Pool** — bounded-concurrency queue decoupling feed pipeline from TopicManager
- **Redis Publisher Queue** — async worker pool with pipelining for non-blocking delivery

### Cache

- **Snapshot Service** — in-memory latest market state per symbol (serves catch-up on reconnect)
- **CachedEvent** — pre-encoded JSON bytes shared across all subscribers (encode once, send many)
- **sync.Pool** — reusable byte buffers across connections (~20MB savings at 5K connections)
- **Topic Map** — `sync.Map` for lock-free topic lookups

### Authentication

- **Admin API tokens** — role-based access (admin/operator/viewer)
- **WebSocket auth** — connect-time token validation
- **Docker secrets** — `_FILE` suffix for sensitive config values

### Storage

- **PostgreSQL** — append-only event log with cursor-based pagination for replay
- **Redis** — service discovery (gateway registration with TTL heartbeat), Pub/Sub channels

### External APIs

- **Exchange adapters** — pluggable interface for NASDAQ, NYSE, Crypto feeds
- **OpenTelemetry / Jaeger** — distributed tracing via OTLP export
- **Prometheus** — metrics scraping
- **Grafana** — dashboard visualization

---

## Tech Stack

### Why Go?

| Question | Answer |
|----------|--------|
| **Why Go?** | Goroutines give you 100K+ concurrent connections with minimal memory (~2KB each). Static binaries = no runtime dependencies. GC handles memory safety under load. Channels make concurrent data flow natural and race-condition-resistant. |
| **Why not Node.js?** | Node is single-threaded. You'd need worker threads or cluster mode for CPU work. JSON serialization is faster in Go (json-iterator). No static binaries — deployment requires Node runtime. Go's type system catches more bugs at compile time. |
| **Why not Java?** | JVM startup is slow (~2-5 seconds). Memory footprint is huge (heap tuning, GC pauses). Java is great for enterprise CRUD, but Go was designed for networked concurrent systems. The goroutine model is simpler than Java's ExecutorService + CompletableFuture dance. |
| **Why not Rust?** | Rust is faster and safer, but development speed matters for a summer project. Go's simplicity (no borrow checker, no lifetimes) means faster iteration. Go's ecosystem for web servers, Redis clients, and Prometheus is mature. Rust would be overkill here. |

### Why these libraries?

| Library | Why | Why not alternative |
|---------|-----|-------------------|
| **gorilla/websocket** | Battle-tested, buffer pool support, widely used in production | `nhooyr.io/websocket` is cleaner API but less battle-tested (kept as alternative in codebase) |
| **json-iterator/go** | 3-6x faster than encoding/json, zero-allocation mode | `encoding/json` is slower; `sonic` (bytedance) is newer and less stable |
| **Chi router** | Lightweight, stdlib-compatible, composable middleware | `gin` is faster but less composable; `echo` similar but Chi follows stdlib patterns |
| **Viper** | Config layering (defaults → YAML → env vars), strict key validation | `envconfig` is simpler but no YAML support; custom solution reinvents the wheel |
| **zerolog** | Zero-allocation structured logging | `zap` is also zero-alloc but zerolog has simpler API and smaller binary |
| **go-redis/v9** | Most popular Go Redis client, pipeline support, cluster-ready | `redigo` is lower-level; `rueidis` is newer but less ecosystem support |

### Why these infrastructure technologies?

| Technology | Why | Why not alternative |
|------------|-----|-------------------|
| **Redis 7.4** | Sub-millisecond Pub/Sub, TTL for service discovery, atomic operations | **Kafka** — overkill for fan-out; adds operational complexity. **RabbitMQ** — heavier, slower for this use case. **NATS** — great but smaller ecosystem. |
| **PostgreSQL 16** | ACID compliance for event log, JSONB for flexible schemas, cursor-based pagination | **MongoDB** — no ACID transactions; eventual consistency is risky for financial data. **MySQL** — weaker JSON support, no native Pub/Sub. **SQLite** — no concurrent writes. |
| **Nginx** | Least-conn load balancing, health checks, battle-tested at scale | **Traefik** — great for dynamic config but Nginx is simpler for static routing. **HAProxy** — more config-heavy. **Cloud LB** — adds vendor lock-in. |
| **Docker** | Multi-stage builds → 15MB images, reproducible deploys | **Podman** — similar but smaller ecosystem. **Nix** — steeper learning curve. |
| **Kubernetes (Kustomize)** | HPA, PDB, NetworkPolicy, industry standard | **Docker Swarm** — simpler but less features. **Nomad** — great but smaller community. **Manual deployment** — doesn't scale. |
| **Prometheus + Grafana** | Industry standard, 50+ custom metrics, alerting | **Datadog** — expensive. **InfluxDB** — less ecosystem. **ELK** — overkill for metrics. |

---

## Data Flow (Step-by-Step)

```
Step 1: Adapter connects to exchange/simulator
        └─ ExchangeAdapter.Connect() opens WebSocket/TCP to data source

Step 2: Raw messages arrive on adapter channel
        └─ adapter.Run() loops, reads frames, pushes RawMessage to channel

Step 3: Normalization Pipeline: Mapper + Validator → Quote
        └─ Exchange-specific format → normalized marketdata.Quote type

Step 4: Feed Pipeline reads Quote, assigns per-symbol sequence number
        └─ Sequencer ensures monotonic ordering per symbol

Step 5: Snapshot Service updates in-memory latest state
        └─ New subscribers get this snapshot before live streaming

Step 6: TopicManager.Publish():
        a. JSON-encode ONCE into CachedEvent (sync.Pool buffers)
        b. RCU swap: copy-on-write subscriber list (lock-free)
        c. Fan-out to all subscriber channels

Step 7: Client Queue (DropOldest, MaxAge 100ms) buffers for slow consumers
        └─ Prevents stale data accumulation

Step 8: Client.writePump() sends pre-encoded bytes (zero re-serialization)
        └─ writePreEncodedMessage() skips JSON encoding entirely

Step 9: WebSocket frame delivered to browser/client
        └─ Client receives {"symbol":"AAPL","price":150.25,"seq":42}
```

**Distributed path:** Step 6 → Redis Publisher (async worker pool) → Redis Pub/Sub channels → Subscriber Gateway → Local TopicManager → Steps 7-9

---

## Technical Challenges

| Challenge | What Went Wrong | How It Was Solved |
|-----------|----------------|-------------------|
| **Thundering herd on restart** | 10K clients reconnect simultaneously, overwhelming the server | Rate limiting (500 conn/sec), randomized shutdown delay (50-200ms), batch Redis SUBSCRIBE, EvictAll to force redistribution |
| **Zombie connections** | Client connected but receiving nothing after Redis failure | EvictAll() sends WebSocket close frame, forcing reconnect to healthy gateway |
| **Memory pressure at scale** | 10K connections × buffer allocation = OOM | sync.Pool for buffer reuse (~20MB savings), GOMEMLIMIT tuning, bounded client queues |
| **Goroutine leaks** | Goroutines stuck on channel sends after shutdown | Deep context.Context propagation, semaphore-based shutdown, read deadline tricks |
| **Metric cardinality explosion** | Labels by symbol/topic = millions of time series | Strict rules: NO labels by symbol, topic, client_id; bounded dimensions only |
| **Snapshot replay race** | Live events arrive during DB replay, silently lost | StartBuffering → LoadCheckpoint → Replay DB → StopBuffering → MarkReady |
| **Stale data detection** | Client sees frozen prices during Redis outage | 5-second stale threshold broadcasts system_degraded notices |
| **Config typo detection** | `REDIS_POTR=6380` silently fails | Strict key validation fails startup on unknown keys |

---

## Tradeoffs

| Decision | Chose | Alternative Considered | Reason |
|----------|-------|----------------------|--------|
| **Drop-on-full queues** | DropOldest with MaxAge | Block/backpressure | Latency-sensitive; stale data is worse than dropped data |
| **JSON serialization** | Pre-encode once (CachedEvent) | Per-client encode | O(N) savings; ~100x improvement at 1K subscribers |
| **Lock-free publish** | RCU + atomic.Pointer | Mutex-locked map | Publish is hot path; reads must be wait-free |
| **Redis Pub/Sub** | Per-symbol channels | Single firehose topic | Avoids single bottleneck; enables gateway-level demand routing |
| **WebSocket library** | gorilla/websocket | nhooyr.io/websocket | Maturity, buffer pool support; alternative kept in codebase |
| **Docker image base** | scratch/alpine | debian | 15MB images, zero OS CVEs |
| **Replay engine** | Virtual clock state machine | Wall-clock replay | Deterministic; supports speed control and seeking |
| **Config approach** | Viper (YAML + env) | Hardcoded / env-only | Layered config; strict validation; Docker secrets support |

---

## Scaling to 10x Traffic

| Dimension | Current | 10x Approach |
|-----------|---------|-------------|
| **Connections** | 10K per gateway | Add gateways (stateless horizontal scale); Nginx least_conn distributes |
| **Message throughput** | ~50K msg/sec | Per-symbol Redis channels distribute load; primary gateway only runs one feed |
| **Fan-out** | Single TopicManager | RCU lock-free publish scales linearly; sharded maps reduce contention |
| **Memory** | ~500MB per gateway | sync.Pool reuse; bounded client queues; GOMEMLIMIT tuning |
| **State** | Snapshot in-memory | Dual-checkpoint persistence survives restarts; checkpoint + DB replay for recovery |
| **Observability** | 50+ metrics | Prometheus remote write to Thanos/Mimir for long-term storage |
| **Redis** | Single instance | Redis Cluster with key-slot sharding for Pub/Sub |
| **PostgreSQL** | Single instance | Read replicas for replay queries; partitioned event_log table |

**Key insight:** Gateways are stateless — no global client state stored. Adding gateways is purely horizontal with no coordination overhead.

---

## Biggest Takeaways

1. **The hot path is everything.** One JSON encode per event (not per subscriber) and lock-free RCU publish are the two highest-impact optimizations.

2. **Drop, don't block.** In real-time systems, stale data is poison. DropOldest with MaxAge > blocking backpressure.

3. **Horizontal scaling requires stateless design.** Gateways hold no global state; Redis is the shared backbone; snapshots survive restarts via persistence.

4. **Observability is not optional.** 50+ metrics, distributed tracing, dynamic log levels, and chaos engineering are what make this production-grade rather than a demo.

5. **Recovery is a first-class feature.** Dual-checkpoint snapshots, event log replay, and the StartBuffering/StopBuffering race fix show that correctness under failure matters more than peak performance.

---
---

# STEP 2: Resume Bullet Deep-Dive — Interview Q&A

## How to Use This Section

For every resume bullet below, study:
1. **What it means** — explain it to a 10-year-old
2. **How the technology works** — internals
3. **Why this choice** — alternatives considered
4. **Failure modes** — what can break and how you handle it
5. **Scale** — numbers, limits, bottlenecks

---

## Bullet 1: "Designed and implemented a distributed, high-throughput real-time market data system in Go capable of handling 10,000+ concurrent WebSocket connections per gateway with sub-millisecond event delivery"

### What exactly does this mean?

I built a system that takes market data (stock prices, bids, asks) from exchanges and pushes it to thousands of traders' screens simultaneously via WebSocket connections. Each gateway server handles 10,000+ connections. The time from when a price changes at the exchange to when a trader sees it is under 1 millisecond.

### How does Go handle 10,000+ concurrent connections?

- **Goroutines:** Lightweight threads (~2KB stack each). 10K goroutines = ~20MB. Compare to Java threads (~1MB each) = 10GB for same count.
- **Scheduler:** Go runtime multiplexes goroutines onto OS threads (M:N scheduling). You don't manage threads — Go does.
- **Channels:** Type-safe communication between goroutines. Replaces shared-memory + locks.
- **netpoller:** Go's event loop integrates with epoll (Linux) / kqueue (macOS) under the hood. Each connection is non-blocking but coded as blocking.

### Why not Node.js for WebSocket handling?

| Factor | Go | Node.js |
|--------|-----|---------|
| Concurrency model | Multi-threaded goroutines | Single-threaded event loop |
| CPU-bound work | Parallel across cores | Blocks event loop |
| Memory per connection | ~2KB (goroutine) | ~50KB (V8 heap per socket) |
| Binary deployment | Static binary, ~15MB | Requires Node runtime |
| JSON performance | json-iterator: ~3x faster | V8 JSON is fast but no zero-alloc mode |

Node handles I/O well but falls behind when you need CPU work (normalization, aggregation) or massive connection counts.

### Why not Java?

- JVM startup: 2-5 seconds vs Go's ~10ms
- Memory: JVM heap tuning required (G1GC, ZGC), baseline ~200MB vs Go's ~10MB
- Thread model: Java threads are OS threads (~1MB stack). You'd use virtual threads (Project Loom) or Netty for similar concurrency, but that adds complexity.
- Go was literally designed for this use case — networked services with many concurrent connections.

### What is sub-millisecond delivery?

The P99 latency from feed ingestion to WebSocket frame send is under 1ms. This is achieved by:
1. Pre-encoding JSON once (CachedEvent) — no per-client encoding
2. Lock-free RCU publish — no mutex contention on hot path
3. Zero-copy sends — write the same pre-encoded bytes to all clients
4. No system calls on publish path — only the final WebSocket write does a syscall

### What scale can it handle?

- **Per gateway:** 10K connections, ~50K msg/sec
- **Horizontally:** Add gateways (stateless), Nginx distributes with least_conn
- **Bottleneck:** Network bandwidth (each msg ~200 bytes × 50K = 10MB/sec per gateway)
- **Tested:** Load tests with 10K connections, benchmarks with profiling

### 10 Follow-Up Questions

1. **Q: How do you handle 10K simultaneous WebSocket connections without running out of file descriptors?**
   A: Each WebSocket connection = 1 TCP socket = 1 file descriptor. Linux default ulimit is 1024. We set `ulimit -n 65535` and use `sysctl net.core.somaxconn`. The application layer doesn't need to think about this — Go's netpoller handles it.

2. **Q: What happens when a client reads slower than you write?**
   A: Per-client bounded queue with DropOldest policy. If the queue is full, the oldest event is dropped. This prevents slow consumers from blocking fast ones. The MaxAge (100ms) also drops stale events.

3. **Q: How do you achieve sub-millisecond latency? What's actually measured?**
   A: We measure time from `Feed.Quote` event creation to `WebSocket.WriteMessage` call. The path is: Feed → Sequencer → Snapshot → TopicManager → ClientQueue → writePump. Each step is in-memory, no syscalls. The only syscall is the final WebSocket frame write.

4. **Q: Why not use epoll directly instead of Go's netpoller?**
   A: Go's netpoller IS epoll under the hood. You don't need to manage it manually. Go's runtime integrates epoll with goroutine scheduling — when a socket is ready, the goroutine is woken up. Direct epoll would mean managing callbacks, state machines, and losing Go's clean blocking syntax.

5. **Q: How do you handle goroutine leaks?**
   A: Deep context.Context propagation from root to every goroutine. ReadPump has a read deadline (60s). WritePump has a write deadline (5s). Both check `ctx.Done()`. On shutdown, `EvictAll()` sends close frames and waits with timeout before force-closing.

6. **Q: What's the memory model for 10K connections?**
   A: Per-connection: goroutine stack (~2-8KB, grows as needed), read buffer (~4KB), write buffer (~4KB). Plus client queue (64 msgs × ~200 bytes = ~12KB). Total: ~30KB per connection. 10K connections = ~300MB. Plus shared state (snapshots, topic maps) = ~500MB total.

7. **Q: How does Go's garbage collector handle this workload?**
   A: Go's GC is concurrent and low-latency (~ sub-millisecond pauses). We minimize GC pressure with: sync.Pool for buffer reuse, pre-encoded JSON (CachedEvent), bounded queues, and `GOMEMLIMIT` to tune the GC trigger threshold.

8. **Q: Why not use HTTP/2 instead of WebSocket?**
   A: WebSocket is full-duplex — server can push without client requesting. HTTP/2 is request-response (server push was removed in the spec). For real-time market data, the server needs to push events immediately, not wait for client polls.

9. **Q: How do you handle connection storms (thundering herd)?**
   A: Token-bucket rate limiter (500 new connections/sec). Randomized shutdown delay (50-200ms) to stagger reconnections. Batch Redis SUBSCRIBE to avoid per-connection setup overhead.

10. **Q: What's the throughput ceiling?**
    A: Theoretical: limited by network bandwidth and JSON serialization speed. Practical: ~50K msg/sec per gateway with 10K connections. At 100K msg/sec, you'd need to shard by symbol across gateways or use binary protocols (FlatBuffers, Protobuf).

---

## Bullet 2: "Built a pluggable Exchange Adapter Framework with automatic normalization, enabling seamless integration of multiple market data providers (NASDAQ, NYSE, Crypto) through a single canonical interface"

### What exactly does this mean?

Each exchange (NASDAQ, NYSE, crypto) sends data in different formats. I built a framework where you implement one interface (`ExchangeAdapter`) and the system automatically converts exchange-specific data into a common format (`Quote`). Adding a new exchange = writing one adapter file.

### How does the adapter pattern work?

```
ExchangeAdapter interface:
  Connect(ctx) error
  Disconnect() error
  Run(ctx) <-chan RawMessage
  Map(raw RawMessage) (Quote, error)  // normalization
  Validate(q Quote) error            // validation
```

- **Registry:** Adapters register themselves by name (e.g., "nasdaq", "crypto")
- **Manager:** Looks up adapter by name from config, manages lifecycle
- **Pipeline:** Feed → Adapter.Run() → Mapper → Validator → Quote → Sequencer → ...

### Why a pluggable framework instead of hardcoded adapters?

- **Open/Closed Principle:** Add new exchanges without modifying core code
- **Testability:** Mock adapters for unit tests
- **Config-driven:** Switch from simulator to NASDAQ via config change, not code change

### Why not just hardcode NASDAQ?

Because you'd need to modify the feed pipeline every time you add an exchange. The adapter framework means the core system never changes — only new adapter files are added.

### How does normalization work?

Exchange A sends: `{"sym": "AAPL", "bid": 150.25, "ask": 150.30, "ts": 1234567890}`
Exchange B sends: `{"symbol": "AAPL", "bid_price": 150.25, "ask_price": 150.30, "timestamp": 1234567890}`

Both map to: `Quote{Symbol: "AAPL", Bid: 150.25, Ask: 150.30, Timestamp: ...}`

The `Mapper` interface handles the exchange-specific → canonical mapping.

### What scale can it handle?

- The adapter itself is not the bottleneck — it's the network connection to the exchange
- Normalization is CPU-bound: ~1μs per quote with json-iterator
- The pipeline is async: adapter goroutine → channel → pipeline goroutine

### 10 Follow-Up Questions

1. **Q: What's the difference between an adapter and a mapper?**
   A: Adapter = connects to the exchange, reads raw bytes. Mapper = converts raw bytes to canonical Quote. They're separate concerns. The adapter handles networking; the mapper handles format translation.

2. **Q: How do you handle exchange API changes?**
   A: The adapter encapsulates exchange-specific logic. If NASDAQ changes their format, only the NASDAQ adapter needs updating. The rest of the system is unaffected.

3. **Q: What happens if an adapter crashes?**
   A: The adapter's `Run()` channel closes. The feed pipeline detects this and can restart the adapter or switch to a fallback. The system continues operating with remaining adapters.

4. **Q: How do you test adapters without a live exchange?**
   A: The simulator adapter generates fake market data with geometric Brownian motion. It's used in development and testing. You can also write a mock adapter implementing the same interface.

5. **Q: Why not use protobuf for normalization instead of JSON?**
   A: JSON is human-readable and debugging is easier. Protobuf would be faster (~2x) but adds schema management complexity. For this project, JSON performance was sufficient. The hot path uses pre-encoded JSON (CachedEvent) anyway.

6. **Q: How do you handle different exchange protocols (WebSocket vs FIX vs TCP)?**
   A: The `ExchangeAdapter` interface is protocol-agnostic. Each adapter implements its own connection logic. NASDAQ adapter uses WebSocket, FIX adapter would use TCP sockets, crypto adapters use REST polling or WebSocket.

7. **Q: What's the validation step?**
   A: `Validator.Check(q Quote)` ensures the quote is sane: price > 0, timestamp not in future, symbol not empty, bid < ask. Invalid quotes are logged and dropped, not propagated.

8. **Q: How do you handle out-of-order messages from exchanges?**
   A: The Sequencer assigns per-symbol monotonically increasing sequence numbers. If a message arrives with a lower sequence number, it's dropped as stale. This happens at the pipeline level, not the adapter.

9. **Q: Can you run multiple adapters simultaneously?**
   A: Yes. Each adapter runs in its own goroutine. Their outputs merge into the feed pipeline. For example, you could run simulator + NASDAQ simultaneously for testing.

10. **Q: How does the adapter handle authentication to exchanges?**
    A: Exchange credentials are in config (Viper). The adapter reads them during `Connect()`. Docker secrets (`_FILE` suffix) keep credentials out of config files and environment variables.

---

## Bullet 3: "Implemented Redis Pub/Sub-based horizontal scaling with per-symbol channel routing, dynamic subscriptions, and service discovery — gateways only subscribe to channels their local clients need"

### What exactly does this mean?

When you have multiple gateway servers, they need to share market data. Instead of each gateway having its own feed, one primary gateway publishes to Redis, and all other gateways subscribe. The key innovation: gateways only subscribe to channels for symbols their connected clients actually want, avoiding a single data firehose.

### How does Redis Pub/Sub work?

- **Publisher:** `PUBLISH market:AAPL {...}` — sends message to all subscribers of that channel
- **Subscriber:** `SUBSCRIBE market:AAPL market:MSFT` — listens on specific channels
- **Channels:** Per-symbol: `market:AAPL`, `market:TSLA`, etc.
- **Delivery:** At-most-once (fire and forget). No persistence, no acknowledgment.

### Why per-symbol channels instead of one big channel?

| Approach | Pros | Cons |
|----------|------|------|
| Single channel (`market:all`) | Simple | Every gateway gets every message, even if no local clients want it |
| Per-symbol channels (`market:AAPL`) | Only subscribe to what you need | More channels, but Redis handles millions efficiently |

Per-symbol channels enable **demand-based routing** — a gateway with only AAPL subscribers only subscribes to `market:AAPL`.

### Why not Kafka?

| Factor | Redis Pub/Sub | Kafka |
|--------|---------------|-------|
| Latency | Sub-millisecond | Milliseconds (batch-oriented) |
| Throughput | ~500K msg/sec | Millions msg/sec |
| Persistence | None (at-most-once) | Configurable (at-least-once) |
| Complexity | Simple (one binary) | Complex (brokers, zookeeper, partitions) |
| Use case | Real-time fan-out | Event streaming, log aggregation |

For this system, latency matters more than persistence. Redis Pub/Sub delivers in microseconds. Kafka adds latency for durability guarantees we don't need (the feed is continuous — missing one tick is fine if the next arrives in <1ms).

### Why not RabbitMQ?

- RabbitMQ is a message broker with routing, queuing, acknowledgments
- Redis Pub/Sub is simpler — no queues, no acks, no routing keys
- For real-time fan-out, the simplicity of Redis Pub/Sub wins
- RabbitMQ adds operational overhead (Erlang runtime, management UI, vhosts)

### What is a Redis channel?

A named pub/sub endpoint. `market:AAPL` is a channel. Publishers write to it, subscribers read from it. Redis maintains the subscriber list in memory. Messages are not stored — they're delivered to current subscribers and discarded.

### What happens if a Redis broker dies?

- Redis Pub/Sub is at-most-once: messages during the outage are lost
- The system handles this via: Snapshot Service (latest state), Event Log (PostgreSQL), and stale detection (5-second threshold)
- Redis Sentinel or Redis Cluster provides HA with automatic failover
- Our service discovery uses Redis with TTL heartbeat — if Redis dies, gateways can't register but can still serve local clients

### What scale can it handle?

- Redis Pub/Sub: ~500K msg/sec on a single instance
- Per-symbol channels: Redis internally uses a hashtable, O(1) publish
- Network: Each message ~200 bytes × 500K = 100MB/sec (network bound)
- Horizontal: Add Redis Cluster nodes for sharding

### 10 Follow-Up Questions

1. **Q: How does a gateway know which channels to subscribe to?**
   A: When a client subscribes to a symbol, the gateway issues `SUBSCRIBE market:SYMBOL` to Redis. When all clients unsubscribe from a symbol, the gateway issues `UNSUBSCRIBE market:SYMBOL`. This is dynamic — subscriptions change as clients connect/disconnect.

2. **Q: What's the difference between Redis Pub/Sub and Redis Streams?**
   A: Pub/Sub is at-most-once (fire and forget). Streams are at-least-once with consumer groups, persistence, and acknowledgments. Streams add latency and complexity. For real-time fan-out, Pub/Sub's simplicity wins.

3. **Q: How do you handle Redis connection failures?**
   A: The Redis subscriber has automatic reconnection with exponential backoff. On reconnect, it re-subscribes to all active channels. During the gap, clients receive stale data (detected by 5-second threshold).

4. **Q: How does service discovery work with Redis?**
   A: Each gateway registers with `SET gateway:ID ... EX 30` (30-second TTL). It re-registers every 15 seconds (heartbeat). If a gateway dies, its key expires and other gateways stop routing to it.

5. **Q: What happens if two gateways publish to the same symbol?**
   A: Only one gateway runs the feed (primary). Other gateways are subscribers only. The primary is elected via service discovery — the first to register wins.

6. **Q: How do you serialize market data for Redis?**
   A: Pre-encoded JSON (CachedEvent). The publisher sends the same bytes to Redis that will be sent to clients. No re-serialization. Redis stores and forwards the raw bytes.

7. **Q: Can Redis handle 10K symbols with per-symbol channels?**
   A: Yes. Redis uses a hashtable for channels — O(1) publish and subscribe. 10K channels is trivial. The limit is network bandwidth, not channel count.

8. **Q: What's the failure mode if Redis memory fills up?**
   A: Pub/Sub messages are not stored in memory — they're delivered and discarded. Memory usage is proportional to subscriber count, not message count. Redis won't OOM from Pub/Sub alone.

9. **Q: How do you handle Redis Cluster vs single Redis?**
   A: In development, single Redis. In production, Redis Sentinel for HA. For 10x scale, Redis Cluster with hash-slot sharding across Pub/Sub channels.

10. **Q: Why not use TCP multicast instead of Redis?**
    A: Multicast is great for LAN but doesn't work across subnets/VLANs. It requires IGMP, doesn't work in cloud environments (AWS, GCP), and has no built-in flow control. Redis works everywhere.

---

## Bullet 4: "Engineered a backpressure-aware client delivery system with three configurable policies (DropOldest, DropNewest, Disconnect), ring buffers, and slow consumer detection to prevent one slow client from affecting others"

### What exactly does this mean?

When a client can't keep up with the data stream, the system has policies to handle it. Instead of letting slow clients block fast ones (head-of-line blocking), we strategically drop messages. Three options: drop oldest (keep newest), drop newest (keep buffer as-is), or disconnect the slow client entirely.

### How does backpressure work?

```
Event arrives for client →
  if client queue is full:
    DropOldest: remove oldest event, push new one
    DropNewest: discard new event, keep buffer as-is
    Disconnect: send WebSocket close frame, clean up
```

### What is a ring buffer?

A circular buffer with fixed capacity. When full, new writes overwrite old data. In Go:

```go
type RingBuffer struct {
    buf   []Event
    head  int
    tail  int
    count int
    size  int
}
```

O(1) push, O(1) pop, no memory allocation after creation.

### Why not use blocking (channel full → block publisher)?

- **Head-of-line blocking:** One slow client blocks the publisher, slowing ALL clients
- **Latency spike:** Blocking introduces variable latency
- **Cascade failure:** If one client is slow, it can slow the entire pipeline

In real-time systems, **dropping is better than blocking**. A dropped message is replaced by the next one within milliseconds. A blocked publisher causes latency spikes across all clients.

### Why DropOldest instead of DropNewest?

- **DropOldest:** Client always sees the latest data. Older data is less valuable in real-time.
- **DropNewest:** Client sees stale data. Newer events are lost.
- **Trade-off:** DropOldest is better for trading (latest price matters). DropNewest might be better for audit trails (never miss an event).

### What is MaxAge?

An event is dropped if it's older than MaxAge (100ms). Even if the buffer isn't full, stale events are purged. This prevents:
- Buffer filling with old events during a temporary network blip
- Client seeing data that's 5 seconds old after reconnecting

### What scale can it handle?

- Per-client queue: 64 events (configurable)
- Drop rate: tracked per client, alerts if > threshold
- Detection: if a client hasn't acked in 5 seconds, marked as slow
- Disconnect: automatic after MaxConsecutiveDrops (configurable)

### 10 Follow-Up Questions

1. **Q: How do you detect a slow consumer?**
   A: Track the time since the client's last successful WebSocket write. If it exceeds the stale threshold (5 seconds), mark as slow. Also track drop rate — if drops exceed a threshold, proactively disconnect.

2. **Q: What happens after a client is disconnected?**
   A: Send WebSocket close frame (code 1008, policy violation). Client receives the close, cleans up its side, and can reconnect. On reconnect, it gets a snapshot (latest state) before live streaming.

3. **Q: Why 64 events per client queue?**
   A: Tuned for the use case. 64 events × 200 bytes = ~12KB per client. At 50K msg/sec across 10K clients, each client gets ~5 msg/sec. 64 events = ~12 seconds of buffer. Enough to absorb brief network glitches without wasting memory.

4. **Q: How do you handle a client that connects, subscribes, but never reads?**
   A: The writePump will fail to send after a few events (queue fills). DropOldest keeps trying. After MaxConsecutiveDrops, the client is disconnected. The readPump's read deadline (60s) also detects dead connections.

5. **Q: What's the difference between backpressure and flow control?**
   A: Flow control = receiver tells sender to slow down (TCP sliding window). Backpressure = sender detects receiver is slow and takes action (drop, buffer, disconnect). WebSocket has no built-in flow control, so we implement backpressure at the application layer.

6. **Q: How do you handle burst traffic (e.g., market open)?**
   A: The ring buffer absorbs brief bursts. Rate limiter on new connections prevents connection storms. Snapshot service ensures clients get current state on reconnect. The system is designed for continuous flow, not request-response.

7. **Q: Why not use TCP flow control?**
   A: TCP flow control prevents the sender from overwhelming the receiver's kernel buffer. But the application still needs to handle the case where the kernel buffer is full (write blocks). Our backpressure operates at the application level, above TCP.

8. **Q: How do you track drop metrics?**
   A: Per-client Prometheus counter: `rtmds_client_drops_total{client_id, policy}`. Aggregated: `rtmds_total_drops_per_second`. Alert if drop rate exceeds threshold.

9. **Q: What's the memory impact of 10K clients × 64 events?**
   A: 10K × 64 × 200 bytes = ~1.28GB. But with sync.Pool and CachedEvent, the actual memory is much lower because events are shared (same pre-encoded bytes for all subscribers of a symbol).

10. **Q: Can you dynamically change the backpressure policy?**
    A: Yes, via config. Change `client_queue.policy` and the system applies it to new connections. Existing connections continue with their current policy until they reconnect.

---

## Bullet 5: "Created a snapshot service with dual-checkpoint rotation and live-event buffering that guarantees zero event loss during crash recovery, enabling instant catch-up for newly connected clients"

### What exactly does this mean?

The snapshot service keeps the latest market state (last bid, ask, volume for each symbol) in memory. Two things:
1. **New clients** get the snapshot immediately (instant catch-up), then switch to live streaming
2. **Crash recovery** uses dual-checkpoint rotation: save state to disk periodically, and during recovery, buffer live events that arrive while replaying from the checkpoint

### How does the snapshot service work?

```
On every Quote event:
  1. Update in-memory state: snapshot[symbol] = quote
  2. Notify all waiting subscribers

On new client connection:
  1. Client subscribes to symbols
  2. Snapshot service sends latest state for each symbol
  3. Client then receives live events
```

### What is dual-checkpoint rotation?

```
Checkpoint A (current) ← live events written here
Checkpoint B (previous) ← last saved state

Rotation:
  1. Save current state to Checkpoint B
  2. Swap: A becomes new checkpoint, B becomes previous
  3. This ensures at least one valid checkpoint always exists
```

### What is live-event buffering during recovery?

Problem: During crash recovery, the system is replaying events from the database. But new live events are also arriving. If you process live events before finishing replay, you might miss events.

Solution:
1. StartBuffering: Mark that live events should be queued, not processed
2. LoadCheckpoint: Load last saved state from disk
3. Replay DB: Replay events from the event log after the checkpoint timestamp
4. StopBuffering: Process all buffered live events
5. MarkReady: System is now live

This guarantees no events are lost between checkpoint and crash.

### Why not just replay the entire event log?

- Event log can be huge (millions of events)
- Replay takes time (seconds to minutes)
- During replay, clients can't connect
- Dual-checkpoint: replay only events since last checkpoint (seconds of data)

### What scale can it handle?

- Snapshot in-memory: ~100 bytes per symbol × 10K symbols = ~1MB
- Checkpoint to disk: < 1ms for 10K symbols (atomic write)
- Recovery: ~1-2 seconds for checkpoint + replay of recent events

### 10 Follow-Up Questions

1. **Q: What happens if the checkpoint file is corrupted?**
   A: Fall back to the previous checkpoint (that's why we have dual-checkpoint). If both are corrupted, replay from the beginning of the event log. The system always has a recovery path.

2. **Q: How do you handle symbols that go inactive?**
   A: TTL-based eviction. If no event for a symbol in 30 minutes (configurable), remove it from the snapshot map. This prevents unbounded memory growth.

3. **Q: What's the difference between a snapshot and the event log?**
   A: Snapshot = latest state (one entry per symbol). Event log = history of all events. Snapshot is for fast catch-up. Event log is for replay/backtesting.

4. **Q: How do you handle snapshot delivery to new clients?**
   A: When a client subscribes, the snapshot service iterates the client's symbols and sends the latest quote for each. This is synchronous — the client gets a burst of snapshots, then transitions to live streaming.

5. **Q: What's the consistency model?**
   A: Eventually consistent. The snapshot might be slightly behind the live feed (by the time it takes to update). For trading, this is acceptable because the live feed immediately follows.

6. **Q: How do you handle concurrent access to the snapshot map?**
   A: `sync.RWMutex` per symbol shard. Reads (snapshot delivery) are lock-free via RCU. Writes (updates) take a write lock. Sharding reduces contention.

7. **Q: What happens if the system crashes during checkpoint rotation?**
   A: The previous checkpoint is still valid. On restart, load the previous checkpoint and replay events since its timestamp. The dual-checkpoint design ensures at least one valid checkpoint survives.

8. **Q: How do you test crash recovery?**
   A: Chaos engineering framework injects SIGKILL at random points. Automated validation checks that the system recovers to the correct state. Soak tests run for hours with periodic crashes.

9. **Q: Why not use Redis for snapshots?**
   A: Redis adds network latency (~0.5ms). In-memory snapshots are sub-microsecond. The snapshot service is on the hot path — every event updates it. Redis would be a bottleneck.

10. **Q: How do you handle snapshot consistency across multiple gateways?**
    A: Each gateway has its own snapshot service. The primary gateway's snapshot is authoritative. Subscriber gateways get snapshots from Redis (primary publishes them). On reconnect, clients always get a fresh snapshot.

---

## Bullet 6: "Implemented OpenTelemetry distributed tracing with W3C trace context propagation across Redis boundaries, creating end-to-end traces from feed ingestion to WebSocket delivery"

### What exactly does this mean?

Every market data event gets a unique trace ID. When the event passes through different services (feed → Redis → gateway → WebSocket), the trace context is propagated. This lets you see the full journey of a single event across all components in Jaeger.

### How does distributed tracing work?

```
1. Feed creates span: "feed.ingest" (trace-id: abc123)
2. Redis Publisher creates span: "redis.publish" (parent: abc123)
3. Redis Subscriber creates span: "redis.subscribe" (parent: abc123)
4. Gateway creates span: "websocket.deliver" (parent: abc123)

Result: Full trace showing latency at each step
```

### What is W3C Trace Context?

A standard header: `traceparent: 00-abc123-span456-01`

- `00`: version
- `abc123`: trace ID (unique per event)
- `span456`: parent span ID
- `01`: trace flags (sampled)

This header is propagated through Redis Pub/Sub messages.

### Why not just log everything?

- Traces show relationships between services (parent-child)
- Logs are per-service; traces are cross-service
- Traces show latency breakdown (how long in Redis vs gateway)
- Traces enable identifying bottlenecks in the pipeline

### Why not Jaeger directly instead of OpenTelemetry?

- OpenTelemetry is the vendor-neutral standard
- Jaeger is one backend; you might switch to Zipkin, Datadog, or Grafana Tempo
- OpenTelemetry SDK instruments once, exports to any backend

### What scale can it handle?

- Sampling: 1% of traces in production (configurable)
- Span creation: ~1μs overhead
- Storage: Jaeger with Elasticsearch/Cassandra backend
- Network: Trace context is ~50 bytes per message (negligible)

### 10 Follow-Up Questions

1. **Q: How do you propagate trace context across Redis Pub/Sub?**
   A: The trace context is serialized into the Redis message payload. The subscriber deserializes it and continues the trace. This creates a cross-process trace spanning primary → Redis → subscriber gateways.

2. **Q: What's the performance overhead of tracing?**
   A: ~1μs per span creation. With 1% sampling, only 1 in 100 events creates a full trace. The overhead is negligible compared to network latency.

3. **Q: How do you decide which traces to sample?**
   A: Configurable sampling rate. In development: 100%. In production: 1% (or head-based sampling: always sample errors, 1% of success).

4. **Q: What information does each span contain?**
   A: Operation name, duration, start time, parent span ID, tags (symbol, gateway ID, client count), and status (OK/ERROR).

5. **Q: How do you trace WebSocket delivery?**
   A: The trace starts at feed ingestion and ends at WebSocket.WriteMessage. Each step (normalize, sequence, snapshot, publish, deliver) is a child span. The final span shows the total end-to-end latency.

6. **Q: Why not use correlation IDs instead of distributed tracing?**
   A: Correlation IDs link log lines. Traces link spans with timing data. Traces show latency breakdown, not just "this event was processed by service X."

7. **Q: How do you handle trace context when a gateway restarts?**
   A: The trace is lost for in-flight events. But the snapshot service ensures clients get current state on reconnect. New events get new traces. The system is designed for graceful degradation.

8. **Q: What's the storage requirement for traces?**
   A: ~1KB per span. At 50K msg/sec with 1% sampling = 500 traces/sec × 5 spans × 1KB = ~2.5MB/sec. Jaeger with Elasticsearch handles this easily.

9. **Q: How do you visualize traces?**
   A: Jaeger UI shows the full trace tree: feed → Redis → gateway → WebSocket. Each span shows latency. You can compare traces over time to identify performance regressions.

10. **Q: Can you trace individual symbols?**
    A: Yes. Add a tag `symbol=AAPL` to spans. In Jaeger, filter by symbol to see the latency for AAPL events specifically. This helps identify if one symbol is causing issues.

---

## Bullet 7: "Built a chaos engineering framework with automated fault injection, validation, and teardown for testing system resilience"

### What exactly does this mean?

A framework that deliberately breaks things (kills processes, injects network delays, fills disks) and automatically checks that the system recovers correctly. This is how you prove the system works under failure, not just under ideal conditions.

### How does chaos engineering work?

```
1. Experiment definition:
   - Target: Redis, PostgreSQL, Gateway
   - Fault: kill, network partition, CPU stress
   - Duration: 30 seconds
   - Validation: health checks pass within 60 seconds after

2. Inject fault:
   - Kill Redis process
   - Or: add 200ms network latency
   - Or: fill disk to 95%

3. Observe:
   - Do health checks fail?
   - Do clients reconnect?
   - Does the system degrade gracefully?

4. Validate:
   - After fault injection, check system state
   - Metrics should recover within expected time
   - No data corruption

5. Teardown:
   - Restore original state
   - Kill injected processes
   - Remove network rules
```

### Why not just write unit tests?

- Unit tests verify individual components work
- Chaos experiments verify the system works when components FAIL
- Real failures are unpredictable — chaos experiments simulate them
- You can't test "Redis dies" with a unit test

### What faults can be injected?

- **Process kill:** SIGKILL to Redis, PostgreSQL, or gateway
- **Network partition:** iptables rules blocking traffic
- **Resource exhaustion:** CPU stress, memory pressure, disk fill
- **Latency injection:** tc netem adding delay to network packets

### What scale can it handle?

- Experiments run on a single environment (dev/staging)
- Automated teardown ensures experiments don't leave state
- Experiments are idempotent — can run repeatedly

### 10 Follow-Up Questions

1. **Q: How do you prevent chaos experiments from breaking production?**
   A: Experiments only run in dev/staging environments. Production uses Kubernetes with PDB (Pod Disruption Budget) to prevent voluntary disruptions. Chaos experiments are gated by approval workflows.

2. **Q: What's the difference between chaos engineering and testing?**
   A: Testing verifies expected behavior. Chaos engineering explores unknown failure modes. You might discover that killing Redis at a specific moment causes a goroutine leak that unit tests never caught.

3. **Q: How do you validate recovery?**
   A: After fault injection, run health checks every 5 seconds. Check that: metrics return to baseline, clients reconnect, no goroutine leaks, no memory leaks. Automated validation reports pass/fail.

4. **Q: What's the most common failure you discovered?**
   A: Goroutine leaks on Redis reconnection. When Redis dies, goroutines stuck on channel sends would leak. Fixed by adding context cancellation and read deadline tricks.

5. **Q: How do you inject network latency?**
   A: Linux `tc` (traffic control) with netem: `tc qdisc add dev eth0 root netem delay 200ms`. This adds 200ms to all outgoing packets. Teardown: `tc qdisc del dev eth0 root`.

6. **Q: How do you handle experiment cleanup?**
   A: Every experiment has a teardown function. If the experiment crashes, a cleanup script runs on next startup. Experiments are idempotent — running teardown twice is safe.

7. **Q: Can you run chaos experiments in production?**
   A: Yes, but carefully. Netflix does this ("Chaos Monkey"). For this system, we limit production chaos to non-critical components (e.g., one gateway instance, not all). Use feature flags to control experiment scope.

8. **Q: What metrics do you track during experiments?**
   A: Error rate, latency P99, goroutine count, memory usage, client disconnects, reconnection time, message drop rate. All tracked in Prometheus with experiment labels.

9. **Q: How do you know when the system has recovered?**
   A: Health checks return OK, metrics are within baseline, no active alerts. The validation function checks all of these and reports pass/fail.

10. **Q: What's the business value of chaos engineering?**
    A: Confidence. You know the system works under failure before it happens in production. This reduces on-call incidents, downtime, and financial loss from missed trades.

---

## Bullet 8: "Designed a production-grade observability stack with 40+ Prometheus metrics, 6 Grafana dashboard categories, and dynamic log level changes without server restart"

### What exactly does this mean?

The system exposes detailed metrics about its internal state. Prometheus scrapes these metrics, Grafana visualizes them in dashboards, and you can change log verbosity at runtime without restarting the server.

### How does Prometheus work?

- **Pull model:** Prometheus scrapes `/metrics` endpoint every 15 seconds
- **Metrics:** Key-value pairs with labels: `rtmds_messages_total{symbol="AAPL", direction="in"}`
- **Storage:** Time-series database optimized for append-only writes
- **Alerting:** PromQL rules that fire alerts when thresholds are breached

### What are the 6 dashboard categories?

1. **Executive:** Business KPIs (messages/sec, active clients, revenue)
2. **Platform:** System health (CPU, memory, goroutines, GC)
3. **Services:** Per-component metrics (feed, gateway, Redis, PostgreSQL)
4. **Infrastructure:** Kubernetes pods, nodes, network
5. **Reliability:** Error rates, recovery times, chaos experiment results
6. **Performance:** Latency percentiles, throughput, bottleneck analysis

### How do dynamic log level changes work?

- Admin API endpoint: `POST /admin/log-level {"level": "debug"}`
- Viper watches config for changes
- zerolog's global level is updated atomically
- No restart required — takes effect immediately

### Why Prometheus instead of Datadog?

| Factor | Prometheus | Datadog |
|--------|------------|---------|
| Cost | Free (open source) | $23/host/month |
| Data retention | Local + Thanos for long-term | Vendor-managed |
| Query language | PromQL (powerful) | Proprietary |
| Self-hosted | Yes | No (SaaS only) |
| Ecosystem | Kubernetes native | Vendor lock-in |

### What scale can it handle?

- Prometheus: 10M time series, 100K samples/sec
- Grafana: 100+ dashboards, 10 concurrent users
- Alerting: 1000+ rules, 10 second evaluation interval

### 10 Follow-Up Questions

1. **Q: How do you prevent metric cardinality explosion?**
   A: Strict rules: NEVER label by symbol, topic, or client_id. Labels are bounded: gateway_id, direction (in/out), status (success/error). Unbounded labels would create millions of time series.

2. **Q: What's a histogram metric?**
   A: A metric that tracks distribution of values (e.g., latency). You define buckets (0.1ms, 1ms, 10ms, 100ms). Prometheus counts how many values fall in each bucket. You can then compute P50, P99, etc.

3. **Q: How do you alert on anomalies?**
   A: PromQL rules with `rate()` and `increase()`. Example: `rate(rtmds_errors_total[5m]) > 0.01` means error rate > 1% over 5 minutes → fire alert.

4. **Q: How do you handle Prometheus storage?**
   A: Local storage with 15-day retention. For long-term: remote write to Thanos or Cortex. For this project, local is sufficient.

5. **Q: What's the overhead of metrics collection?**
   A: ~1μs per metric increment (atomic operation). 40 metrics × 50K events/sec = 2ms/sec overhead. Negligible.

6. **Q: How do you export metrics from Go?**
   A: `prometheus/client_golang` library. Define counters, gauges, histograms. Expose via `/metrics` HTTP endpoint. Prometheus scrapes it.

7. **Q: How do you debug a production issue with metrics?**
   A: Check Grafana dashboards for anomalies. Compare current values to baseline. Use PromQL to drill down: `rate(rtmds_messages_total{gateway="1"}[1m])` shows message rate for gateway 1.

8. **Q: What's the difference between counters and gauges?**
   A: Counter = monotonically increasing (total messages ever). Gauge = can go up or down (current goroutine count). Counters are used with `rate()` to compute per-second rates.

9. **Q: How do you handle metric scraping during deployments?**
   A: Kubernetes rolling updates ensure at least one pod is running. Prometheus scrapes whichever pod is healthy. New pods register with service discovery and are scraped automatically.

10. **Q: Can you add new metrics without restarting?**
    A: Yes. Metrics are registered at startup. But you can add new metric names via code changes (which require deployment). Dynamic log level changes are different — those don't require restart.

---

## Bullet 9: "Achieved zero-allocation hot path through sync.Pool buffer recycling, pre-encoded JSON via CachedEvent, and sync.RWMutex sharding to minimize GC pressure under high-frequency trading workloads"

### What exactly does this mean?

The hot path (event processing) allocates zero heap memory. This means the garbage collector never runs during normal operation, eliminating GC pauses that would cause latency spikes.

### What is sync.Pool?

A pool of reusable objects. When you need a buffer, get one from the pool. When done, return it. No allocation, no GC.

```go
var bufPool = sync.Pool{
    New: func() interface{} { return make([]byte, 4096) },
}

buf := bufPool.Get().([]byte)
// use buf...
bufPool.Put(buf)
```

### What is CachedEvent?

Pre-encoded JSON bytes shared across all subscribers. When an event arrives:
1. JSON-encode once into a byte slice
2. Store in CachedEvent
3. All subscribers get the same byte slice (no re-encoding)

At 1K subscribers, this saves 999 JSON encodings per event.

### What is RCU (Read-Copy-Update)?

A lock-free synchronization pattern:
1. **Read:** Reader gets current pointer (no lock)
2. **Copy:** Writer copies data, modifies copy
3. **Update:** Writer atomically swaps pointer to new copy

Readers never block. Writers pay the copy cost. Perfect for read-heavy workloads (publish is read-heavy: many subscribers, few updates).

### What is sync.RWMutex sharding?

Instead of one global lock, use N locks (shards):

```go
type ShardedMap struct {
    shards [256]struct {
        sync.RWMutex
        m map[string]*Client
    }
}
```

Hash the key to select a shard. Reduces contention by 256x.

### Why zero-allocation matters?

- Each allocation = eventual GC work
- GC pauses = latency spikes (even with Go's low-latency GC)
- At 50K events/sec, even 1 allocation/event = 50K allocations/sec = GC pressure
- Zero allocation = GC never runs during normal operation

### What scale can it handle?

- 50K events/sec with zero heap allocations
- GC pause: effectively zero during normal operation
- Memory: stable (no growth from allocations)
- Latency: consistent (no GC-induced spikes)

### 10 Follow-Up Questions

1. **Q: How do you verify zero allocations?**
   A: `go test -bench -benchmem` shows `allocs/op`. Also `runtime.ReadMemStats` to track heap allocations. Profiling with `pprof` shows allocation hotspots.

2. **Q: What happens when sync.Pool runs out of buffers?**
   A: `New` function creates a new buffer. This happens during startup or after GC clears the pool. During normal operation, buffers are reused.

3. **Q: How does CachedEvent handle updates?**
   A: When a new event arrives for a symbol, a new CachedEvent is created (encoding the new event). The old CachedEvent is still valid for subscribers who haven't read it yet. This is safe because CachedEvent is immutable after creation.

4. **Q: Why not use Protobuf instead of JSON?**
   A: Protobuf would be ~2x faster for encoding but adds schema management complexity. JSON is human-readable and sufficient. The zero-allocation optimization already makes JSON fast enough.

5. **Q: How do you handle GC clearing sync.Pool?**
   A: Go's GC clears sync.Pool on every cycle. But new buffers are created on demand. The pool is a performance optimization, not a correctness requirement. The system works correctly with or without pool hits.

6. **Q: What's the difference between sync.Pool and a custom buffer pool?**
   A: sync.Pool is GC-aware (clears on GC cycle). Custom pools persist across GC. sync.Pool is simpler and sufficient for this use case.

7. **Q: How do you measure GC impact?**
   A: `GODEBUG=gctrace=1` logs GC events. `runtime.ReadMemStats` shows `PauseNs`. In production, track GC pause as a Prometheus metric.

8. **Q: Can you eliminate ALL allocations?**
   A: Not 100% — channel operations, goroutine creation, and some runtime internals allocate. But the hot path (event processing) is allocation-free. That's the goal.

9. **Q: How does RCU handle concurrent reads and writes?**
   A: Reads are lock-free (atomic load of pointer). Writes copy the data, modify the copy, then atomically swap the pointer. Readers see either the old or new version — never a partially updated version.

10. **Q: What's the memory impact of pre-encoded JSON?**
    A: Each CachedEvent is ~200 bytes. With 10K symbols, that's ~2MB. Shared across all subscribers — no per-client copy. This is a trade-off: more memory for less CPU.

---

## Bullet 10: "Delivered complete Kubernetes deployment with HPA, PDB, NetworkPolicy, Ingress, Kustomize overlays, and Docker multi-stage builds producing ~15MB scratch images"

### What exactly does this mean?

The system is deployed on Kubernetes with production-grade configuration:
- **HPA:** Automatically scales gateway pods based on CPU/connection count
- **PDB:** Ensures at least one pod is always running during maintenance
- **NetworkPolicy:** Restricts which pods can communicate
- **Ingress:** External access via HTTP/HTTPS
- **Kustomize:** Environment-specific configuration without templating
- **Docker:** Multi-stage build → 15MB final image (Alpine scratch)

### How does HPA work?

- Monitors metrics (CPU, memory, custom metrics like connections)
- If metrics exceed threshold, adds pods
- If metrics drop, removes pods
- Configured with min/max replicas and target metrics

### How does Docker multi-stage build work?

```dockerfile
# Stage 1: Build
FROM golang:1.26 AS builder
RUN go build -o /app

# Stage 2: Runtime
FROM scratch
COPY --from=builder /app /app
CMD ["/app"]
```

Result: Only the binary is in the final image. No Go runtime, no OS, no dependencies. ~15MB.

### Why scratch instead of Alpine?

- scratch = empty image. Zero CVEs, zero OS overhead.
- Alpine = minimal Linux (~5MB). Has shell, package manager.
- scratch is smaller but harder to debug (no shell). For production, scratch is ideal.

### What is Kustomize?

A Kubernetes configuration management tool. Instead of Helm templates, Kustomize uses overlays:

```
base/
  deployment.yaml
  service.yaml
overlays/
  dev/
    kustomization.yaml (patches base)
  prod/
    kustomization.yaml (patches base differently)
```

No templating language to learn. Just YAML patches.

### What scale can it handle?

- HPA: scales from 1 to 100 pods
- Kubernetes: 5000 nodes, 150K pods per cluster
- Docker: 15MB image = fast pulls, fast starts
- PDB: ensures availability during rolling updates

### 10 Follow-Up Questions

1. **Q: How does HPA know to scale based on WebSocket connections?**
   A: Custom metrics adapter exposes `rtmds_active_connections` to HPA. HPA targets 8000 connections per pod. If a pod has 9000 connections, HPA adds a pod.

2. **Q: What's the difference between HPA and VPA?**
   A: HPA scales horizontally (more pods). VPA scales vertically (more CPU/memory per pod). For stateless gateways, horizontal scaling is better — you can add cheap pods instead of expensive ones.

3. **Q: How do you handle zero-downtime deployments?**
   A: Kubernetes rolling update + PDB ensures at least one pod is running. The new pod starts, passes health checks, and the old pod is terminated. WebSocket connections drain gracefully.

4. **Q: Why Kustomize instead of Helm?**
   A: Kustomize is native to Kubernetes (kubectl apply -k). No templating language. Simpler for this use case. Helm is better for complex charts with many dependencies.

5. **Q: How do you handle secrets in Kubernetes?**
   A: Kubernetes Secrets (base64-encoded). For production: use external secrets operator (Vault, AWS Secrets Manager). Docker secrets with `_FILE` suffix for container-level secrets.

6. **Q: What's the purpose of NetworkPolicy?**
   A: Restricts pod-to-pod communication. Example: only gateways can talk to Redis, only Redis can talk to PostgreSQL. Reduces attack surface.

7. **Q: How do you handle DNS resolution in Kubernetes?**
   A: Kubernetes provides internal DNS. Services are reachable by name: `redis-service`, `postgres-service`. No hardcoded IPs.

8. **Q: How do you debug a crashing pod?**
   A: `kubectl logs <pod>` for logs. `kubectl describe pod <pod>` for events. `kubectl exec -it <pod> -- /bin/sh` for shell access (not possible with scratch images — use debug sidecar).

9. **Q: What's the difference between Deployment and StatefulSet?**
   A: Deployment = stateless pods (gateways). StatefulSet = stateful pods (Redis, PostgreSQL). StatefulSet provides stable network identity and persistent storage.

10. **Q: How do you handle resource limits?**
    A: Kubernetes resource limits prevent pods from consuming too much CPU/memory. Configured in deployment spec: `resources.limits.memory: 512Mi`, `resources.limits.cpu: 500m`. Go's `GOMEMLIMIT` is set to match.

---

## Bullet 11: "Implemented ordered component lifecycle management with deterministic startup/shutdown sequences, graceful WebSocket drain with randomized delays, and automatic zombie connection eviction"

### What exactly does this mean?

Components start and stop in a specific order to prevent dependencies from being unavailable. On shutdown, WebSocket connections are drained gracefully (clients are notified and given time to reconnect) instead of being killed abruptly.

### What is the startup order?

```
Config → Logger → Metrics → EventLog(Postgres) → TopicManager → 
SnapshotService → Gateway → RedisPublisher → Feed → Pipeline → 
RedisSubscriber → Discovery → HTTPServer
```

Each component depends on the previous ones. Starting them out of order causes panics.

### What is graceful WebSocket drain?

On shutdown:
1. Stop accepting new connections
2. Send close frame to all existing clients (with randomized delay 50-200ms)
3. Wait for clients to disconnect
4. Force-close any remaining connections after timeout
5. Exit

Randomized delay prevents all clients from reconnecting simultaneously (thundering herd).

### What is zombie connection eviction?

A zombie connection is a client that connected but is not receiving data (e.g., after a network partition). The system detects these and evicts them:
- If a client hasn't acked in 60 seconds, evict
- EvictAll() sends close frames to all clients on gateway restart
- Clients reconnect to healthy gateways

### Why not just kill everything on shutdown?

- Clients see abrupt disconnection (WebSocket error)
- No time to reconnect to another gateway
- Data loss during the reconnection window
- Bad user experience

### What scale can it handle?

- Startup: ~2 seconds for all components
- Shutdown: ~5 seconds (including drain timeout)
- Zombie detection: 60 seconds
- Randomized delay: 50-200ms (spreads reconnections)

### 10 Follow-Up Questions

1. **Q: How do you implement deterministic startup order?**
   A: The `app` package defines a dependency graph. Components are started in topological order. If a component fails to start, all dependent components are skipped.

2. **Q: What happens if a component fails to start?**
   A: The startup sequence aborts. Already-started components are shut down in reverse order. This prevents partial startup where some components are running but dependencies are missing.

3. **Q: How does the randomized shutdown delay prevent thundering herd?**
   A: If all clients reconnect at the same time, the gateway is overwhelmed. Randomized delay (50-200ms) spreads reconnections over 150ms, reducing peak connection rate.

4. **Q: What's the difference between graceful and abrupt shutdown?**
   A: Graceful: send close frames, wait for clients, clean up. Abrupt: kill process, clients see errors. Graceful reduces client-side disruption.

5. **Q: How do you handle SIGTERM vs SIGKILL?**
   A: SIGTERM = graceful shutdown (handled). SIGKILL = immediate termination (not handled). Kubernetes sends SIGTERM first, then SIGKILL after grace period.

6. **Q: What's the shutdown timeout?**
   A: Configurable (default: 30 seconds). After timeout, force-kill remaining goroutines and exit. Prevents hanging on a stuck component.

7. **Q: How do you handle in-flight events during shutdown?**
   A: Context cancellation propagates to all goroutines. In-flight events are either completed or dropped. The system prioritizes correctness (no corruption) over completeness (some events may be lost).

8. **Q: Can you restart individual components without full restart?**
   A: Dynamic log level changes don't require restart. But component restart (e.g., feed) requires full restart. The lifecycle manager handles this.

9. **Q: How do you test shutdown behavior?**
   A: Chaos experiments inject SIGTERM at random points. Automated validation checks that the system recovers cleanly. Load tests verify that clients reconnect within expected time.

10. **Q: What's the zombie connection eviction timeout?**
    A: 60 seconds without a successful write. The writePump tracks the last successful write time. If it exceeds 60 seconds, the connection is evicted.

---

## Bullet 12: "Built a token-bucket rate limiter for WebSocket connections (500 new connections/sec) to prevent thundering herd effects during gateway restarts"

### What exactly does this mean?

When a gateway restarts, all 10K clients try to reconnect simultaneously. A token-bucket rate limiter limits new connections to 500/sec, spreading the reconnection over 20 seconds instead of a single spike.

### How does a token-bucket rate limiter work?

```
Bucket capacity: 500 tokens
Refill rate: 500 tokens/sec

Connection request:
  if bucket has tokens:
    consume 1 token, allow connection
  else:
    reject connection (client retries)
```

Tokens are added at a constant rate. Bursts are limited by bucket capacity.

### Why token-bucket instead of leaky-bucket?

| Algorithm | Behavior |
|-----------|----------|
| Token bucket | Allows bursts (up to bucket capacity), then rate limits |
| Leaky bucket | Constant rate, no bursts |

Token bucket is better for this use case because:
- Allows brief bursts (e.g., 10 clients reconnect at once)
- Rate limits sustained high rates (prevents thundering herd)

### Why not just let all clients reconnect?

- Server overwhelmed: 10K simultaneous TCP handshakes, TLS negotiations, subscription setups
- Memory spike: 10K new goroutines allocated simultaneously
- Redis overwhelmed: 10K simultaneous SUBSCRIBE commands
- Clients see errors: server can't handle the load

### What scale can it handle?

- Bucket capacity: 500 tokens (burst)
- Refill rate: 500 tokens/sec (sustained)
- At 500 conn/sec, 10K clients reconnect in 20 seconds
- Memory: ~100 bytes for the rate limiter state

### 10 Follow-Up Questions

1. **Q: How do you handle clients that get rate-limited?**
   A: Client receives a 429 Too Many Requests response. Client retries with exponential backoff (1s, 2s, 4s, ...). After a few retries, the client connects successfully.

2. **Q: What happens if the rate limiter itself fails?**
   A: The rate limiter is in-memory. If the gateway restarts, the limiter resets. This is acceptable because the restart is exactly when rate limiting is needed — and clients will reconnect gradually anyway.

3. **Q: Why 500 connections/sec specifically?**
   A: Tuned for the system's capacity. At 500 conn/sec, the server can handle the TCP handshakes, TLS negotiations, and subscription setups without overload. This is a configurable value.

4. **Q: Can you rate-limit by IP address?**
   A: Yes. The rate limiter can be per-IP or global. Per-IP prevents one client from monopolizing the connection budget. Global prevents the server from being overwhelmed.

5. **Q: How does the rate limiter interact with the load balancer?**
   A: Nginx distributes connections across gateways. Each gateway has its own rate limiter. The load balancer doesn't know about rate limits — it just distributes connections.

6. **Q: What's the difference between rate limiting and throttling?**
   A: Rate limiting = reject excess requests. Throttling = slow down excess requests (add delay). Rate limiting is simpler and better for WebSocket connections (clients can retry).

7. **Q: How do you handle rate limiting in a multi-gateway setup?**
   A: Each gateway has its own rate limiter. Nginx distributes connections evenly. If one gateway is overloaded, Nginx routes to less loaded gateways.

8. **Q: Can you dynamically adjust the rate limit?**
   A: Yes, via config. Change `rate_limit.connections_per_sec` and the rate limiter updates. Useful for scaling up during high-traffic periods.

9. **Q: How do you test the rate limiter?**
   A: Load tests with 10K concurrent connections. Verify that connection rate stays below 500/sec. Monitor metrics for rejected connections.

10. **Q: What's the memory impact of the rate limiter?**
    A: ~100 bytes for the bucket state. Negligible compared to connection state (~30KB per connection). The rate limiter is a lightweight safeguard.
