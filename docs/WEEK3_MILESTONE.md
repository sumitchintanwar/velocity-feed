# Real-Time Market Data System (RTMDS) -- Project Progress

**Last Updated:** 2026-06-24
**Current Phase:** End of Week 3 -- Distributed Systems
**Status:** Architecture reviewed, core systems implemented, load tested, multi-gateway distributed with Redis Pub/Sub

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture Summary](#2-architecture-summary)
3. [Week 1 -- Core System](#3-week-1--core-system)
4. [Week 2 -- Concurrency & Scale](#4-week-2--concurrency--scale)
5. [Week 3 -- Distributed Systems](#45-week-3--distributed-systems-days-15-21)
6. [Codebase Inventory](#5-codebase-inventory)
7. [Performance Benchmarks](#6-performance-benchmarks)
8. [Load Test Results](#7-load-test-results)
9. [Architecture Reviews](#8-architecture-reviews)
10. [Known Issues and Technical Debt](#9-known-issues-and-technical-debt)
11. [Week 4+ Roadmap](#10-week-4-roadmap)
12. [Multi-Gateway Deployment](#11-multi-gateway-deployment-new)

---

## 1. Project Overview

A production-grade **Real-Time Market Data Distribution System** built in Go. Simulates a stock exchange feed, distributes market updates over WebSocket to thousands of concurrent clients with low latency, backpressure, and observability.

**Target Metrics:**

| Metric | Target |
|:---|:---|
| Throughput | 10,000+ updates/sec |
| Concurrent Clients | 5,000 WebSocket connections |
| End-to-End Latency (P99) | < 50ms |
| Memory (5K clients) | < 512 MB |

**Tech Stack:**

| Area | Technology |
|:---|:---|
| Language | Go 1.26 |
| HTTP Router | go-chi/chi v5 |
| WebSocket (server) | gorilla/websocket |
| WebSocket (client SDK) | nhooyr.io/websocket |
| Configuration | spf13/viper |
| Logging | rs/zerolog |
| Metrics | prometheus/client_golang |
| Containerization | Docker + Docker Compose |
| Monitoring | Prometheus + Grafana |

---

## 2. Architecture Summary

```
Feed Generator (Simulator)
       |
       |  Run(ctx) -> <-chan Quote
       v
  Pipeline (Feed -> Publisher)
       |
       |  for q := range quotes { publisher.Publish(ctx, q) }
       v
  TopicManager (MemoryManager, COW fan-out, 16 shards)
       |
       |  Publish(ctx, MarketEvent) -- lock-free hot path
       v
  WebSocket Gateway (per-client goroutines, sharded client maps)
       |
       |  writePump reads pre-encoded bytes, writes to conn
       v
  WebSocket Clients
```

**Data Flow:**
1. **Simulator** generates `Quote` structs at configurable intervals
2. **Pipeline** reads from the feed channel and calls `TopicManager.Publish()`
3. **TopicManager** fans out to all subscribers registered for that symbol using COW atomic pointers
4. **WebSocket Client** reads from its subscription channel and writes pre-encoded JSON
5. **Client** receives the quote over the wire

**Key Interfaces (Dependency Inversion):**

| Interface | Package | Purpose |
|:---|:---|:---|
| `Feed` | `internal/marketdata` | Abstracts upstream data sources |
| `MarketEvent` | `internal/marketdata` | Base abstraction for all events |
| `Manager` | `internal/topicmanager` | Subscribe, Unsubscribe, Publish |
| `Handle` | `internal/topicmanager` | Per-subscription handle (C(), Done(), Cancel()) |
| `Bus` | `internal/pubsub` | In-memory pub/sub (MemoryBus) |

---

## 3. Week 1 -- Core System

### Day 1: Project Setup
- Go module (`github.com/sumit/rtmds`), directory layout, Makefile, Dockerfile
- Docker Compose with server, Redis, Prometheus, Grafana

### Day 2: Feed Generator
- `internal/marketdata/simulator/` -- random-walk price generation
- Configurable symbols, tick rate, volatility
- `Feed` interface for future real feed providers (Alpaca, Polygon, Binance)

### Day 3: Publisher Service
- `internal/pubsub/memory.go` -- MemoryBus with sharded subscribers
- Non-blocking publish (drop on full), snapshot cache for latest price

### Day 4: Topic Manager
- `internal/topicmanager/memory.go` -- sharded RWMutex + COW subscriber lists
- FNV-1a zero-allocation hashing for shard selection
- 16 shards default, topic tombstoning on empty

### Day 5: WebSocket Gateway
- `internal/websocket/gateway.go` -- per-client readPump + writePump goroutines
- Sharded client maps (32 shards), atomic active count
- Ping/pong heartbeat (54s interval, 60s timeout)

### Day 6: Live Data Streaming
- End-to-end pipeline wired in `internal/app/app.go`
- HTTP router with `/ws`, `/health`, `/health/detail`, `/ready`, `/metrics`

### Day 7: Benchmark v1
- Component-level benchmarks for TopicManager, Gateway, Pipeline
- Integration tests using httptest

---

## 4. Week 2 -- Concurrency & Scale

### Day 8-9: Worker Pool
- `internal/workerpool/pool.go` -- bounded-concurrency worker pool
- Config: 8 workers, 4096 queue capacity, 5s shutdown timeout
- Non-blocking enqueue with panic recovery, graceful drain on shutdown
- Prometheus metrics: tasks received/completed/failed/dropped, queue depth, active workers

### Day 10-11: Backpressure
- `internal/backpressure/` -- pluggable policy engine
- Policies: `DropOldest` (primary), `DropNewest`, `Disconnect` (extreme slow consumers)
- `Ring` buffer: fixed-capacity, power-of-2, atomic counters, max-age enforcement
- `Channel` wrapper: forwardLoop goroutine, 10-bucket sliding window drop rate tracking
- `CachedChannel`: optimized for pre-encoded JSON events (zero-copy fan-out)

### Day 12: Subscriber Buffers
- `internal/clientqueue/` -- per-client isolated queues
- `Queue` / `CachedQueue` backed by backpressure.Channel
- `Manager`: 16-shard queue registry, aggregate metrics (no per-client label cardinality)
- Per-client stats: enqueued, sent, dropped, depth

### Day 13: Rate Limiting
- `internal/ratelimit/` -- token bucket algorithm
- `Bucket`: refill logic, burst capping, per-client isolation
- `Limiter`: 16-shard client map, background evictor (5min TTL)
- Limits: 10 conn/sec, 50 subscribe/sec, 1000 max subscriptions per client
- `AllowAndSubscribe`: atomic check+increment (fixes race condition)

### Day 14: Load Testing Tool
- `internal/loadtest/` -- dedicated load test framework
- `Pool`: manages N WebSocket clients with semaphore-limited concurrency
- `LatencyCollector`: sharded (16 shards) lock-free latency collection
- `ThroughputCounter`: atomic message counter
- Auto-saves results to `docs/results/` as markdown

### Day 15: Prometheus Metrics
- `internal/platform/metrics.go` -- 30+ metrics across all layers
- Feed layer: messages received, errors, data staleness
- Distribution: broadcasts, drops, active subscribers, subscription events
- WebSocket: connections, messages, bytes, delivery latency, ping latency, auth failures
- Transport: HTTP request count and duration
- Sub-package metrics: topic publish latency, client queue stats, backpressure stats, worker pool stats, rate limiter stats
- **Cardinality discipline**: NO labels by symbol, topic, client_id, or IP

### Day 16: Benchmarks and Scalability Analysis
- `internal/bench/` -- end-to-end benchmarks and backpressure benchmarks
- `internal/*/bench_test.go` -- per-package benchmarks
- TopicManager: 153 ns/op (1 sub), 718 ns/op (100 subs), 6.5M ops/sec (1 sub)
- Gateway: 689K deliveries/sec (100 clients), 3.6M ops/sec concurrent publish
- Critical finding: subs_500 latency cliff (18,659 ns vs 629 ns at 100 subs) -- caused by O(N) JSON serialization
- Identified 6 ranked optimizations with pre-encode JSON as #1 priority

---

## 4.5 Week 3 -- Distributed Systems (Days 15-21)

### Day 15-16: Redis Pub/Sub

- `internal/distribution/redisbus/publisher.go` (239 lines) -- Redis-backed Publisher with async worker pool, non-blocking publish, topic-based per-symbol channel routing (`market:AAPL`, `market:MSFT`, etc.), configurable workers/queue size, drop-on-full semantics
- `internal/distribution/redisbus/subscriber.go` (326 lines) -- Redis-backed Subscriber with dynamic subscribe/unsubscribe per symbol, batch subscribe, staleness detection, stale-data callback, listen loop forwarding to local TopicManager
- `internal/distribution/redisbus/redisbus_test.go` (377 lines) -- Comprehensive test suite: single publish, single subscribe, multi-gateway fan-out (3 gateways), close/drain, queue full, ping, channel naming
- Integration in `internal/app/app.go` -- Redis client creation, publisher with configurable worker pool (4 default, 32 for benchmark mode), subscriber with stale-data callback broadcasting degradation notices to WebSocket clients, lifecycle management
- Configuration: `RedisConfig` struct with `Enabled`, `Addr`, `Password`, `DB` fields, configurable via env vars (`RTMDS_REDIS_ENABLED`, `RTMDS_REDIS_ADDR`)
- Design docs: `docs/design/REDIS_INTEGRATION_DESIGN.md`, `docs/reviews/redis_integration_review.md`

### Day 17-18: Multi-Gateway Architecture

- `docker-compose.multigateway.yml` -- 3 gateway instances (gateway1:9091 primary w/ feed generator + Redis publisher/subscriber, gateway2:9092 subscriber-only, gateway3:9093 subscriber-only)
- `nginx/nginx.conf` -- `least_conn` upstream with 3 gateways, `max_fails=3`, `fail_timeout=30s`, WebSocket proxying, gateway ID header forwarding, `proxy_next_upstream` for failover
- `nginx/nginx-benchmark.conf` -- Optimized for benchmarking with 5 gateways, faster failover (`max_fails=2`, `fail_timeout=10s`), `keepalive 64`
- Primary vs subscriber-only gateways: `Feed.Enabled` flag controls whether a gateway runs the feed generator + Redis publisher
- Makefile targets: `gw-up`, `gw-down`, `gw-logs`, `gw-health`, `gw-test` for multi-gateway management
- Design docs: `docs/design/MULTI_GATEWAY_ARCHITECTURE.md`, `docs/reviews/multi_gateway_review.md`

### Day 19: Sticky Sessions

- Stateless "fat client" pattern -- clients own their subscription state, on reconnect re-send SUBSCRIBE messages to whichever gateway Nginx routes them to
- Gateway ID header: `rtmds-gateway-id` set on every WebSocket upgrade response and `/health`, `/health/detail`, `/ready` endpoints
- Gateway ID auto-generation from port number: `GetGatewayID()` returns `"gateway-{port}"`
- `docker-compose.sticky.yml` -- Dedicated deployment with `RTMDS_SERVER_GATEWAY_ID` set per gateway (`gateway-9091`, `gateway-9092`, `gateway-9093`)
- Design docs: `docs/design/STICKY_SESSION_DESIGN.md`, `docs/reviews/sticky_session_review.md`

### Day 20: Service Discovery

- `internal/discovery/registry.go` (304 lines) -- Redis-based service discovery with:
  - `Register()` -- Redis pipeline: SET key with TTL + SADD to active set
  - `Deregister()` -- DEL key + SREM from active set
  - `List()` -- SMEMBERS on active set + MGET for batch fetch
  - `StartHeartbeat()` -- Background goroutine refreshing TTL every 10s
  - Deep health check: unhealthy gateway skips heartbeat, TTL expires, self-deregisters
- `internal/discovery/registry_test.go` (397 lines) -- Tests: Register, Deregister, List Multiple, Get, Count, Stale Filtering, TTL Config, Health Check
- HTTP endpoint: `GET /gateways` returns JSON list of all active gateways with count
- Configuration: `DiscoveryConfig` with `Enabled`, `TTL` (30s default), `HeartbeatInterval` (10s default)
- Design docs: `docs/design/SERVICE_DISCOVERY_DESIGN.md`, `docs/reviews/service_discovery_review.md`

### Day 21: Distributed Topic Routing + Chaos Testing + Distributed Benchmarks

**Distributed Topic Routing:**
- `internal/topicmanager/router.go` (432 lines) -- `DistributedRouter` with sharded per-symbol local subscriber tracking (16 shards, FNV-1a hash), dynamic Redis subscribe/unsubscribe, `Reconcile()` self-healing (5s interval)
- `internal/topicmanager/groups.go` (101 lines) -- `DefaultTopicGroups` maps symbols to group channels: `market:equities`, `market:crypto`, `market:forex`, `market:futures`
- Integration tests: `test/integration/distributed_test.go` (479 lines) -- End-to-end Redis publisher -> subscriber -> router -> local client, cross-gateway delivery, dynamic subscription lifecycle
- Design docs: `docs/design/DISTRIBUTED_TOPIC_ROUTING_DESIGN.md`, `docs/reviews/distributed_routing_review.md`

**Chaos Testing:**
- `scripts/chaos_test.sh` (626 lines) -- Shell framework: pre-flight checks, 4 scenarios (Kill Gateway 1, Kill Gateway 2, Restart Redis, Rolling Restart All Gateways), recovery verification, automated results generation
- `test/integration/chaos_test.go` (428 lines) -- 6 Go integration tests: kill gateway, restart Redis, rolling restart, combined failure, data flow verification
- Results: `docs/results/chaos_results/CHAOS_TESTING_RESULTS.md` -- All 4 scenarios PASSED, recovery times <5s (gateway crash) and <10s (Redis restart)
- Design docs: `docs/design/CHAOS_TESTING_DESIGN.md`, `docs/reviews/chaos_testing_review.md`

**Distributed Benchmarks:**
- `cmd/benchmark/main.go` (582 lines) -- Standalone benchmark client with configurable parameters, WebSocket connection with exponential backoff, latency histogram (P50/P95/P99/P99.9), JSON output
- `scripts/run_benchmark.sh` (348 lines) -- Benchmark suite orchestrator: runs 1/3/5 gateway scenarios, collects system metrics, generates comparison report with scaling efficiency calculations
- `scripts/collect_metrics.sh` (175 lines) -- Periodic metrics collection: Redis ops/sec, memory, clients, channels, gateway CPU/memory
- `docker-compose.benchmark.yml` -- 5-gateway benchmark stack with resource limits (2 CPU, 512MB per gateway)
- Design docs: `docs/design/DISTRIBUTED_BENCHMARKING_STRATEGY.md`, `docs/reviews/distributed_benchmark_review.md`

**Scaling Reports:**
- `docs/results/benchmark/BENCHMARK_REPORT.md` -- Full benchmark report with scaling analysis:
  - Throughput scaling: 13 msg/sec (1gw) -> 40 msg/sec (3gw, 103% efficiency) -> 40 msg/sec (5gw, 62% efficiency)
  - Latency stability: P50 stable at 1.00ms across all configs
  - Bottleneck identified: Feed generator is the throughput bottleneck, not gateways
- 10+ load test result files from June 23-24, 2026

---

## 5. Codebase Inventory

### Go Packages (20 internal + 1 public)

```
internal/
  app/              DI root, lifecycle management (app.go)
  backpressure/     Pluggable backpressure policies (policy.go, ring.go, channel.go, metrics.go)
  bench/            End-to-end and backpressure benchmarks
  clientqueue/      Per-client queues with manager (queue.go, config.go)
  config/           Viper-backed configuration (config.go)
  discovery/        Redis-based service discovery with TTL registry (registry.go)
  distribution/     Legacy Hub (replaced by MemoryBus)
  distribution/redisbus/  Redis Pub/Sub publisher and subscriber (publisher.go, subscriber.go)
  feed/             Pipeline connecting Feed to Publisher (pipeline.go)
  loadtest/         Load test tool (client.go, pool.go, latency.go, config.go)
  marketdata/       Domain types: Quote, Bar, MarketEvent, Feed, Simulator
  platform/         Logger, Metrics, Lifecycle interfaces
  pubsub/           Bus interface + MemoryBus (pubsub.go, memory.go)
  ratelimit/        Token bucket rate limiter (bucket.go, limiter.go, config.go)
  topicmanager/     Topic-based routing with COW fan-out (topicmanager.go, memory.go, router.go, groups.go)
  transport/        HTTP router + middleware (router.go, middleware.go)
  websocket/        WebSocket gateway + client (gateway.go, client.go, messages.go)
  workerpool/       Bounded worker pool (pool.go, metrics.go)

pkg/
  client/           Public Go client SDK

cmd/
  benchmark/        Standalone distributed benchmark client (main.go)
```

### Test Files (30+ total)

- **Unit tests (15):** workerpool, clientqueue, feed, topicmanager, websocket, ratelimit, backpressure (2), pubsub, simulator, loadtest (2)
- **Benchmark tests (10):** workerpool, clientqueue (2), feed, topicmanager, websocket, transport, backpressure, pubsub, simulator
- **End-to-end benchmarks (2):** `internal/bench/e2e_bench_test.go`, `internal/bench/backpressure_bench_test.go`
- **Integration tests (3):** `test/integration/server_test.go`, `test/integration/distributed_test.go`, `test/integration/chaos_test.go`
- **Redis Pub/Sub tests (1):** `internal/distribution/redisbus/redisbus_test.go`
- **Service Discovery tests (1):** `internal/discovery/registry_test.go`

### Infrastructure

- `cmd/server/main.go` -- entry point with signal handling and graceful shutdown
- `cmd/benchmark/main.go` -- standalone distributed benchmark client
- `Makefile` -- build, test, lint, docker, multi-gateway targets
- `Dockerfile` -- multi-stage build (golang:1.23-alpine -> scratch)
- `Dockerfile.benchmark` -- dedicated Dockerfile for benchmark client
- `docker-compose.yml` -- server, Redis, Prometheus, Grafana
- `docker-compose.multigateway.yml` -- 3 gateway instances + Redis
- `docker-compose.sticky.yml` -- 3 gateways behind Nginx with service discovery
- `docker-compose.benchmark.yml` -- 5-gateway benchmark stack with resource limits
- `docker-compose.dev.yml` -- development environment
- `nginx/nginx.conf` -- Nginx load balancer config for 3 gateways
- `nginx/nginx-benchmark.conf` -- Nginx config for 5-gateway benchmarks
- `scripts/ws_client.py` -- Python WebSocket test client
- `scripts/chaos_test.sh` -- Chaos testing framework (626 lines)
- `scripts/run_benchmark.sh` -- Benchmark suite orchestrator (348 lines)
- `scripts/collect_metrics.sh` -- Periodic metrics collection (175 lines)
- `deployments/prometheus/prometheus.yml` -- scrape config

---

## 6. Performance Benchmarks

### TopicManager -- Internal Routing Layer

| Benchmark | ns/op | B/op | allocs/op | Throughput |
|:---|---:|---:|---:|---:|
| Publish 1 sub | 153 | 112 | 1 | 6.5M ops/s |
| Publish 10 subs | 239 | 112 | 1 | 4.2M ops/s |
| Publish 100 subs | 718 | 112 | 1 | 1.4M ops/s |
| Publish 1,000 subs | 6,557 | 112 | 1 | 153k ops/s |
| Publish parallel 100 subs | 530 | 112 | 1 | 1.9M ops/s |
| Publish parallel 1,000 subs | 3,475 | 112 | 1 | 288k ops/s |
| Publish parallel 10,000 subs | 43,587 | 112 | 1 | 23k ops/s |
| Subscribe 1 topic | 11,056 | 5,489 | 13 | -- |
| Subscribe 100 topics | 52,215 | 13,533 | 411 | -- |
| Mixed 80/20 | 1,115 | 1,549 | 3 | -- |

**Key insight:** Publish hot-path is 1 alloc constant through 10,000 subscribers. COW atomic pointer design works.

### WebSocket Gateway -- End-to-End

| Benchmark | ns/op | B/op | allocs/op | Throughput |
|:---|---:|---:|---:|---:|
| Connect (serial) | 325,763 | 42,632 | 162 | 3k conns/s |
| Connect (parallel) | 171,482 | 42,735 | 162 | 6k conns/s |
| Publish 1 client | 129 | 112 | 1 | 7.8M ops/s |
| Publish 100 clients | 596 | 114 | 1 | 1.7M ops/s |
| End-to-end 100 clients | 1,451 | 284 | 2 | 689K deliveries/s |
| ConcurrentPublish 100 clients | 279 | 113 | 1 | 3.6M ops/s |

### Critical Finding: subs_500 Latency Cliff

| Subscribers | ns/op | Allocs | Expected (linear) | Ratio |
|---:|---:|---:|---:|---:|
| 1 | 103 | 1 | -- | -- |
| 10 | 163 | 1 | ~160 | 1.0x |
| 50 | 349 | 1 | ~600 | 0.6x |
| 100 | 629 | 1 | ~1,100 | 0.6x |
| **500** | **18,659** | **11** | **~3,200** | **5.8x** |

**Root cause:** O(N) JSON serialization in `writePump` via `conn.WriteJSON`. At 500 subscribers, GC pressure from 500 simultaneous `json.Marshal` calls inflates latency 5.8x.

### Ranked Optimizations

| Rank | Optimization | Latency | Memory | Complexity | Status |
|:---:|:---|:---:|:---:|:---:|:---|
| 1 | Pre-encode JSON, broadcast bytes | 5-10x | Down 85% | Medium | ✅ Done |
| 2 | Batch COW subscribe rebuild | 5x (subscribe) | Down 80% | Medium | ✅ Done |
| 3 | Pool Unsubscribe rebuild slice | 1.5x (disconnect) | Down 90% | Low | ✅ Done |
| 4 | FNV-1a vs maphash.Hash | 15% (hot path) | Elim 112B/op | Low | ✅ Done |
| 5 | WriteBufferPool in Upgrader | -- | 20 MB at 5k | Trivial | ✅ Done |
| 6 | Remove dead `send` channel | -- | 256 MB at 5k | Trivial | ✅ Done |

---

## 7. Load Test Results

### 5,000 Client Load Test (2026-06-24 05:28:31 IST)

| Parameter | Value |
|:---|:---|
| Connections Attempted | 5,000 |
| Connections Succeeded | 1,645 |
| Connections Failed | 3,355 (67.1%) |
| Connect Time | 2m 15s |
| Messages Received | 116,755 |
| Throughput | 11,676 msg/sec |
| P50 Latency | 1m 17s |
| P95 Latency | 2m 13s |
| P99 Latency | 2m 21s |

### Load Test Analysis Summary

| Metric | 1K Clients | 5K Clients | 10K Clients |
|:---|:---|:---|:---|
| Connection Rate | ~25/sec | ~31/sec | ~24/sec |
| P99 Latency | 45 sec | 2m 10s | 4m 3s |
| Throughput | 22,224 msg/s | 10,200 msg/s | 3,912 msg/s |
| Failure Rate | 0.5% | 4.5% | 42.2% |

### Four Catastrophic Systemic Failures Identified

1. **Connection Establishment Serialization:** Hard bottleneck at ~25-30 conns/sec. Market open requiring 5,000 reconnects in 5s takes ~3 minutes.
2. **Catastrophic Latency Accumulation:** P99 latency in MINUTES despite 10s test duration. Messages queued indefinitely rather than dropped.
3. **Throughput Collapse (O(N) Degradation):** 10K clients receive 80% FEWER total messages/sec than 1K clients. CPU consumed by GC thrashing.
4. **Connection Failures:** 42.2% failure rate at 10K clients. Resource exhaustion before reaching target.

---

## 8. Architecture Reviews

### Review Summary

| Document | Component | Critical | High | Medium | Status |
|:---|:---|:---:|:---:|:---:|:---|
| ARCHITECTURE_REVIEW | End-to-End | 4 | 0 | 2 | Not production-ready |
| CODEBASE_ARCHITECTURE_REVIEW | Go Implementation | 2 | 0 | 4 | Not production-ready |
| FEED_REVIEW | Feed & Distribution | 4 | 0 | 12 | Cannot sustain 10k/sec |
| PUBLISHER_REVIEW | Topic Manager Design | 0 | 0 | 4 | Design document |
| TOPIC_MANAGER_REVIEW | Topic Manager | 2 | 7 | 4 | Not production-ready |
| WEBSOCKET_GATEWAY_REVIEW | WebSocket Gateway | 1 | 8 | 4 | Critical fix required |
| OPTIMIZATION_REVIEW | sync.Map Optimization | 3 | 0 | 3 | Perf win, correctness fail |
| load_test_analysis | Load Test Results | 4 | 0 | 3 | Fails at every scale |
| BACKPRESSURE_REVIEW | Backpressure | 3 | 2 | 1 | Critical bugs found |
| SUBSCRIBER_BUFFERING_REVIEW | Client Queues | 1 | 3 | 5 | Cardinality explosion |
| rate_limiting_review | Rate Limiter | 2 | 0 | 5 | Race + memory leak |
| observability_review | Observability | 0 | 0 | 4 | Missing metrics |

### Critical Findings Across All Reviews

**Architecture:**
- 4 design mistakes: God Publisher bottleneck, undefined backpressure, conflating topics with transport, missing system-wide cancellation
- 10-second WebSocket death: HTTP WriteTimeout cancels r.Context(), killing all WS connections after 10s

**Concurrency:**
- Worker Pool: Enqueue after Shutdown panics (send on closed channel), goroutine leak on timeout
- Backpressure: Missed wakeup (consumer starvation), hot-loop ring drain (broken DropOldest), disconnect policy bypass
- Rate Limiter: AllowSubscription race condition, client map never evicts stale entries (memory leak)
- sync.Map optimization: Lost updates (RCU race), initialization race, ghost subscriber (delete race)

**Performance:**
- O(N) JSON serialization causes non-linear degradation past 100 subscribers
- Per-client Prometheus labels create unbounded cardinality (will OOM Prometheus)
- Buffer stack allocates 315 MB at 5K clients (23% over budget)

---

## 9. Known Issues and Technical Debt

### Critical (Must Fix Before Week 3)

| ID | Component | Issue | Status |
|:---|:---|:---|:---|
| C1 | WebSocket Gateway | `r.Context()` + WriteTimeout kills all WS connections after 10s | ✅ Fixed |
| C2 | Worker Pool | `Enqueue` after `Shutdown` panics: send on closed channel | ✅ Fixed |
| C3 | Backpressure | Missed wakeup causes consumer starvation/deadlock | ✅ Fixed |
| C4 | Backpressure | Hot-loop ring drain makes Ring buffer useless | ✅ Fixed |
| C5 | Rate Limiter | `AllowSubscription` race: `activeSubs` check+increment not atomic | ✅ Fixed |
| C6 | Rate Limiter | Client map grows monotonically, no TTL eviction | ✅ Fixed |
| C7 | ClientQueue | Per-client Prometheus labels create unbounded cardinality | ✅ Fixed |

### High Priority

| ID | Component | Issue | Status |
|:---|:---|:---|:---|
| H1 | Worker Pool | Bridge goroutine leak on shutdown timeout | ✅ Fixed |
| H2 | Worker Pool | Workers ignore `ctx.Done()`, stuck in blocking Publish | ✅ Fixed |
| H3 | Backpressure | Disconnect policy bypassed (drops happen in forwardLoop, not ring) | ✅ Fixed |
| H4 | Backpressure | `consecutiveDrops` never reset on success | ✅ Fixed |
| H5 | ClientQueue | `Queue.Close()` ordering loses buffered events | ✅ Fixed |
| H6 | Gateway | Double `conn.Close()` breaks graceful close handshake | ✅ Fixed |
| H7 | Gateway | `send` channel allocated but never used (256 MB wasted at 5K) | ✅ Fixed |

### Medium Priority

| ID | Component | Issue | Status |
|:---|:---|:---|:---|
| M1 | Gateway | Map never shrinks after disconnect storm | ✅ Fixed |
| M2 | Gateway | Control channel drops subscription confirmations | ✅ Fixed |
| M3 | ClientQueue | `Manager.mu` global lock (should shard like TopicManager) | ✅ Fixed |
| M4 | Rate Limiter | Rate check and cap check are uncorrelated | ✅ Fixed |
| M5 | Rate Limiter | Global RWMutex on client map (should shard) | ✅ Fixed |
| M6 | Benchmarks | Subscribe 100 topics = 411 allocs (should batch by shard) | ✅ Fixed |

---

## 10. Week 4+ Roadmap

### Week 3 -- Distributed Systems (Days 15-21) ✅ COMPLETED

| Day | Task | Status |
|:---|:---|:---|
| 15 | Redis Pub/Sub | ✅ Fully implemented with async workers, topic-based channels, staleness detection |
| 16 | Multi-Gateway | ✅ 3 deployment modes, Nginx load balancing, up to 5 gateways |
| 17 | Sticky Sessions | ✅ Stateless fat-client pattern with gateway ID headers |
| 18 | Service Discovery | ✅ Redis TTL registry, heartbeat, health-aware deregistration |
| 19 | Distributed Topic Routing | ✅ Sharded DistributedRouter, dynamic Redis subscriptions, topic groups |
| 20 | Chaos Testing | ✅ Shell framework + Go tests (6 scenarios), all PASSED |
| 21 | Distributed Benchmark + Scaling Reports | ✅ Dedicated client, orchestration for 1/3/5 gateways, full scaling analysis |

### Week 4 -- Reliability (Days 22-28)

| Day | Task |
|:---|:---|
| 22 | Persistent Event Log (PostgreSQL / BadgerDB) |
| 23 | Replay API |
| 24 | Snapshot Service (new subscribers get latest price) |
| 25 | Recovery After Restart |
| 26 | Dead Connection Detection (heartbeat protocol) |
| 27 | Automatic Reconnect + Subscription Restoration |
| 28 | Message Ordering Guarantees |

### Week 5 -- Production Engineering (Days 29-35)

| Day | Task |
|:---|:---|
| 29 | Structured Logging (slog or zap) |
| 30 | Distributed Tracing (OpenTelemetry) |
| 31 | Grafana Dashboards |
| 32 | Health Checks (/health, /readiness, /liveness) |
| 33 | Configuration System (YAML + env overrides) |
| 34 | Docker Compose (one-command deployment) |
| 35 | Kubernetes Deployment |

### Week 6 -- Trading Infrastructure Features (Days 36-42)

| Day | Task |
|:---|:---|
| 36 | Market Sessions (PRE-MARKET, OPEN, CLOSED) |
| 37 | Symbol Metadata Service |
| 38 | Aggregation Engine (OHLC candlesticks) |
| 39 | Top Movers (gainers/losers) |
| 40 | Watchlists |
| 41 | Alert Engine (price threshold triggers) |
| 42 | Portfolio Stream (real-time PnL) |

### Week 7 -- Resume Gold (Days 43-49)

| Day | Task |
|:---|:---|
| 43 | Benchmark Suite (100K updates/sec, 10K subscribers target) |
| 44 | Architecture Diagrams |
| 45 | Design Document (architecture, tradeoffs, scaling strategy) |
| 46 | Failure Analysis Document |
| 47 | Performance Tuning (pprof) |
| 48 | Optimization Report |
| 49 | Resume Bullet Creation |

---

## Appendix: Prometheus Metrics Inventory

### Feed Layer (3 metrics)
- `rtmds_feed_messages_received_total` (CounterVec: provider)
- `rtmds_feed_errors_total` (CounterVec: provider, kind)
- `rtmds_feed_data_staleness_seconds` (Gauge)

### Distribution Layer (4 metrics)
- `rtmds_distribution_broadcasts_total` (Counter)
- `rtmds_distribution_events_dropped_total` (Counter)
- `rtmds_distribution_subscribers_active` (Gauge)
- `rtmds_distribution_subscription_events_total` (CounterVec: action)

### WebSocket Layer (13 metrics)
- `rtmds_websocket_connections_active` (Gauge)
- `rtmds_websocket_connections_opened_total` (Counter)
- `rtmds_websocket_connections_closed_total` (Counter)
- `rtmds_websocket_connection_attempts_total` (Counter)
- `rtmds_websocket_active_subscriptions` (Gauge)
- `rtmds_websocket_slow_consumers` (Gauge)
- `rtmds_websocket_messages_written_total` (Counter)
- `rtmds_websocket_write_errors_total` (Counter)
- `rtmds_websocket_bytes_sent_total` (Counter)
- `rtmds_websocket_message_size_bytes` (Histogram)
- `rtmds_websocket_delivery_latency_seconds` (Histogram)
- `rtmds_websocket_ping_latency_seconds` (Histogram)
- `rtmds_websocket_auth_failures_total` (Counter)
- `rtmds_websocket_handshake_duration_seconds` (Histogram)

### Transport Layer (2 metrics)
- `rtmds_http_requests_total` (CounterVec: method, route, status)
- `rtmds_http_request_duration_seconds` (HistogramVec: method, route)

### Topic Layer (2 metrics)
- `rtmds_topic_publish_latency_seconds` (Histogram)
- `rtmds_topic_publish_operations_total` (Counter)

### Client Queue (5 metrics)
- `rtmds_client_queue_enqueued_total` (Counter)
- `rtmds_client_queue_sent_total` (Counter)
- `rtmds_client_queue_dropped_total` (Counter)
- `rtmds_client_queue_closed_total` (Counter)
- `rtmds_client_queue_total_depth` (Gauge)

### Backpressure (5 metrics)
- `rtmds_backpressure_events_dropped_total` (Counter)
- `rtmds_backpressure_buffer_occupancy_ratio` (Histogram)
- `rtmds_backpressure_consecutive_drops` (Gauge)
- `rtmds_backpressure_consumer_disconnects_total` (Counter)
- `rtmds_backpressure_send_latency_seconds` (Histogram)

### Worker Pool (8 metrics)
- `rtmds_worker_tasks_received_total` (Counter)
- `rtmds_worker_tasks_completed_total` (Counter)
- `rtmds_worker_tasks_failed_total` (Counter)
- `rtmds_worker_tasks_dropped_total` (Counter)
- `rtmds_worker_active_workers` (Gauge)
- `rtmds_worker_queue_depth` (Gauge)
- `rtmds_worker_task_duration_seconds` (Histogram)
- `rtmds_worker_queue_wait_seconds` (Histogram)

### Rate Limiter (4 metrics)
- `rtmds_ratelimit_connect_allowed_total` / `_rejected_total` (Counter)
- `rtmds_ratelimit_subscribe_allowed_total` / `_rejected_total` (Counter)
- `rtmds_ratelimit_unsubscribe_allowed_total` / `_rejected_total` (Counter)
- `rtmds_ratelimit_hits_total` (CounterVec: type)

### Build Info (1 metric)
- `rtmds_build_info` (GaugeVec: version, revision)

**Total: 47 metrics** with bounded cardinality (no per-symbol, per-client, or per-IP labels).

---

## 11. Multi-Gateway Deployment (New)

### Overview

The system now supports horizontal scaling with multiple independent gateway instances sharing a single Redis Pub/Sub bus.

### Architecture

```text
                    Feed Generator
                           ↓
                       Publisher
                           ↓
                    Redis Pub/Sub
                      ↓       ↓
               Gateway 1   Gateway 2   Gateway 3
               (primary)   (subscriber) (subscriber)
                   ↓           ↓           ↓
               Clients     Clients     Clients
```

### Configuration

| Variable | Gateway 1 | Gateway 2 | Gateway 3 | Description |
|----------|-----------|-----------|-----------|-------------|
| `RTMDS_SERVER_PORT` | 9091 | 9092 | 9093 | HTTP/WebSocket listen port |
| `RTMDS_REDIS_ENABLED` | true | true | true | Enable Redis Pub/Sub |
| `RTMDS_REDIS_ADDR` | redis:6379 | redis:6379 | redis:6379 | Redis connection address |
| `RTMDS_FEED_ENABLED` | true | false | false | Run feed generator |
| `RTMDS_FEED_SYMBOLS` | AAPL,MSFT,... | AAPL,MSFT,... | AAPL,MSFT,... | Symbols to stream |

### Launch

```bash
# Build and start 3 gateways + Redis
docker compose -f docker-compose.multigateway.yml up --build -d

# Or use Makefile
make gw-up
```

### Verify

```bash
# Health check all gateways
make gw-health

# Test WebSocket connectivity
make gw-test

# View logs
make gw-logs
```

### Test End-to-End

```bash
# Connect to any gateway
wscat -c ws://localhost:9091/ws
> {"action":"subscribe","symbols":["AAPL"]}

# All gateways receive the same data from Redis
wscat -c ws://localhost:9092/ws
> {"action":"subscribe","symbols":["AAPL"]}
```

### Components by Gateway

| Gateway | Feed | Pipeline | Redis Pub | Redis Sub | WS Gateway |
|---------|:----:|:--------:|:---------:|:---------:|:----------:|
| Gateway 1 (primary) | Yes | Yes | Yes | Yes | Yes |
| Gateway 2 (subscriber) | No | No | No | Yes | Yes |
| Gateway 3 (subscriber) | No | No | No | Yes | Yes |

### Files

- `docker-compose.multigateway.yml` — Docker Compose for 3 gateways + Redis
- `internal/config/config.go` — `feed.enabled` option
- `internal/app/app.go` — Shared Redis client, feed-gated pipeline
- `docs/design/MULTI_GATEWAY_ARCHITECTURE.md` — Full architecture doc with Quick Start

### Tear Down

```bash
docker compose -f docker-compose.multigateway.yml down -v

# Or use Makefile
make gw-down
```
