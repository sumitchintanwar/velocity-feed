# Subscriber Buffering Design — Architecture Review

**Reviewer perspective:** Principal Distributed Systems Engineer  
**Target:** `internal/clientqueue` + `internal/backpressure` + integration in `internal/topicmanager`  
**Assumed load:** 5,000 concurrent clients, 10,000+ updates/sec  
**Focus:** Memory risks, scalability issues, queue growth problems

---

## Architecture Summary — The Buffer Chain

The system now has a **4-layer buffer stack** for `PolicyDropOldest` and `PolicyDisconnect`:

```
TopicManager.Publish()
       │
       ▼
┌──────────────────────────────┐
│ Layer 1: Ring Buffer         │  backpressure.Ring
│ cap: 256 (rounded to 256)    │  256 × MarketEvent (~200B)
│ overwrite-oldest on full     │  = 50 KB per client
└───────────┬──────────────────┘
            │ forwardLoop goroutine
            ▼
┌──────────────────────────────┐
│ Layer 2: forwardC channel    │  chan MarketEvent
│ cap: min(256, 64) = 64       │  64 × MarketEvent
│ non-blocking send from ring  │  = 12.5 KB per client
└───────────┬──────────────────┘
            │ clientqueue.Queue.C()
            ▼
┌──────────────────────────────┐
│ Layer 3: Handle.C()          │  Returned to writePump
│ (this IS forwardC)           │  Same channel, no copy
└───────────┬──────────────────┘
            │ writePump select loop
            ▼
┌──────────────────────────────┐
│ Layer 4: bytes.Buffer pool   │  Per-write JSON encoding
│ Pooled via sync.Pool         │  Amortized across clients
└──────────────────────────────┘
```

For `PolicyDropNewest`, the chain is simpler — just a single `chan MarketEvent` of capacity 256.

---

## 1. Memory Risks

### Risk M1 — Buffer Stack Multiplication (Medium)

The design document [Per-Subscriber_Buffering_Design.md](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/docs/Per-Subscriber_Buffering_Design.md) models memory as:

```
Clients × QueueSize × MessageSize
5,000 × 256 × 200B = 256 MB
```

But the actual implementation allocates **two** buffers per client for DropOldest/Disconnect:

| Buffer | Per-Client | At 5,000 Clients |
|:---|---:|---:|
| Ring (`[]MarketEvent`, cap 256) | ~50 KB | **250 MB** |
| forwardC (`chan MarketEvent`, cap 64) | ~12.5 KB | **62.5 MB** |
| Go channel overhead (hchan struct) | ~96 B | ~0.5 MB |
| dropBucketWindow (10 × int64) | 160 B | ~0.8 MB |
| sync.Mutex + sync.Cond + atomics | ~200 B | ~1 MB |
| **Total per-client** | **~63 KB** | **~315 MB** |

The design document assumes 256 MB. The actual implementation allocates **315 MB** — 23% over budget. The Ring and forwardC together hold 320 events worth of data, not 256.

**Why this matters:** At 5,000 clients, the buffer memory is the dominant allocation in the process. A 23% overrun isn't catastrophic, but it means memory projections in capacity planning documents are wrong. If the QueueSize is increased to 500 (the upper recommendation), the overrun grows proportionally.

---

### Risk M2 — Per-Client Prometheus Label Cardinality Explosion (Critical)

**File:** [queue.go:184–214](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/clientqueue/queue.go#L184-L214)

```go
enqueuedTotal: f.NewCounterVec(prometheus.CounterOpts{
    Name: "enqueued_total",
}, []string{"client_id"}),
```

Every `QueueMetrics` struct creates 5 metric vectors (`CounterVec`, `GaugeVec`) with `client_id` as a label dimension. Each call to `.WithLabelValues(q.id).Inc()` creates a new time series in Prometheus.

**At 5,000 clients:**
- 5 metrics × 5,000 unique `client_id` values = **25,000 time series**
- Plus the `backpressure.Metrics` (5 more metrics, but these are singletons — shared across all channels)

**Memory cost in Prometheus:** Each time series costs approximately 1–3 KB of memory in Prometheus's TSDB. 25,000 series ≈ **25–75 MB** in the Prometheus server, plus scrape overhead.

**The real problem:** When clients disconnect, their `client_id` label values are **never cleaned up** from the Prometheus registry. `Manager.Remove()` calls `q.Close()`, but Prometheus `CounterVec` and `GaugeVec` do not support deleting label values. Over time, with client churn (connect → disconnect → reconnect with new UUID), the number of unique `client_id` values grows monotonically. After 24 hours of operation with moderate churn:

```
5,000 concurrent × 10 reconnects/day = 50,000 unique label values
50,000 × 5 metrics = 250,000 stale time series
```

This is a well-known anti-pattern in Prometheus: **unbounded label cardinality**. It causes Prometheus scrape slowdowns, increased memory consumption, and eventually OOM on the Prometheus server.

**Fix:** Remove the `client_id` label entirely. Use aggregate counters (total across all clients) and expose per-client stats via a dedicated `/debug/queues` HTTP endpoint instead of Prometheus labels. Alternatively, use `prometheus.DeleteLabelValues()` in `Queue.Close()` — but this requires `CounterVec.DeleteLabelValues()` support which is fragile in practice.

---

### Risk M3 — Ring Buffer Retains MarketEvent References After Pop (Low)

**File:** [ring.go:79–91](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/backpressure/ring.go#L79-L91)

```go
func (r *Ring) Pop() (marketdata.MarketEvent, bool) {
    // ...
    ev := r.buf[r.tail&r.mask]
    r.tail++
    return ev, true
}
```

After `Pop()`, the slot `r.buf[oldTail&r.mask]` still holds a reference to the `MarketEvent` interface value. If `MarketEvent` is a `*Quote` (pointer type), the ring buffer prevents the GC from collecting it until the slot is overwritten by a future `Push()`.

**At steady state:** This doesn't matter — the ring is continuously overwritten. But during idle periods (market close, weekends), all 256 slots hold references to the last 256 events. For 5,000 clients, that's `5,000 × 256 = 1,280,000` events pinned in memory that could otherwise be collected.

**Fix:** Zero the slot after Pop:
```go
r.buf[r.tail&r.mask] = nil
r.tail++
```

---

## 2. Scalability Issues

### Issue S1 — forwardLoop Goroutine Per Client (Medium)

**File:** [channel.go:116–118](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/backpressure/channel.go#L116-L118)

```go
case PolicyDropOldest, PolicyDisconnect:
    c.ring = NewRing(cfg.BufferSize)
    c.forwardC = make(chan marketdata.MarketEvent, min(cfg.BufferSize, 64))
    c.wg.Add(1)
    go c.forwardLoop()
```

Each `DropOldest` or `Disconnect` channel spawns a dedicated goroutine for forwarding events from the Ring to forwardC. At 5,000 clients, that's 5,000 `forwardLoop` goroutines.

**Combined goroutine count:**

| Component | Goroutines | Total at 5,000 |
|:---|---:|---:|
| readPump (per client) | 1 | 5,000 |
| writePump (per client) | 1 | 5,000 |
| forwardLoop (per client) | 1 | 5,000 |
| Worker Pool | 8 | 8 |
| Pipeline | 1 | 1 |
| HTTP server | goroutine-per-request | variable |
| **Total** | | **~15,008** |

Each goroutine costs ~2 KB of stack (Go's minimum). 15,000 goroutines = 30 MB of stack memory. The Go scheduler handles 15,000 goroutines without issue on modern hardware, but the forwardLoop goroutines are largely idle (parked on `sync.Cond.Wait`), contributing scheduling overhead without proportional benefit.

**The design question:** Does the Ring + forwardLoop architecture actually provide value over a plain `chan MarketEvent` with non-blocking send?

For `PolicyDropOldest`: **Yes, in theory.** A plain channel can only drop-newest (via `select/default`). The ring buffer enables drop-oldest. But as identified in the backpressure review, the current `forwardLoop` has a `default` case that drops events instead of blocking, which means the ring never actually buffers anything. Once that bug is fixed (forwardLoop blocks on forwardC), the architecture is sound.

For `PolicyDropNewest`: The code correctly uses a plain `chan MarketEvent` — no ring, no forwardLoop. This is the right design.

---

### Issue S2 — Manager.mu Global Lock on Queue Lifecycle (Medium)

**File:** [queue.go:222–243](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/clientqueue/queue.go#L222-L243)

```go
type Manager struct {
    mu      sync.RWMutex
    queues  map[string]*Queue
    // ...
}
```

`Manager.Create()` and `Manager.Remove()` both take an exclusive lock on `mu`. `Manager.Get()` takes a read lock. During market open (5,000 connections in ~10 seconds), `Create()` is called 5,000 times, each acquiring the global write lock.

**This is the same pattern as the original `subMu` in the TopicManager** — a single global lock serializing all client lifecycle operations. The TopicManager fixed this by sharding the subscriber registry into 16 sub-shards. The `clientqueue.Manager` has not.

**Impact:** At 500 creates/sec, each holding the lock for ~1 µs (map insert + queue construction), total lock hold time is 500 µs/sec = 0.05%. Not catastrophic, but as queue creation becomes more expensive (e.g., if Prometheus registration grows), this will become the bottleneck.

---

### Issue S3 — AggregateStats Iterates All Queues Under Lock (Low)

**File:** [queue.go:312–332](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/clientqueue/queue.go#L312-L332)

```go
func (m *Manager) AggregateStats() AggregateStats {
    m.mu.RLock()
    defer m.mu.RUnlock()

    for _, q := range m.queues {
        stats.TotalEnqueued += q.enqueued.Load()
        stats.TotalSent += q.sent.Load()
        stats.TotalDropped += q.TotalDropped()
        stats.TotalDepth += q.Len()
    }
    // ...
}
```

This iterates all 5,000 queues under a read lock. Each iteration loads 4 atomics and calls `q.Len()` (which acquires the Ring's mutex). At 5,000 clients, this function takes approximately:

```
5,000 × (4 atomic loads + 1 mutex acquire) ≈ 5,000 × 50 ns = 250 µs
```

If `AggregateStats()` is called from a Prometheus scrape endpoint (every 15 seconds), the 250 µs read lock blocks all `Create()` and `Remove()` operations. Under normal conditions this is acceptable, but if the stats endpoint is scraped more frequently (e.g., Grafana at 1s intervals), it creates measurable contention.

---

### Issue S4 — Manager.CloseAll Sequential Drain (Medium)

**File:** [queue.go:335–347](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/clientqueue/queue.go#L335-L347)

```go
func (m *Manager) CloseAll() {
    // ... copies queue slice under lock ...
    for _, q := range queues {
        q.Close()  // sequential, one at a time
    }
}
```

`q.Close()` calls `c.wg.Wait()` which waits for the `forwardLoop` goroutine to exit. If the forwardLoop is blocked on `forwardC <- ev` (which it should be after the bug fix), each `Close()` must wait for the forwardLoop to wake up, check `stopCh`, and exit.

At 5,000 clients, closing sequentially takes:

```
5,000 × (Broadcast wakeup + goroutine exit) ≈ 5,000 × 10 µs = 50 ms
```

This is acceptable for a graceful shutdown. But if any `forwardLoop` is stuck (e.g., due to the missed-wakeup bug), `CloseAll` blocks indefinitely.

**Fix:** Close all queues concurrently with a `sync.WaitGroup`, and add a timeout.

---

## 3. Queue Growth Problems

### Issue QG1 — Legacy Channel Path Has No Drop Tracking (Medium)

When the TopicManager is created with `New()` (not `NewWithQueue()`), subscribers get a plain `chan MarketEvent` of capacity 256:

```go
sub.ch = make(chan marketdata.MarketEvent, defaultBuffer)
```

And the publish path does:

```go
select {
case sub.ch <- event:
default:
    // silently dropped — no metric, no log, no counter
}
```

**The problem:** There is no observability for drops on the legacy path. If the system is running in legacy mode (no clientqueue), a slow subscriber's events are silently dropped with no metric, no counter, no alert. The design document describes "Queue-Based Backpressure" with metrics at every layer, but the legacy path provides none.

---

### Issue QG2 — Queue.Close() Ordering vs writePump (Medium)

**File:** [queue.go:154–161](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/clientqueue/queue.go#L154-L161)

```go
func (q *Queue) Close() {
    q.closeOnce.Do(func() {
        q.closed.Store(true)     // 1. Reject new sends
        q.ch.Close()             // 2. Stop forwardLoop, close forwardC
        close(q.closeCh)         // 3. Signal Done()
        // ...
    })
}
```

The WebSocket `writePump` selects on `handle.Done()` (which returns `q.closeCh`) to detect shutdown. The ordering is:

1. `q.ch.Close()` stops the forwardLoop and closes `forwardC`.
2. `close(q.closeCh)` fires `Done()`.

But `backpressure.Channel.Close()` internally does:

```go
close(c.stopCh)          // signal forwardLoop
c.hasData.Broadcast()    // wake it
c.wg.Wait()              // wait for it to exit
// drain remaining ring events into forwardC
close(c.forwardC)        // close the consumer channel
```

After `close(c.forwardC)`, any pending events in forwardC are still readable. The writePump sees `Done()` fire (from step 3 in Queue.Close) and may exit its select loop before reading the remaining events from forwardC.

**Result:** Events buffered in forwardC at the moment of shutdown are lost. The writePump exits before draining them.

**Severity:** Medium. During normal shutdown, the number of events in forwardC is typically small (0–64). But during a burst-then-shutdown scenario, up to 64 events may be lost per client.

**Fix:** The writePump should drain `handle.C()` after `Done()` fires, before exiting. Or: `Queue.Close()` should close `closeCh` first (so writePump knows shutdown is coming), then close the backpressure channel, and writePump should continue reading from `C()` until the channel is closed.

---

## Summary Table

| ID | Category | Severity | Finding |
|:---|:---|:---:|:---|
| **M2** | Memory | **Critical** | Per-client Prometheus labels create unbounded metric cardinality. 5,000 clients × 5 metrics = 25,000 series. With churn, grows monotonically. Will OOM the Prometheus server. |
| M1 | Memory | Medium | Buffer stack allocates 315 MB at 5,000 clients, 23% over the 256 MB design budget (Ring + forwardC double-buffer). |
| S1 | Scalability | Medium | 5,000 forwardLoop goroutines (one per client). Adds 10 MB stack + scheduling overhead. Necessary for DropOldest but adds to the 15,000 total goroutine count. |
| S2 | Scalability | Medium | `Manager.mu` is a global lock on queue lifecycle — same anti-pattern as the original `subMu`. Should be sharded. |
| S4 | Scalability | Medium | `CloseAll` drains queues sequentially. Blocks if any forwardLoop is stuck. Should close concurrently with a timeout. |
| QG1 | Queue Growth | Medium | Legacy (non-clientqueue) path silently drops events with zero observability. No metric, no counter, no alert. |
| QG2 | Queue Growth | Medium | `Queue.Close()` ordering causes writePump to exit before draining forwardC. Up to 64 events lost per client on shutdown. |
| M3 | Memory | Low | Ring buffer retains MarketEvent references after Pop. Prevents GC during idle periods. ~250 MB of pinned dead references at 5,000 clients. |
| S3 | Scalability | Low | `AggregateStats` iterates 5,000 queues under read lock. 250 µs hold time blocks Create/Remove. |
