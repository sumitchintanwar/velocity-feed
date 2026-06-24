# Rate Limiting Implementation — Architecture Review

**Reviewer perspective:** Principal Distributed Systems Engineer  
**Target:** [bucket.go](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/ratelimit/bucket.go), [limiter.go](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/ratelimit/limiter.go), [config.go](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/ratelimit/config.go)  
**Assumed load:** 5,000+ concurrent clients, 10,000+ updates/sec  
**Focus:** Bypass possibilities, memory growth, scalability

---

## Verdict

The token bucket algorithm is correctly implemented: refill logic is sound, burst capping works, and per-client isolation is proven by a thorough test suite. The Prometheus metrics correctly use aggregate counters (no per-client labels) — a lesson learned from the clientqueue cardinality explosion.

However, the implementation has **two bypass vulnerabilities** that allow a determined client to exceed its limits, **one memory leak** that grows linearly with client churn, and a **concurrency bug** in the subscription cap that can be exploited under parallel requests.

---

## 1. Bypass Possibilities

### Bypass B1 — AllowSubscription Race Condition (Critical)

**File:** [limiter.go:74–87](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/ratelimit/limiter.go#L74-L87) and [limiter.go:90–97](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/ratelimit/limiter.go#L90-L97)

```go
func (l *Limiter) AllowSubscription(clientID string) bool {
    // ...
    cl := l.getOrCreate(clientID)        // acquires l.mu
    if cl.activeSubs >= l.cfg.MaxSubscriptions {
        return false
    }
    cl.activeSubs++                      // NOT under any lock
    return true
}

func (l *Limiter) ReleaseSubscription(clientID string) {
    l.mu.RLock()
    cl, ok := l.clients[clientID]
    l.mu.RUnlock()
    if ok && cl.activeSubs > 0 {
        cl.activeSubs--                  // NOT under any lock
    }
}
```

**The bug:** `getOrCreate` acquires `l.mu` (write lock) to find or create the `clientLimits` struct, then releases it. The subsequent `cl.activeSubs >= MaxSubscriptions` check and `cl.activeSubs++` increment happen **with no lock held**.

If two goroutines call `AllowSubscription("c1")` concurrently when `activeSubs = MaxSubscriptions - 1`:

1. Thread A: `getOrCreate("c1")` returns `cl` (lock released)
2. Thread B: `getOrCreate("c1")` returns same `cl` (lock released)
3. Thread A: `cl.activeSubs (999) >= 1000` → false → `cl.activeSubs++` → 1000
4. Thread B: `cl.activeSubs (999) >= 1000` → false → `cl.activeSubs++` → 1001

**Result:** Client exceeds `MaxSubscriptions` by 1. Under sustained parallel requests from a multi-threaded client, the cap is progressively bypassed.

The same race exists on `ReleaseSubscription` — two concurrent releases can decrement `activeSubs` below zero (the `> 0` check is not atomic with the decrement).

**Fix:** Either:
- Use `atomic.Int64` for `activeSubs` with `CompareAndSwap` for the check-then-increment
- Hold the Bucket's `mu` (or a per-client mutex) across the check+increment

---

### Bypass B2 — Uncorrelated Rate Check and Subscription Cap (Medium)

The design document describes a multi-layer protection model:

```
Layer 2: Subscribe Requests (rate limit — AllowSubscribe)
Layer 3: Maximum Active Subscriptions (cap — AllowSubscription)
```

These are two independent calls. The current code (not yet integrated into the gateway) will presumably call them like:

```go
if !limiter.AllowSubscribe(clientID) {
    reject("rate limited")
}
if !limiter.AllowSubscription(clientID) {
    reject("too many subscriptions")
}
tm.Subscribe(clientID, symbols...)
```

**The bypass:** A client can call `AllowSubscribe` (consuming a rate token) without ever calling `AllowSubscription` — or vice versa. The checks are not transactional. Specifically:

1. Client sends 100 subscribe requests rapidly.
2. `AllowSubscribe` passes for the first 100 (burst = 100).
3. `AllowSubscription` rejects 95 of them (MaxSubscriptions = 5).
4. The rate limiter has consumed 100 tokens, but only 5 subscriptions were created.
5. Client must now wait for tokens to refill — **punished for being rate-limited by the cap**, not by actual abuse.

This isn't a bypass per se, but it means the two limits interfere destructively. A legitimate client reconnecting and subscribing to 50 symbols may exhaust rate tokens on attempts that are rejected by the cap, then be unable to subscribe to anything until tokens refill.

**Fix:** Combine the checks into a single `AllowAndSubscribe(clientID, n int)` method that atomically verifies both the rate limit and the cap.

---

### Bypass B3 — Identity Rotation (Not Yet Exploitable)

**File:** [limiter.go:116–133](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/ratelimit/limiter.go#L116-L133)

The rate limiter is keyed by `clientID`. If a client can connect with a new UUID on each connection (which is the current WebSocket gateway design — `uuid.New()` per connection), it gets fresh buckets every time.

**Current status:** The rate limiter is not yet wired into the gateway, so this is a design concern, not an active exploit. But when integrating:

- If `clientID = conn.RemoteAddr()`, clients behind NAT share a single bucket (unfair).
- If `clientID = uuid.New()` (current gateway design), every reconnect gets fresh tokens (bypassable).
- If `clientID = auth_token` (not implemented), proper per-user limiting is achieved.

**Recommendation:** The design document specifies "per client identity" rate limiting. This requires authentication. Without it, the rate limiter can always be bypassed by reconnecting.

---

### Bypass B4 — Connection-Level Rate Limiting Missing

The design document specifies Layer 1: "10 New Connections/sec per client identity." This is not implemented. `Limiter` only tracks subscribe/unsubscribe/subscription-cap. A client can open thousands of WebSocket connections without any rate limiting.

This is the most impactful gap — connection churn is the primary DoS vector for WebSocket gateways. Connection establishment involves TLS handshake + HTTP upgrade + gorilla/websocket `Upgrade()` + two goroutine spawns. This is vastly more expensive than a subscribe request.

---

## 2. Memory Growth

### Issue MG1 — Client Map Never Evicts Stale Entries (Critical)

**File:** [limiter.go:116–133](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/ratelimit/limiter.go#L116-L133)

```go
func (l *Limiter) getOrCreate(clientID string) *clientLimits {
    // ... creates entry if not found ...
    cl = newClientLimits(l.cfg)
    l.clients[clientID] = cl
}
```

`RemoveClient` exists, but it is only called if the gateway explicitly invokes it on disconnect. If the gateway fails to call `RemoveClient` (e.g., due to a panic in the disconnect handler, or if the client's goroutine leaks), the entry persists forever.

**Per-client memory cost:**

| Field | Size |
|:---|---:|
| Map entry (string key + pointer) | ~64 B |
| `clientLimits` struct | ~32 B |
| `subscribe` Bucket (mutex + 4 floats + time) | ~88 B |
| `unsubscribe` Bucket | ~88 B |
| **Total per client** | **~272 B** |

At 5,000 concurrent clients with 10 reconnects/day:

```
Day 1: 50,000 entries × 272 B = 13.6 MB
Day 7: 350,000 entries × 272 B = 95 MB
Day 30: 1,500,000 entries × 272 B = 408 MB
```

This is a **slow memory leak** that will eventually consume significant memory.

**Fix:** Add a background goroutine that periodically scans `l.clients` and evicts entries that haven't been accessed (no `Allow*` call) within a configurable TTL (e.g., 5 minutes). Use a `lastAccessed time.Time` field on `clientLimits`.

---

## 3. Scalability

### Issue SC1 — Global RWMutex on Client Map (Medium)

**File:** [limiter.go:116–133](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/ratelimit/limiter.go#L116-L133)

```go
func (l *Limiter) getOrCreate(clientID string) *clientLimits {
    l.mu.RLock()
    cl, ok := l.clients[clientID]
    l.mu.RUnlock()
    if ok {
        return cl
    }

    l.mu.Lock()
    cl, ok = l.clients[clientID]
    if !ok {
        cl = newClientLimits(l.cfg)
        l.clients[clientID] = cl
    }
    l.mu.Unlock()
    return cl
}
```

The double-checked locking pattern is correct. In steady state (all clients already connected), `getOrCreate` only takes an `RLock` — multiple goroutines can proceed in parallel.

**The problem:** During market open (5,000 new connections in 10 seconds), every first call per client takes the write lock (`l.mu.Lock()`). 5,000 write locks × ~100 ns each = ~500 µs of serialized time. Not catastrophic, but identical to the pattern fixed in TopicManager by sharding.

**At scale:** `AllowSubscribe` is called from the client's `readPump` goroutine. With 5,000 clients sending subscribe requests concurrently, all 5,000 goroutines compete for `l.mu.RLock()`. Go's `RWMutex` writer-preference means any new client connection (write lock) temporarily blocks all concurrent `AllowSubscribe` calls.

**Fix:** Shard the client map. Use `fnv1a(clientID) & shardMask` to distribute clients across 16 sub-maps, each with its own lock. This mirrors the TopicManager shard design and reduces write-lock contention by 16×.

---

### Issue SC2 — time.Now() Called on Every Allow() (Low)

**File:** [bucket.go:76–88](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/ratelimit/bucket.go#L76-L88)

```go
func (b *Bucket) refill() {
    now := time.Now()
    elapsed := now.Sub(b.lastRefill).Seconds()
    // ...
}
```

`time.Now()` is a vDSO call on Linux but a syscall on Windows. On your i5-1135G7 (Windows), each `time.Now()` costs ~25–50 ns. With two buckets per client (subscribe + unsubscribe), each `AllowSubscribe` call performs one `time.Now()`.

At 5,000 clients × 50 subscribe requests/sec = 250,000 calls/sec × 50 ns = **12.5 ms/sec** spent in `time.Now()`. This is ~1.25% of one core — measurable but not critical.

**Optimization (if needed):** Use a coarse-grained clock that updates every millisecond via a background goroutine. Token bucket refill does not need nanosecond precision — millisecond accuracy is sufficient.

---

## 4. Additional Observations

### AllowN Atomicity Gap

**File:** [bucket.go:58–73](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/ratelimit/bucket.go#L58-L73)

```go
func (b *Bucket) AllowN(n int) bool {
    // ...
    if b.tokens >= float64(n) {
        b.tokens -= float64(n)
        return true
    }
    return false  // tokens NOT consumed
}
```

When `AllowN(5)` fails (only 3 tokens available), **no tokens are consumed**. This is the standard token bucket behavior and is correct. However, it creates a subtle interaction:

A client calls `AllowSubscribeN("c1", 5)` — rejected (only 3 tokens). The client then sends 5 individual `AllowSubscribe("c1")` calls. The first 3 succeed (consuming the tokens that `AllowN` left untouched), and 2 are rejected.

**This is not a bug** — it is inherent to the token bucket algorithm. But it means `AllowN` is not equivalent to N sequential `Allow` calls. The design should decide: should `AllowSubscribeN` be used for batch subscribe requests (subscribe to 5 symbols at once)? If so, the semantics should be documented — "AllowN is all-or-nothing."

---

### Prometheus Metrics — Correct Design

The metrics in [limiter.go:138–183](file:///e:/Sumit%20Codes/Season/GS_Summer_Analyst_27/Real%20Time%20Market%20Data%20System/internal/ratelimit/limiter.go#L138-L183) use **aggregate counters** with a fixed-cardinality `type` label (`"subscribe"`, `"unsubscribe"`, `"max_subscriptions"`). This is the correct pattern — learned from the `clientqueue` cardinality explosion. The total series count is bounded at 7 (4 counters + 3 label values on the CounterVec), regardless of client count.

---

## Summary Table

| ID | Category | Severity | Finding |
|:---|:---|:---:|:---|
| **B1** | Bypass | **Critical** | `AllowSubscription` / `ReleaseSubscription` race: `activeSubs` is read, checked, and incremented with no lock held. Two concurrent goroutines can both pass the cap check and increment, exceeding `MaxSubscriptions`. |
| **MG1** | Memory | **Critical** | Client map grows monotonically. No TTL eviction. 272 bytes per client, never freed unless `RemoveClient` is explicitly called. At 50k entries/day = 408 MB after 30 days. |
| B2 | Bypass | Medium | Rate check (`AllowSubscribe`) and cap check (`AllowSubscription`) are uncorrelated. A client's rate tokens are consumed even when the cap rejects the request, punishing legitimate burst behavior. |
| B3 | Bypass | Medium | Rate limiter keyed by `clientID`. Without authentication, a client reconnects with a new UUID and gets fresh tokens. |
| B4 | Bypass | Medium | No connection-level rate limiting (Layer 1 from the design doc). Connection establishment is the most expensive operation and is completely unthrottled. |
| SC1 | Scalability | Medium | Global `RWMutex` on `l.clients`. Write-lock contention during market-open (5,000 new clients). Should shard by clientID. |
| SC2 | Scalability | Low | `time.Now()` on every `Allow()`. On Windows, ~50 ns per call. At 250k calls/sec = 12.5 ms/sec of CPU. Optimizable with a coarse clock. |
