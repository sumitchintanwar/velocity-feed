# Backpressure Implementation Review

I've reviewed the backpressure implementation (`internal/backpressure/*`), specifically focusing on your requested areas. There are several critical issues in the current design that will severely impact performance and stability at scale.

## 1. Memory Growth

### Unbounded Capacity Growth in `dropWindow`
**Severity:** Critical  
**Location:** `channel.go:recordDrop()`

The `recordDrop()` method manages time-windowed drop tracking by appending to the `c.dropWindow` slice and reslicing to remove old entries:
```go
c.dropWindow = append(c.dropWindow, now)
// ...
c.dropWindow = c.dropWindow[i:]
```
While reslicing hides the old elements, it **does not reduce the capacity** of the underlying array. In a scenario where a subscriber is stalled during a publish rate of 100,000 msgs/sec, a default 10-second window will accumulate 1,000,000 `time.Time` structs.
- This results in ~24MB of memory per stalled subscriber.
- If 10,000 subscribers stall or lag (e.g., during a network partition), memory usage will balloon to **~240 GB**, causing an OOM crash.

**Recommendation:** Replace the slice of timestamps with a sliding window of buckets (e.g., a circular array of 10 integers representing drops per second) to maintain O(1) space complexity.

---

## 2. Starvation

### Global Goroutine Starvation (Busy Waiting)
**Severity:** Critical  
**Location:** `channel.go:forwardLoop()`

When the ring buffer is empty, the `forwardLoop` goroutine falls back to a spin-wait:
```go
ev, ok := c.ring.Pop()
if !ok {
    time.Sleep(time.Microsecond)
    continue
}
```
With 10,000 subscribers, this spawns 10,000 goroutines that wake up every microsecond when idle. This will completely overwhelm the Go runtime scheduler, driving CPU utilization to 100% due to context switches, and effectively starving the publisher goroutines of CPU cycles.

**Recommendation:** Use a synchronization primitive (like `sync.Cond` or a `chan struct{}` signaling channel) to safely park the `forwardLoop` goroutine when the ring is empty, and wake it up during `Push()`.

### "Consecutive" Drops are Actually Cumulative
**Severity:** High  
**Location:** `channel.go:sendDisconnect()` & `sendDropNewest()`

The `consecutiveDrops` counter is incremented on every drop but is **never reset** upon a successful delivery.
This means `MaxConsecutiveDrops` behaves as an absolute lifetime limit. A subscriber that drops 1 message a day will be forcefully disconnected after 100 days, even if it successfully processed billions of messages in between.

**Recommendation:** Reset `c.consecutiveDrops.Store(0)` inside `Send()` whenever an event is successfully added to the buffer without dropping.

---

## 3. Fairness

### Mutex Contention from Spin-Popping
**Severity:** Medium  
**Location:** `ring.go:Pop()`

Because `forwardLoop` spins continuously (or with 1µs sleeps), it acquires the `r.mu` lock inside `Pop()` at a massive frequency. Mutexes in Go are not strictly fair; the publisher calling `Push()` might have to contend heavily with the consumer's `forwardLoop` just to insert an event. This pushes latency back onto the publisher, which the `Topic_Manager_Architecture_Review.md` explicitly warns against.

**Recommendation:** If keeping the forward loop, consider batching pops (e.g., pulling all available events under a single lock acquisition) or migrating to a lock-free ring buffer implementation using `atomic` operations.

---

## 4. Failure Modes

### Silent Drops in the Default Policy (Observability Failure)
**Severity:** High  
**Location:** `channel.go:sendDropOldest()`

The default config uses `PolicyDropOldest`. However, `sendDropOldest()` directly calls `c.ring.Push(ev)` but completely ignores whether the ring buffer overwrote an event:
```go
func (c *Channel) sendDropOldest(ev marketdata.MarketEvent) bool {
    written := c.ring.Push(ev)
    // ... no check for overwritten events!
}
```
Unlike `sendDisconnect()`, it never increments `c.totalDropped` or updates the Prometheus metric `c.metrics.EventsDroppedTotal.Inc()`.
As a result, a consumer could drop 99% of its messages, but the system's observability metrics and `Channel.TotalDropped()` API will dangerously report **0 drops**.

**Recommendation:** Update `sendDropOldest` to check if `c.ring.TotalDropped()` increased after the push (similar to `sendDisconnect`), and update metrics accordingly.

### Incomplete State Reset
**Severity:** Low  
**Location:** `channel.go:ResetDrops()`

`ResetDrops()` clears the `consecutiveDrops` counter but fails to clear the `dropWindow` slice. If a consumer is "forgiven" and reset, but the window is still saturated from recent drops, the very next dropped event could instantly trigger a sustained drop rate disconnect.

**Recommendation:** Clear or reallocate `c.dropWindow` inside `ResetDrops()` (`c.dropWindow = nil`).


# Backpressure Implementation Review

**Reviewer perspective:** Principal Distributed Systems Engineer
**Target:** `internal/backpressure/channel.go` & `ring.go`
**Focus Areas:** Memory growth, starvation, fairness, failure modes

---

## Verdict

The implementation introduces a flexible policy-based backpressure mechanism with a well-designed circular ring buffer and fixed-memory drop window. **Memory growth is perfectly bounded.**

However, the concurrency logic connecting the `Ring` to the `forwardC` channel contains **three critical failure modes** that lead to starvation, complete loss of buffering, and broken disconnect policies.

---

## 1. Failure Modes & Starvation

### Failure Mode 1: Missed Wakeup (Consumer Starvation / Deadlock)
The `forwardLoop` uses a `sync.Cond` to park when the ring buffer is empty. But the condition variable's lock (`c.hasData.L`) is never acquired by the producer during `Push()`. 
```go
// In Ring.Push()
if r.notify != nil {
    r.notify.Signal() // Fired without holding c.hasData.L
}
```
**The Race:**
1. Consumer calls `Peek()` under `hasData.L`, sees the ring is empty.
2. *Context Switch*
3. Producer calls `Push()`, adds an event, and calls `Signal()`.
4. Consumer resumes, calls `c.hasData.Wait()`, and goes to sleep.
**Result:** The signal was sent into the void. The consumer sleeps forever, permanently starving the client of all future updates. To fix this, `Push()` must acquire the condition lock before signaling, or the `Cond` must use `r.mu` directly.

### Failure Mode 2: Hot-Loop Ring Drain (Broken `DropOldest`)
When the downstream `forwardC` is full, the `forwardLoop` does this:
```go
select {
case c.forwardC <- ev:
default:
    // forwardC is full — drop the event.
    c.totalDropped.Add(1)
}
```
**The Bug:** Instead of blocking when the client is slow, the `forwardLoop` immediately falls through to the `default` case, drops the event, and instantly loops back to pop the *next* event from the ring. 
**Result:** If a client slows down, `forwardLoop` goes into a hot-spin, completely draining the 4,096-capacity ring buffer in microseconds. The `Ring` buffer becomes useless because it is never allowed to actually buffer anything. 
**Fix:** `forwardLoop` must **block** on `forwardC <- ev`. If it blocks, the ring buffer fills up, and the producer's `Push()` method correctly overwrites the oldest events in the ring.

### Failure Mode 3: Disconnect Policy Bypass
The `PolicyDisconnect` relies on `ring.TotalDropped()` to increment the `consecutiveDrops` counter and trigger a client disconnect.
**The Bug:** Because of Failure Mode 2, the ring buffer is never full. The drops are happening in the `forwardLoop`'s `default` case. The producer's `sendDisconnect` method sees `newDropped == prevDropped` on the ring, and completely ignores the thousands of drops happening downstream in the forwarder.
**Result:** A slow client will never be disconnected because the drop accounting is disconnected from where the drops actually occur.

---

## 2. Memory Growth

**Verdict: Excellent.**
1. **Ring Buffer:** Uses a pre-allocated slice of interface/struct values (`make([]marketdata.MarketEvent, capacity)`). It correctly uses a bitwise mask `& r.mask` for wrap-around, ensuring the slice never grows. Memory is strictly $O(\text{BufferSize})$.
2. **Drop Window (`dropBucketWindow`):** Instead of keeping an unbounded slice of `time.Time` structs to track recent drops, it uses a 10-element `int64` circular array to bucket drops by second. This guarantees exactly 160 bytes of memory overhead regardless of whether the drop rate is 1/sec or 1,000,000/sec. 

---

## 3. Fairness

**Verdict: Good (Once bugs are fixed)**
The intention of `DropOldest` is to provide the fairest outcome for market data:
*   **Producer Fairness:** The producer never blocks. It is immune to slow consumers and avoids cascading backpressure.
*   **Consumer Fairness:** When a consumer lags, it loses historical ticks but receives the absolute freshest price when it wakes up. This is correct for trading systems where a 100ms old price is worse than no price.

### Summary of required fixes:
1. Remove the `default:` case in `forwardLoop` so it blocks on `forwardC <- ev`.
2. Unify the condition variable lock. `c.hasData = sync.NewCond(&c.ring.mu)` is the standard pattern, allowing the consumer to `Wait()` atomically against the same mutex the producer locks.
