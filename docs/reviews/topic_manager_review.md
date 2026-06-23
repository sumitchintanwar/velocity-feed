# Topic Manager — Distributed Systems Review
**Reviewer perspective:** Distributed Systems Engineer  
**Target:** 10,000 updates/sec  
**Document:** `TOPIC_MANAGER.md` (Week 1 Architecture Design)

---

## Overview

The structural direction is sound: dual-index (Topic→Subscribers and Subscriber→Topics), sharded RW locks, async fan-out. The problems are in the **concurrent operations on two independent data structures** — a class of bugs that don't show up in single-threaded reasoning but surface immediately under concurrent load. There are also two latent deadlocks and four memory growth vectors, none of which are addressed by the mitigations listed in Section 15.

---

## DEADLOCKS

### DL1 — AB-BA Lock Ordering Violation (Will Deadlock in Production)

The design maintains two independent data structures:
- **Topic Registry** (sharded, Topic → Subscriber Set)
- **Subscriber Registry** (Subscriber → Topic Set)

These are separate locks. Operations that must update both create a classic AB-BA deadlock:

**Subscribe (Thread 1) acquires locks in order: Topic → Subscriber**
```
1. Acquire Write Lock on Topic Shard A (for AAPL)
2. Add subscriber to AAPL's set
3. Acquire Write Lock on Subscriber Registry
4. Add AAPL to subscriber's topic set
5. Release both locks
```

**Disconnect (Thread 2) acquires locks in order: Subscriber → Topic**
```
1. Acquire Write Lock on Subscriber Registry
2. Read subscriber's topic set → [AAPL]
3. Acquire Write Lock on Topic Shard A (to remove subscriber from AAPL)
4. Remove subscriber from AAPL's set
5. Release both locks
```

**Deadlock scenario:**
```
T=0   Thread 1 acquires Topic Shard A lock (Subscribe)
T=0   Thread 2 acquires Subscriber Registry lock (Disconnect)
T=1   Thread 1 tries to acquire Subscriber Registry lock → BLOCKED (Thread 2 holds it)
T=1   Thread 2 tries to acquire Topic Shard A lock → BLOCKED (Thread 1 holds it)
T=∞   Both threads wait forever
```

This is the textbook AB-BA deadlock. It is guaranteed to occur whenever Subscribe and Disconnect run concurrently on the same subscriber. At 10k/sec with frequent client reconnects, this will happen within seconds of load testing.

**Fix:** Define and enforce a global lock acquisition order. Always acquire Topic Registry locks before Subscriber Registry locks — never the reverse. All operations (Subscribe, Unsubscribe, Disconnect) must acquire in the same order, even if they don't use both locks, to prevent future violations.

---

### DL2 — Re-entrant Lock During Delivery Callback

Section 9 states Publish holds a **read lock** on the Topic Registry shard while it reads subscriber references. Section 13 says subscribers have a **Status** field. Section 16 puts Redis as a subscriber.

Consider this sequence:

```
1. Publish acquires RLock on Shard A
2. Publish calls subscriber.Deliver(event) for Redis adapter
3. Redis connection drops → Redis adapter calls Unsubscribe (cleanup)
4. Unsubscribe tries to acquire WLock on Shard A
5. WLock waits for all RLocks to release
6. The Publish goroutine that holds RLock is blocked waiting for Deliver() to return
```

`sync.RWMutex` is **not re-entrant**. Step 6 means the Publish goroutine holds RLock and is waiting for a call that is waiting for WLock, which is waiting for RLock. The goroutine is deadlocked with itself via the callback chain.

This is particularly insidious because it only happens on Redis connection loss — a failure path that may not be tested.

**Fix:** Publish must never call subscriber callbacks while holding a lock. The correct pattern: acquire RLock, copy subscriber references, release RLock, then call callbacks with no locks held. The Snapshot Strategy (Section 11) describes copying references but does not specify that the lock must be released *before* calling callbacks — this must be made explicit.

---

## RACE CONDITIONS

### RC1 — Dual-Index Update Creates a Window of Inconsistent State

Subscribe must update two structures: Topic Registry and Subscriber Registry. The document implies these are separate locks. Every update to both structures has a gap between the two writes where state is inconsistent.

**Subscribe gap:**
```
T=0   Lock Topic Registry (Shard A) → Add subscriber to AAPL → Unlock
                ↑ window: subscriber is in AAPL topic but NOT in their own topic set
T=1   Lock Subscriber Registry → Add AAPL to subscriber's topics → Unlock
```

**What can happen in the window:**
A concurrent Disconnect reads the Subscriber Registry (sees AAPL not present yet), removes nothing from Topic Registry for AAPL, then exits. The subscriber is now disconnected but remains permanently in AAPL's subscriber set. Every subsequent AAPL publish attempts delivery to a dead subscriber.

This is a **phantom subscription** — a disconnected subscriber leaking into the live delivery path. At 10k publishes/sec, this means 10k failed delivery attempts/sec per phantom subscriber, consuming worker pool capacity.

**Fix:** Both index updates must be atomic with respect to Disconnect. Either:
1. Use a single lock covering both structures (sacrifices sharding benefit)
2. Use a two-phase protocol: mark-for-deletion before acquiring locks, check the mark in Subscribe before committing

---

### RC2 — Snapshot Strategy Does Not Protect Subscriber Queue from Close

Section 11 (Snapshot Strategy): Copy subscriber references before dispatching. This correctly prevents the subscriber list from being mutated during iteration. But it does **not** protect against the subscriber's outbound queue being closed mid-delivery.

**The race:**
```
T=0   Publish takes snapshot: [A, B, C]
T=0   Publish releases lock
T=1   Client B disconnects → its outbound queue is closed
T=2   Publish attempts to send to B's queue → send on closed channel → PANIC
```

The document claims the snapshot means "Subscribers Can Leave without affecting active delivery." This is false. The snapshot protects the *list*, not the *queue state*. A departing subscriber's queue becoming invalid between snapshot and delivery is the actual hazard.

**Fix:** The subscriber struct must have a `closed` atomic flag. The delivery path must check this flag before sending. Alternatively, sending to a subscriber queue must use a non-panicking select with a `done` channel that is closed (not the queue) on disconnect.

---

### RC3 — Status Field Has No Defined Synchronization

Section 13 says each subscriber has a `Status` field. Status is read during delivery (to skip disconnected subscribers) and written during disconnect. There is no lock specified for the Status field.

In Go, concurrent read and write to a variable without synchronization is a data race — the race detector will flag it, and on certain CPU architectures it will produce torn reads. `sync/atomic` with a defined status enum, or reading Status under the subscriber's own lock, is required. The document doesn't specify either.

---

## SCALABILITY LIMITATIONS

### SL1 — Per-Publish Snapshot Allocation Creates GC Saw-Tooth at 10k/sec

Section 11 (Snapshot Strategy): "Copy Subscriber References before dispatching."

**Quantified cost at target throughput:**

| Variable | Value |
|---|---|
| Topics with active subscribers | 100 |
| Subscribers per popular topic (AAPL) | 500 |
| Publish rate | 10,000/sec |
| Snapshot size per publish | 500 × 8 bytes = 4 KB |
| Snapshot allocation rate | 10,000 × 4 KB = **40 MB/sec** |
| Snapshot lifetime | Duration of fan-out (~1ms) |
| Peak concurrent snapshots | 10,000 × 0.001s = **10 snapshots × 4 KB = 40 KB** |

The 40 MB/sec figure is the throughput at which the GC must collect this ephemeral data. Under any GC pause, snapshots accumulate. The resulting pattern: allocation ramps up → GC fires → latency spike → allocation ramps up → GC fires. This is the classic saw-tooth that kills p99 latency in high-throughput Go systems.

**Fix:** Use atomic Copy-on-Write (COW) for the subscriber list. Writes create a new list and atomically swap the pointer. Publish reads the atomic pointer — no allocation, no lock, no copy. The old list is released by the GC naturally when no readers hold it. COW eliminates per-publish allocation entirely.

---

### SL2 — RWMutex Writer Starvation Blocks Subscriptions Under Load

Section 9: Publish = Read Lock, Subscribe = Write Lock.

`sync.RWMutex` in Go prevents write starvation by blocking new readers once a writer is waiting. This is correct. But the implication is that a burst of subscribe events at market open (when thousands of clients connect simultaneously) blocks all publishes until every pending write lock is granted.

At 10k publishes/sec with 1000 clients connecting simultaneously at market open:
- Each subscribe acquires a write lock
- Write lock waits for all current readers (publish goroutines) to finish
- 1000 pending write locks → 1000 × (average publish hold time) = subscription latency spike
- During this window, publish throughput may degrade as new readers queue behind pending writers

This is not catastrophic but it is measurable. The document claims "Publishing should never block unnecessarily" — but under burst subscribe scenarios it will.

---

### SL3 — Disconnect Requires Acquiring N+1 Locks (Not Analyzed by the Document)

Section 5 (Reverse Index) describes Disconnect as:
```
Lookup Subscriber → Get Topics → Remove Directly
```

"Remove Directly" requires acquiring a write lock on every Topic Registry shard that contains any of the subscriber's topics. A subscriber subscribed to AAPL (Shard 1), MSFT (Shard 2), GOOG (Shard 3), BTCUSD (Shard 4) requires 4 write locks + 1 Subscriber Registry write lock.

The document does not specify the acquisition order for these 5 locks. Any inconsistency between Disconnect and Subscribe creates the DL1 deadlock above. Furthermore, holding multiple write locks simultaneously blocks all publishes to all those topics for the entire cleanup duration — a "disconnect storm" when many clients disconnect simultaneously (e.g., market close) can halt throughput system-wide.

---

### SL4 — Worker Pool Is Undesigned, Making Fan-Out a Bottleneck

Section 12 recommends "Subscriber Queues → Worker Delivery" but specifies nothing about the worker pool:

- How many workers?
- Is it a fixed pool or dynamic?
- What is the task queue capacity?
- How are workers assigned to subscribers?

At 10k publishes/sec with 500 subscribers per topic, the worker pool receives:
```
10,000 events/sec × 500 subscribers = 5,000,000 delivery tasks/sec
```

If the task queue is a channel with a fixed buffer and workers can't keep up, the task queue fills. The Publish path then blocks trying to enqueue a delivery task — which breaks the design invariant that "Publishing should never block unnecessarily."

With an unbounded task queue (a common default), the queue becomes the memory growth vector: task accumulation is functionally identical to subscriber queue accumulation (MG2 below).

---

## MEMORY GROWTH

### MG1 — Topic Entries Are Never Deleted

When all subscribers leave a topic, the Topic Registry retains:
```
AAPL → {} (empty subscriber set)
```

For a market data system, the topic space is not static:
- An options chain for a single equity has hundreds of strike/expiry combinations
- Cryptocurrency platforms add/delist symbols continuously
- A replay service may generate topics for historical symbols

Without explicit topic tombstoning (delete the map entry when the subscriber set reaches zero), the Topic Registry grows monotonically. In a long-running process (trading session is 6.5 hours), the accumulation of empty topic entries consumes memory and degrades map iteration performance during snapshot creation.

**Fix:** Delete the map entry when the last subscriber is removed from a topic. Protect with the write lock already held during unsubscribe.

---

### MG2 — Outbound Queue Has No Defined Capacity or Eviction Policy

Section 13: Each subscriber has an "Outbound Queue." The document lists `subscriber_queue_depth` as a metric and `dropped_messages` as a metric — implying there IS a cap and a drop policy — but neither the cap nor the policy is specified anywhere.

**Memory growth at 10k/sec with an uncapped queue:**

| Variable | Value |
|---|---|
| Slow subscriber queue growth rate | 10,000 events/sec |
| Event size | ~200 bytes (JSON) |
| Queue growth per second | 2 MB/sec per slow subscriber |
| With 10 slow subscribers | 20 MB/sec |
| After 60 seconds | 1.2 GB heap growth |

Without a defined cap, a single slow client causes OOM within minutes under sustained load. The document names this as Bottleneck 3 and says "Per Subscriber Queue" is the mitigation — but the mitigation is the source of the problem.

**Fix:** Specify a maximum queue depth (e.g., 1024 events) with an explicit overflow policy per subscriber class: drop-head (discard oldest), drop-tail (discard newest), or disconnect-on-overflow. This must be in the design, not deferred.

---

### MG3 — Zombie Subscriber Accumulation Without Concrete Cleanup Design

Section 15, Bottleneck 3: "Connection Cleanup, Heartbeat Monitoring" are named as mitigations. Neither is designed.

**What "zombie subscriber" means concretely:**

A client's TCP connection goes silent (network partition, client crash, mobile background). The WebSocket layer does not detect this until the next write attempt or heartbeat timeout. During the detection window:
- The subscriber remains in the Topic Registry
- The subscriber remains in the Subscriber Registry  
- The subscriber's outbound queue accumulates events (MG2 above)
- Worker pool slots are consumed attempting delivery

In a mobile client scenario (clients connecting from phones), disconnect-without-FIN is the **common case**, not the exception. The detection window (WebSocket ping timeout, typically 60 seconds) is the period during which a zombie subscriber consumes all its allocated resources.

With 100 zombie subscribers at 10k/sec and 1024 queue depth: 100 × 1024 × 200 bytes = **20 MB** of stranded data. With 1000 zombie subscribers (possible at scale): 200 MB.

**Minimum required design:** specify the heartbeat interval, the ping-pong protocol, the timeout value, and who is responsible for calling Disconnect on timeout. The Topic Manager should expose a `Disconnect(subscriberID)` method, and the WebSocket layer should call it on heartbeat failure.

---

### MG4 — Snapshot Allocation Amplifies GC Pressure During Pause Recovery

This extends SL1. During a GC pause, no new objects are collected. Snapshot allocations continue at 40 MB/sec. When the GC pause ends and the GC runs, it must collect the backlog of snapshots accumulated during the pause, plus current ones. This causes **GC pause chaining**: a long pause leads to a large collection, which itself temporarily pauses allocation, creating another saw-tooth. Under extreme load, this can prevent the system from ever catching up to a clean heap state.

The combination of snapshot allocation (SL1) + outbound queue accumulation (MG2) + task queue growth (SL4) creates three independent heap growth vectors that compound during any system slowdown. The design has no mechanism to bound total heap usage under load.

---

## Summary

| Category | Issue | Severity |
|---|---|---|
| Deadlock | AB-BA lock ordering: Subscribe (Topic→Subscriber) vs Disconnect (Subscriber→Topic) | **Critical** |
| Deadlock | Re-entrant lock: subscriber callback calling Unsubscribe while Publish holds RLock | **Critical** |
| Race | Dual-index update gap: phantom subscriptions from Disconnect in inconsistency window | **High** |
| Race | Snapshot does not protect subscriber queue from close: panic on delivery | **High** |
| Race | Status field read/write without defined synchronization | **Medium** |
| Scalability | Per-publish O(N) snapshot allocation: 40 MB/sec GC pressure at 10k/sec | **High** |
| Scalability | RWMutex writer starvation blocks subscribes during burst (market open) | **Medium** |
| Scalability | Disconnect acquires N+1 locks with undefined ordering | **High** |
| Scalability | Worker pool undesigned — task queue becomes throughput bottleneck | **High** |
| Memory | Topic entries never deleted — monotonic growth | **Medium** |
| Memory | Outbound queue has no cap — OOM under slow clients | **High** |
| Memory | Zombie subscriber accumulation — no concrete heartbeat design | **High** |
| Memory | Snapshot allocation amplifies GC during pause recovery | **Medium** |

**The two critical items (DL1, DL2) must be resolved before any concurrent load testing.** They are not observable in unit tests — they only manifest under real concurrent operations. Run `go test -race` and a concurrent stress test with simultaneous Subscribe/Disconnect/Publish goroutines before any other work.
