# Topic Manager Architecture Review

## Focus: Mutexes, RWMutexes, Contention, and Lock-Free Alternatives

### Assumptions

Current architecture:

```text
Topic Manager

Topic -> Subscriber Set
Subscriber -> Topic Set

Subscribe()
Unsubscribe()
Publish()
```

Workload characteristics:

```text
Subscribers: 10,000+

Read Heavy

Publish Heavy

Subscription Changes Relatively Rare
```

Examples:

```text
Publish:
100,000 operations/sec

Subscribe:
100 operations/sec

Unsubscribe:
100 operations/sec
```

This is a classic read-dominated workload.

---

# Current Design

Assume the registry is structured as:

```text
Topic
    ->
Subscriber Set
```

Example:

```text
AAPL
    ->
{1,2,3,4,5}

MSFT
    ->
{3,4,8}

BTCUSD
    ->
{9,10}
```

Publish path:

```text
Lookup Topic
      ↓
Get Subscribers
      ↓
Fan-Out
```

Subscribe path:

```text
Modify Subscriber Set
```

Unsubscribe path:

```text
Remove Subscriber
```

---

# Problem 1: Global Mutex

Naive implementation:

```text
Topic Manager
      ↓
Single Mutex
```

All operations acquire the same lock.

Example:

```text
Publish(AAPL)
Publish(MSFT)
Publish(BTCUSD)

Subscribe(AAPL)

Unsubscribe(MSFT)
```

Every operation contends for the same resource.

---

## Why It Fails

At scale:

```text
100,000 publishes/sec
```

becomes:

```text
100,000 lock acquisitions/sec
```

All serialized behind one mutex.

Result:

```text
High CPU

Lock Contention

Increased Latency
```

Eventually:

```text
Publish Throughput
=
Lock Throughput
```

which is a terrible scaling property.

---

# Problem 2: Publish Is The Hot Path

In a market data system:

```text
Publish >>> Subscribe
```

Typical ratio:

```text
1000:1

or

10000:1
```

Optimizing write operations while slowing publishes is the wrong tradeoff.

The architecture should optimize for:

```text
Publish Performance
```

above everything else.

---

# RWMutex Review

A common improvement:

```text
RWMutex
```

Publish:

```text
RLock()
```

Subscribe:

```text
Lock()
```

Unsubscribe:

```text
Lock()
```

---

## Benefits

Multiple readers proceed concurrently.

Example:

```text
Publish(AAPL)

Publish(MSFT)

Publish(BTCUSD)
```

can execute simultaneously.

This removes most contention compared to a standard mutex.

---

## Why RWMutex Helps

Workload:

```text
99.9% Reads
```

RWMutex allows:

```text
Many Readers

Few Writers
```

which matches market data workloads well.

---

# Hidden RWMutex Costs

RWMutex is not free.

Internally it must track:

```text
Reader Count

Waiting Writers

Wakeups
```

which creates overhead.

For very short critical sections:

```text
Lookup Topic
```

RWMutex overhead can become noticeable.

---

## Writer Starvation

Heavy publishing creates:

```text
Thousands Of Readers
```

A writer attempting:

```text
Subscribe()

Unsubscribe()
```

must wait.

Result:

```text
Subscription Latency Increases
```

Not catastrophic for market data systems, but worth understanding.

---

# Contention Analysis

Assume:

```text
500 Topics

100,000 Publishes/sec
```

Global RWMutex:

```text
All Topics
      ↓
Same Lock
```

Problem:

```text
Publish(AAPL)

blocks on

Subscribe(BTCUSD)
```

even though they are unrelated.

This is unnecessary contention.

---

# Recommended Design: Topic-Level Locking

Instead of:

```text
One Global Lock
```

Use:

```text
AAPL Lock

MSFT Lock

BTCUSD Lock
```

Architecture:

```text
Topic
    →
Subscriber Set
    →
RWMutex
```

Now:

```text
Publish(AAPL)
```

does not interact with:

```text
Publish(BTCUSD)
```

at all.

---

## Benefits

Reduces contention dramatically.

Lock scope becomes:

```text
Per Topic
```

rather than:

```text
Entire Registry
```

This is usually the first major scalability improvement.

---

# Sharded Locking

As topic count grows:

```text
10,000+
Topics
```

per-topic locks become expensive to manage.

Alternative:

```text
Shard 1

Shard 2

Shard 3

Shard N
```

Example:

```text
hash(topic) % N
```

determines ownership.

Architecture:

```text
Shard
      ↓
Registry
      ↓
RWMutex
```

Benefits:

```text
Limited Lock Count

Reduced Contention

Predictable Scaling
```

---

# Publish Fan-Out Bottleneck

A hidden issue:

```text
Publish(AAPL)
```

often does:

```text
Acquire Lock
Get Subscribers
Iterate Subscribers
Release Lock
```

If:

```text
AAPL
=
5000 Subscribers
```

the lock is held during a large iteration.

Result:

```text
Long Critical Sections
```

which increases contention.

---

# Snapshot Strategy

Preferred design:

```text
Acquire Read Lock
```

```text
Copy Subscriber References
```

```text
Release Lock
```

```text
Fan-Out Outside Lock
```

Architecture:

```text
Lock
  ↓
Snapshot
  ↓
Unlock
  ↓
Delivery
```

Benefits:

```text
Tiny Critical Sections

Low Contention
```

This is a common production optimization.

---

# Lock-Free Alternatives

Eventually even RWMutex becomes expensive.

At high publish rates:

```text
500k+

1M+

5M+ updates/sec
```

engineers often move toward lock-free reads.

---

# Copy-On-Write Subscriber Lists

Concept:

```text
Subscribers
```

stored as immutable snapshots.

Example:

```text
Current Snapshot
```

```text
[A,B,C,D]
```

New subscription:

```text
Create New Snapshot
```

```text
[A,B,C,D,E]
```

Replace pointer atomically.

Readers:

```text
No Locks
```

Publish path:

```text
Load Pointer
Iterate
```

only.

---

## Advantages

Publish becomes:

```text
Lock-Free
```

which is ideal for:

```text
Read Heavy
```

systems.

---

## Costs

Subscribe:

```text
O(N)
```

because the list must be copied.

Example:

```text
5000 Subscribers
```

requires:

```text
Copy 5000 References
```

per modification.

---

## Why It Works

Market data workloads typically look like:

```text
Millions Of Publishes

Few Subscription Changes
```

making copy-on-write extremely attractive.

---

# Atomic Pointer Swap Architecture

Advanced design:

```text
Topic
      ↓
Atomic Pointer
      ↓
Immutable Subscriber List
```

Publish:

```text
Read Pointer
```

No locking.

Subscribe:

```text
Copy
Modify
Swap
```

This pattern is heavily used in high-performance event distribution systems.

---

# Recommended Architecture For 10,000 Subscribers

### Avoid

```text
Single Global Mutex
```

---

### Acceptable

```text
Global RWMutex
```

Suitable for early versions.

---

### Better

```text
Sharded Registry

Shard-Level RWMutex
```

Provides significantly better scalability.

---

### Production Grade

```text
Immutable Subscriber Lists

Atomic Pointer Swaps

Lock-Free Reads
```

Optimized for:

```text
Heavy Publish Traffic
```

which is exactly the workload profile of a market data platform.

---

# Final Assessment

For a system targeting:

```text
10,000 Subscribers
100,000+ Publishes/sec
```

I would rank the approaches as:

| Design                      | Scalability | Complexity |
| --------------------------- | ----------- | ---------- |
| Global Mutex                | Poor        | Low        |
| Global RWMutex              | Fair        | Low        |
| Topic-Level RWMutex         | Good        | Medium     |
| Sharded RWMutex             | Very Good   | Medium     |
| Copy-On-Write + Atomic Swap | Excellent   | High       |

If this were being built for a quantitative trading environment, I would start with:

```text
Sharded Registry
+
RWMutex
+
Snapshot Fan-Out
```

because it offers the best balance of performance, simplicity, debuggability, and operational safety.

Only move to lock-free copy-on-write structures after profiling proves that lock contention is a measurable bottleneck in production.
