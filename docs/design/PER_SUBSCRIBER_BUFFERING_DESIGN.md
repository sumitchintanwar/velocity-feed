# Per-Subscriber Buffering Design

## Real-Time Market Data Platform

### Goal

Design a buffering architecture that enables:

* Isolated client queues
* Burst absorption
* Slow consumer protection
* Predictable memory usage
* Stable operation under load

Target scale:

```text
5000 Concurrent Clients
10000+ Market Updates/sec
```

The buffering layer sits between:

```text
Feed Generator
      ↓
Publisher
      ↓
Topic Manager
      ↓
WebSocket Gateway
      ↓
Client Queues
      ↓
Client Connections
```

---

# 1. Problem Statement

Not all clients consume data at the same speed.

Example:

```text
Client A
1 Gbps Connection

Client B
Mobile Network

Client C
Browser Tab In Background
```

Market updates continue arriving regardless of client performance.

Without buffering:

```text
Market Update
      ↓
Write Socket
```

every publish operation becomes dependent on network speed.

This creates a major scalability problem.

---

# 2. Why Per-Subscriber Buffers Exist

Without isolation:

```text
Market Update
      ↓

Client A

Client B

Client C
```

A single slow client can delay the entire fan-out pipeline.

Result:

```text
Increased Latency

Blocked Publishers

Reduced Throughput
```

---

## Desired Architecture

Each client owns an independent queue.

```text
                 Topic Manager
                        ↓

      +---------------+---------------+
      |               |               |
      ↓               ↓               ↓

 Client A Queue  Client B Queue  Client C Queue
      ↓               ↓               ↓

 WebSocket A     WebSocket B     WebSocket C
```

Benefits:

```text
Isolation

Failure Containment

Independent Backpressure
```

---

# 3. Core Design Principle

A client connection should never be allowed to directly influence:

```text
Publisher

Topic Manager

Other Clients
```

The only component affected by a slow client should be:

```text
That Client
```

---

# 4. Queue Ownership Model

Each client owns:

```text
Client State

Outbound Queue

Write Worker

Connection Metadata
```

Example:

```text
Client 123

Queue:
[Msg1]
[Msg2]
[Msg3]
```

The queue acts as a temporary buffer between:

```text
Production Rate
```

and

```text
Consumption Rate
```

---

# 5. Message Flow

## Normal Flow

```text
Market Update
      ↓

Topic Manager
      ↓

Client Queue
      ↓

Write Loop
      ↓

WebSocket
      ↓

Client
```

The publisher completes once the update is enqueued.

Actual network transmission happens later.

---

# 6. Burst Handling

## Scenario

Normal traffic:

```text
1000 Updates/sec
```

Burst:

```text
20000 Updates/sec
```

for a short period.

Without buffering:

```text
Immediate Congestion
```

occurs.

---

## With Buffering

```text
Burst
      ↓

Queue Absorbs Spike
      ↓

Client Continues Receiving
```

Benefits:

```text
Smoother Delivery

Reduced Latency Spikes

Fewer Disconnects
```

---

# 7. Queue Sizing Strategy

Queue size is one of the most important architectural decisions.

---

## Small Queues

Example:

```text
Queue Capacity = 50
```

Benefits:

```text
Low Memory Usage

Fast Failure Detection
```

Drawbacks:

```text
Poor Burst Tolerance
```

A short spike may trigger drops.

---

## Large Queues

Example:

```text
Queue Capacity = 10000
```

Benefits:

```text
Excellent Burst Absorption
```

Drawbacks:

```text
Higher Memory Usage

Higher Latency
```

Messages may sit in the queue for a long time.

---

# 8. Queue Depth vs Latency

Large queues hide problems.

Example:

```text
Queue Depth
5000 Messages
```

Client appears connected.

However:

```text
Data Is Seconds Behind
```

which is unacceptable for market data.

---

## Market Data Principle

Fresh data is more valuable than old data.

Therefore:

```text
Smaller Queues
```

are often preferred.

---

# 9. Memory Tradeoffs

Assume:

```text
5000 Clients
```

Queue size:

```text
100 Messages
```

Memory requirement:

```text
5000 × 100
=
500,000 Buffered Messages
```

---

## Increasing Capacity

Queue size:

```text
1000 Messages
```

Results:

```text
5,000,000 Buffered Messages
```

Potentially:

```text
Hundreds Of MBs

Or Several GBs
```

depending on payload size.

---

## Key Observation

Memory usage scales as:

```text
Clients
×
Queue Size
×
Message Size
```

This becomes a major operational concern.

---

# 10. Slow Consumer Problem

Example:

```text
Producer
10000 msg/sec

Client
100 msg/sec
```

Queue fills continuously.

Eventually:

```text
Queue Full
```

The system must react.

---

# 11. Slow Consumer Protection

A production market data system must protect itself.

---

## Option 1: Block Producer

```text
Queue Full
      ↓
Producer Waits
```

Advantages:

```text
No Data Loss
```

Problems:

```text
One Client
Can Affect Entire System
```

Not recommended.

---

## Option 2: Drop Messages

```text
Queue Full
      ↓
Drop New Messages
```

Advantages:

```text
Stable System
```

Problems:

```text
Data Loss
```

Must be monitored carefully.

---

## Option 3: Disconnect Client

```text
Queue Full
      ↓
Client Removed
```

Advantages:

```text
Protects Platform
```

Very common in market data systems.

---

# 12. Market Data Specific Strategy

Most exchanges and trading systems prioritize:

```text
Current Data
```

over

```text
Historical Data
```

for live feeds.

Therefore:

```text
Slow Client
      ↓
Disconnect
```

is often preferable to:

```text
Infinite Buffering
```

---

# 13. Backpressure Interaction

Backpressure should exist at multiple layers.

---

## Layer 1

Client Queue

```text
Queue Filling
```

indicates client problems.

---

## Layer 2

Gateway

```text
Many Full Queues
```

indicates delivery problems.

---

## Layer 3

Topic Manager

```text
Delivery Workers Saturated
```

indicates platform pressure.

---

## Layer 4

Publisher

```text
Task Queue Growing
```

indicates upstream overload.

---

# 14. Recommended Backpressure Model

```text
Feed Generator
      ↓

Publisher Queue
      ↓

Topic Manager
      ↓

Client Queue
      ↓

WebSocket
```

Each layer should expose:

```text
Queue Depth

Utilization

Drop Rate
```

allowing bottlenecks to be identified.

---

# 15. Queue Monitoring

Critical metrics:

```text
queue_depth

queue_capacity

messages_enqueued

messages_sent

messages_dropped
```

---

## Slow Consumer Metrics

```text
slow_clients

queue_full_events

forced_disconnects
```

These metrics often identify problems before users complain.

---

# 16. Failure Scenarios

## Network Degradation

```text
Client Network Slows
```

Result:

```text
Queue Growth
```

System response:

```text
Detect

Warn

Disconnect
```

---

## Burst Event

Example:

```text
Fed Announcement

Market Open

Earnings Release
```

Traffic spikes dramatically.

Queues absorb short-term load.

---

## Stalled Client

Client stops reading.

Queue fills.

Connection removed automatically.

---

# 17. Recommended Production Design

For:

```text
5000 Concurrent Clients
```

Use:

### Client Isolation

```text
One Queue Per Client
```

---

### Queue Type

```text
Bounded Queue
```

Never unbounded.

---

### Queue Capacity

```text
100–500 Messages
```

Initial production range.

Tune based on benchmarks.

---

### Slow Consumer Policy

```text
Detect

Warn

Disconnect
```

rather than blocking publishers.

---

### Backpressure

```text
Queue Metrics

Drop Metrics

Disconnect Metrics
```

at every layer.

---

# Architecture Summary

```text
                      Market Update
                             ↓

                      Topic Manager
                             ↓

      +----------------------+----------------------+
      |                      |                      |
      ↓                      ↓                      ↓

 Client A Queue       Client B Queue       Client C Queue
      ↓                      ↓                      ↓

 Write Worker         Write Worker         Write Worker
      ↓                      ↓                      ↓

 WebSocket A          WebSocket B          WebSocket C
```

---

# Final Recommendation

For a market data platform targeting:

```text
5000 Clients
10000+ Updates/sec
```

the optimal design is:

```text
Per-Subscriber Bounded Queues
+
Independent Write Loops
+
Queue-Based Backpressure
+
Slow Consumer Disconnection
```

This architecture provides:

```text
Client Isolation

Predictable Memory

Burst Absorption

Operational Stability

Horizontal Scalability
```

while ensuring that no individual client can degrade the performance of the broader market data platform.
