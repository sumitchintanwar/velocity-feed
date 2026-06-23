# Worker Pool Design

## Real-Time Market Data Platform

### Author

Principal Engineer Review

### Goal

Design a production-grade Worker Pool subsystem capable of processing market data workloads while providing:

* Bounded concurrency
* Controlled resource utilization
* Predictable latency
* Graceful shutdown
* Horizontal scalability

Target throughput:

```text
10,000+ updates/sec
```

The Worker Pool must integrate with the existing architecture:

```text
Feed Generator
      ↓
Publisher
      ↓
Topic Manager
      ↓
WebSocket Gateway
```

without becoming a bottleneck.

---

# 1. Why A Worker Pool Exists

## Problem

A naïve design often creates:

```text
One Goroutine
Per Update
```

Example:

```text
10000 updates/sec
      ↓
10000 goroutines/sec
```

This creates:

* Scheduler pressure
* Excessive allocations
* Increased context switching
* Unpredictable latency

---

## Goal

Limit concurrency.

Instead of:

```text
10000 Tasks
      ↓
10000 Goroutines
```

Use:

```text
10000 Tasks
      ↓
Task Queue
      ↓
Fixed Worker Pool
      ↓
N Workers
```

This creates predictable system behavior.

---

# 2. High-Level Architecture

```text
                    Market Updates
                           ↓

                    Task Creation
                           ↓

                  +----------------+
                  |   Task Queue   |
                  +----------------+
                           ↓

       +-----------+-----------+-----------+
       |           |           |           |
       ↓           ↓           ↓           ↓

    Worker 1    Worker 2    Worker 3   Worker N

       ↓           ↓           ↓           ↓

                  Process Task
                           ↓

                    Output Channel
                           ↓

                    Next Component
```

---

# 3. Worker Pool Responsibilities

The Worker Pool should only:

```text
Receive Tasks

Schedule Work

Execute Work

Handle Shutdown
```

It should not:

```text
Manage WebSockets

Manage Topics

Store Market Data

Perform Routing Decisions
```

Those belong elsewhere.

---

# 4. Task Abstraction

The worker pool processes units of work.

Examples:

```text
Publish Market Update

Fan-Out Update

Serialize Payload

Persist Event

Replay Event
```

The pool should remain generic.

Workers process tasks without understanding business semantics.

---

# 5. Bounded Concurrency

## Requirement

Concurrency must be explicitly limited.

Example:

```text
Worker Count = 32
```

Maximum parallel execution:

```text
32 Tasks
```

regardless of incoming load.

---

## Why?

Benefits:

```text
Predictable Memory

Predictable CPU

Controlled Scheduling
```

Without bounds:

```text
Load Spike
      ↓
Unlimited Goroutines
      ↓
Resource Exhaustion
```

---

# 6. Queue Design

## Purpose

Absorb bursts.

Example:

```text
Normal
10k Updates/sec

Burst
50k Updates/sec
```

Workers continue processing while the queue absorbs temporary spikes.

---

## Architecture

```text
Producer
      ↓

Task Queue
      ↓

Workers
```

The queue separates:

```text
Production Rate
```

from

```text
Consumption Rate
```

---

# 7. Queue Capacity Strategy

## Unbounded Queue

Bad Design.

Example:

```text
Load Continues Increasing
```

Queue grows forever.

Results:

```text
Memory Explosion
```

Eventually:

```text
OOM
```

---

## Bounded Queue

Recommended.

Example:

```text
Queue Size = 10000
```

Benefits:

```text
Predictable Memory
```

System becomes self-protecting.

---

# 8. Queue Full Behavior

When the queue is full:

```text
Producer
      ↓
Cannot Enqueue
```

Three possible strategies exist.

---

## Strategy 1

Block Producer

```text
Producer Waits
```

Benefits:

```text
No Data Loss
```

Costs:

```text
Latency Increases
```

---

## Strategy 2

Drop New Tasks

```text
Queue Full
      ↓
Reject Update
```

Benefits:

```text
Stable System
```

Costs:

```text
Possible Data Loss
```

---

## Strategy 3

Backpressure

```text
Queue Full
      ↓
Slow Upstream Components
```

Preferred for market systems.

---

# 9. Worker Lifecycle

## Startup

System initialization:

```text
Create Queue
Create Workers
Register Metrics
```

Workers enter:

```text
Waiting State
```

---

## Idle State

Worker waits for tasks.

```text
Task Queue
      ↓
Receive Task
```

No busy polling.

No CPU waste.

---

## Active State

Worker receives:

```text
Task
```

and executes:

```text
Validation

Processing

Publishing
```

depending on workload.

---

## Completion State

Task completed.

Worker returns to:

```text
Waiting State
```

for additional work.

---

# 10. Graceful Shutdown

## Goal

Shutdown without losing work.

---

## Shutdown Sequence

```text
Stop Accepting Tasks
        ↓

Drain Queue
        ↓

Finish Running Tasks
        ↓

Stop Workers
        ↓

Exit
```

---

## Why Important?

Bad shutdown:

```text
Process Kill
```

causes:

```text
Dropped Updates

Incomplete Processing

Corrupted State
```

---

## Production Requirement

All in-flight tasks should complete whenever possible.

---

# 11. Worker Count Selection

## Too Few Workers

Example:

```text
2 Workers
```

Problems:

```text
Queue Growth

Increased Latency
```

---

## Too Many Workers

Example:

```text
1000 Workers
```

Problems:

```text
Scheduler Overhead

Context Switching

Memory Waste
```

---

## Recommended Approach

Base worker count on:

```text
CPU Cores

Workload Type
```

---

### CPU-Bound Work

Example:

```text
Serialization

Compression

Aggregation
```

Workers ≈ CPU cores.

---

### I/O-Bound Work

Example:

```text
Redis

Disk

Network
```

Workers can exceed CPU count.

---

# 12. Integration With Market Data Platform

## Publisher Stage

```text
Market Update
      ↓
Worker Pool
      ↓
Topic Manager
```

Benefits:

```text
Controlled Publishing Rate
```

---

## Fan-Out Stage

```text
Topic Manager
      ↓
Worker Pool
      ↓
Subscriber Delivery
```

Benefits:

```text
Parallel Delivery
```

without unbounded goroutine creation.

---

## Persistence Stage

Future:

```text
Market Update
      ↓
Persistence Pool
```

separate from delivery workloads.

---

# 13. Bottlenecks

## Bottleneck 1

Queue Saturation

Example:

```text
Incoming
50k/sec

Processing
10k/sec
```

Queue fills continuously.

Symptoms:

```text
Latency Growth
```

---

## Bottleneck 2

Slow Tasks

Example:

```text
Serialization
```

takes too long.

Result:

```text
Workers Occupied
```

Queue builds up.

---

## Bottleneck 3

Lock Contention

Workers share:

```text
Maps

Registries

Caches
```

Heavy locking reduces throughput.

---

## Bottleneck 4

GC Pressure

High allocation workloads:

```text
Create
Process
Discard
```

generate garbage.

Symptoms:

```text
Latency Spikes
```

---

## Bottleneck 5

Uneven Work Distribution

Example:

```text
BTCUSD
5000 Subscribers

AAPL
50 Subscribers
```

BTC tasks take longer.

Workers become imbalanced.

---

# 14. Scaling Strategies

## Single Queue

Simple.

```text
Queue
      ↓
Workers
```

Good for Week 1.

---

## Multiple Queues

Future.

```text
High Priority Queue

Normal Queue

Low Priority Queue
```

Supports prioritization.

---

## Sharded Worker Pools

Future.

```text
Pool A
BTC

Pool B
Equities

Pool C
Replay
```

Benefits:

```text
Isolation
```

between workloads.

---

# 15. Performance Analysis

Target:

```text
10000 Updates/sec
```

Example:

```text
32 Workers

Queue Size 10000
```

System behavior:

```text
Updates Arrive
      ↓

Queued
      ↓

Processed
      ↓

Delivered
```

Monitor:

```text
Queue Depth

Worker Utilization

Latency
```

continuously.

---

# 16. Observability

## Throughput

```text
tasks_received_total

tasks_completed_total

tasks_failed_total
```

---

## Queue Metrics

```text
queue_depth

queue_capacity

queue_rejections
```

---

## Worker Metrics

```text
active_workers

idle_workers

worker_utilization
```

---

## Performance Metrics

```text
task_latency

queue_wait_time

processing_time
```

---

## Resource Metrics

```text
memory_usage

cpu_usage

goroutine_count
```

---

# 17. Tradeoffs

| Design Choice     | Benefit            | Cost                        |
| ----------------- | ------------------ | --------------------------- |
| Small Worker Pool | Lower memory       | Higher latency              |
| Large Worker Pool | Higher throughput  | More scheduling overhead    |
| Large Queue       | Burst tolerance    | Higher memory               |
| Small Queue       | Predictable memory | Earlier backpressure        |
| Blocking Queue    | No drops           | Increased latency           |
| Drop Strategy     | Stable system      | Data loss                   |
| Backpressure      | Protects system    | Reduced upstream throughput |

---

# 18. Recommended Production Design

For a market data platform targeting:

```text
10000+ Updates/sec
5000+ Connections
```

Recommended architecture:

```text
Feed Generator
      ↓

Publisher
      ↓

Worker Pool
      ↓

Topic Manager
      ↓

Fan-Out Worker Pool
      ↓

WebSocket Gateway
```

Characteristics:

```text
Bounded Concurrency

Bounded Queues

Backpressure

Graceful Shutdown

Observable Metrics
```

The worker pool should act as a resource-governance layer, ensuring that no workload can create unbounded goroutines, consume unlimited memory, or destabilize the platform under load. This is the pattern commonly used in high-throughput market infrastructure, matching the expectations of a Goldman Sachs, Jane Street, Two Sigma, or Citadel-style systems design discussion.
