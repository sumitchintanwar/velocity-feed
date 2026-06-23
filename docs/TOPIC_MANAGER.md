# In-Memory Topic Manager Design

## Real-Time Market Data Distribution System

### Version

Week 1 Architecture Design

### Purpose

The Topic Manager is responsible for managing topic subscriptions and routing market updates to interested subscribers.

It acts as the bridge between:

```text
Market Updates
        ↓
Topic Manager
        ↓
Subscribers
```

The design must support:

* Subscribe
* Unsubscribe
* Publish
* 10,000+ concurrent subscribers
* Low-latency fan-out
* Future distributed implementations

No network transport should be embedded into the Topic Manager.

---

# 1. Problem Statement

Market data platforms distribute updates by topic.

Examples:

```text
AAPL
MSFT
GOOG
BTCUSD
ETHUSD
```

Clients subscribe to topics.

Example:

```text
Client A → AAPL

Client B → AAPL, MSFT

Client C → BTCUSD
```

When a new market update arrives:

```text
AAPL Price Update
```

Only interested subscribers should receive it.

---

# 2. Core Responsibilities

## Subscribe

Register a subscriber to a topic.

Example:

```text
Client 1001
    ↓
Subscribe
    ↓
AAPL
```

The Topic Manager records the relationship.

---

## Unsubscribe

Remove subscriber interest.

Example:

```text
Client 1001
    ↓
Unsubscribe
    ↓
AAPL
```

The mapping should be removed efficiently.

---

## Publish

Distribute a market update.

Example:

```text
AAPL Update
      ↓
Topic Manager
      ↓
All AAPL Subscribers
```

Fan-out should be performed with minimal latency.

---

# 3. High-Level Architecture

```text
                 Market Update
                        ↓

              +------------------+
              |  Topic Manager   |
              +------------------+
                        |
          +-------------+-------------+
          |             |             |
          ↓             ↓             ↓

     Subscriber    Subscriber    Subscriber
         A             B             C
```

---

# 4. Topic Registry Design

## Primary Data Structure

The central structure should maintain:

```text
Topic
    →
Subscriber Set
```

Conceptually:

```text
AAPL
    →
{1,2,3,4,5}

MSFT
    →
{2,5,7}

BTCUSD
    →
{9,10}
```

---

## Why a Set?

Membership checks become efficient.

Operations:

```text
Subscribe
Unsubscribe
Contains
```

remain constant time on average.

---

## Subscriber Lookup

Each topic should maintain:

```text
Unique Subscribers
```

Benefits:

* No duplicates
* Fast removal
* Fast existence checks

---

# 5. Reverse Index

A second structure should be maintained.

Conceptually:

```text
Subscriber
      →
Subscribed Topics
```

Example:

```text
Client 100

→ AAPL
→ MSFT
→ GOOG
```

---

## Why?

Without a reverse index:

```text
Disconnect Client
```

requires scanning every topic.

Complexity becomes expensive.

---

## With Reverse Index

Disconnect becomes:

```text
Lookup Subscriber
      ↓
Get Topics
      ↓
Remove Directly
```

Much more efficient.

---

# 6. Recommended Data Model

## Topic Registry

```text
Topic
    →
Subscriber Set
```

Used for:

```text
Publish
```

---

## Subscriber Registry

```text
Subscriber
    →
Topic Set
```

Used for:

```text
Disconnect
Unsubscribe All
Cleanup
```

---

# 7. Concurrency Model

## Problem

Operations happen simultaneously.

Examples:

```text
Subscribe
Unsubscribe
Publish
Disconnect
```

All may occur concurrently.

---

## Design Goal

Publishing should never block unnecessarily.

Publishing is the hot path.

Subscriptions are relatively infrequent.

---

# 8. Locking Strategy

## Naive Global Lock

Example:

```text
Single Mutex
```

Protects everything.

Architecture:

```text
Subscribe
      ↓

Global Lock

Publish
      ↓

Global Lock
```

---

## Problems

At scale:

```text
10000 Subscribers
```

Publish traffic dominates.

Result:

```text
Lock Contention
```

Symptoms:

* Increased latency
* Reduced throughput
* CPU spikes

---

# 9. Recommended Locking Model

## Read-Mostly Design

Publishing is far more common than subscribing.

Typical ratio:

```text
Publishes
    >>
Subscriptions
```

Use a design optimized for reads.

---

## Read-Write Lock

Publishing:

```text
Read Lock
```

Subscribe:

```text
Write Lock
```

Benefits:

```text
Multiple Publishers
```

can proceed concurrently.

---

## Topic-Level Locking

Avoid one lock for the entire registry.

Instead:

```text
AAPL Lock

MSFT Lock

BTCUSD Lock
```

Benefits:

```text
AAPL Subscribe
```

does not block:

```text
BTC Publish
```

---

# 10. Sharded Registry Design

For larger scale:

```text
Shard 1
Shard 2
Shard 3
Shard N
```

Topics are distributed across shards.

Example:

```text
AAPL → Shard 1

MSFT → Shard 2

BTCUSD → Shard 3
```

Each shard maintains:

```text
Own Registry
Own Lock
```

Benefits:

* Lower contention
* Better CPU utilization
* Higher throughput

---

# 11. Publish Strategy

## Publish Flow

```text
Market Update
       ↓

Lookup Topic
       ↓

Get Subscriber Set
       ↓

Fan-Out
```

Publishing should avoid modifications.

Only read operations.

---

## Snapshot Strategy

When publishing:

```text
Copy Subscriber References
```

before dispatching.

Benefits:

```text
Subscribers Can Join
Subscribers Can Leave
```

without affecting active delivery.

---

# 12. Fan-Out Model

## Synchronous Fan-Out

Example:

```text
Publish
      ↓

Send To A
Send To B
Send To C
```

Problems:

```text
Slow Subscriber
```

slows everyone.

---

## Recommended

Asynchronous delivery.

Architecture:

```text
Publish
      ↓

Subscriber Queues
      ↓

Worker Delivery
```

Benefits:

* Isolation
* Better latency
* Reduced blocking

---

# 13. Subscriber Representation

Each subscriber should maintain:

```text
ID

Outbound Queue

Status

Metadata
```

The Topic Manager only tracks references.

It should not know transport details.

---

# 14. Performance Analysis

## Target

```text
10,000 Subscribers
```

---

## Example

```text
100 Topics

100 Subscribers Per Topic
```

Produces:

```text
10,000 Total Relationships
```

Very manageable in memory.

---

## Publish Cost

Complexity:

```text
O(Number Of Subscribers For Topic)
```

Example:

```text
AAPL
500 Subscribers
```

Publish must touch:

```text
500 Targets
```

This is unavoidable.

---

# 15. Major Bottlenecks

## Bottleneck 1

Global Lock Contention

Symptoms:

```text
High CPU
Poor Scalability
```

Mitigation:

```text
Shard Locks
Topic Locks
```

---

## Bottleneck 2

Slow Subscribers

Example:

```text
WebSocket Client
```

becomes slow.

Result:

```text
Publish Pipeline Blocks
```

Mitigation:

```text
Per Subscriber Queue
```

---

## Bottleneck 3

Memory Growth

Example:

```text
Disconnected Client
```

not removed.

Result:

```text
Subscription Leak
```

Mitigation:

```text
Connection Cleanup
Heartbeat Monitoring
```

---

## Bottleneck 4

Massive Fan-Out

Example:

```text
BTCUSD

5000 Subscribers
```

Single update becomes:

```text
5000 Deliveries
```

Mitigation:

```text
Batch Delivery
Worker Pools
```

---

# 16. Future Redis Integration

Current:

```text
Topic Manager
      ↓
In-Memory Subscribers
```

Future:

```text
Topic Manager
      ↓

Redis Adapter
      ↓

Other Instances
```

The Topic Manager should expose abstractions only.

Redis becomes another subscriber.

No redesign required.

---

# 17. Observability

Metrics should include:

## Subscription Metrics

```text
active_topics

active_subscribers

subscriptions_total
```

---

## Publish Metrics

```text
messages_published

messages_delivered

delivery_latency
```

---

## Queue Metrics

```text
subscriber_queue_depth

dropped_messages
```

---

## Registry Metrics

```text
topic_count

subscriber_count
```

---

# 18. Architecture Summary

```text
                    Market Update
                           ↓

                 +------------------+
                 |  Topic Manager   |
                 +------------------+
                           |
          +----------------+----------------+
          |                                 |
          ↓                                 ↓

 Topic Registry                    Subscriber Registry

 Topic → Subscribers         Subscriber → Topics

          |
          ↓

     Fan-Out Engine
          |
          ↓

   Subscriber Queues
          |
          ↓

   Delivery Workers
```

---

# Recommended Production Design

For a market data platform targeting 10,000+ subscribers:

### Registry

```text
Topic → Subscriber Set
Subscriber → Topic Set
```

### Locking

```text
Sharded RW Locks
```

### Publishing

```text
Read-Optimized
```

### Delivery

```text
Asynchronous Fan-Out
```

### Scalability

```text
Topic Sharding
Per Subscriber Queues
```

### Future Ready

```text
Redis
Kafka
NATS
```

can be added as adapters without changing the Topic Manager architecture.

This design comfortably supports 10,000 subscribers and provides a clear path toward distributed topic routing in later phases of the platform.
