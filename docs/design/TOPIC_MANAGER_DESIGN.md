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


---

# Sub-component: MESSAGE_ORDERING_GUARANTEES_DESIGN
                        ↓

                    Publisher
                        ↓

                 Redis Pub/Sub
                        ↓

      +----------------------------------+
      |                                  |
      ▼                                  ▼

 Gateway 1                       Gateway N
      ↓                                  ↓

 WebSocket Clients              WebSocket Clients
```

Requirements:

```text
Ordered Delivery Per Symbol

Distributed Architecture

Redis Backend

Horizontal Scalability

Replay Compatibility
```

---

# 1. Why Ordering Matters

Market data consumers assume:

```text
Newer Prices Arrive After Older Prices
```

Example:

```text
AAPL

250.10
250.15
250.20
```

Correct ordering:

```text
250.10

→

250.15

→

250.20
```

---

Incorrect ordering:

```text
250.20

→

250.10

→

250.15
```

causes:

```text
Invalid Market State

Incorrect Analytics

Bad Trading Decisions
```

---

# 2. What Ordering Guarantee Do We Need?

There are multiple levels of ordering.

---

## Global Ordering

Every message in the system has one universal order.

Example:

```text
AAPL

MSFT

NVDA

TSLA
```

all share one sequence.

---

## Per-Symbol Ordering

Each symbol maintains its own ordering.

Example:

```text
AAPL

1
2
3
4
```

and

```text
MSFT

1
2
3
4
```

independently.

---

# 3. Recommended Guarantee

For market data systems:

```text
Per-Symbol Ordering
```

is the correct choice.

---

Reason:

Consumers typically care about:

```text
Price Evolution
```

for a symbol.

Not:

```text
Cross-Symbol Global Ordering
```

---

# 4. Ordering Scope

Guarantee:

```text
AAPL Updates
```

arrive in order.

Example:

```text
AAPL #100

AAPL #101

AAPL #102
```

---

Do NOT guarantee:

```text
AAPL vs MSFT
```

ordering.

Example:

```text
AAPL #100

MSFT #200

AAPL #101
```

is acceptable.

---

# 5. Sequence Numbers

Ordering should be based on:

```text
Sequence Numbers
```

not timestamps.

---

Why?

Timestamps may have:

```text
Clock Drift

Precision Issues

Network Delays
```

---

Sequence numbers are:

```text
Deterministic

Monotonic

Easy To Validate
```

---

# 6. Per-Symbol Sequence Numbers

Each symbol maintains:

```text
Next Sequence Number
```

Example:

```text
AAPL

1001

1002

1003
```

---

MSFT:

```text
5001

5002

5003
```

---

Independent ordering streams.

---

# 7. Event Structure

Every update should contain:

```text
Symbol

Sequence Number

Timestamp

Price

Volume
```

Example:

```text
AAPL

Sequence: 1003

Price: 250.50
```

---

# 8. Update Generation Flow

Recommended flow:

```text
Feed Generator
      ↓

Assign Sequence Number
      ↓

Publish Event
      ↓

Redis
```

---

Sequence assignment must occur:

```text
Before Publishing
```

---

# 9. Redis Ordering Characteristics

Redis Pub/Sub provides:

```text
In-Order Delivery
```

for messages published on a channel.

---

Example:

```text
Publish

1

2

3
```

Subscribers receive:

```text
1

2

3
```

in order.

---

Important:

This ordering only applies:

```text
Per Channel

Per Publisher Stream
```

---

# 10. Redis Limitation

Redis does NOT provide:

```text
Durable Ordering

Replay

Gap Recovery
```

---

If a subscriber disconnects:

```text
Messages Lost
```

---

Ordering guarantees alone are insufficient.

Replay support is required.

---

# 11. Channel Mapping Strategy

Recommended:

```text
One Channel Per Symbol
```

Example:

```text
market:AAPL

market:MSFT

market:NVDA
```

---

Benefits:

```text
Natural Ordering

Independent Scaling

Simpler Reasoning
```

---

Alternative:

```text
Single Global Channel
```

---

Problems:

```text
Higher Fan-Out

More Filtering

Harder Scaling
```

---

# 12. Gateway Ordering Requirements

Gateways must preserve:

```text
Redis Arrival Order
```

---

Bad design:

```text
Receive Update
      ↓

Multiple Worker Pools
      ↓

Concurrent Delivery
```

---

Potential result:

```text
1003 Delivered

Before

1002
```

---

# 13. Recommended Gateway Model

Per-symbol processing path:

```text
Redis Message
      ↓

Topic Manager
      ↓

Subscriber Queue
      ↓

WebSocket Writer
```

---

Messages remain:

```text
FIFO
```

through the pipeline.

---

# 14. Per-Subscriber Buffers

Client buffers must preserve:

```text
Insertion Order
```

Example:

```text
1001

1002

1003
```

---

Buffer outputs:

```text
1001

1002

1003
```

---

Never:

```text
Parallel Reordering
```

within a subscriber queue.

---

# 15. Detecting Ordering Violations

Clients should track:

```text
Last Sequence Number
```

per symbol.

---

Example:

Current:

```text
AAPL
1005
```

Received:

```text
1007
```

---

Gap detected:

```text
1006 Missing
```

---

Client can request:

```text
Replay
```

---

# 16. Gap Detection

Example:

```text
Expected

1006
```

Received:

```text
1008
```

---

Client identifies:

```text
Missing

1006

1007
```

---

Action:

```text
Pause Processing

Request Replay
```

---

# 17. Replay Integration

Replay service becomes critical.

Recovery flow:

```text
Gap Detected
      ↓

Replay Request
      ↓

Fetch Missing Events
      ↓

Apply In Order
      ↓

Resume Streaming
```

---

# 18. Distributed Gateway Considerations

Example:

```text
Gateway 1

Gateway 2

Gateway 3
```

---

Clients connected to different gateways should still see:

```text
Same Symbol Order
```

---

Why?

All gateways consume:

```text
Same Redis Stream
```

in the same order.

---

Thus:

```text
AAPL #1001

AAPL #1002

AAPL #1003
```

arrives identically everywhere.

---

# 19. Scaling Tradeoffs

## Stronger Ordering

Example:

```text
Global Sequence Numbers
```

Advantages:

```text
Total Ordering
```

Disadvantages:

```text
Central Coordination

Scalability Limits

Higher Latency
```

---

## Per-Symbol Ordering

Advantages:

```text
Independent Streams

Horizontal Scalability

Lower Coordination
```

---

Disadvantages:

```text
No Global Ordering
```

---

# 20. Multi-Publisher Limitation

If multiple publishers generate:

```text
AAPL
```

updates simultaneously:

```text
Publisher A

Publisher B
```

ordering becomes difficult.

---

Recommendation:

```text
Single Logical Publisher
Per Symbol
```

---

Benefits:

```text
Simple Ordering

Deterministic Sequences
```

---

# 21. Failure Scenario

Gateway restart.

Example:

```text
Client Last Seen

1000
```

---

Reconnect occurs.

Current sequence:

```text
1015
```

---

Recovery:

```text
Replay

1001 → 1015
```

---

Ordering preserved.

---

# 22. Operational Metrics

Track:

```text
market_sequence_gap_total

market_out_of_order_total

market_replay_requests_total

market_replay_gap_size

market_latest_sequence
```

---

Useful alerts:

```text
Gap Detection Spike

Replay Request Spike

Out-Of-Order Delivery
```

---

# 23. Recommended Architecture

```text
              Feed Generator
                     ↓

         Per-Symbol Sequencer
                     ↓

              Redis Pub/Sub
                     ↓

              Gateway Pool
                     ↓

         Per-Subscriber FIFO Queue
                     ↓

              WebSocket Client
                     ↓

         Sequence Validation
                     ↓

          Replay If Required
```

---

# Ordering Guarantees Summary

Guaranteed:

```text
AAPL

1001

1002

1003

1004
```

delivered in order.

---

Not Guaranteed:

```text
AAPL #1003

vs

MSFT #2005
```

relative ordering.

---

# Limitations

Redis Pub/Sub provides:

```text
Live Delivery
```

but not:

```text
Persistence

Replay

Exactly Once Delivery
```

Therefore:

```text
Ordering
+
Sequence Numbers
+
Replay Service
```

must work together.

---

# Final Recommendation

Implement:

```text
Per-Symbol Sequence Numbers
```

and guarantee:

```text
Ordered Delivery Per Symbol
```

through:

```text
Single Symbol Sequencer
      ↓

Redis Channel
      ↓

FIFO Subscriber Queue
      ↓

Sequence Validation
      ↓

Replay Recovery
```

Avoid:

```text
Global Ordering

Cross-Symbol Coordination

Multi-Writer Symbol Streams
```

This provides the best balance between:

```text
Correctness

Scalability

Low Latency

Operational Simplicity
```

while supporting a distributed Redis-backed market data platform serving tens of thousands of concurrent clients.

---

# Sub-component: DISTRIBUTED_TOPIC_ROUTING_DESIGN
* Topic-based subscriptions
* Horizontal scalability

Target architecture:

```text
                    Feed Generator
                           ↓

                       Publisher
                           ↓

                    Redis Pub/Sub
                           ↓

     +-----------+-----------+-----------+
     |           |           |           |
     ↓           ↓           ↓           ↓

 Gateway 1   Gateway 2   Gateway 3   Gateway N

     ↓           ↓           ↓           ↓

 Clients     Clients     Clients     Clients
```

Requirements:

```text
Clients Can Connect Anywhere

Topic-Based Subscriptions

Efficient Routing

Horizontal Scaling
```

---

# 1. Problem Statement

In a single-node system:

```text
Topic Manager
      ↓
Subscribers
```

routing is straightforward.

Example:

```text
AAPL
    →
Client A
Client B
Client C
```

When an update arrives:

```text
AAPL Update
```

it is delivered directly.

---

## Distributed Challenge

With multiple gateways:

```text
Gateway 1

Gateway 2

Gateway 3
```

subscribers become distributed.

Example:

```text
AAPL Subscribers

Client A → Gateway 1

Client B → Gateway 2

Client C → Gateway 3
```

The platform must route updates to all interested clients regardless of where they are connected.

---

# 2. High-Level Architecture

```text
                    Feed Generator
                           ↓

                       Publisher
                           ↓

                    Redis Pub/Sub
                           ↓

    +-------------+-------------+-------------+
    |             |             |             |
    ▼             ▼             ▼             ▼

 Gateway 1    Gateway 2    Gateway 3    Gateway N

    ▼             ▼             ▼

 Topic Mgr    Topic Mgr    Topic Mgr

    ▼             ▼             ▼

 Clients      Clients      Clients
```

---

# 3. Routing Principles

The system should satisfy:

```text
Publish Once

Route Everywhere

Deliver Locally
```

Publisher should never know:

```text
Client Location

Gateway Location

Subscriber Count
```

Routing logic belongs to gateways.

---

# 4. Subscription Flow

## Step 1

Client connects.

Example:

```text
Client A
      ↓
Gateway 2
```

---

## Step 2

Client subscribes.

Example:

```text
SUBSCRIBE AAPL
```

---

## Step 3

Gateway updates local Topic Manager.

```text
Gateway 2

AAPL
   →
Client A
```

---

## Step 4

Gateway ensures it receives AAPL updates.

Depending on routing strategy:

```text
Subscribe To Redis Channel
```

or

```text
Already Receiving Updates
```

---

# 5. Local Subscription Registry

Each gateway maintains:

```text
Topic
   →
Subscribers
```

Example:

```text
AAPL
   →
Client A
Client B
```

Stored locally.

---

## Why Local Registries?

Advantages:

```text
Fast Lookups

No Network Hop

Low Latency
```

Message delivery remains local.

---

# 6. Message Flow

Market update:

```text
AAPL
250.25
```

generated.

---

### Step 1

Feed Generator creates update.

```text
Feed Generator
      ↓
Publisher
```

---

### Step 2

Publisher sends update.

```text
Publisher
      ↓
Redis
```

---

### Step 3

Redis distributes update.

```text
Redis
      ↓

Gateway 1

Gateway 2

Gateway 3
```

---

### Step 4

Each gateway receives update.

```text
AAPL Update
```

---

### Step 5

Local Topic Manager performs routing.

Example:

```text
Gateway 2

AAPL
  →
Client A
```

Only matching subscribers receive the update.

---

# 7. Routing Model Options

Several routing models exist.

---

# Option 1

Global Broadcast

Architecture:

```text
Publisher
      ↓

Redis Channel

market-data
```

All updates enter one channel.

---

## Flow

```text
Redis
      ↓

All Gateways
```

Every gateway receives every update.

---

## Advantages

Very simple.

Easy implementation.

No subscription coordination.

---

## Disadvantages

Large amount of unnecessary traffic.

Example:

```text
Gateway Receives

AAPL

BTCUSD

ETHUSD

TSLA

...
```

even when local clients need only AAPL.

---

## Scalability

Good initially.

Weak at very large scale.

---

# Option 2

Per-Symbol Channels

Example:

```text
market:AAPL

market:MSFT

market:TSLA
```

---

## Flow

Gateway subscribes only to required symbols.

Example:

```text
Client Subscribes To AAPL
      ↓
Gateway Subscribes To
market:AAPL
```

---

## Advantages

Efficient traffic.

Minimal filtering.

---

## Disadvantages

Large channel count.

Example:

```text
10000 Symbols
=
10000 Redis Channels
```

Operational complexity grows.

---

# Option 3

Topic Group Routing (Recommended)

Group related symbols.

Example:

```text
market:equities

market:crypto

market:forex

market:futures
```

---

## Example

```text
market:equities
```

contains:

```text
AAPL

MSFT

TSLA

NVDA
```

---

## Benefits

Balances:

```text
Channel Count

Traffic Volume

Operational Simplicity
```

---

# 8. Dynamic Subscription Management

Gateway subscribes to Redis channels only when needed.

---

## Example

First client requests:

```text
AAPL
```

Gateway subscribes to:

```text
market:equities
```

---

Additional clients:

```text
AAPL

MSFT

NVDA
```

require no new Redis subscriptions.

---

When last subscriber leaves:

```text
No Equities Clients
```

Gateway may unsubscribe.

---

# 9. Message Routing Inside Gateway

Gateway receives:

```text
AAPL Update
```

---

Routing process:

```text
Redis Message
      ↓

Topic Manager Lookup
      ↓

Subscriber List
      ↓

Client Queues
      ↓

WebSocket Delivery
```

---

# 10. Scaling Implications

## Scaling Gateways

Current:

```text
3 Gateways
```

Future:

```text
20 Gateways
```

No publisher changes required.

Redis naturally distributes updates.

---

## Scaling Clients

Current:

```text
5000 Clients
```

Future:

```text
50000 Clients
```

Connections distribute across gateways.

Each gateway routes only for its own clients.

---

## Scaling Topics

Current:

```text
100 Symbols
```

Future:

```text
10000 Symbols
```

Channel strategy becomes increasingly important.

---

# 11. Gateway Independence

A key principle:

```text
No Gateway Should Know
About Other Gateways
```

Gateway responsibilities:

```text
Local Connections

Local Subscriptions

Local Delivery
```

Redis handles distribution.

---

## Benefits

```text
Simple Architecture

Independent Scaling

Fault Isolation
```

---

# 12. Failure Scenario: Gateway Crash

Example:

```text
Gateway 2
```

fails.

---

Effects:

```text
Connected Clients Disconnect
```

Only those clients are affected.

---

Other gateways:

```text
Continue Operating
```

normally.

---

## Recovery

Clients reconnect.

May land on:

```text
Gateway 1

Gateway 3

Gateway N
```

and rebuild subscriptions.

---

# 13. Failure Scenario: Redis Outage

Redis unavailable.

Effects:

```text
No New Topic Updates
```

---

Existing connections remain:

```text
Connected
```

but data flow stops.

---

Mitigation:

```text
Redis Sentinel

Redis Cluster

Managed Redis
```

---

# 14. Performance Characteristics

## Publish Cost

Publisher:

```text
Publish Once
```

regardless of subscriber count.

---

## Gateway Cost

Each gateway performs:

```text
Topic Lookup

Local Fan-Out
```

only for connected clients.

---

## Routing Complexity

Topic lookup:

```text
O(1)
```

with hash-based topic maps.

---

Fan-out:

```text
O(Number Of Subscribers)
```

for a given topic.

---

# 15. Tradeoffs

| Design Choice        | Benefits               | Drawbacks                          |
| -------------------- | ---------------------- | ---------------------------------- |
| Global Broadcast     | Simple                 | High traffic                       |
| Per-Symbol Channels  | Precise routing        | Large channel count                |
| Topic Group Channels | Balanced approach      | Additional routing logic           |
| Local Registries     | Fast delivery          | Duplicate subscription state       |
| Central Registry     | Single source of truth | Additional latency                 |
| Gateway Independence | Easy scaling           | Rebuild subscriptions on reconnect |

---

# Recommended Production Architecture

```text
                    Feed Generator
                           ↓

                       Publisher
                           ↓

                    Redis Pub/Sub
                           ↓

      +----------------------------------+
      |                                  |
      ▼                                  ▼

 Gateway Pool (Independent Instances)

      ▼
 Local Topic Manager
      ▼
 Local Subscribers
      ▼
 WebSocket Delivery
```

Channel strategy:

```text
market:equities

market:crypto

market:forex

market:futures
```

Routing strategy:

```text
Publish Once

Distribute Via Redis

Route Locally

Deliver To Subscribers
```

---

# Final Recommendation

For a distributed market data platform with:

```text
Multiple Gateways

Redis Pub/Sub

Topic-Based Subscriptions
```

the recommended routing model is:

```text
Redis Pub/Sub
+
Topic Group Channels
+
Local Topic Managers
+
Independent Gateways
```

This architecture provides:

```text
Horizontal Scalability

Low Latency

Simple Operations

Fault Isolation

Gateway Independence
```

while supporting growth from:

```text
Thousands Of Clients
```

to

```text
Tens Of Thousands Of Clients
```

without requiring changes to the Publisher or subscription model.
