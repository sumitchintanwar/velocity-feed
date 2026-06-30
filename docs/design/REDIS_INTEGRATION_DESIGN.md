# Redis Pub/Sub Integration Design

## Real-Time Market Data Platform

### Author

Principal Distributed Systems Engineer

### Goal

Extend the current market data platform to support:

* Multiple WebSocket Gateway instances
* Distributed message delivery
* Horizontal scalability
* Instance independence
* Future multi-node deployment

Current architecture:

```text
Feed Generator
      ↓
Publisher
      ↓
Topic Manager
      ↓
WebSocket Gateway
```

Target architecture:

```text
Feed Generator
      ↓
Publisher
      ↓
Redis Pub/Sub
      ↓
+---------------------+
| Gateway Instance 1  |
+---------------------+

+---------------------+
| Gateway Instance 2  |
+---------------------+

+---------------------+
| Gateway Instance N  |
+---------------------+
```

---

# 1. Why Redis Pub/Sub?

## Problem

Current design assumes:

```text
Single Process

Single Topic Manager

Single Gateway
```

Example:

```text
Client A
Client B
Client C
```

all connected to:

```text
Gateway #1
```

This works initially but limits scalability.

---

## Scaling Problem

Suppose:

```text
5000 Clients
```

becomes:

```text
50000 Clients
```

One gateway eventually reaches:

```text
CPU Limits

Memory Limits

Network Limits
```

Horizontal scaling becomes necessary.

---

## Goal

Allow:

```text
Gateway #1

Gateway #2

Gateway #3

Gateway #N
```

to receive the same market updates.

Redis Pub/Sub becomes the distribution layer.

---

# 2. High-Level Architecture

## Distributed Design

```text
                 Feed Generator
                        ↓

                   Publisher
                        ↓

                 Redis Pub/Sub
                        ↓
    +-------------------+-------------------+
    |                   |                   |
    ↓                   ↓                   ↓

 Gateway 1         Gateway 2          Gateway N
    ↓                   ↓                   ↓

 Local Topic      Local Topic       Local Topic
 Manager          Manager           Manager
    ↓                   ↓                   ↓

 Clients          Clients           Clients
```

---

# 3. Responsibilities

## Publisher

Responsible for:

```text
Receive Market Updates

Serialize Events

Publish To Redis
```

It does NOT know:

```text
Connected Clients

Subscriptions

Gateway State
```

---

## Redis

Responsible for:

```text
Cross-Instance Distribution
```

Redis becomes the message bus.

---

## Gateway

Responsible for:

```text
Subscribe To Redis Channels

Maintain Local Clients

Perform Fan-Out
```

---

## Topic Manager

Responsible for:

```text
Local Subscription Registry
```

Each gateway maintains its own registry.

---

# 4. Message Flow

## Example

Market update:

```text
AAPL
Price = 250.25
```

---

### Step 1

Feed Generator creates update.

```text
Feed Generator
      ↓
Market Update
```

---

### Step 2

Publisher receives update.

```text
Publisher
      ↓
Publish To Redis
```

---

### Step 3

Redis broadcasts.

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
Redis Message
      ↓
Local Topic Manager
```

---

### Step 5

Local fan-out.

Example:

```text
Gateway 1
```

has:

```text
Client A

Client B
```

subscribed.

Only those clients receive the update.

---

# 5. Redis Channel Mapping

Channel design is critical.

---

# Option 1

Single Global Channel

```text
market-data
```

All updates published to:

```text
market-data
```

---

## Advantages

Simple.

```text
One Publisher

One Subscriber
```

---

## Problems

Every gateway receives:

```text
Every Symbol
```

Example:

```text
AAPL

BTCUSD

ETHUSD

TSLA
```

even if no clients need them.

Creates unnecessary work.

---

# Option 2

Per-Symbol Channels

Example:

```text
market:AAPL

market:MSFT

market:TSLA

market:BTCUSD
```

---

## Advantages

Natural mapping.

```text
Symbol
=
Redis Channel
```

---

## Problems

Large channel count.

Example:

```text
10000 Symbols
=
10000 Redis Channels
```

Operational complexity increases.

---

# Option 3

Topic-Based Channels (Recommended)

Group symbols.

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

Fan-Out Efficiency

Operational Simplicity
```

---

# 6. Gateway Subscription Model

Each gateway subscribes only to channels it needs.

Example:

```text
Gateway #1
```

contains:

```text
AAPL Clients
MSFT Clients
```

Gateway subscribes to:

```text
market:equities
```

---

## Dynamic Subscription

When first client arrives:

```text
Subscribe(AAPL)
```

Gateway determines:

```text
Need Equities Channel
```

and subscribes.

---

When last client leaves:

```text
Unsubscribe(AAPL)
```

Gateway may stop consuming that channel.

---

# 7. Local Topic Manager

Redis should NOT replace the Topic Manager.

Redis distributes updates.

Topic Manager performs:

```text
Client Routing
```

Architecture:

```text
Redis Message
      ↓

Gateway

      ↓

Local Topic Manager

      ↓

Subscribers
```

This avoids:

```text
Redis Per Client Operations
```

which would not scale.

---

# 8. Scalability Characteristics

## Single Gateway

```text
5000 Clients
```

---

## Multiple Gateways

```text
Gateway 1
5000 Clients

Gateway 2
5000 Clients

Gateway 3
5000 Clients
```

Total:

```text
15000 Clients
```

without changing Publisher logic.

---

## Horizontal Scaling

Add another instance:

```text
Gateway N
```

and connect it to Redis.

No architecture changes required.

---

# 9. Failure Scenario: Gateway Crash

Example:

```text
Gateway #2
```

fails.

---

Result:

```text
Clients Disconnect
```

Only clients on that instance are affected.

---

Other gateways:

```text
Continue Operating
```

normally.

---

## Recovery

Gateway restarts.

```text
Reconnect To Redis
```

```text
Rebuild Local State
```

```text
Accept Clients
```

---

# 10. Failure Scenario: Redis Restart

Most important failure case.

Example:

```text
Redis Unavailable
```

---

Consequences

```text
No New Message Distribution
```

Publisher cannot distribute updates.

Gateways stop receiving updates.

---

Result:

```text
Market Data Stalls
```

until Redis recovers.

---

## Mitigation

Use:

```text
Redis Sentinel

Redis Cluster

Managed Redis
```

for high availability.

---

# 11. Failure Scenario: Slow Gateway

Example:

```text
Gateway #3
```

experiences:

```text
High CPU

Slow Clients
```

---

Effects

Local queues grow.

However:

```text
Gateway #1

Gateway #2
```

remain healthy.

Redis isolates instances from each other.

---

# 12. Failure Scenario: Subscriber Explosion

Example:

```text
Market Open
```

suddenly causes:

```text
10000 New Clients
```

---

Effects

```text
Connection Surge

Subscription Surge
```

Redis continues distributing updates.

Load is distributed across gateways.

---

# 13. Redis Pub/Sub Limitations

Redis Pub/Sub is:

```text
Fire And Forget
```

---

Meaning:

```text
No Persistence

No Replay

No Acknowledgments
```

If a subscriber disconnects:

```text
Messages Are Lost
```

---

## Example

```text
Gateway Disconnects
```

for:

```text
10 Seconds
```

Messages during outage:

```text
Cannot Be Recovered
```

---

# 14. Future Evolution

As requirements grow:

```text
Replay

Persistence

Guaranteed Delivery
```

Redis Pub/Sub becomes insufficient.

---

Potential future architecture:

```text
Feed Generator
      ↓

Publisher
      ↓

Redis Streams
      ↓

Gateways
```

or

```text
Apache Kafka
```

---

Benefits:

```text
Replay

Retention

Consumer Groups

Durability
```

---

# 15. Tradeoffs

| Design Choice        | Benefit           | Cost                     |
| -------------------- | ----------------- | ------------------------ |
| Single Gateway       | Simplicity        | No horizontal scaling    |
| Redis Pub/Sub        | Easy scaling      | No persistence           |
| Per-Symbol Channels  | Precise routing   | Large channel count      |
| Global Channel       | Simple            | High unnecessary traffic |
| Topic-Based Channels | Balanced approach | Additional routing logic |
| Redis Cluster        | High availability | Operational complexity   |
| Redis Streams/Kafka  | Durability        | Higher complexity        |

---

# Recommended Production Design

```text
                 Feed Generator
                        ↓

                   Publisher
                        ↓

                 Redis Pub/Sub
                        ↓

     +----------------+----------------+
     |                |                |
     ↓                ↓                ↓

 Gateway 1      Gateway 2       Gateway N
     ↓                ↓                ↓

 Local Topic    Local Topic     Local Topic
 Manager        Manager         Manager
     ↓                ↓                ↓

 WebSocket      WebSocket       WebSocket
 Clients        Clients         Clients
```

Channel strategy:

```text
market:equities

market:crypto

market:forex

market:futures
```

Characteristics:

```text
Horizontal Scaling

Gateway Independence

Simple Operations

Low Latency

Easy Deployment
```

---

# Final Recommendation

For the next stage of a market data platform, Redis Pub/Sub is the ideal intermediate architecture because it provides:

```text
Multi-Gateway Support

Topic-Based Distribution

Horizontal Scalability

Low Operational Overhead

Low Latency
```

while keeping the system simple.

Use:

```text
Redis Pub/Sub
+
Local Topic Managers
+
Topic-Based Redis Channels
```

today.

Plan a future migration toward:

```text
Redis Streams
```

or

```text
Apache Kafka
```

when requirements include:

```text
Replay

Persistence

Guaranteed Delivery

Historical Recovery
```

This progression mirrors how many production market data platforms evolve from a simple distributed fan-out layer into a fully durable event streaming architecture.
