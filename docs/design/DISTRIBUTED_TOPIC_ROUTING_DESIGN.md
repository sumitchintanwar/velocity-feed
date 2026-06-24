# Distributed Topic Routing Design

## Real-Time Market Data Platform

### Author

Principal Distributed Systems Engineer

### Goal

Design a distributed topic routing architecture that supports:

* Multiple gateway instances
* Clients connected to any gateway
* Redis Pub/Sub backend
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
