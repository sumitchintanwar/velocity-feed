# Sticky Session Support Design

## WebSocket-Based Market Data Platform

### Author

Principal Distributed Systems Engineer

### Goal

Design sticky session support for a horizontally scalable WebSocket-based market data platform.

Requirements:

* Multiple gateway instances
* Load balancer
* Persistent WebSocket connections
* Horizontal scalability
* High availability

Target architecture:

```text
                     Load Balancer
                            │
                            ▼

      +------------+------------+------------+
      |            |            |            |
      ▼            ▼            ▼            ▼

  Gateway 1   Gateway 2   Gateway 3   Gateway N
      │            │            │            │
      ▼            ▼            ▼            ▼

 WebSocket    WebSocket    WebSocket    WebSocket
   Clients      Clients      Clients      Clients

      │            │            │
      └────────────┴────────────┘
                    │
                    ▼

              Redis Pub/Sub
```

---

# 1. What Is A Sticky Session?

A sticky session ensures that a client is routed to the same backend server across requests.

Example:

```text
Client A
      ↓
Gateway 3
```

Future requests:

```text
Client A
      ↓
Gateway 3
```

continue reaching the same gateway.

---

## Without Sticky Sessions

```text
Request 1
Client A
      ↓
Gateway 1

Request 2
Client A
      ↓
Gateway 4

Request 3
Client A
      ↓
Gateway 2
```

Client affinity does not exist.

---

## With Sticky Sessions

```text
Client A
      ↓
Gateway 3

Client A
      ↓
Gateway 3

Client A
      ↓
Gateway 3
```

Client remains attached to one gateway.

---

# 2. Why Sticky Sessions Exist

Sticky sessions were originally introduced for stateful applications.

Example:

```text
User Login State

Shopping Cart

Session Cache
```

stored locally on a server.

---

Without stickiness:

```text
Request
      ↓
Different Server
```

which cannot find local state.

Result:

```text
Lost Session

Authentication Errors

Missing Context
```

---

# 3. WebSocket Specific Behavior

WebSockets are fundamentally different from HTTP.

HTTP:

```text
Request
Response
Disconnect
```

Every request is routed independently.

---

WebSocket:

```text
Connect
      ↓

Persistent TCP Connection
      ↓

Hours Of Communication
```

Once established:

```text
Client
      ↓
Gateway
```

remains fixed.

---

# Important Observation

A WebSocket connection is naturally sticky.

Example:

```text
Client
      ↓
Gateway 2
```

The load balancer only participates:

```text
During Connection Establishment
```

After that:

```text
Traffic Flows Directly
```

between client and gateway.

---

# 4. Why Sticky Sessions May Still Matter

Although active WebSocket connections are already sticky, reconnect behavior introduces challenges.

Example:

```text
Gateway Restart
```

causes:

```text
Client Disconnect
```

Then:

```text
Reconnect
```

occurs.

Without stickiness:

```text
Old Gateway → Gateway 2

Reconnect → Gateway 5
```

Client lands on a different gateway.

---

Potential consequences:

```text
Subscription Rebuild

Cache Misses

Warm-Up Delays
```

---

# 5. Current Gateway Architecture

Assume:

```text
Load Balancer
      ↓

Gateway Pool
      ↓

Redis Pub/Sub
```

Each gateway maintains:

```text
Connection State

Subscription State

Client Buffers
```

locally.

---

# Why Gateway State Matters

Example:

```text
Gateway 2
```

stores:

```text
Client A

Subscribed:
AAPL
MSFT
NVDA
```

If reconnect reaches:

```text
Gateway 5
```

that information is gone.

Client must rebuild state.

---

# 6. Sticky Session Strategies

Several approaches exist.

---

# Strategy 1: Source IP Affinity

Routing:

```text
Hash(IP Address)
      ↓
Gateway
```

Example:

```text
192.168.x.x
      ↓
Gateway 2
```

---

## Advantages

Simple.

Supported by most load balancers.

---

## Disadvantages

Not reliable.

Example:

```text
Corporate NAT

Mobile Networks

Carrier NAT
```

Many users share IPs.

Creates uneven load.

---

# Strategy 2: Cookie-Based Affinity

Load balancer issues:

```text
Session Cookie
```

Example:

```text
gateway=3
```

Future requests:

```text
Cookie
      ↓
Gateway 3
```

---

## Advantages

Accurate routing.

Widely supported.

---

## Disadvantages

Additional session management.

More useful for HTTP than WebSockets.

---

# Strategy 3: Consistent Hashing

Route using:

```text
Client ID
```

or:

```text
Account ID
```

Architecture:

```text
Client ID
      ↓

Hash Function
      ↓

Gateway Selection
```

---

Example:

```text
Client 12345
      ↓
Gateway 4
```

Always.

---

## Advantages

Predictable placement.

Good distribution.

No cookies required.

---

## Disadvantages

Gateway additions require rebalancing.

---

# Strategy 4: Stateless Gateways

Preferred modern design.

Gateway stores only:

```text
Live Connections
```

while subscription state is externalized.

Example:

```text
Redis

Database

Distributed Registry
```

---

Result:

```text
Reconnect
      ↓
Any Gateway
```

works.

No affinity required.

---

# 7. Alternative: Fully Stateless Architecture

Architecture:

```text
Client
      ↓

Load Balancer
      ↓

Any Gateway
      ↓

Redis Pub/Sub
      ↓

Distributed State
```

Gateway crash:

```text
Reconnect
      ↓
Different Gateway
```

No operational issue.

---

Benefits:

```text
Easy Scaling

Easy Failover

No Sticky Sessions Needed
```

---

# 8. Load Balancer Behavior

For WebSockets:

```text
HTTP Upgrade
      ↓
WebSocket
```

Load balancer chooses a gateway once.

After upgrade:

```text
Connection Pinned
```

to that gateway.

---

Therefore:

```text
Sticky Sessions
```

mainly affect:

```text
Reconnect Behavior
```

not active traffic.

---

# 9. Failure Scenario: Gateway Crash

Example:

```text
Gateway 3
```

fails.

---

Clients:

```text
Disconnected
```

---

Reconnect:

```text
Load Balancer
      ↓
Gateway 7
```

---

### With Sticky Sessions

May attempt:

```text
Gateway 3
```

which no longer exists.

Extra failover logic required.

---

### Without Sticky Sessions

Reconnect immediately lands on:

```text
Healthy Gateway
```

Often preferable.

---

# 10. Failure Scenario: Gateway Scaling Event

Example:

```text
5 Gateways
      ↓
10 Gateways
```

---

With affinity:

```text
Existing Clients
Remain On Old Nodes
```

Load imbalance occurs.

---

Without affinity:

```text
Natural Rebalancing
```

happens as clients reconnect.

---

# 11. Tradeoffs

| Approach               | Benefits            | Drawbacks               |
| ---------------------- | ------------------- | ----------------------- |
| Source IP Affinity     | Simple              | Poor distribution       |
| Cookie Affinity        | Accurate            | Session management      |
| Consistent Hashing     | Predictable routing | Rebalancing complexity  |
| Stateless Gateways     | Simplest operations | Requires external state |
| Strong Sticky Sessions | Faster reconnects   | Operational complexity  |

---

# 12. Market Data Platform Considerations

Market data systems prioritize:

```text
Availability

Scalability

Low Latency

Fault Tolerance
```

More than:

```text
User Session Continuity
```

Unlike e-commerce applications.

---

Client subscriptions can usually be:

```text
Re-Sent

Rebuilt

Recovered
```

after reconnect.

---

Therefore:

```text
Gateway Affinity
```

is often less valuable than:

```text
Fast Recovery
```

and:

```text
Operational Simplicity
```

---

# Recommended Architecture

```text
                     Load Balancer
                            │
                            ▼

      +------------+------------+------------+
      |            |            |            |
      ▼            ▼            ▼            ▼

  Gateway 1   Gateway 2   Gateway 3   Gateway N

      │            │            │
      └────────────┴────────────┘
                    │
                    ▼

              Redis Pub/Sub

                    │
                    ▼

          Distributed State Layer
```

Characteristics:

```text
Stateless Gateways

No Hard Affinity

Reconnect To Any Gateway

Externalized State

Horizontal Scalability
```

---

# Final Recommendation

For a WebSocket-based market data platform targeting:

```text
50,000+ Concurrent Connections
```

I would avoid strict sticky sessions.

Recommended approach:

```text
Stateless Gateways
+
Redis Pub/Sub
+
Least Connections Load Balancing
+
External Subscription State
```

Reasons:

```text
Simpler Operations

Better Fault Tolerance

Easier Scaling

Faster Recovery

More Even Load Distribution
```

Sticky sessions are useful when application state exists only on a gateway.

In a modern market data platform, the better architectural goal is:

```text
Remove The Need For Sticky Sessions
```

rather than making sticky sessions a critical dependency.
