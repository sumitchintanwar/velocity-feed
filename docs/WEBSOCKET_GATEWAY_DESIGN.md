# WebSocket Gateway Design

## Real-Time Market Data Distribution System

### Version

Week 1 Architecture Design

### Purpose

The WebSocket Gateway is responsible for maintaining persistent client connections and delivering real-time market data updates.

It acts as the external access layer between the Market Data Platform and connected clients.

The gateway must support:

* Persistent connections
* Subscribe requests
* Unsubscribe requests
* Connection lifecycle management
* 5,000 concurrent connections
* Low-latency message delivery

The gateway should remain independent from:

* Redis
* Kafka
* Persistence
* Market data generation

---

# 1. System Position

## High-Level Architecture

```text
Feed Generator
      ↓

Publisher Service
      ↓

Topic Manager
      ↓

WebSocket Gateway
      ↓

Connected Clients
```

The gateway does not generate market data.

The gateway only:

* Accepts client connections
* Manages subscriptions
* Delivers updates

---

# 2. Core Responsibilities

## Connection Management

Maintain long-lived WebSocket connections.

Example:

```text
Client
    ↓
WebSocket Upgrade
    ↓
Persistent Connection
```

Connections remain active until:

* Client disconnects
* Timeout occurs
* Server shutdown
* Network failure

---

## Subscription Management

Allow clients to express interest in topics.

Example:

```text
Subscribe
    ↓
AAPL
```

The gateway forwards subscription requests to the Topic Manager.

---

## Unsubscription Management

Allow clients to stop receiving updates.

Example:

```text
Unsubscribe
    ↓
AAPL
```

The gateway updates subscription state accordingly.

---

## Message Delivery

Receive updates from the Topic Manager and deliver them to interested clients.

Example:

```text
AAPL Update
      ↓
Gateway
      ↓
Subscribed Clients
```

---

# 3. Connection Lifecycle

## Connection Establishment

```text
Client
      ↓

TCP Connection
      ↓

WebSocket Upgrade
      ↓

Connection Registered
      ↓

Ready
```

At this stage:

```text
Client ID Assigned
Connection Created
Resources Allocated
```

---

## Active Phase

Client may:

```text
Subscribe
Unsubscribe
Receive Updates
Send Heartbeats
```

The connection remains healthy.

---

## Disconnect Phase

Disconnect may occur because of:

```text
Client Close

Network Failure

Idle Timeout

Server Shutdown
```

Resources must be cleaned up.

---

## Cleanup Phase

Required actions:

```text
Remove Connection

Remove Subscriptions

Release Resources

Update Metrics
```

Failure to perform cleanup leads to memory leaks.

---

# 4. Client Management

## Client Registry

The gateway should maintain a registry of active clients.

Conceptually:

```text
Client ID
      →
Connection
```

Example:

```text
Client 1
Client 2
Client 3
```

The registry provides:

```text
Lookup

Registration

Removal
```

---

## Client State

Each client should maintain:

```text
Connection Reference

Subscription List

Connection Status

Outbound Queue
```

The gateway tracks state but not business logic.

---

## Why Separate Client Objects?

Benefits:

```text
Isolation

Easier Cleanup

Better Monitoring

Independent Delivery
```

Each client becomes a manageable unit.

---

# 5. Recommended Concurrency Model

## Design Goal

Support:

```text
5000 Concurrent Connections
```

without excessive contention.

---

## Per-Connection Ownership Model

Each connection owns its state.

Conceptually:

```text
Client
      ↓

Read Loop

Write Loop
```

This minimizes synchronization complexity.

---

## Recommended Goroutines

### Read Goroutine

Responsible for:

```text
Incoming Messages

Subscribe Requests

Unsubscribe Requests

Heartbeats
```

Only reads from the socket.

---

### Write Goroutine

Responsible for:

```text
Market Updates

Heartbeat Responses

System Messages
```

Only writes to the socket.

---

## Why Separate Read and Write?

Benefits:

```text
No Concurrent Socket Writes

Clear Ownership

Reduced Race Conditions
```

This is the standard production pattern.

---

# 6. Message Flow

## Subscribe Flow

```text
Client
      ↓

Subscribe(AAPL)
      ↓

Gateway
      ↓

Topic Manager
      ↓

Subscription Added
```

---

## Unsubscribe Flow

```text
Client
      ↓

Unsubscribe(AAPL)
      ↓

Gateway
      ↓

Topic Manager
      ↓

Subscription Removed
```

---

## Market Data Flow

```text
Feed Generator
      ↓

Publisher
      ↓

Topic Manager
      ↓

Gateway
      ↓

Client Queue
      ↓

WebSocket Write
```

The gateway should not participate in topic matching.

Topic Manager handles routing.

---

# 7. Outbound Delivery Model

## Problem

Clients consume data at different speeds.

Example:

```text
Client A
Fast

Client B
Slow
```

Without isolation:

```text
Slow Client
      ↓
Blocks Gateway
```

---

## Recommended Design

Each client owns:

```text
Outbound Queue
```

Architecture:

```text
Market Update
      ↓

Client Queue
      ↓

Write Goroutine
      ↓

WebSocket
```

Benefits:

```text
Isolation

Backpressure Control

Failure Containment
```

---

# 8. Queue Strategy

## Purpose

Absorb bursts.

Example:

```text
1000 Updates
```

arrive in a short period.

Queue smooths delivery.

---

## Bounded Queues

Never allow unlimited growth.

Bad:

```text
Infinite Queue
```

Result:

```text
Memory Explosion
```

---

## Recommended

```text
Fixed Capacity Queue
```

When full:

```text
Drop Messages

Disconnect Client

Apply Policy
```

depending on business requirements.

---

# 9. Failure Handling

## Client Disconnect

Example:

```text
Browser Closed
```

Detection:

```text
Write Failure

Read Failure

Heartbeat Timeout
```

Response:

```text
Cleanup
```

---

## Slow Consumer

Example:

```text
Client Cannot Process Updates
```

Symptoms:

```text
Growing Queue
```

Response:

```text
Warning

Drop Messages

Disconnect
```

depending on policy.

---

## Network Failure

Example:

```text
Wi-Fi Lost
```

Connection becomes unreachable.

Response:

```text
Timeout Detection
Cleanup
```

---

## Gateway Failure

Future deployments should support:

```text
Multiple Gateway Instances
```

allowing clients to reconnect elsewhere.

---

# 10. Heartbeat Strategy

## Purpose

Detect dead connections.

Without heartbeats:

```text
Connection Appears Alive
```

while the client is gone.

---

## Recommended Flow

```text
Server Ping
      ↓

Client Pong
      ↓

Connection Healthy
```

Missing responses indicate failure.

---

# 11. Locking Strategy

## Avoid Global Locks

Bad:

```text
5000 Clients
      ↓

Single Lock
```

Results:

```text
High Contention
```

---

## Recommended

### Client Registry Lock

Protects:

```text
Client Add

Client Remove

Lookup
```

---

### Client-Owned State

Connection state should be owned by the client.

Benefits:

```text
Minimal Shared State

Lower Contention

Higher Throughput
```

---

# 12. Performance Considerations

## Connection Count

Target:

```text
5000 Connections
```

---

## Goroutine Count

Recommended:

```text
2 Goroutines Per Connection
```

Results:

```text
10000 Goroutines
```

This is acceptable in Go.

---

## Memory Usage

Major contributors:

```text
Connection Objects

Outbound Queues

Subscription State
```

Careful queue sizing is critical.

---

## Hot Path

Most frequent operation:

```text
Market Update
      ↓
Client Delivery
```

Optimize for:

```text
Low Allocation

Minimal Locking

Fast Fan-Out
```

---

# 13. Observability

## Connection Metrics

```text
active_connections

connections_opened

connections_closed
```

---

## Subscription Metrics

```text
active_subscriptions

subscriptions_added

subscriptions_removed
```

---

## Delivery Metrics

```text
messages_sent

messages_dropped

delivery_latency
```

---

## Queue Metrics

```text
queue_depth

slow_consumers
```

---

## Error Metrics

```text
socket_errors

heartbeat_failures

disconnects
```

---

# 14. Future Redis Integration

Current:

```text
Topic Manager
      ↓
Gateway
      ↓
Clients
```

Future:

```text
Redis Pub/Sub
      ↓
Gateway Cluster
      ↓
Clients
```

Each gateway instance subscribes to Redis topics.

No gateway redesign required.

---

# 15. Complete Architecture

```text
                  Feed Generator
                         ↓

                  Publisher Service
                         ↓

                   Topic Manager
                         ↓

                WebSocket Gateway
                         ↓

             +-----------+-----------+
             |           |           |
             ↓           ↓           ↓

          Client A    Client B    Client C

             ↓           ↓           ↓

      Read Loop     Read Loop    Read Loop

      Write Loop    Write Loop   Write Loop

             ↓           ↓           ↓

       Outbound     Outbound     Outbound
         Queue        Queue        Queue
```

---

# Recommended Production Design

For a market data platform targeting 5,000 concurrent WebSocket connections:

### Client Management

```text
Client Registry
Per-Client State
```

### Concurrency

```text
Read Goroutine
Write Goroutine
Per Connection
```

### Delivery

```text
Per-Client Queues
Asynchronous Fan-Out
```

### Failure Handling

```text
Heartbeat Detection
Slow Consumer Protection
Automatic Cleanup
```

### Scalability

```text
Gateway Clustering
Redis Integration
Horizontal Scaling
```

### Future Ready

```text
Redis
Kafka
NATS
Replay Service
```

can be added without modifying the WebSocket Gateway's core architecture.

The gateway should remain a lightweight connection-management and delivery layer while all routing and business logic stay in the Topic Manager and Publisher Service.
