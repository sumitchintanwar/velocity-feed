# Snapshot Service Design

**Visual Diagram:** [Snapshot Flow Diagram](../diagrams/sequence/SNAPSHOT_FLOW_DIAGRAM.md)
## Real-Time Market Data Platform

### Author

Principal Distributed Systems Engineer

### Goal

Design a Snapshot Service that maintains the latest market state for all symbols and provides instant catch-up for newly connected clients.

Current architecture:

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

 Gateway Pool                    Event Log
                                        ↓
                                   Replay API
```

New component:

```text
                 Snapshot Service
```

Requirements:

```text
Latest Price Per Symbol

Instant Client Catch-Up

Replay Integration

Low-Latency Reads

Simple Implementation
```

---

# 1. Why A Snapshot Service?

Without snapshots:

```text
New Client Connects
```

↓

```text
Wait For Next Update
```

↓

```text
State Becomes Available
```

Problem:

A client may wait several seconds before receiving useful data.

---

## Example

Current market:

```text
AAPL = 250.50

MSFT = 510.20

NVDA = 1820.10
```

A new client connects.

Without snapshots:

```text
Client Knows Nothing
```

until fresh updates arrive.

---

## Goal

Allow:

```text
Connect
      ↓

Get Latest State
      ↓

Subscribe To Live Updates
```

Result:

```text
Instant Market View
```

---

# 2. Snapshot Service Responsibilities

The Snapshot Service provides:

```text
Latest State Per Symbol

Fast Read Access

Client Catch-Up

Replay Starting Point
```

The Snapshot Service does NOT provide:

```text
Historical Storage

Long-Term Replay

WebSocket Delivery
```

---

# 3. High-Level Architecture

```text
                 Feed Generator
                        ↓

                    Publisher
                        ↓

                 Redis Pub/Sub
                        ↓

             Snapshot Service
                        ↓

             In-Memory Snapshot

                        ↓

                 Gateway Cluster
                        ↓

                  Clients
```

---

# 4. Core Design Principle

Snapshots represent:

```text
Current State
```

Replay represents:

```text
Historical State
```

Think of snapshots as:

```text
Latest Known Truth
```

while replay provides:

```text
Journey To Reach That Truth
```

---

# 5. Data Model

Store one record per symbol.

Example:

```text
AAPL
  Price: 250.50
  Volume: 100
  Timestamp: T1

MSFT
  Price: 510.20
  Volume: 200
  Timestamp: T2

NVDA
  Price: 1820.10
  Volume: 50
  Timestamp: T3
```

Only the newest update is retained.

---

# 6. Recommended Data Structure

Use:

```text
Hash Map

Symbol
   →
Latest Snapshot
```

Example:

```text
AAPL
   →
Snapshot

MSFT
   →
Snapshot

NVDA
   →
Snapshot
```

---

## Why?

Lookup complexity:

```text
O(1)
```

Reads remain extremely fast.

---

# 7. Snapshot Structure

Each snapshot contains:

```text
Symbol

Price

Volume

Timestamp

Exchange

Sequence Number
```

Example:

```text
AAPL

250.50

100

10:00:00.123

NASDAQ

123456
```

---

# 8. Update Flow

Market update arrives:

```text
AAPL
251.00
```

---

Flow:

```text
Publisher
      ↓

Redis
      ↓

Snapshot Service
```

---

Snapshot update:

```text
Old

AAPL
250.50
```

↓

```text
New

AAPL
251.00
```

---

Only latest value remains.

---

# 9. Read Flow

Client connects.

Requests:

```text
Current Market State
```

---

Gateway:

```text
Fetch Snapshot
```

↓

```text
Return Latest Data
```

↓

```text
Subscribe To Live Updates
```

---

Result:

```text
Immediate Synchronization
```

---

# 10. Integration With WebSocket Gateway

Connection sequence:

```text
Client Connects
```

↓

```text
Subscription Request
```

↓

```text
Gateway Fetches Snapshot
```

↓

```text
Snapshot Sent
```

↓

```text
Live Stream Begins
```

---

Client receives:

```text
Current State

Then

Future Updates
```

---

# 11. Integration With Replay

Snapshots alone are insufficient.

Example:

```text
Last Snapshot

10:05:00
```

Client disconnects at:

```text
10:05:01
```

Reconnects at:

```text
10:10:00
```

---

Problem:

Client missed:

```text
5 Minutes Of Updates
```

---

Solution:

```text
Snapshot
+
Replay
```

---

Recovery flow:

```text
Client Reconnects
```

↓

```text
Last Sequence Number
```

↓

```text
Replay Missing Events
```

↓

```text
Apply Snapshot
```

↓

```text
Resume Live Stream
```

---

# 12. Snapshot + Replay Architecture

```text
                Event Log
                    ↓

               Replay API
                    ↓

                 Client

                    ↑

             Snapshot Service
                    ↑

               Live Updates
```

---

Benefits:

```text
Fast Recovery

Accurate Recovery

Minimal Replay Window
```

---

# 13. Sequence Number Support

Each update should contain:

```text
Monotonic Sequence Number
```

Example:

```text
1001

1002

1003

1004
```

---

Snapshot stores:

```text
Latest Sequence
```

Example:

```text
AAPL

Price: 251.00

Sequence: 1004
```

---

This allows:

```text
Replay From 1005
```

instead of replaying everything.

---

# 14. Memory Tradeoffs

Memory consumption depends on:

```text
Number Of Symbols
```

not:

```text
Number Of Updates
```

---

Example:

```text
100 Symbols
```

↓

```text
100 Snapshots
```

---

Example:

```text
10,000 Symbols
```

↓

```text
10,000 Snapshots
```

---

Still manageable.

---

# 15. Example Memory Calculation

Assume:

```text
200 Bytes Per Snapshot
```

---

100 Symbols:

```text
20 KB
```

---

10,000 Symbols:

```text
2 MB
```

---

100,000 Symbols:

```text
20 MB
```

---

Very reasonable for modern systems.

---

# 16. Persistence Options

## Option 1

Pure In-Memory

```text
Fastest

Simplest
```

But snapshots disappear on restart.

---

## Option 2

Periodic Persistence

```text
Memory
      ↓

Periodic Save
      ↓

Disk
```

---

Benefits:

```text
Fast Reads

Quick Recovery
```

---

Recommended for production.

---

# 17. Failure Scenario

Snapshot Service crashes.

---

Expected behavior:

```text
Live Data Continues
```

through Redis.

---

Recovery:

```text
Rebuild Snapshot
```

from:

```text
Event Log
```

or

```text
Persisted Snapshot
```

---

# 18. Scalability Characteristics

Read complexity:

```text
O(1)
```

---

Update complexity:

```text
O(1)
```

---

Memory complexity:

```text
O(Number Of Symbols)
```

---

Very predictable scaling behavior.

---

# 19. Future Evolution

Phase 1:

```text
Single In-Memory Snapshot Service
```

---

Phase 2:

```text
Persisted Snapshots
```

---

Phase 3:

```text
Distributed Snapshot Store
```

using:

```text
Redis

KeyDB

Distributed Cache
```

---

Phase 4:

```text
Multi-Region Snapshot Replication
```

for global deployments.

---

# Recommended Architecture

```text
                 Feed Generator
                        ↓

                    Publisher
                        ↓

                 Redis Pub/Sub
                        ↓

                Snapshot Service
                        ↓

         HashMap<Symbol, Snapshot>

                        ↓

                Gateway Cluster
                        ↓

                   Clients

                        ↑

                 Replay API
                        ↑

                  Event Log
```

---

# Final Recommendation

Implement the Snapshot Service as:

```text
In-Memory Symbol Map
```

containing:

```text
Latest Price

Latest Volume

Timestamp

Sequence Number
```

Integration strategy:

```text
Connect
      ↓

Fetch Snapshot
      ↓

Replay Missing Events
      ↓

Subscribe To Live Updates
```

This provides:

```text
Instant Catch-Up

Fast Reads

Simple Design

Low Memory Usage

Replay Compatibility
```

while maintaining a clear separation between:

```text
Current State
```

and

```text
Historical State
```

which is a foundational pattern in production-grade market data platforms.