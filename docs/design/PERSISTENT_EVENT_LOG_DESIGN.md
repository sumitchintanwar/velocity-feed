# Persistent Event Log Design

## Real-Time Market Data Platform

### Author

Principal Distributed Systems Engineer

### Goal

Design a persistent event log for a distributed real-time market data platform.

Current architecture:

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
```

New requirement:

```text
Persist Every Market Update
```

to support:

```text
Replay

Historical Analysis

Recovery

Future Event Sourcing
```

Requirements:

```text
Fast Writes

Simple Implementation

Replay Support

Low Operational Complexity
```

---

# 1. Why a Persistent Event Log?

Current architecture uses:

```text
Redis Pub/Sub
```

which is:

```text
Low Latency

Fast

Ephemeral
```

---

## Problem

If a gateway disconnects:

```text
Messages Are Lost
```

If Redis restarts:

```text
Messages Are Lost
```

If a client reconnects:

```text
Historical Updates Cannot Be Recovered
```

---

## Goal

Every market update should be written to durable storage.

Architecture becomes:

```text
                 Feed Generator
                        ↓

                    Publisher
                        ↓

            +-----------------------+
            |                       |
            ▼                       ▼

       Event Log              Redis Pub/Sub
                                    ↓
                              Gateway Cluster
```

---

# 2. Event Log Responsibilities

The event log should provide:

```text
Durable Storage

Sequential Writes

Time-Based Queries

Future Replay Support
```

The event log should NOT provide:

```text
Live Fan-Out

Subscription Routing

WebSocket Delivery
```

Those remain Redis responsibilities.

---

# 3. Workload Characteristics

Market data workloads are unique.

Typical behavior:

```text
Very High Write Rate

Mostly Append Operations

Rare Updates

Rare Deletes
```

Example:

```text
AAPL Update

MSFT Update

TSLA Update

NVDA Update
```

Each update becomes a new event.

---

# 4. Candidate Storage Options

Evaluate:

```text
PostgreSQL

BadgerDB

SQLite
```

---

# 5. PostgreSQL

## Overview

Traditional relational database.

Widely used.

Production proven.

---

## Advantages

```text
Reliable

Durable

Powerful Queries

Strong Tooling

Backups

Partitioning
```

---

## Example Strength

Replay query:

```text
Give Me All AAPL Updates
Between T1 And T2
```

Very easy.

---

## Operational Benefits

```text
Monitoring

Replication

Managed Services

Backups
```

are mature.

---

## Disadvantages

Market data is primarily:

```text
Append Only
```

PostgreSQL provides many features:

```text
Joins

Constraints

Relations
```

that are not heavily used.

---

## Scalability

Excellent for:

```text
Thousands

Tens Of Thousands
```

updates/sec.

Eventually requires:

```text
Partitioning

Sharding
```

for extreme scale.

---

# 6. BadgerDB

## Overview

Embedded LSM-tree database written in Go.

Key-value store.

No external server.

---

## Advantages

```text
Very Fast Writes

Simple Deployment

Embedded

Excellent Sequential Throughput
```

---

## Write Characteristics

Ideal for:

```text
Append Heavy Workloads
```

such as market data.

---

## Disadvantages

Operational tooling is limited.

Replay queries become harder.

Example:

```text
Find All Events
For AAPL
Between Timestamps
```

requires custom indexing.

---

## Scalability

Excellent local performance.

Poorer operational experience than PostgreSQL.

---

# 7. SQLite

## Overview

Embedded relational database.

Single file.

Very simple.

---

## Advantages

```text
Extremely Simple

No Server

Easy Backup

Easy Deployment
```

---

## Disadvantages

Single-writer architecture.

Limited concurrency.

Not designed for sustained high-volume market data ingestion.

---

## Scalability

Good for:

```text
Development

Testing

Prototypes
```

Not ideal for production event logging.

---

# 8. Comparison

| Feature               | PostgreSQL | BadgerDB  | SQLite    |
| --------------------- | ---------- | --------- | --------- |
| Durability            | Excellent  | Excellent | Excellent |
| Write Performance     | Good       | Excellent | Moderate  |
| Replay Queries        | Excellent  | Moderate  | Good      |
| Operational Tooling   | Excellent  | Limited   | Minimal   |
| Deployment Complexity | Moderate   | Low       | Very Low  |
| Production Readiness  | Excellent  | Good      | Limited   |
| Horizontal Growth     | Good       | Limited   | Poor      |

---

# 9. Recommended Approach

For this project stage:

```text
PostgreSQL
```

is the best choice.

Reasons:

```text
Simple

Production Proven

Easy Replay

Easy Operations

Future Friendly
```

You will spend less time building storage infrastructure and more time building market data functionality.

---

# 10. Event Schema Design

The log should behave like:

```text
Append Only Journal
```

---

## Event Structure

Each event should contain:

```text
Event ID

Timestamp

Symbol

Price

Volume

Exchange

Event Type
```

---

## Example Event

```text
Event ID: 123456

Timestamp:
2026-06-24T10:00:00.123Z

Symbol:
AAPL

Price:
250.25

Volume:
100

Exchange:
NASDAQ
```

---

# 11. Primary Key Strategy

Use:

```text
Monotonically Increasing Event ID
```

Benefits:

```text
Fast Inserts

Natural Ordering

Replay Cursor Support
```

---

## Example

```text
Event 1001

Event 1002

Event 1003
```

Replay becomes:

```text
Replay From Event 1001
```

---

# 12. Write Path

## Current Flow

```text
Feed Generator
      ↓

Publisher
      ↓

Redis
```

---

## New Flow

```text
Feed Generator
      ↓

Publisher
      ↓

Event Log Write
      ↓

Redis Publish
```

---

## Sequence

### Step 1

Market update arrives.

---

### Step 2

Persist update.

```text
Write Event
```

---

### Step 3

Successful write.

---

### Step 4

Publish to Redis.

```text
Redis Pub/Sub
```

---

This guarantees:

```text
Persist Before Publish
```

---

# 13. Asynchronous Variant

Alternative:

```text
Publisher
      ↓

Redis Publish
      ↓

Event Writer Queue
      ↓

Database
```

---

Benefits:

```text
Lower Publish Latency
```

---

Tradeoff:

```text
Possible Data Loss
```

during crashes.

---

For early production:

```text
Persist Then Publish
```

is preferable.

---

# 14. Replay Architecture

Future replay service:

```text
Replay Service
      ↓

Event Log
      ↓

Historical Events
```

---

Example request:

```text
Replay AAPL

10:00 → 10:05
```

---

Replay service streams:

```text
Stored Events
```

back to clients.

---

# 15. Retention Strategy

Without retention:

```text
Storage Grows Forever
```

---

## Time-Based Retention

Example:

```text
Keep 30 Days
```

Delete older data.

---

## Tiered Retention

Example:

```text
7 Days Hot Storage

30 Days Warm Storage

Archive Older Data
```

---

Benefits:

```text
Controlled Storage Growth
```

---

# 16. Partitioning Strategy

As data volume grows:

Partition by:

```text
Date
```

Example:

```text
events_2026_06_24

events_2026_06_25

events_2026_06_26
```

---

Benefits:

```text
Faster Queries

Easier Cleanup

Simpler Archiving
```

---

# 17. Scalability Limitations

## PostgreSQL Bottlenecks

Eventually:

```text
Write Throughput

Storage Size

Index Growth
```

become challenges.

---

## Typical Evolution

Stage 1:

```text
PostgreSQL
```

↓

Stage 2:

```text
Partitioned PostgreSQL
```

↓

Stage 3:

```text
Event Streaming Platform
```

Example:

```text
Apache Kafka

Redpanda

ClickHouse
```

for very large datasets.

---

# 18. Failure Scenarios

## Database Restart

Expected:

```text
Writes Pause

Reconnect

Resume
```

---

## Redis Failure

Expected:

```text
Data Still Persisted
```

Replay remains possible.

---

## Gateway Failure

No impact on persistence.

Events remain stored.

---

# 19. Recommended Architecture

```text
                 Feed Generator
                        ↓

                    Publisher
                        ↓

              PostgreSQL Event Log
                        ↓

                  Redis Pub/Sub
                        ↓

                 Gateway Cluster
```

Characteristics:

```text
Durable

Simple

Replayable

Operationally Mature
```

---

# 20. Evolution Path

Phase 1:

```text
PostgreSQL
```

↓

Phase 2:

```text
Partitioned PostgreSQL
```

↓

Phase 3:

```text
Replay Service
```

↓

Phase 4:

```text
Kafka / Redpanda
```

if event volume eventually requires distributed log storage.

---

# Final Recommendation

For a market data platform at this stage, use:

```text
PostgreSQL
```

as the persistent event log.

Reasons:

```text
Fast Enough

Production Proven

Excellent Replay Support

Simple Operations

Future Growth Path
```

Schema approach:

```text
Append-Only Events Table

Sequential Event IDs

Timestamp Indexing

Symbol Indexing
```

Retention strategy:

```text
Time-Based Retention

Daily Partitions

Archived Historical Data
```

This provides the best balance between:

```text
Simplicity

Reliability

Replay Capability

Operational Maturity
```

while keeping the architecture easy to understand and implement.
