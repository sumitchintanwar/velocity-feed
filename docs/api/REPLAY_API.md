# Replay API Design
## Real-Time Market Data Platform

### Author

Principal Distributed Systems Engineer

### Goal

Design a Replay API that enables retrieval and replay of historical market data.

Current architecture:

```text
                 Feed Generator
                        ↓

                    Publisher
                        ↓

              Persistent Event Log
                        ↓

                  Redis Pub/Sub
                        ↓

                 Gateway Cluster
```

Requirements:

```text
Query Historical Events

Replay From Timestamp

Replay By Symbol

Support Large Datasets

Production-Grade Performance
```

Future use cases:

```text
Client Recovery

Backtesting

Historical Analysis

Market Data Verification

Replay Services
```

---

# 1. Why A Replay API?

Current system provides:

```text
Live Market Data
```

via Redis Pub/Sub and WebSockets.

However:

```text
Redis Pub/Sub
```

is ephemeral.

Messages are lost when:

```text
Clients Disconnect

Gateways Restart

Redis Restarts
```

A replay API provides:

```text
Historical Recovery

Gap Filling

Time-Based Queries
```

using the persistent event log.

---

# 2. Replay API Responsibilities

The Replay API should provide:

```text
Historical Queries

Time-Based Replay

Symbol-Based Replay

Pagination

Efficient Retrieval
```

The Replay API should NOT provide:

```text
Live Streaming

Subscription Routing

Real-Time Fan-Out
```

Those remain responsibilities of the live market data system.

---

# 3. High-Level Architecture

```text
                Client
                   │
                   ▼

              Replay API
                   │
                   ▼

         Persistent Event Log
                   │
                   ▼

             PostgreSQL
```

---

# 4. Core Query Types

The API should support three primary access patterns.

---

## Query By Timestamp

Example:

```text
Give Me All Events
Since 10:00:00 UTC
```

Used for:

```text
Recovery

Gap Filling

Catch-Up Processing
```

---

## Query By Symbol

Example:

```text
Give Me AAPL Events
```

Used for:

```text
Analysis

Charting

Research
```

---

## Query By Symbol And Time Range

Example:

```text
AAPL

10:00 → 10:15
```

Used for:

```text
Historical Replay

Market Reconstruction
```

---

# 5. Event Model

Each stored event contains:

```text
Event ID

Timestamp

Symbol

Price

Volume

Exchange

Event Type
```

Example:

```text
Event ID: 102345

Timestamp:
2026-06-24T10:00:00.123Z

Symbol:
AAPL

Price:
250.25

Volume:
100
```

---

# 6. API Design Principles

Replay APIs should be:

```text
Predictable

Cursor-Based

Efficient

Time-Oriented
```

Avoid:

```text
Full Dataset Scans

Large Responses

Unbounded Queries
```

---

# 7. Replay By Timestamp

Purpose:

```text
Resume Data Processing
```

Example request:

```text
Replay Events

From:
2026-06-24T10:00:00Z
```

---

Expected behavior:

```text
Return Events

Ordered By Timestamp
```

---

Use cases:

```text
Client Recovery

Gateway Recovery

Operational Investigation
```

---

# 8. Replay By Symbol

Purpose:

```text
Retrieve History
For One Instrument
```

Example:

```text
Symbol:
AAPL
```

---

Returns:

```text
AAPL Events Only
```

---

Benefits:

```text
Smaller Dataset

Efficient Queries
```

---

# 9. Replay By Symbol And Time Range

Most common query.

Example:

```text
Symbol:
AAPL

Start:
10:00

End:
10:05
```

---

Returns:

```text
All AAPL Events

Within Window
```

---

Used for:

```text
Charts

Research

Backtesting

Client Recovery
```

---

# 10. Replay Modes

Two replay modes should exist.

---

## Historical Fetch

Returns data immediately.

Example:

```text
All Events
Between T1 And T2
```

---

Characteristics:

```text
Fast

Batch-Oriented
```

---

## Timed Replay

Replays events using original timestamps.

Example:

```text
Event 1
10:00:00.001

Event 2
10:00:00.010

Event 3
10:00:00.025
```

---

Replay emits:

```text
Events
With Original Timing
```

---

Used for:

```text
Simulation

Backtesting

System Testing
```

---

# 11. Pagination Strategy

Pagination is mandatory.

Without pagination:

```text
Millions Of Events

Single Response
```

becomes impossible.

---

# Offset Pagination

Example:

```text
Page 1

Page 2

Page 3
```

---

Problems:

```text
Large Offsets

Slow Queries

Poor Scalability
```

---

Not recommended.

---

# Cursor Pagination

Preferred approach.

Example:

```text
Start After Event 10000
```

---

Client receives:

```text
Next Cursor
```

for subsequent requests.

---

Benefits:

```text
Stable Performance

Efficient Queries

Simple Replay Continuation
```

---

# 12. Cursor Design

Use:

```text
Event ID
```

or

```text
Timestamp + Event ID
```

as cursor.

Example:

```text
Last Event:
105000
```

Next query:

```text
Start After:
105000
```

---

Advantages:

```text
Sequential Reads

Fast Index Access

Minimal Memory Usage
```

---

# 13. Database Indexing

Replay performance depends heavily on indexing.

Required indexes:

```text
Timestamp

Symbol

Symbol + Timestamp
```

---

Examples:

```text
Timestamp Queries

Symbol Queries

Range Queries
```

become efficient.

---

Without indexes:

```text
Table Scan
```

occurs.

Performance degrades dramatically.

---

# 14. Query Performance Concerns

Market data grows rapidly.

Example:

```text
10,000 Updates/Sec
```

Produces:

```text
864 Million Events/Day
```

at full sustained rate.

---

Potential issues:

```text
Large Tables

Slow Queries

Index Growth

Storage Costs
```

---

# 15. Partitioning Strategy

Use:

```text
Time-Based Partitions
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
Smaller Indexes

Faster Queries

Simpler Retention
```

---

# 16. Replay Performance Flow

Recommended flow:

```text
Replay Request
      ↓

Partition Selection
      ↓

Index Lookup
      ↓

Cursor Scan
      ↓

Response Page
```

---

Avoid:

```text
Full Database Scans
```

at all costs.

---

# 17. Scaling Implications

## Small Scale

```text
Single PostgreSQL Instance
```

Works well.

---

## Medium Scale

```text
Partitioned PostgreSQL
```

Provides:

```text
Good Query Performance

Manageable Storage
```

---

## Large Scale

Eventually:

```text
Billions Of Events
```

become common.

---

Potential evolution:

```text
PostgreSQL
      ↓

Partitioned PostgreSQL
      ↓

ClickHouse
      ↓

Distributed Event Storage
```

---

# 18. Replay Service Isolation

Replay traffic should never compete with live traffic.

Bad architecture:

```text
Live Queries
+
Replay Queries
```

sharing the same resources.

---

Recommended:

```text
Live System
      ↓

Replay Service
      ↓

Read Replica
```

---

Benefits:

```text
Replay Cannot Impact Live Market Data
```

---

# 19. Large Replay Requests

Potential problem:

```text
Replay Entire Day

For All Symbols
```

---

Could produce:

```text
Millions Of Events
```

---

Protection mechanisms:

```text
Maximum Time Window

Maximum Records

Rate Limiting
```

---

Example:

```text
Max 100,000 Events

Per Request
```

---

# 20. Retention Implications

Replay capability depends on retention policy.

Example:

```text
30 Days
```

Retention means:

```text
Events Older Than 30 Days

Unavailable
```

---

Alternative:

```text
Archive Storage
```

for long-term history.

---

# 21. Recommended API Architecture

```text
                 Client
                    │
                    ▼

               Replay API
                    │
                    ▼

          Query Coordinator
                    │
                    ▼

          PostgreSQL Event Log
```

Supported queries:

```text
Replay By Timestamp

Replay By Symbol

Replay By Time Range

Cursor Pagination
```

---

# 22. Operational Metrics

Track:

```text
Replay Requests/Sec

Average Query Time

P95 Query Time

Rows Returned

Database CPU

Database I/O
```

Monitor:

```text
Slow Queries

Large Result Sets

Failed Requests
```

---

# 23. Recommended Limits

Suggested limits:

```text
1000 Events/Page

Cursor Pagination

24 Hour Max Window

100,000 Event Max Response
```

These prevent abuse and maintain predictable performance.

---

# Final Recommendation

Build the Replay API around:

```text
Timestamp-Based Queries
+
Symbol-Based Queries
+
Cursor Pagination
```

using:

```text
PostgreSQL

Time-Based Partitioning

Indexed Event Storage
```

Architecture:

```text
Replay API
      ↓

Indexed Event Log
      ↓

Partitioned Storage
```

This approach provides:

```text
Fast Historical Queries

Efficient Replay

Predictable Performance

Simple Implementation

Future Scalability
```

while supporting:

```text
Client Recovery

Historical Analysis

Backtesting

Market Reconstruction
```

without impacting the live market data path.