# Symbol Metadata Service Design
## Production-Grade Market Data Platform

### Author

Principal Engineer – Market Data Infrastructure

### Goal

Design a production-grade Symbol Metadata Service that provides centralized reference data for all financial instruments across multiple exchanges.

Current Platform:

```text
             Exchange Adapter Framework
                        │
                        ▼

          Market Data Normalization Layer
                        │
                        ▼

              Symbol Metadata Service
                        │
                        ▼

                 Publisher Service
                        │
                        ▼

                  Redis Pub/Sub
                        │
                        ▼

             Distributed Gateways

                        │

     Replay • Snapshot • PostgreSQL Event Log
```

Objectives:

```text
Centralized Reference Data

Fast Symbol Lookup

Exchange Independence

Consistent Instrument Metadata

Low Latency

Future Extensibility
```

---

# 1. Design Philosophy

Market data messages should contain only dynamic information:

```text
Price

Quantity

Timestamp

Sequence Number
```

Static instrument information belongs in a dedicated service.

Examples:

```text
Asset Class

Currency

Exchange

Trading Status

Trading Hours

Tick Size

Lot Size
```

Separating static metadata from live market data improves consistency and avoids duplication.

---

# 2. Responsibilities

The Symbol Metadata Service is responsible for:

```text
Symbol Lookup

Exchange Lookup

Reference Data

Trading Calendar

Trading Status

Instrument Metadata

Metadata Caching
```

It is NOT responsible for:

```text
Price Distribution

Order Routing

Replay

Snapshot

Publishing
```

---

# 3. High-Level Architecture

```text
          Exchange Adapters
                  │
                  ▼

      Symbol Metadata Service
                  │
      ┌───────────┼───────────┐
      ▼           ▼           ▼

 Publisher    Replay API   Snapshot

                  │

          WebSocket Gateway
```

Every platform component retrieves metadata from a single source.

---

# 4. Metadata Model

Each instrument should maintain metadata such as:

```text
Canonical Symbol

Exchange Symbol

Exchange

Asset Class

Base Currency

Quote Currency

Trading Status

Trading Hours

Tick Size

Lot Size

Instrument Name

ISIN (Future)

CUSIP (Future)

MIC Code (Future)
```

This becomes the authoritative reference record.

---

# 5. Architecture

```text
Client

↓

Metadata API

↓

In-Memory Cache

↓

Persistent Storage

↓

Administrative Update Process
```

The cache serves nearly all read requests.

Persistent storage serves as the source of truth.

---

# 6. Storage Strategy

Metadata changes infrequently.

Recommended architecture:

```text
Persistent Database

↓

Startup Load

↓

In-Memory Cache

↓

Fast Lookups
```

Database options include:

```text
PostgreSQL

SQLite (Development)

Managed SQL Service
```

PostgreSQL is recommended for production because it integrates naturally with the existing platform.

---

# 7. Why In-Memory Cache?

Market data systems perform millions of symbol lookups.

Database queries for every lookup would introduce unnecessary latency.

Instead:

```text
Startup

↓

Load Metadata

↓

Memory

↓

Constant-Time Lookup
```

Target lookup latency:

```text
Microseconds
```

---

# 8. Cache Design

Primary cache:

```text
Canonical Symbol

↓

Metadata Record
```

Secondary indexes:

```text
Exchange

↓

Exchange Symbols
```

```text
Asset Class

↓

Instrument List
```

```text
Currency

↓

Instrument List
```

These indexes support efficient filtering.

---

# 9. Data Structures

Recommended logical structures:

```text
Symbol Index

Exchange Index

Asset Class Index

Currency Index

Trading Status Index
```

All should be optimized for read-heavy workloads.

---

# 10. Lookup Flow

```text
Lookup Request

↓

In-Memory Cache

↓

Metadata Record

↓

Return Response
```

Database access occurs only during:

```text
Startup

Administrative Updates

Cache Refresh
```

---

# 11. Update Strategy

Metadata rarely changes.

Recommended update workflow:

```text
Administrative Change

↓

Database Update

↓

Cache Refresh

↓

New Metadata Available
```

Updates should not interrupt ongoing market data processing.

---

# 12. Cache Refresh

Possible strategies:

```text
Periodic Refresh

Event-Driven Refresh

Manual Reload
```

Recommended:

```text
Event-Driven

+

Scheduled Verification
```

This minimizes stale metadata while reducing unnecessary reloads.

---

# 13. Trading Status

Each instrument should maintain its current status.

Typical values:

```text
Trading

Halted

Auction

Pre-Market

Post-Market

Suspended

Closed
```

Gateways and clients can use this information for display purposes.

---

# 14. Trading Hours

Trading sessions vary by exchange.

Metadata should include:

```text
Timezone

Open Time

Close Time

Holiday Calendar Reference

Session Type
```

Future enhancements:

```text
Multiple Daily Sessions

Half Days

Special Trading Events
```

---

# 15. Asset Classes

Supported asset classes may include:

```text
Equities

ETF

Forex

Crypto

Futures

Options

Bonds

Indices
```

Asset class information enables filtering and analytics.

---

# 16. Currency Information

Metadata should distinguish between:

```text
Base Currency

Quote Currency

Settlement Currency (Future)
```

Example:

```text
BTC-USD

Base

BTC

Quote

USD
```

---

# 17. APIs

The service should expose logical APIs for:

### Symbol Lookup

Input:

```text
Canonical Symbol
```

Output:

```text
Complete Metadata
```

---

### Exchange Lookup

Input:

```text
Exchange

Exchange Symbol
```

Output:

```text
Canonical Symbol

Metadata
```

---

### Asset Class Lookup

Input:

```text
Asset Class
```

Output:

```text
Matching Instruments
```

---

### Currency Lookup

Input:

```text
Currency
```

Output:

```text
Associated Instruments
```

---

### Trading Status Lookup

Input:

```text
Symbol
```

Output:

```text
Current Trading Status
```

---

# 18. Validation

Validate metadata before loading.

Checks include:

```text
Unique Canonical Symbol

Unique Exchange Mapping

Valid Asset Class

Valid Currency Codes

Valid Trading Hours

Required Fields Present
```

Reject invalid records during import.

---

# 19. Error Handling

Recoverable errors:

```text
Unknown Symbol

Unknown Exchange

Missing Metadata
```

Action:

```text
Return Not Found

Log Event

Increment Metrics
```

---

Fatal errors:

```text
Corrupted Metadata

Duplicate Primary Keys

Cache Initialization Failure
```

Action:

```text
Fail Startup

Raise Alert

Prevent Service Readiness
```

---

# 20. Performance Considerations

Target characteristics:

```text
Millions Of Lookups Per Minute

Constant-Time Reads

Minimal Memory Allocation

Read-Optimized Access
```

The service should prioritize read performance over write throughput.

---

# 21. Concurrency Model

Reads vastly outnumber writes.

Recommended model:

```text
Read-Mostly Cache

↓

Occasional Atomic Refresh
```

Avoid locking on every lookup.

Metadata updates should be infrequent and coordinated.

---

# 22. Observability

Metrics:

```text
Lookup Count

Lookup Latency

Cache Hit Rate

Cache Refresh Count

Unknown Symbol Count

Metadata Version
```

Logs:

```text
Metadata Import

Refresh Events

Validation Failures

Administrative Changes
```

Tracing:

```text
API Request

↓

Cache Lookup

↓

Response
```

---

# 23. Extension Strategy

Future metadata fields may include:

```text
Tick Size

Lot Size

Minimum Order Quantity

Exchange Fees

Market Segment

Sector

Industry

Settlement Rules

Corporate Actions
```

New fields should be additive to preserve backward compatibility.

---

# 24. Security

Restrict metadata modifications.

Read access:

```text
Platform Services

Replay

Snapshot

Gateways
```

Write access:

```text
Administrative Processes Only
```

Maintain audit logs for all metadata changes.

---

# 25. Common Design Mistakes

## Embedding Metadata In Market Events

Bad:

```text
Every Price Update

↓

Includes Trading Hours

Currency

Asset Class
```

This wastes bandwidth and duplicates static data.

---

## Database Lookups Per Event

Bad:

```text
Market Update

↓

Database Query

↓

Publish
```

Use an in-memory cache instead.

---

## Multiple Sources Of Truth

Avoid storing metadata independently in:

```text
Gateway

Replay

Snapshot

Publisher
```

Maintain one authoritative service.

---

## Weak Validation

Loading malformed metadata can lead to:

```text
Incorrect Routing

Unknown Symbols

Replay Errors

Client Display Issues
```

Validate before publishing updates.

---

## Tight Coupling

The Metadata Service should remain independent of exchange adapters and downstream consumers.

It should provide a stable API regardless of data source.

---

# 26. Production Architecture

```text
                 Metadata Database

                        │

                 Startup Load

                        │

              In-Memory Cache

          ┌─────────────┼─────────────┐
          ▼             ▼             ▼

   Publisher      Replay API     Gateway

          │             │             │

      Symbol Lookups Throughout Platform
```

---

# 27. Recommended Package Structure

```text
internal/

    metadata/
        service/
        cache/
        storage/
        validator/
        importer/
        updater/
        api/

    models/
        metadata/

    config/

    observability/
```

Responsibilities:

```text
service/
    Core lookup orchestration

cache/
    In-memory indexes

storage/
    Persistent metadata access

validator/
    Metadata validation

importer/
    Bulk metadata loading

updater/
    Cache refresh workflow

api/
    Internal query interfaces
```

---

# 28. How Production Trading Systems Manage Symbol Metadata

Institutional trading platforms maintain a centralized Security Master or Instrument Reference Service that acts as the authoritative source for all instrument metadata.

Key characteristics include:

```text
Single Source Of Truth

Read-Optimized In-Memory Cache

Persistent Database

Canonical Instrument Identifiers

Multiple Secondary Indexes

Administrative Update Pipeline

Versioned Metadata

Strict Validation

Audit Logging
```

Nearly every platform component—including market data distribution, replay, risk, pricing, compliance, and trading applications—queries this centralized metadata service rather than maintaining local copies.

---

# Final Recommendation

Build the Symbol Metadata Service as a dedicated reference-data component positioned between the Exchange Adapter Framework and downstream platform services.

Architecture:

```text
Persistent Storage

↓

Metadata Loader

↓

Validation

↓

In-Memory Cache

↓

Metadata APIs

↓

Platform Services
```

Core capabilities:

```text
Canonical Symbol Lookup

Exchange Lookup

Asset Class Lookup

Currency Lookup

Trading Status

Trading Hours
```

Operational principles:

```text
Read-Optimized

Single Source Of Truth

Strict Validation

Atomic Cache Refresh

Versioned Metadata

Comprehensive Observability
```

This architecture provides:

```text
Microsecond Symbol Lookups

Consistent Instrument Metadata

Easy Exchange Onboarding

Scalable Reference Data Management

Operational Simplicity

Production-Grade Reliability
```

and closely reflects the Security Master architecture used in institutional trading platforms, market data infrastructures, and global investment banks.