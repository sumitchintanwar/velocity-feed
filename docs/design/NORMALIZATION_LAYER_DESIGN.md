# Market Data Normalization Layer Design
## Production-Grade Market Data Platform

### Author

Principal Engineer – Market Data Infrastructure

### Goal

Design a production-grade Market Data Normalization Layer that transforms heterogeneous exchange-specific market data into a canonical internal event model for downstream services.

Current Platform:

```text
                 Exchange Adapter Framework
                           │
                           ▼

              Market Data Normalization Layer
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
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼

     Snapshot Service   Replay API   PostgreSQL Event Log
```

Objectives:

```text
Exchange Independence

Canonical Event Model

Data Validation

Schema Evolution

High Throughput

Low Latency

Future Extensibility
```

---

# 1. Design Philosophy

Every exchange publishes data differently.

Examples:

```text
Different Field Names

Different Timestamp Formats

Different Price Precision

Different Symbol Formats

Different Message Structures
```

The rest of the platform should never understand these differences.

Instead:

```text
Exchange Message

↓

Normalization Layer

↓

Canonical Internal Event
```

Every downstream component consumes the same internal format.

---

# 2. Responsibilities

The Normalization Layer is responsible for:

```text
Protocol-Agnostic Transformation

Canonical Data Modeling

Field Mapping

Validation

Timestamp Standardization

Schema Versioning

Error Reporting
```

It is NOT responsible for:

```text
Business Logic

Publishing

Persistence

Routing

Client Delivery
```

---

# 3. High-Level Architecture

```text
             Exchange Adapter Framework
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼

   Binance        Coinbase         FIX Feed

        │              │              │
        └──────────────┼──────────────┘
                       │

              Normalization Layer

                       │

             Canonical Event Model

                       │

                 Publisher Service

                       │

                 Redis Pub/Sub
```

---

# 4. Internal Pipeline

The normalization pipeline consists of multiple stages.

```text
Receive Exchange Message

↓

Parse Exchange Payload

↓

Field Mapping

↓

Timestamp Normalization

↓

Symbol Normalization

↓

Validation

↓

Canonical Event Creation

↓

Publish
```

Each stage has a single responsibility.

---

# 5. Canonical Event Model

Every market event should conform to a unified schema.

Typical fields include:

```text
Schema Version

Exchange

Symbol

Event Type

Price

Quantity

Bid

Ask

Timestamp

Sequence Number

Metadata
```

This model becomes the internal contract for every downstream service.

---

# 6. Transformation Pipeline

### Stage 1

Receive raw exchange message.

Input:

```text
Exchange-Specific Payload
```

---

### Stage 2

Parse exchange format.

Examples:

```text
JSON

FIX

Binary TCP

CSV (Simulation)

Custom Binary
```

Output:

```text
Exchange Object
```

---

### Stage 3

Field mapping.

Example:

```text
Exchange A

lastPrice

↓

price
```

```text
Exchange B

tradePrice

↓

price
```

Every field maps into the canonical schema.

---

### Stage 4

Timestamp normalization.

Incoming timestamps may be:

```text
Milliseconds

Microseconds

Nanoseconds

ISO-8601

Exchange Epoch
```

Normalize into one internal format.

---

### Stage 5

Symbol normalization.

Example:

```text
BTCUSD

↓

BTC-USD
```

or

```text
BTC/USD

↓

BTC-USD
```

Internal consumers should never handle exchange-specific symbol formats.

---

### Stage 6

Validation.

Validate:

```text
Required Fields

Numeric Values

Timestamp Validity

Sequence Integrity

Supported Symbols
```

---

### Stage 7

Canonical event creation.

Output:

```text
Normalized Market Event
```

Ready for publishing.

---

# 7. Validation Strategy

Validation protects downstream services from malformed data.

Validation categories:

```text
Structural Validation

Business Validation

Protocol Validation

Schema Validation
```

---

## Structural Validation

Verify:

```text
Required Fields Present

Correct Data Types

Supported Event Types
```

---

## Business Validation

Examples:

```text
Positive Price

Positive Quantity

Valid Symbol

Known Exchange
```

---

## Timestamp Validation

Verify:

```text
Timestamp Exists

Within Acceptable Drift

Correct Precision
```

Reject obviously invalid timestamps.

---

## Sequence Validation

Where available:

```text
Sequence Number Present

Monotonic

No Corruption
```

Missing sequence numbers may be acceptable for some exchanges.

---

# 8. Error Handling

Errors should be isolated.

A malformed message should never stop:

```text
Normalization Pipeline

Publisher

Other Exchanges
```

Bad messages are rejected individually.

---

## Recoverable Errors

Examples:

```text
Malformed Message

Unknown Symbol

Invalid Price

Unsupported Event Type
```

Action:

```text
Drop Message

Increment Metrics

Structured Log
```

Continue processing.

---

## Fatal Errors

Examples:

```text
Corrupted Parser

Configuration Failure

Unsupported Schema Version
```

Action:

```text
Stop Adapter

Report Health Failure

Trigger Alert
```

---

# 9. Error Classification

Recommended categories:

```text
Parse Errors

Validation Errors

Schema Errors

Transformation Errors

Configuration Errors

Internal Errors
```

Each category should have dedicated metrics.

---

# 10. Performance Considerations

Target:

```text
100,000+

Events Per Second
```

Normalization must introduce minimal latency.

Goals:

```text
Zero Blocking

Minimal Allocations

Linear Processing

Predictable Latency
```

---

# 11. Pipeline Concurrency

Each adapter may normalize independently.

Architecture:

```text
Adapter

↓

Normalization Worker

↓

Publisher
```

Adapters do not share mutable transformation state.

Benefits:

```text
Fault Isolation

Parallel Processing

Simpler Concurrency
```

---

# 12. Memory Strategy

Avoid unnecessary object creation.

Prefer:

```text
Streaming Processing

Reusable Buffers

Immutable Canonical Events
```

This reduces:

```text
Garbage Collection

Latency Spikes
```

---

# 13. Schema Versioning

The canonical schema evolves over time.

Every normalized event should include:

```text
Schema Version
```

Example:

```text
v1

↓

v2

↓

v3
```

Consumers can determine compatibility using the version field.

---

# 14. Backward Compatibility

New schema versions should:

```text
Add Fields

Avoid Breaking Existing Consumers
```

Prefer:

```text
Optional Additions
```

Avoid:

```text
Field Removal

Field Renaming

Type Changes
```

---

# 15. Schema Evolution Strategy

Recommended approach:

```text
Current Version

↓

Support Previous Version

↓

Deprecate

↓

Remove After Migration
```

This enables rolling upgrades across distributed systems.

---

# 16. Extension Strategy

Supporting a new exchange should require only:

```text
New Adapter

Field Mapping

Transformation Rules
```

No changes should be required to:

```text
Publisher

Redis

Replay

Snapshot

Gateway
```

---

# 17. Observability

Metrics:

```text
Messages Normalized

Validation Failures

Parse Errors

Dropped Messages

Transformation Latency
```

Logs:

```text
Malformed Messages

Schema Errors

Validation Failures

Transformation Failures
```

Tracing:

```text
Adapter

↓

Normalization

↓

Publisher
```

---

# 18. Testing Strategy

Recommended testing levels:

```text
Unit Tests

Parser Tests

Transformation Tests

Validation Tests

Schema Compatibility Tests

Performance Tests

Integration Tests
```

Replay recorded exchange data during testing.

---

# 19. Common Design Mistakes

## Exposing Exchange Models

Bad:

```text
Publisher

↓

BinanceTrade
```

Good:

```text
Publisher

↓

Canonical Market Event
```

---

## Mixing Business Logic

Normalization should not decide:

```text
Routing

Persistence

Subscriptions
```

Only transform and validate.

---

## Exchange-Specific Logic Everywhere

Keep exchange-specific rules inside adapters and mapping definitions.

Do not spread them across downstream services.

---

## No Schema Version

Without versioning:

```text
Rolling Deployments

↓

Compatibility Issues
```

Always version the canonical schema.

---

## Weak Validation

Accepting malformed events risks:

```text
Replay Corruption

Snapshot Corruption

Ordering Issues

Client Errors
```

Validate before publishing.

---

# 20. Production Architecture

```text
                 Exchange Adapter
                        │
                        ▼

                Protocol Parser

                        │
                        ▼

                Field Mapping

                        │
                        ▼

           Timestamp Normalization

                        │
                        ▼

             Symbol Normalization

                        │
                        ▼

                 Validation

                        │
                        ▼

          Canonical Market Event

                        │
                        ▼

              Publisher Service

                        │
                        ▼

                 Redis Pub/Sub
```

---

# 21. How Production Trading Systems Organize Normalization

Institutional trading platforms typically isolate normalization into its own layer between exchange adapters and the internal messaging infrastructure.

Characteristics include:

```text
Canonical Event Model

Strict Validation

Schema Versioning

Per-Exchange Mapping Rules

Protocol Independence

Independent Testing

High Throughput

Low Allocation Processing
```

Every downstream system—including replay, persistence, pricing engines, analytics, gateways, and monitoring—consumes the same normalized event model, allowing new exchanges to be added without modifying core platform components.

---

# 22. Recommended Package Structure

```text
internal/

    normalization/
        pipeline/
        mapper/
        validator/
        schema/
        versioning/
        errors/

    models/
        canonical/
        exchange/

    adapters/

    publisher/

    config/
```

Responsibilities:

```text
pipeline/
    Transformation orchestration

mapper/
    Exchange-to-canonical field mapping

validator/
    Structural and business validation

schema/
    Canonical event definitions

versioning/
    Schema evolution and compatibility

errors/
    Error classification and reporting
```

---

# Final Recommendation

Build the Market Data Normalization Layer as a dedicated processing stage between the Exchange Adapter Framework and the Publisher Service.

Architecture:

```text
Exchange Adapter

↓

Parser

↓

Field Mapper

↓

Timestamp Normalizer

↓

Symbol Normalizer

↓

Validator

↓

Canonical Event Model

↓

Publisher
```

Core principles:

```text
Protocol Independent

Canonical Internal Schema

Strict Validation

Versioned Events

Fault Isolation

Minimal Latency
```

Operational capabilities:

```text
Per-Stage Metrics

Structured Logging

Distributed Tracing

Schema Evolution

Independent Testing

High Throughput
```

This architecture enables:

```text
Exchange Independence

Consistent Internal Data

Safe Schema Evolution

Easy Onboarding Of New Exchanges

Operational Simplicity

Production-Grade Reliability
```

and reflects the normalization architecture commonly used in institutional market data platforms at global investment banks, exchanges, and high-frequency trading firms.