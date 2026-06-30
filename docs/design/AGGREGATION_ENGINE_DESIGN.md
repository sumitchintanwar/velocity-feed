# Production-Grade Aggregation Engine Design
## Distributed Real-Time Market Data Platform

**Role:** Principal Engineer – Low-Latency Market Data Systems

**Version:** 1.0

---

# Table of Contents

1. Purpose
2. Design Goals
3. High-Level Architecture
4. Event Flow
5. Aggregation Windows
6. Tick Data Handling
7. OHLC Generation
8. VWAP Calculation
9. Concurrent Symbol Processing
10. State Management
11. Fault Tolerance
12. Integration with Replay & Event Log
13. Performance Considerations
14. Prometheus Metrics
15. Common Implementation Mistakes
16. Production Readiness Checklist
17. Final Recommendations

---

# 1. Purpose

The Aggregation Engine transforms a continuous stream of normalized market data into derived market data products that are consumed by trading systems, dashboards, analytics, and historical replay services.

Typical outputs include:

- Tick stream
- OHLC candles
- VWAP
- Multi-timeframe aggregates

The engine should operate continuously with low latency while supporting thousands of symbols simultaneously.

---

# 2. Design Goals

A production-grade Aggregation Engine should provide:

- Low-latency processing
- High throughput
- Deterministic aggregation
- Per-symbol ordering
- Horizontal scalability
- Efficient memory usage
- Fault tolerance
- Replay compatibility
- Extensible aggregation framework
- Operational observability

---

# 3. High-Level Architecture

```text
Exchange Adapters
        │
        ▼
Market Data Normalization
        │
        ▼
Redis Pub/Sub
        │
        ▼
Aggregation Engine
        │
        ├─────────────── Tick Stream
        │
        ├─────────────── OHLC
        │
        ├─────────────── VWAP
        │
        └─────────────── Future Aggregations
                          (EMA, SMA, ATR, etc.)
        │
        ▼
Publisher
        │
        ▼
Gateways / Replay / Storage
```

The Aggregation Engine acts as a stateless processor for incoming events while maintaining in-memory aggregation state.

---

# 4. Event Flow

```text
Normalized Tick
        │
        ▼
Symbol Dispatcher
        │
        ▼
Per-Symbol Aggregator
        │
        ├── Tick Output
        ├── Candle Update
        ├── VWAP Update
        └── Window Management
        │
        ▼
Publisher
```

Each tick updates the aggregation state for its symbol before derived events are published.

---

# 5. Aggregation Windows

Aggregation windows define the duration over which market events are summarized.

### Supported Windows

- Tick (no aggregation)
- 1 Second
- 5 Seconds
- 15 Seconds
- 30 Seconds
- 1 Minute
- 5 Minutes
- 15 Minutes
- 1 Hour
- 1 Day

### Window Alignment

Windows should align to fixed time boundaries rather than the arrival time of the first event.

Examples:

- 10:00:00 → 10:00:59
- 10:01:00 → 10:01:59

This ensures deterministic replay and consistent results across distributed systems.

### Window Lifecycle

```text
Window Opens
      │
Receive Ticks
      │
Update State
      │
Window Closes
      │
Publish Aggregate
      │
Initialize Next Window
```

Late or out-of-order events should follow a clearly defined policy (e.g., reject, adjust within a grace period, or emit corrections).

---

# 6. Tick Data Handling

Tick data represents the raw market events.

Each tick should be:

- Validated
- Timestamped
- Ordered (per symbol)
- Forwarded immediately
- Used to update aggregation state

The tick stream remains the source of truth for all higher-level aggregates.

---

# 7. OHLC Generation

Each aggregation window maintains the following values:

- **Open** – Price of the first tick in the window.
- **High** – Highest traded price during the window.
- **Low** – Lowest traded price during the window.
- **Close** – Price of the final tick in the window.

Additional commonly stored fields:

- Volume
- Trade count
- First timestamp
- Last timestamp
- Sequence range

### Candle Lifecycle

```text
First Tick
      │
Open = Price
High = Price
Low = Price
Close = Price
      │
Subsequent Ticks
      │
Update High
Update Low
Update Close
Accumulate Volume
Increment Trade Count
      │
Window End
      │
Publish Candle
```

Windows without trades may either be omitted or synthesized according to downstream requirements.

---

# 8. VWAP Calculation

VWAP (Volume Weighted Average Price) represents the average traded price weighted by volume.

The engine maintains:

- Running price × volume total
- Running volume total

At any point:

```text
VWAP = Σ(Price × Volume) / Σ(Volume)
```

### Per Window

Each aggregation window maintains independent VWAP state.

### Reset Behavior

VWAP resets at the start of every aggregation window unless session-level VWAP is explicitly required.

### Edge Cases

- Zero-volume windows
- Invalid trade sizes
- Correct handling of partial fills
- Numeric precision to avoid cumulative rounding errors

---

# 9. Concurrent Symbol Processing

Production systems process many symbols simultaneously.

### Recommended Model

```text
Dispatcher
      │
 ┌────┼────┐
 ▼    ▼    ▼
AAPL  MSFT BTC
 │     │     │
Aggregator Aggregator Aggregator
```

Each symbol owns its aggregation state, eliminating contention between unrelated symbols.

### Benefits

- Independent processing
- Natural parallelism
- Better cache locality
- Reduced lock contention
- Easier scaling

Load distribution may be based on consistent hashing or partitioning.

---

# 10. State Management

The Aggregation Engine maintains short-lived in-memory state.

### Per Symbol

- Active windows
- OHLC values
- VWAP accumulators
- Trade count
- Volume
- Last sequence number
- Last timestamp

### Characteristics

- Mutable
- Window-scoped
- Recreated during replay if necessary

State should be isolated per symbol and cleaned up after window completion.

---

# 11. Fault Tolerance

The Aggregation Engine should tolerate process failures without compromising correctness.

### Recovery Sources

- PostgreSQL Event Log
- Replay API
- Snapshot Service

### Recovery Flow

```text
Restart
     │
Load Snapshot (optional)
     │
Replay Missing Events
     │
Rebuild Aggregation State
     │
Resume Live Processing
```

Replay should reproduce aggregates deterministically.

---

# 12. Integration with Replay & Event Log

### Event Log

The PostgreSQL Event Log stores normalized events and serves as the authoritative historical record.

### Replay

Replay reconstructs the exact event stream to rebuild aggregation state or reproduce historical outputs.

### Snapshot Service

Snapshots provide recent aggregation state to reduce replay duration during recovery.

### Benefits

- Deterministic recovery
- Historical analytics
- Backtesting
- Auditability

---

# 13. Performance Considerations

To support high-throughput, low-latency workloads:

- Partition work by symbol
- Minimize allocations
- Reuse aggregation state where practical
- Avoid unnecessary serialization
- Batch downstream publishes when appropriate
- Favor cache-friendly data layouts
- Limit synchronization to symbol ownership boundaries
- Monitor and bound memory usage

The architecture should scale linearly with additional symbols and CPU cores where possible.

---

# 14. Prometheus Metrics

A production Aggregation Engine should expose metrics across throughput, latency, correctness, and resource usage.

## Throughput

- `aggregation_ticks_processed_total`
- `aggregation_candles_published_total`
- `aggregation_vwap_updates_total`

## Latency

- `aggregation_processing_duration_seconds`
- `aggregation_window_close_duration_seconds`

Use histograms for latency distributions.

## State

- `aggregation_active_symbols`
- `aggregation_active_windows`
- `aggregation_state_entries`

Use gauges for current state.

## Errors

- `aggregation_invalid_events_total`
- `aggregation_late_events_total`
- `aggregation_out_of_order_events_total`
- `aggregation_replay_failures_total`

## Recovery

- `aggregation_snapshot_restores_total`
- `aggregation_replay_duration_seconds`

Operational dashboards should surface these metrics for monitoring and alerting.

---

# 15. Common Implementation Mistakes

Avoid:

- Window alignment based on first event arrival
- Sharing mutable state across symbols
- Global locks around aggregation
- Ignoring out-of-order events
- Excessive object allocations
- Poor numeric precision for VWAP
- Unbounded memory growth
- Publishing incomplete candles
- Mixing wall-clock time with event time
- Failing to rebuild state deterministically during replay
- Insufficient observability

---

# 16. Production Readiness Checklist

## Architecture

- [ ] Modular aggregation pipeline
- [ ] Extensible aggregation framework
- [ ] Clear separation between ingestion and aggregation

## Aggregation

- [ ] Tick support
- [ ] OHLC generation
- [ ] VWAP calculation
- [ ] Multiple time windows
- [ ] Deterministic window alignment

## Concurrency

- [ ] Per-symbol isolation
- [ ] Scalable partitioning
- [ ] Minimal lock contention

## State

- [ ] In-memory aggregation state
- [ ] Automatic cleanup
- [ ] Snapshot compatibility

## Fault Tolerance

- [ ] Replay integration
- [ ] Snapshot restoration
- [ ] Deterministic recovery

## Observability

- [ ] Prometheus metrics
- [ ] Structured logging
- [ ] OpenTelemetry tracing
- [ ] Health checks

## Performance

- [ ] Low allocations
- [ ] Efficient memory usage
- [ ] Bounded latency
- [ ] Throughput validation

## Operations

- [ ] Graceful shutdown
- [ ] Startup validation
- [ ] Configuration management
- [ ] Capacity monitoring

---

# 17. Final Recommendations

A production-grade Aggregation Engine should be designed as a deterministic, low-latency streaming processor that converts normalized market events into higher-level market data products.

### Core Responsibilities

- Consume normalized tick data
- Maintain per-symbol aggregation state
- Generate OHLC candles
- Compute VWAP
- Support multiple concurrent aggregation windows
- Publish derived events with deterministic ordering

### Key Architectural Characteristics

- Fixed, deterministic window alignment
- Per-symbol state isolation
- Efficient in-memory processing
- Replay-driven recovery
- Snapshot-assisted startup
- Rich observability through metrics, logging, and tracing
- Horizontal scalability through symbol partitioning

When integrated with the Replay API, PostgreSQL Event Log, Snapshot Service, and observability stack, the Aggregation Engine becomes a resilient and extensible component suitable for production-grade real-time market data platforms.