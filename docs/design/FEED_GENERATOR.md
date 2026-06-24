# Exchange Feed Simulator Design

## Real-Time Market Data Distribution System

### Version

Week 1 Architecture Design

### Goal

Design a realistic exchange feed simulator capable of generating market events for testing a real-time market data platform.

Target throughput:

- 1,000+ updates/sec minimum
- Easily scalable to 10,000+ updates/sec
- Deterministic and configurable
- Realistic market behavior

---

# 1. Objectives

The simulator should emulate a market exchange by continuously generating price updates for configurable symbols.

The generated feed must support:

- Multiple symbols
- Variable update rates
- Adjustable volatility
- High throughput
- Timestamp accuracy
- Realistic price movement

The simulator should behave similarly to a simplified market data feed from an exchange.

---

# 2. Functional Requirements

## Configurable Symbols

Example:

```text
AAPL
MSFT
GOOG
TSLA
NVDA
BTCUSD
ETHUSD
```

Each symbol maintains independent state.

State includes:

- Current price
- Previous price
- Volatility profile
- Tick size
- Update frequency

---

## Configurable Update Rate

Examples:

```text
10 updates/sec
100 updates/sec
1000 updates/sec
```

Configuration should allow:

- Global rate
- Per-symbol rate

Example:

```text
AAPL   = 100 updates/sec
MSFT   = 50 updates/sec
BTCUSD = 500 updates/sec
```

---

## Configurable Volatility

Volatility controls magnitude of movement.

Example:

```text
Low Volatility
0.05%

Medium Volatility
0.25%

High Volatility
1.0%
```

This should influence:

- Tick size
- Price variance
- Direction changes

---

## Timestamps

Every event should contain:

```text
Event Time
Generation Time
Sequence Number
```

Timestamp precision:

```text
Microseconds
or
Nanoseconds
```

depending on platform requirements.

---

# 3. Market Event Model

Each generated update should conceptually contain:

```text
Symbol
Price
Timestamp
Sequence Number
Source
```

Future additions:

```text
Bid
Ask
Volume
Trade Size
Exchange ID
```

---

# 4. Realistic Price Movement

## Problem

Random numbers alone create unrealistic behavior.

Bad example:

```text
100
101
99
103
95
110
```

This resembles noise rather than market movement.

---

## Recommended Approach

Use a Random Walk Model.

Each new price is derived from:

```text
Current Price
+
Small Random Change
```

Result:

```text
100.00
100.03
100.01
100.05
100.08
100.06
```

This produces smooth movement.

---

## Drift Component

Real markets trend.

Introduce directional bias.

Example:

```text
Bullish Drift
+
Random Noise
```

Produces:

```text
100
100.1
100.2
100.25
100.4
100.3
100.5
```

---

## Mean Reversion

Markets frequently return toward equilibrium.

Concept:

```text
Distance From Fair Value
```

influences movement probability.

Benefits:

- Prevents runaway prices
- More realistic simulation

---

## Volatility Scaling

Different symbols should behave differently.

Example:

```text
AAPL
Low movement

BTCUSD
High movement
```

Volatility coefficient adjusts:

```text
Price Delta Magnitude
```

---

# 5. Simulator Architecture

## High-Level Flow

```text
+---------------------+
| Symbol Registry     |
+----------+----------+
           |
           v

+---------------------+
| Price Generator     |
+----------+----------+
           |
           v

+---------------------+
| Event Publisher     |
+----------+----------+
           |
           v

+---------------------+
| Distribution Layer  |
+---------------------+
```

---

# 6. Goroutine Strategy

## Goal

Avoid one goroutine per update.

That does not scale.

---

## Recommended Model

### One Goroutine Per Symbol

```text
AAPL Generator
MSFT Generator
GOOG Generator
BTC Generator
```

Each maintains:

```text
Current Price
Sequence Number
Volatility
```

Benefits:

- Independent state
- Easy reasoning
- Natural parallelism

---

## Alternative

### Symbol Partition Workers

For larger scale:

```text
Worker 1
100 Symbols

Worker 2
100 Symbols

Worker 3
100 Symbols
```

Benefits:

- Lower goroutine count
- Better CPU cache locality
- Easier scaling

Recommended once symbols exceed several hundred.

---

# 7. Channel Design

## Purpose

Decouple generation from distribution.

---

## Generation Channel

```text
Price Generator
        |
        v
   Event Channel
```

Generator produces updates.

Publisher consumes updates.

---

## Buffered Channels

Unbuffered channels create unnecessary blocking.

Recommended:

```text
Buffered Event Queue
```

Benefits:

- Burst tolerance
- Reduced contention
- Better throughput

---

## Backpressure

If consumers become slower than producers:

```text
Channel fills
```

System should:

- Record metrics
- Detect congestion
- Apply throttling strategy

Never allow unlimited memory growth.

---

# 8. Update Generation Strategy

## Naive Approach

```text
Sleep
Generate
Publish
```

Problems:

- Scheduling overhead
- Timing drift
- Poor scalability

---

## Recommended Approach

Use periodic tick scheduling.

Each worker:

```text
Wake Up
Generate Batch
Publish Batch
```

rather than generating one update at a time.

---

## Batch Generation

Instead of:

```text
Generate 1 Event
Send

Generate 1 Event
Send
```

Generate:

```text
100 Events
Send Batch
```

Benefits:

- Fewer context switches
- Better cache efficiency
- Higher throughput

---

# 9. Throughput Analysis

Target:

```text
1000 updates/sec
```

Example:

```text
100 symbols
10 updates/sec
```

Produces:

```text
1000 updates/sec
```

---

Example:

```text
500 symbols
20 updates/sec
```

Produces:

```text
10000 updates/sec
```

Architecture should comfortably support this range.

---

# 10. Bottlenecks

## Bottleneck 1: Excessive Goroutines

Bad:

```text
One goroutine per update
```

Result:

- Scheduler pressure
- Increased context switching

---

## Bottleneck 2: Channel Contention

Many producers writing to one channel.

Result:

```text
Lock contention
```

Symptoms:

- Increased latency
- CPU spikes

Mitigation:

```text
Multiple worker channels
Channel sharding
```

---

## Bottleneck 3: Logging

Logging every event is catastrophic.

Example:

```text
10000 updates/sec
```

generates massive I/O pressure.

Recommendation:

```text
Aggregate Metrics
Sampled Logging
```

---

## Bottleneck 4: Memory Allocation

Creating new objects for every update.

Result:

```text
GC Pressure
```

Symptoms:

- Latency spikes
- Throughput degradation

Mitigation:

```text
Object Reuse
Batch Processing
```

---

## Bottleneck 5: Timestamp Generation

High-frequency timestamp creation can become expensive.

Mitigation:

```text
Batch Timestamp Capture
```

or

```text
Single Clock Read Per Batch
```

when appropriate.

---

# 11. Scaling Strategy

## Single Process

Suitable for:

```text
1,000 - 50,000 updates/sec
```

---

## Multi-Worker Architecture

Suitable for:

```text
50,000 - 500,000 updates/sec
```

Use:

```text
Symbol Partitions
```

across worker pools.

---

## Distributed Simulation

Suitable for:

```text
Millions of updates/sec
```

Use:

```text
Simulator Node 1
Simulator Node 2
Simulator Node 3
```

Each responsible for a symbol subset.

---

# 12. Monitoring Metrics

The simulator should expose:

## Throughput

```text
updates_generated_total
updates_generated_per_second
```

---

## Latency

```text
generation_latency
publish_latency
```

---

## Queue Health

```text
channel_depth
queue_utilization
```

---

## Symbol Metrics

```text
updates_per_symbol
```

---

## Error Metrics

```text
generation_failures
dropped_updates
```

---

# 13. Week 1 Deliverables

## Architecture

- Simulator design completed
- Event model defined
- Symbol registry defined

## Concurrency Model

- Goroutine strategy documented
- Channel strategy documented
- Backpressure strategy documented

## Performance Design

- 1000+ updates/sec target validated
- Bottlenecks identified
- Scaling roadmap defined

## Documentation

- Sequence diagrams
- Architecture diagrams
- Future exchange integration plan

---

# Success Criteria

A successful Week 1 simulator architecture:

- Supports configurable symbols
- Supports configurable volatility
- Supports configurable update rates
- Produces realistic price movement
- Generates accurate timestamps
- Achieves 1,000+ updates/sec
- Can evolve into a production-grade exchange feed simulator without architectural redesign
