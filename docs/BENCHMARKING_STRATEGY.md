# Benchmarking Strategy

## Real-Time Market Data Distribution Platform

### Version

Performance Engineering Design

### Goal

Establish a rigorous benchmarking methodology for a real-time market data platform.

The objective is not merely to obtain a throughput number.

The objective is to answer:

```text id="j1w4lo"
How much load can the system sustain?

What is the latency distribution?

Where are bottlenecks located?

How does the system degrade under stress?

What are the scalability limits?
```

This is the style of analysis expected in quantitative trading firms and low-latency engineering teams.

---

# 1. Why Benchmarking Matters

Most engineers measure:

```text id="qexmkg"
Requests Per Second
```

and stop.

That is insufficient.

A production trading platform requires understanding:

```text id="gm2v5q"
Latency

Throughput

Resource Consumption

Failure Modes

Scalability Characteristics
```

A system delivering:

```text id="v4rf8m"
100,000 messages/sec
```

is not useful if:

```text id="z4qux7"
P99 latency = 2 seconds
```

---

# 2. Benchmarking Philosophy

Measure the system as a pipeline.

```text id="e7dwtv"
Feed Generator
      ↓

Publisher
      ↓

Topic Manager
      ↓

WebSocket Gateway
      ↓

Client
```

Every stage must be benchmarked independently.

Then benchmark the entire system.

---

# 3. Core Metrics

## Throughput

Measures:

```text id="xjlwmw"
Messages Processed Per Second
```

Examples:

```text id="t4f9cl"
1,000 msg/sec
10,000 msg/sec
100,000 msg/sec
```

Questions:

```text id="6r95o0"
Maximum sustainable rate?

Rate before degradation?

Rate before failure?
```

---

## Latency

Measures:

```text id="w0x4ul"
Time Between
Generation
and
Consumption
```

Example:

```text id="4hl2w7"
Feed Generated
12:00:00.000

Client Received
12:00:00.002
```

Latency:

```text id="8pt2ys"
2 milliseconds
```

---

## Memory

Measures:

```text id="42e10t"
Heap Usage

Allocation Rate

GC Activity
```

Questions:

```text id="6j2z92"
Does memory grow linearly?

Do we leak memory?

How expensive is GC?
```

---

## Goroutines

Measures:

```text id="j05m3m"
Concurrency Cost
```

Questions:

```text id="vwwggs"
Expected count?

Growth pattern?

Leaks?
```

---

# 4. Latency Metrics

Never measure:

```text id="f6hrfo"
Average Latency
```

alone.

Average hides problems.

---

## Required Percentiles

Measure:

```text id="3p6s17"
P50

P95

P99

P99.9
```

Example:

```text id="7k9uik"
P50  = 2ms

P95  = 5ms

P99  = 15ms

P99.9 = 70ms
```

This immediately reveals tail latency.

---

## Why Tail Latency Matters

In trading systems:

```text id="3tdw1j"
Worst Case
```

often matters more than:

```text id="8nqhl5"
Average Case
```

One delayed update can be more damaging than thousands of fast updates.

---

# 5. Throughput Metrics

Measure:

```text id="t84cqt"
Generated Events/sec

Published Events/sec

Delivered Events/sec
```

Example:

```text id="q2jb04"
Generator
100k/sec

Publisher
100k/sec

Gateway
85k/sec
```

Immediately identifies bottlenecks.

---

# 6. Benchmark Levels

## Level 1

Component Benchmarks

Benchmark components independently.

Examples:

```text id="jgrw8w"
Publisher

Topic Manager

Gateway
```

Goal:

```text id="ptoqfx"
Find Local Bottlenecks
```

---

## Level 2

Subsystem Benchmarks

Example:

```text id="q8f0lk"
Publisher
+
Topic Manager
```

Goal:

```text id="b6w76t"
Measure Interaction Costs
```

---

## Level 3

End-To-End Benchmarks

Example:

```text id="esysxj"
Generator
→ Publisher
→ Topic Manager
→ Gateway
→ Client
```

Goal:

```text id="4pm6xe"
Real Production Behavior
```

---

# 7. Throughput Testing Plan

## Test 1

Baseline

```text id="ot7s74"
1 Symbol

10 Subscribers
```

Purpose:

```text id="3svokh"
Sanity Check
```

---

## Test 2

Medium Load

```text id="z3n1me"
100 Symbols

1000 Subscribers
```

Purpose:

```text id="byc92m"
Normal Production Load
```

---

## Test 3

Heavy Load

```text id="x6q62x"
1000 Symbols

5000 Connections
```

Purpose:

```text id="qv57zz"
Stress Behavior
```

---

## Test 4

Saturation Test

Increase load continuously.

Example:

```text id="7s6t3w"
10k/sec

20k/sec

50k/sec

100k/sec
```

until degradation appears.

Determine:

```text id="n9eq0o"
Breaking Point
```

---

# 8. Latency Testing Plan

## Timestamp Injection

Every market update should carry:

```text id="f7h93x"
Generation Timestamp
```

Client records:

```text id="l7ehmz"
Receive Timestamp
```

Latency:

```text id="3mhtdh"
Receive
-
Generate
```

---

## Histogram Analysis

Store latency in histograms.

Example:

```text id="a0hnmn"
0-1ms

1-2ms

2-5ms

5-10ms

10ms+
```

This reveals system behavior under load.

---

# 9. Memory Analysis

## Questions

Measure:

```text id="wjfwm0"
Heap Size

Allocation Rate

GC Frequency
```

---

## Load Progression

Observe memory at:

```text id="1w7mq8"
1k subscribers

2k subscribers

5k subscribers

10k subscribers
```

Expected:

```text id="c48wzc"
Linear Growth
```

Bad sign:

```text id="4o6p7y"
Exponential Growth
```

---

## Memory Leak Test

Run continuously.

Duration:

```text id="a8b4lu"
1 Hour

6 Hours

24 Hours
```

Look for:

```text id="1e9smc"
Unbounded Memory Growth
```

---

# 10. Goroutine Analysis

## Expected Behavior

Connections:

```text id="bjcjsh"
5000 Clients
```

Model:

```text id="91ccm6"
Read Loop

Write Loop
```

Expected:

```text id="j4y2b0"
≈ 10000 Goroutines
```

plus background workers.

---

## Leak Detection

Monitor:

```text id="gxhrmw"
Goroutine Count
```

during:

```text id="uxppay"
Connect

Disconnect

Reconnect
```

Expected:

```text id="6z5vzx"
Stable Count
```

Bad sign:

```text id="lgcefr"
Count Only Increases
```

---

# 11. Backpressure Testing

Critical for market systems.

---

## Scenario

Subscriber becomes slow.

```text id="dvgw5i"
Producer
10000 msg/sec

Consumer
100 msg/sec
```

Observe:

```text id="nk9j7j"
Queue Growth

Latency Growth

Memory Growth
```

---

## Desired Behavior

System should:

```text id="qk7bfo"
Drop Messages

Throttle

Disconnect
```

according to policy.

Not:

```text id="h6kkpn"
Crash
```

---

# 12. Failure Testing

Benchmarking includes failures.

---

## Subscriber Failure

```text id="j8cf6k"
Disconnect Client
```

Observe:

```text id="v4s8wj"
Cleanup Speed
```

---

## Burst Load

Example:

```text id="2fqfnd"
100x Normal Traffic
```

Observe:

```text id="7wlh6y"
Latency

Dropped Messages

Recovery Time
```

---

## Topic Explosion

Example:

```text id="6e5shm"
100,000 Topics
```

Observe:

```text id="3hqtqv"
Registry Performance
```

---

# 13. CPU Analysis

Measure:

```text id="2mkvxf"
CPU Usage

Scheduler Activity

Context Switching
```

Questions:

```text id="fjlwm7"
Where is CPU spent?

Locking?

Serialization?

Fan-Out?
```

---

# 14. Scaling Analysis

## Subscriber Scaling

Test:

```text id="uxvhly"
100

1000

5000

10000
```

connections.

Measure:

```text id="6wz7gw"
Latency

Throughput

Memory
```

---

## Topic Scaling

Test:

```text id="9icxx6"
100 Topics

1000 Topics

10000 Topics
```

Measure:

```text id="cb9gfh"
Lookup Cost

Publish Cost
```

---

# 15. Benchmark Dashboard

Track continuously:

```text id="k8wlye"
Throughput

P50

P95

P99

P99.9

Memory

GC

Goroutines

CPU

Queue Depth
```

---

# 16. Goldman Sachs Interview Discussion

A senior engineer interview is unlikely to ask:

```text id="s3zqz6"
How many messages/sec?
```

Instead:

```text id="pv3p5t"
What happens at saturation?

How do you identify bottlenecks?

How do you measure tail latency?

How does the system fail?

How does performance scale?

How would you validate production readiness?
```

The strongest answer demonstrates understanding that:

```text id="nzzwuj"
Performance
=
Throughput
+
Latency
+
Resource Usage
+
Failure Characteristics
```

not a single benchmark number.

---

# Benchmark Success Criteria

A production-ready market data platform should provide:

### Throughput

```text id="uxgrsl"
Sustainable Messages/sec
```

### Latency

```text id="4z6gje"
P50
P95
P99
P99.9
```

### Memory

```text id="z4yjlwm"
Stable Growth
No Leaks
```

### Goroutines

```text id="c3nyl7"
Predictable Count
No Leaks
```

### Resilience

```text id="cnvywg"
Graceful Degradation
Backpressure Handling
Failure Isolation
```

A benchmark is considered complete only when it explains not just how fast the system is, but why it performs that way and where it will eventually break.
