# Distributed Benchmarking Strategy
## Real-Time Market Data Platform

### Author

Principal Distributed Systems Engineer

### Goal

Design a distributed benchmarking strategy for a horizontally scalable market data platform.

Architecture:

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

     ▼             ▼             ▼

 WebSocket    WebSocket    WebSocket
  Clients      Clients      Clients
```

Benchmark targets:

```text
1 Gateway

3 Gateways

5 Gateways
```

Primary metrics:

```text
Cluster Throughput

Publish Latency

Subscriber Latency

Redis Utilization
```

---

# 1. Benchmarking Goals

The objective is not simply measuring speed.

The objective is understanding:

```text
Scaling Efficiency

System Bottlenecks

Capacity Limits

Latency Characteristics

Failure Points
```

Key questions:

```text
How many updates/sec can the cluster sustain?

How does latency change as gateways increase?

Does Redis become the bottleneck?

Does throughput scale linearly?
```

---

# 2. Benchmarking Philosophy

Distributed systems must be measured as a whole.

Avoid:

```text
Benchmarking Components In Isolation
```

Instead measure:

```text
End-To-End Flow
```

Example:

```text
Feed Generator
      ↓

Publisher
      ↓

Redis
      ↓

Gateway
      ↓

Client
```

This provides realistic results.

---

# 3. Benchmark Environment

All benchmark runs should be performed using:

```text
Dedicated Infrastructure

Stable Network Conditions

Consistent Hardware
```

Avoid:

```text
Developer Laptops

Shared Machines

Background Workloads
```

---

# 4. Test Topology

## Scenario A

Single Gateway

```text
Feed Generator
      ↓

Publisher
      ↓

Redis
      ↓

Gateway 1
      ↓

Clients
```

Purpose:

```text
Baseline Performance
```

---

## Scenario B

Three Gateways

```text
Feed Generator
      ↓

Publisher
      ↓

Redis
      ↓

Gateway 1

Gateway 2

Gateway 3
```

Purpose:

```text
Initial Horizontal Scaling
```

---

## Scenario C

Five Gateways

```text
Feed Generator
      ↓

Publisher
      ↓

Redis
      ↓

Gateway 1

Gateway 2

Gateway 3

Gateway 4

Gateway 5
```

Purpose:

```text
Production-Scale Evaluation
```

---

# 5. Cluster Throughput

## Definition

Total updates successfully delivered across the cluster.

Formula:

```text
Cluster Throughput

=

Messages Delivered

÷

Time
```

---

## Example

If:

```text
100,000 Messages
```

are delivered in:

```text
10 Seconds
```

Then:

```text
10,000 Messages/Sec
```

cluster throughput.

---

## Why It Matters

Measures:

```text
Overall Capacity

Scaling Efficiency

Fan-Out Performance
```

---

# 6. Publish Latency

## Definition

Time from publisher submission to Redis acceptance.

Measurement:

```text
Publisher
      ↓

Redis Publish
```

---

## Formula

```text
Publish Latency

=

Redis Publish Time
-
Publish Start Time
```

---

## Why It Matters

High publish latency often indicates:

```text
Redis Saturation

Network Congestion

Serialization Overhead
```

---

# 7. Subscriber Latency

## Definition

Time from update creation until client receives update.

Measurement:

```text
Feed Generator
      ↓

Publisher
      ↓

Redis
      ↓

Gateway
      ↓

Client
```

---

## Formula

```text
Subscriber Latency

=

Client Receive Timestamp
-
Update Creation Timestamp
```

---

## Why It Matters

This is the most important user-facing metric.

Represents:

```text
Market Data Freshness
```

---

# 8. Redis Utilization

Redis is often the first shared bottleneck.

Monitor:

```text
CPU

Memory

Network

Command Rate

Connected Clients
```

---

## Key Metrics

```text
used_memory

connected_clients

instantaneous_ops_per_sec

pubsub_channels

pubsub_patterns

network_input_bytes

network_output_bytes
```

---

# 9. Benchmark Load Profiles

## Low Load

```text
1,000 Updates/Sec
```

Purpose:

```text
Baseline Validation
```

---

## Medium Load

```text
10,000 Updates/Sec
```

Purpose:

```text
Expected Production Traffic
```

---

## High Load

```text
50,000 Updates/Sec
```

Purpose:

```text
Stress Testing
```

---

## Extreme Load

```text
100,000+ Updates/Sec
```

Purpose:

```text
Failure Point Discovery
```

---

# 10. Client Distribution

For fair testing:

```text
Connections Must Be Evenly Distributed
```

Example:

### 1 Gateway

```text
10,000 Clients
```

↓

```text
Gateway 1
```

---

### 3 Gateways

```text
10,000 Clients
```

↓

```text
3333 Clients

3333 Clients

3334 Clients
```

---

### 5 Gateways

```text
10,000 Clients
```

↓

```text
2000 Clients Per Gateway
```

---

# 11. Benchmark Scenario: 1 Gateway

Architecture:

```text
Publisher
      ↓

Redis
      ↓

Gateway 1
```

Measure:

```text
Throughput

Latency

Memory

CPU
```

Purpose:

```text
Establish Baseline
```

Expected:

```text
Lowest Infrastructure Cost

Highest Single-Node Load
```

---

# 12. Benchmark Scenario: 3 Gateways

Architecture:

```text
Publisher
      ↓

Redis
      ↓

Gateway 1

Gateway 2

Gateway 3
```

Measure:

```text
Throughput Increase

Latency Stability

Redis Load
```

Expected:

```text
Improved Throughput

Reduced Per-Gateway CPU

Improved Connection Capacity
```

---

# 13. Benchmark Scenario: 5 Gateways

Architecture:

```text
Publisher
      ↓

Redis
      ↓

Gateway 1

Gateway 2

Gateway 3

Gateway 4

Gateway 5
```

Measure:

```text
Horizontal Scaling Efficiency
```

Expected:

```text
Higher Connection Capacity

Higher Fan-Out Capacity
```

Potential bottleneck:

```text
Redis
```

---

# 14. Scaling Efficiency

A critical benchmark metric.

Formula:

```text
Scaling Efficiency

=

Observed Throughput

÷

Expected Throughput
```

---

Example:

Baseline:

```text
1 Gateway

10,000 Msg/Sec
```

Expected:

```text
5 Gateways

50,000 Msg/Sec
```

Observed:

```text
42,000 Msg/Sec
```

Efficiency:

```text
84%
```

---

## Interpretation

```text
90-100%
Excellent

80-90%
Good

70-80%
Acceptable

<70%
Investigate Bottlenecks
```

---

# 15. Latency Percentiles

Never rely solely on averages.

Measure:

```text
P50

P95

P99

P99.9
```

---

Example:

```text
P50  = 3ms

P95  = 8ms

P99  = 20ms

P99.9 = 75ms
```

---

## Why?

Market data systems care about tail latency.

Averages hide spikes.

---

# 16. Resource Utilization

For every benchmark capture:

```text
CPU Usage

Memory Usage

Network Throughput

Goroutine Count

Queue Depth
```

Per component:

```text
Publisher

Redis

Gateways
```

---

# 17. Bottleneck Identification

## Gateway Bottleneck

Symptoms:

```text
High CPU

Growing Queues

Stable Redis
```

---

## Redis Bottleneck

Symptoms:

```text
High Redis CPU

High Publish Latency

Stable Gateways
```

---

## Network Bottleneck

Symptoms:

```text
Increasing Subscriber Latency

Packet Loss

Network Saturation
```

---

# 18. Recommended Benchmark Matrix

| Scenario | Clients | Updates/Sec |
|-----------|----------|-------------|
| Small | 1,000 | 1,000 |
| Medium | 5,000 | 10,000 |
| Large | 10,000 | 50,000 |
| Stress | 25,000 | 100,000 |
| Extreme | 50,000 | Max Sustainable |

Run each scenario for:

```text
5 Minutes

15 Minutes

30 Minutes
```

to expose stability issues.

---

# 19. Success Criteria

## Throughput

```text
Linear Scaling
```

as gateways increase.

---

## Publish Latency

```text
Low And Stable
```

under increasing load.

---

## Subscriber Latency

```text
Predictable

Low P99
```

under peak traffic.

---

## Redis Utilization

```text
Below Saturation

No Command Backlog

No Memory Pressure
```

---

# 20. Benchmark Reporting

Each benchmark should produce:

```text
Cluster Throughput

Publish Latency (P50/P95/P99)

Subscriber Latency (P50/P95/P99)

Redis CPU

Redis Memory

Gateway CPU

Gateway Memory

Connection Count

Dropped Messages
```

Graph trends over time.

Focus on:

```text
Throughput

Latency

Resource Consumption
```

simultaneously.

---

# Recommended Benchmark Progression

```text
1 Gateway
      ↓

3 Gateways
      ↓

5 Gateways
      ↓

Increased Client Count
      ↓

Increased Update Rate
      ↓

Stress Testing
```

This reveals:

```text
Scaling Efficiency

Redis Limits

Gateway Limits

Operational Capacity
```

before production deployment.

---

# Final Recommendation

For a distributed market data platform, benchmark the system as a complete cluster rather than individual services.

Track:

```text
Cluster Throughput

Publish Latency

Subscriber Latency

Redis Utilization
```

across:

```text
1 Gateway

3 Gateways

5 Gateways
```

The primary success criterion is not maximum throughput alone.

The primary success criterion is:

```text
Sustained Throughput

Predictable Latency

Efficient Scaling

Controlled Resource Usage
```

while maintaining stable operation under production-level load.