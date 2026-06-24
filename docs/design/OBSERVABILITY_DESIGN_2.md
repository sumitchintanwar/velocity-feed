# Observability Design

## Real-Time Market Data Distribution Platform

### Goal

Design a comprehensive observability strategy that provides visibility into:

* Throughput
* Latency
* Subscriber activity
* Connection health
* Message loss
* Resource utilization

Target architecture:

```text
Feed Generator
      ↓

Publisher
      ↓

Worker Pool
      ↓

Topic Manager
      ↓

WebSocket Gateway
      ↓

Clients
```

The primary objective is to answer:

```text
Is the system healthy?

Can it meet SLAs?

Where is the bottleneck?

Are clients receiving data?

Are we dropping messages?
```

---

# 1. Observability Pillars

A production market data platform should provide:

```text
Metrics

Logs

Tracing
```

For Week 1:

```text
Metrics
```

are the highest priority.

Metrics provide:

```text
Real-Time Visibility

Alerting

Capacity Planning

Performance Analysis
```

---

# 2. Metric Types

Prometheus metrics generally fall into three categories.

---

# Counters

## Definition

Counters only increase.

Example:

```text
0
1
2
3
...
```

They never decrease except on process restart.

---

## Use Cases

Track events.

Examples:

```text
Messages Published

Messages Delivered

Messages Dropped

Connections Opened
```

---

## Why Counters?

Questions answered:

```text
How many messages have we published?

How many messages have failed?

How many clients connected today?
```

---

# Gauges

## Definition

Gauges move up and down.

Example:

```text
100
95
102
87
```

---

## Use Cases

Track current state.

Examples:

```text
Active Connections

Current Subscribers

Queue Depth

Worker Count
```

---

## Why Gauges?

Questions answered:

```text
How many clients are connected right now?

How many subscribers exist?

How deep is the queue?
```

---

# Histograms

## Definition

Histograms measure distributions.

Instead of:

```text
Average Latency
```

they capture:

```text
P50

P95

P99

P99.9
```

---

## Use Cases

Measure latency.

Examples:

```text
Publish Latency

Delivery Latency

Queue Wait Time

Connection Duration
```

---

## Why Histograms?

Market data systems care about:

```text
Tail Latency
```

more than averages.

Example:

```text
Average
2ms

P99
50ms
```

This immediately reveals a problem.

---

# 3. Metrics by Component

## Feed Generator

Responsible for:

```text
Market Data Creation
```

---

### Counters

```text
feed_messages_generated_total

feed_generation_errors_total
```

---

### Gauges

```text
feed_active_symbols

feed_generation_rate
```

---

# Publisher Service

Responsible for:

```text
Receiving Updates

Publishing Internally
```

---

### Counters

```text
publisher_messages_received_total

publisher_messages_published_total

publisher_publish_failures_total
```

---

### Histograms

```text
publisher_publish_latency_seconds
```

Measures:

```text
Receive Update
      ↓
Publish Update
```

---

# Worker Pool

Responsible for:

```text
Task Execution
```

---

### Counters

```text
worker_tasks_received_total

worker_tasks_completed_total

worker_tasks_failed_total
```

---

### Gauges

```text
worker_active_workers

worker_idle_workers

worker_queue_depth
```

---

### Histograms

```text
worker_task_duration_seconds

worker_queue_wait_seconds
```

---

# Topic Manager

Responsible for:

```text
Subscriptions

Fan-Out
```

---

### Counters

```text
topic_subscriptions_added_total

topic_subscriptions_removed_total

topic_publish_operations_total
```

---

### Gauges

```text
topic_active_topics

topic_active_subscribers

topic_subscribers_per_topic
```

---

### Histograms

```text
topic_publish_latency_seconds

topic_fanout_duration_seconds
```

Measures:

```text
Publish
      ↓
Fan-Out Complete
```

---

# WebSocket Gateway

Responsible for:

```text
Client Connections

Delivery
```

---

### Counters

```text
websocket_connections_opened_total

websocket_connections_closed_total

websocket_messages_sent_total

websocket_messages_dropped_total

websocket_write_errors_total
```

---

### Gauges

```text
websocket_active_connections

websocket_active_subscriptions

websocket_slow_consumers

websocket_queue_depth
```

---

### Histograms

```text
websocket_delivery_latency_seconds

websocket_connection_duration_seconds

websocket_write_duration_seconds
```

---

# 4. Core Business Metrics

These are the most important metrics for operators.

---

# Messages Per Second

Measures throughput.

Counter:

```text
publisher_messages_published_total
```

Prometheus derives:

```text
Messages/Sec
```

using rate calculations.

---

# Publish Latency

Histogram:

```text
publisher_publish_latency_seconds
```

Track:

```text
P50

P95

P99

P99.9
```

---

# Subscriber Count

Gauge:

```text
topic_active_subscribers
```

Represents:

```text
Current Subscriber Population
```

---

# Dropped Messages

Counter:

```text
websocket_messages_dropped_total
```

Critical metric.

Questions:

```text
Are clients losing updates?

Are queues overflowing?
```

---

# WebSocket Connections

Gauge:

```text
websocket_active_connections
```

Represents:

```text
Current Connected Clients
```

---

# 5. Queue Observability

Queues are common bottlenecks.

Monitor:

```text
worker_queue_depth

client_queue_depth

publisher_queue_depth
```

---

## Why?

Growing queues often indicate:

```text
Slow Consumers

CPU Saturation

Backpressure
```

before failures occur.

---

# 6. Backpressure Metrics

Critical for market data systems.

---

### Counters

```text
queue_overflow_total

backpressure_events_total

slow_consumer_disconnects_total
```

---

### Gauges

```text
queue_utilization_percent
```

---

Questions:

```text
Are queues filling?

Are clients keeping up?

Are we approaching saturation?
```

---

# 7. Resource Metrics

Platform health metrics.

---

## Memory

```text
process_resident_memory_bytes

go_memstats_heap_alloc_bytes
```

---

## CPU

```text
process_cpu_seconds_total
```

---

## Goroutines

```text
go_goroutines
```

---

## Garbage Collection

```text
go_gc_duration_seconds
```

---

# 8. Alerting Recommendations

## Throughput Drop

Alert when:

```text
Messages/Sec
```

drops unexpectedly.

May indicate:

```text
Feed Failure

Publisher Failure
```

---

## Latency Spike

Alert when:

```text
P99 Publish Latency
```

exceeds SLA.

Example:

```text
P99 > 50ms
```

---

## Connection Loss

Alert when:

```text
websocket_active_connections
```

drops sharply.

---

## Queue Saturation

Alert when:

```text
Queue Utilization > 80%
```

for sustained periods.

---

## Message Drops

Alert immediately when:

```text
websocket_messages_dropped_total
```

begins increasing.

---

# 9. Recommended Dashboard

## Throughput

```text
Messages Generated/sec

Messages Published/sec

Messages Delivered/sec
```

---

## Latency

```text
P50 Publish Latency

P95 Publish Latency

P99 Publish Latency

P99.9 Publish Latency
```

---

## Subscribers

```text
Active Subscribers

Subscribers Per Topic
```

---

## Connections

```text
Active Connections

Connection Rate

Disconnect Rate
```

---

## Reliability

```text
Dropped Messages

Queue Overflows

Slow Consumer Disconnects
```

---

## Resources

```text
CPU

Memory

GC

Goroutines
```

---

# 10. Recommended Metric Naming Convention

Use:

```text
component_metric_unit
```

Examples:

```text
publisher_messages_published_total

publisher_publish_latency_seconds

topic_active_subscribers

worker_queue_depth

websocket_active_connections

websocket_messages_sent_total

websocket_messages_dropped_total

websocket_delivery_latency_seconds

slow_consumer_disconnects_total
```

---

# Production Metric Checklist

## Throughput

```text
publisher_messages_published_total
websocket_messages_sent_total
```

---

## Latency

```text
publisher_publish_latency_seconds
topic_fanout_duration_seconds
websocket_delivery_latency_seconds
```

---

## Subscriber Activity

```text
topic_active_subscribers
topic_active_topics
```

---

## Reliability

```text
websocket_messages_dropped_total
queue_overflow_total
slow_consumer_disconnects_total
```

---

## Connections

```text
websocket_active_connections
websocket_connections_opened_total
websocket_connections_closed_total
```

---

## Resources

```text
go_goroutines
go_memstats_heap_alloc_bytes
process_cpu_seconds_total
```

---

# Final Recommendation

For a production-grade market data platform, observability should focus on five primary business metrics:

```text
Messages/sec

Publish Latency

Subscriber Count

Dropped Messages

WebSocket Connections
```

Implement these using:

```text
Counters
→ Event Tracking

Gauges
→ Current State

Histograms
→ Latency Distribution
```

A system is only production-ready when operators can answer, in real time:

```text
How much traffic is flowing?

How fast is delivery?

How many clients are connected?

Are messages being dropped?

Where is the bottleneck?
```

without inspecting logs or restarting services.
