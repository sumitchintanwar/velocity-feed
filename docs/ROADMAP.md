# Real-Time Market Data Distribution System

*A Production-Grade Distributed Systems Project in Go*

## Overview

Modern stock exchanges generate massive volumes of market updates every second.

Example market updates:

```json
{
  "symbol": "AAPL",
  "price": 203.41,
  "timestamp": 1719000000
}
```

Thousands of traders, algorithms, and applications require these updates in real time.

Directly sending updates from the exchange to every client is inefficient and unscalable. Instead, financial institutions use **Market Data Distribution Systems**.

```text
Exchange Feed
      │
      ▼
Market Data Server
      │
 ┌────┼────┐
 ▼    ▼    ▼
T1    T2   T3
```

This project simulates a production-grade market data platform similar to those used by trading firms and investment banks.

---

# System Architecture

## Core Components

### Feed Generator

Simulates stock exchange feeds.

Responsibilities:

* Generate market updates
* Configurable symbols
* Configurable message rates
* Support burst traffic

Example:

```json
{
  "symbol": "AAPL",
  "price": 203.41,
  "timestamp": 1719000000
}
```

---

### Publisher Service

Receives updates from the feed generator.

Responsibilities:

* Receive market updates
* Validate messages
* Forward updates to topic manager

Flow:

```text
Feed Generator
      │
      ▼
 Publisher
```

---

### Topic Manager

Maintains symbol-specific topics.

Examples:

```text
AAPL
GOOG
MSFT
TSLA
```

Responsibilities:

* Topic creation
* Subscriber registration
* Message broadcasting

Core APIs:

```go
Subscribe()
Unsubscribe()
Publish()
```

---

### Subscriber Service

Tracks client subscriptions.

Example:

```text
Client A → AAPL
Client B → AAPL + GOOG
Client C → MSFT
```

---

### WebSocket Gateway

Maintains persistent client connections.

Responsibilities:

* Connection management
* Subscription handling
* Real-time update delivery

Example request:

```json
{
  "action": "subscribe",
  "symbol": "AAPL"
}
```

---

# Technologies

| Area                  | Technology            |
| --------------------- | --------------------- |
| Language              | Go                    |
| Streaming             | WebSockets            |
| RPC                   | gRPC                  |
| Distributed Messaging | Redis Pub/Sub         |
| Containers            | Docker                |
| Monitoring            | Prometheus            |
| Visualization         | Grafana               |
| Tracing               | OpenTelemetry         |
| Storage               | PostgreSQL / BadgerDB |
| Orchestration         | Kubernetes            |

---

# Engineering Challenges

## Slow Consumers

Scenario:

```text
Producer = 10,000 msgs/sec
Consumer = 100 msgs/sec
```

Possible strategies:

* Buffer messages
* Drop oldest
* Drop newest
* Disconnect client

Concepts:

* Backpressure
* Flow control
* Queue management

---

## Horizontal Scaling

```text
             Load Balancer
                   │
       ┌───────────┼───────────┐
       ▼           ▼           ▼
   Gateway-1   Gateway-2   Gateway-3
```

Goals:

* Support thousands of clients
* Distribute subscriber load
* Improve availability

---

## Reliability

Handle:

* Gateway crashes
* Redis failures
* Network partitions
* Client disconnects

Capabilities:

* Automatic reconnect
* Subscription recovery
* Event replay

---

# Development Roadmap

# Week 1 — Core System

## Day 1 — Project Setup

Structure:

```text
cmd/
internal/
pkg/
docker/
benchmarks/
```

Setup:

* Go modules
* Docker
* Makefile
* CI pipeline

Deliverables:

```bash
go test ./...
make run
```

---

## Day 2 — Feed Generator

Features:

* Configurable symbols
* Configurable rate
* Realistic tick generation

Example:

```text
1000 msgs/sec
```

Learn:

* Goroutines
* Channels

---

## Day 3 — Publisher Service

Flow:

```text
Feed
 ↓
Publisher
```

Implement:

```go
Publish(update)
```

---

## Day 4 — Topic Manager

Implement:

```go
Subscribe()
Unsubscribe()
Publish()
```

Topics:

```text
AAPL
GOOG
MSFT
```

---

## Day 5 — WebSocket Gateway

Implement:

* Connection management
* Subscription protocol
* Update delivery

---

## Day 6 — Live Data Streaming

End-to-end pipeline:

```text
Feed
 ↓
Publisher
 ↓
Topic Manager
 ↓
WebSocket
 ↓
Client
```

---

## Day 7 — Benchmark v1

Measure:

* Throughput
* Latency
* Memory usage

---

# Week 2 — Concurrency & Scale

## Day 8 — Worker Pool

Replace:

```go
go handle()
```

With:

```go
WorkerPool
```

Concepts:

* Bounded concurrency
* Task queues

---

## Day 9 — Lock Optimization

Upgrade:

```go
Mutex
```

to

```go
RWMutex
```

Benchmark:

* Before
* After

---

## Day 10 — Backpressure

Scenario:

```text
Producer = 10k/sec
Consumer = 100/sec
```

Policies:

* Block
* Drop oldest
* Drop newest

---

## Day 11 — Subscriber Buffers

Implement:

```go
ClientBuffer
```

Handle:

* Burst traffic
* Slow consumers

---

## Day 12 — Rate Limiting

Implement:

* Token Bucket algorithm

Protect:

* WebSocket gateway
* Subscription endpoints

---

## Day 13 — Load Testing

Benchmark:

```text
1,000 subscribers
5,000 subscribers
10,000 subscribers
```

---

## Day 14 — Metrics

Expose Prometheus metrics:

* Throughput
* Latency
* Active subscribers
* Queue depth

---

# Week 3 — Distributed Systems

## Day 15 — Redis Pub/Sub

Replace in-memory broadcasting.

Architecture:

```text
Gateway A
Gateway B
Gateway C
      │
      ▼
    Redis
```

---

## Day 16 — Multi-Gateway Architecture

```text
Load Balancer
 ├── Gateway 1
 ├── Gateway 2
 └── Gateway 3
```

---

## Day 17 — Sticky Sessions

Ensure clients reconnect to the correct gateway.

---

## Day 18 — Service Discovery

Implement:

* Dynamic registration
* Gateway discovery

---

## Day 19 — Distributed Topic Routing

Allow any gateway to serve:

```text
AAPL
GOOG
MSFT
```

---

## Day 20 — Chaos Testing

Simulate:

* Gateway crashes
* Redis outages
* Network failures

---

## Day 21 — Distributed Benchmark

Measure:

* Cluster throughput
* Cross-node latency

---

# Week 4 — Reliability

## Day 22 — Persistent Event Log

Store updates using:

* PostgreSQL
* BadgerDB

---

## Day 23 — Replay API

Example:

```json
{
  "symbol": "AAPL",
  "from": "10:00"
}
```

---

## Day 24 — Snapshot Service

New subscribers immediately receive:

```text
Latest known price
```

---

## Day 25 — Recovery After Restart

Restore:

* Topics
* Subscriptions
* State

---

## Day 26 — Dead Connection Detection

Heartbeat protocol:

```text
PING
PONG
```

---

## Day 27 — Automatic Reconnect

Capabilities:

* Client reconnect
* Subscription restoration

---

## Day 28 — Message Ordering

Guarantee:

```text
seq=1
seq=2
seq=3
```

No reordering.

---

# Week 5 — Production Engineering

## Day 29 — Structured Logging

Options:

* slog
* zap

---

## Day 30 — Distributed Tracing

Using OpenTelemetry.

Trace:

```text
Feed
 ↓
Publisher
 ↓
Gateway
 ↓
Client
```

---

## Day 31 — Grafana Dashboards

Visualize:

* Throughput
* Latency
* Failures

---

## Day 32 — Health Checks

Endpoints:

```text
/health
/readiness
/liveness
```

---

## Day 33 — Configuration System

Features:

* YAML configs
* Environment overrides

---

## Day 34 — Docker Compose

One-command deployment.

```bash
docker compose up
```

---

## Day 35 — Kubernetes Deployment

Deploy services on Kubernetes.

---

# Week 6 — Trading Infrastructure Features

## Day 36 — Market Sessions

States:

```text
PRE-MARKET
OPEN
CLOSED
```

---

## Day 37 — Symbol Metadata Service

Example:

```json
{
  "symbol": "AAPL",
  "exchange": "NASDAQ"
}
```

---

## Day 38 — Aggregation Engine

Generate:

* 1-second OHLC
* 1-minute OHLC

Candlestick data.

---

## Day 39 — Top Movers

Calculate:

* Top Gainers
* Top Losers

---

## Day 40 — Watchlists

User-specific subscriptions.

---

## Day 41 — Alert Engine

Example:

```text
AAPL > 220
```

Trigger notifications.

---

## Day 42 — Portfolio Stream

Real-time updates:

* Portfolio value
* Unrealized PnL
* Exposure

---

# Week 7 — Resume Gold

## Day 43 — Benchmark Suite

Target:

```text
100,000 updates/sec
10,000 subscribers
```

---

## Day 44 — Architecture Diagrams

Create professional diagrams:

* System architecture
* Scaling architecture
* Data flow

---

## Day 45 — Design Document

Include:

* Architecture
* Tradeoffs
* Scaling strategy
* Reliability strategy

---

## Day 46 — Failure Analysis

Document:

* Redis failure
* Gateway crash
* Slow consumer scenarios

---

## Day 47 — Performance Tuning

Use:

```bash
go tool pprof
```

Analyze:

* CPU usage
* Memory usage

---

## Day 48 — Optimization Report

Show measurable improvements:

* Latency reduction
* Throughput increase
* Memory reduction

---

## Day 49 — Resume Bullet Creation

Example:

> Built a distributed market data platform in Go supporting 100,000+ market updates/sec and 10,000 concurrent WebSocket subscribers with Redis-backed horizontal scaling, replayable event logs, Prometheus monitoring, OpenTelemetry tracing, and automatic failure recovery.

```

# Why This Project Stands Out

This project demonstrates:

- Distributed Systems
- Concurrency
- Networking
- Low-Latency Design
- Reliability Engineering
- Scalability
- Observability
- Trading Infrastructure Concepts

It provides a realistic backend engineering story that closely resembles systems used by market data teams, electronic trading platforms, hedge funds, and investment banks.
```
