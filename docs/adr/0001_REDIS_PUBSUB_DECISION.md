# ADR 0001: Selection of Redis Pub/Sub for Market Data Routing

**Date:** 2026-06-28
**Status:** Approved
**Purpose:** To document the decision to use Redis Pub/Sub as the primary messaging backbone instead of Kafka or RabbitMQ.
**Intended Audience:** Architects, Platform Engineers.
**Maintenance Strategy:** Immutable historical record. Do not modify unless the architecture actively migrates away from Redis.

## Context
The Real-Time Market Data System (RTMDS) must broadcast hundreds of thousands of ticks per second to stateless WebSocket gateways. The messaging layer must enforce sub-millisecond latency. 

## Options Considered
1. **Apache Kafka:** Excellent durability and replay capabilities, but introduces higher baseline latency (due to disk flushing) and complex consumer group management.
2. **RabbitMQ:** Good routing flexibility via exchanges, but historically struggles with massive fan-out latency compared to in-memory systems.
3. **Redis Pub/Sub:** Entirely in-memory, providing microsecond-level publish/subscribe latency. However, it lacks durability (fire-and-forget).

## Decision
We decided to adopt **Redis Pub/Sub** for the live dissemination hot-path. 

## Consequences
- **Positive:** We achieve our sub-millisecond latency SLA for live market data.
- **Negative:** If a Gateway disconnects, the messages are lost instantly from Redis.
- **Mitigation:** We explicitly designed a parallel *Persistence Tier* using PostgreSQL to satisfy the durability requirements (see ADR 0002). This CQRS-style separation of concerns ensures the live feed remains infinitely fast, while historical replays are handled out-of-band.
