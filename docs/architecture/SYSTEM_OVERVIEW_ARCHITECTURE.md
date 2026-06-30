# System Overview: Real-Time Market Data System (RTMDS)

**Visual Diagram:** [System Architecture Diagram](../diagrams/architecture/SYSTEM_ARCHITECTURE_DIAGRAM.md)

**Purpose:** To define the high-level business goals, system capabilities, and overarching purpose of the Real-Time Market Data System.
**Intended Audience:** Engineering Leadership, Architects, SREs, and onboarding engineers.
**Maintenance Strategy:** Update this document whenever a major architectural capability is added or removed (e.g., introducing a new asset class or data consumption mechanism).

---

## 1. Business Purpose

The Real-Time Market Data System (RTMDS) is a high-throughput, low-latency distributed platform designed to ingest, process, and broadcast financial market data (such as trade ticks and order book updates) to downstream consumers. 

The primary business objective is to provide a highly reliable data dissemination backbone that guarantees:
- **Sub-millisecond routing latency** for live data.
- **Strict chronological ordering** of market events.
- **Fault-tolerant recovery** enabling clients to replay missed messages during network disruptions.

## 2. Core Capabilities

### 2.1 Live Data Dissemination
The system ingests raw market data from upstream exchanges (simulated via the `Feed Generator`) and broadcasts it to hundreds or thousands of connected clients in real time using WebSocket connections.

### 2.2 Historical Replay
Consumers who experience localized network drops can request historical packets (by timestamp or sequence ID). The system seamlessly queries its immutable event store (PostgreSQL) and replays the exact sequence of missed ticks, ensuring clients never suffer silent data loss.

### 2.3 State Snapshots
Instead of forcing new clients to consume the entire history of an order book from the start of the trading day, the platform periodically aggregates the current state of a financial instrument into a "Snapshot". Clients fetch the latest snapshot and then subscribe to live deltas, drastically reducing initialization time.

### 2.4 Administrative Control Plane
An integrated, role-based Operational Administration API allows Site Reliability Engineers (SREs) to safely pause data feeds, drain traffic, or dynamically alter logging levels across the cluster without interrupting active trading workflows.

## 3. High-Level Characteristics

| Characteristic | Design Implementation |
|----------------|-----------------------|
| **Latency** | In-memory message routing via Redis Pub/Sub. |
| **Durability** | Asynchronous flushing to PostgreSQL event store. |
| **Scalability** | Horizontally scalable Gateway nodes (stateless). |
| **Observability** | 100% trace coverage (OpenTelemetry/Jaeger) and PromQL metrics. |
