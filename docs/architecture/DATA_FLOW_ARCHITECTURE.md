# Data Flow Architecture

**Visual Diagram:** [Market Data Flow Diagram](../diagrams/architecture/MARKET_DATA_FLOW_DIAGRAM.md)

**Purpose:** To illustrate the step-by-step movement of a market data packet through the RTMDS platform.
**Intended Audience:** Backend Engineers, Integration Engineers.
**Maintenance Strategy:** Must be updated if the routing mechanism (Redis) or connection protocol (WebSocket) is fundamentally changed.

---

## 1. Live Market Data Dissemination Flow

The primary hot path of the system is designed to minimize latency. 

```mermaid
sequenceDiagram
    participant Exchange as Feed Generator
    participant Pub as Publisher
    participant Redis as Redis Pub/Sub
    participant GW as Gateway
    participant Client as Trading Client

    Exchange->>Pub: HTTP POST (Market Tick)
    activate Pub
    Note over Pub: Assign SeqID<br/>Inject TraceID
    Pub->>Redis: PUBLISH (Topic: AAPL)
    deactivate Pub
    
    Redis-->>GW: Message Received
    activate GW
    Note over GW: Extract TraceID<br/>Determine Subscribers
    GW->>Client: WebSocket Frame (Market Tick)
    deactivate GW
```

## 2. Recovery (Replay) Flow

When a client detects a sequence gap (e.g., received sequence `105`, but the last known sequence was `100`), they must request a historical replay.

```mermaid
sequenceDiagram
    participant Client as Trading Client
    participant GW as Gateway
    participant Replay as Replay API
    participant PG as PostgreSQL

    Note over Client: Detects Gap (100 -> 105)
    Client->>GW: REST GET /replay?start=101&end=104
    GW->>Replay: Forward Request
    Replay->>PG: SELECT * FROM events WHERE seq BETWEEN 101 AND 104
    PG-->>Replay: Result Set
    Replay-->>GW: JSON Response
    GW-->>Client: HTTP 200 OK (Array of missing ticks)
    Note over Client: Reconstructs State
```

## 3. Snapshot Initialization Flow

When a client boots up at the start of the day, it uses the Snapshot API to get the current state before subscribing to the live feed.

```mermaid
sequenceDiagram
    participant Client as Trading Client
    participant Snap as Snapshot API
    participant PG as PostgreSQL
    participant GW as Gateway

    Client->>Snap: REST GET /snapshot/AAPL
    Snap->>PG: Query latest aggregated state
    PG-->>Snap: Snapshot Data
    Snap-->>Client: HTTP 200 OK (Current Price: $150.00)
    Note over Client: Bootstrapped
    Client->>GW: WebSocket Upgrade
    Client->>GW: SUBSCRIBE AAPL
    GW-->>Client: Live ticks stream...
```
