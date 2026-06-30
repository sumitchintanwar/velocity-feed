# RTMDS Core Sequence Diagrams

This document contains the critical sequence diagrams illustrating the flow of data through the Real-Time Market Data System.

## 1. Market Data Publication & Fan-Out

This diagram shows how a tick originates at an upstream exchange, is normalized by the Publisher, routed across the Redis Pub/Sub cluster, and ultimately broadcast to connected WebSocket clients.

```mermaid
%%{init: {'theme': 'dark'}}%%
sequenceDiagram
    autonumber
    participant Exchange as Upstream Exchange
    participant Publisher as Feed Publisher
    participant Redis as Redis Pub/Sub
    participant Gateway as WebSocket Gateway
    participant Client as WS Client

    Exchange->>Publisher: Raw Tick Data (TCP/UDP)
    Publisher->>Publisher: Normalize to JSON / Binary
    Publisher->>Redis: PUBLISH rtmds.ticker.AAPL
    Redis-->>Gateway: Broadcast to Subscribed Node
    Gateway->>Gateway: Read into Ring Buffer
    Gateway->>Client: Send WebSocket Frame
```

## 2. Client Subscription Workflow

This diagram illustrates the process of a client establishing a connection, authenticating, and subscribing to a topic.

```mermaid
sequenceDiagram
    autonumber
    participant Client as WS Client
    participant Ingress as K8s Ingress (TLS)
    participant Auth as Auth Middleware
    participant Gateway as WebSocket Gateway
    participant TopicMgr as Topic Manager
    participant Redis as Redis Pub/Sub

    Client->>Ingress: GET /ws (Upgrade)
    Ingress->>Auth: Validate JWT
    Auth-->>Ingress: OK (Entitlements)
    Ingress->>Gateway: Forward Connection
    Gateway-->>Client: 101 Switching Protocols
    
    Client->>Gateway: {"action": "subscribe", "symbols": ["AAPL"]}
    Gateway->>TopicMgr: Check Entitlements
    TopicMgr-->>Gateway: Authorized
    Gateway->>TopicMgr: Register Client to Topic
    TopicMgr->>Redis: SUBSCRIBE rtmds.ticker.AAPL (if first client on node)
    Redis-->>TopicMgr: Subscribed
    Gateway-->>Client: {"status": "success", "message": "Subscribed to AAPL"}
```

## 3. Time-Travel Replay Workflow

This diagram shows how the system handles historical data requests via the Replay API.

```mermaid
sequenceDiagram
    autonumber
    participant Client as REST Client
    participant ReplayAPI as Replay Service
    participant Postgres as PostgreSQL (TimescaleDB)
    participant Redis as Redis Storage (Metadata)

    Client->>ReplayAPI: GET /replay?symbol=AAPL&start=T1&end=T2
    ReplayAPI->>Redis: Check Cache for requested time window
    alt Cache Miss
        ReplayAPI->>Postgres: SELECT * FROM ticks WHERE symbol='AAPL' AND time BETWEEN T1 AND T2
        Postgres-->>ReplayAPI: Return Raw Rows
        ReplayAPI->>ReplayAPI: Serialize to JSON Stream
    else Cache Hit
        Redis-->>ReplayAPI: Return Cached Stream
    end
    ReplayAPI-->>Client: 200 OK (Chunked Transfer-Encoding)
```
