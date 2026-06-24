# Multi-Gateway Architecture Review

This document provides a technical review of the horizontally scalable WebSocket Gateway architecture (`MULTI_GATEWAY_ARCHITECTURE.md`), focusing on scalability, operational concerns, failure scenarios, and gateway coordination.

## 1. Scalability

> [!TIP]
> **Load Balancing Strategy**
> The choice of **Least Connections** for load balancing is correct for long-lived WebSockets. Round-robin often leads to severe imbalances because WebSocket lifespans vary wildly. 
> However, be aware of the "Thundering Herd" problem: if one gateway crashes, 5,000 clients will instantly reconnect, hitting the Load Balancer simultaneously. The LB might route them all to the gateway that currently has the lowest connections, immediately overwhelming it. You may want to implement connection rate limiting at the LB or gateway level to smooth out reconnects.

> [!WARNING]
> **Data Processing Bottleneck**
> As noted in the Redis review, if the gateways are subscribed to a single global Redis channel, adding more gateways horizontally scales *client connections*, but it does *not* scale data processing. Every new gateway must deserialize every market data update. If market throughput reaches 50,000 messages/sec, all 10 gateways will burn CPU processing those 50,000 messages, regardless of how many clients are connected.

## 2. Operational Concerns

> [!CAUTION]
> **Blind Load Balancing (Health Checks)**
> The architecture relies on the Load Balancer to distribute traffic. However, a gateway's health isn't just "is the TCP port open?". If a gateway loses its connection to Redis, it will continue to accept WebSocket clients but won't send them any data. 
> **Recommendation:** Implement a `/health` HTTP endpoint on the gateways that returns `503 Service Unavailable` if the Redis connection is dropped. Configure the LB to use this endpoint so it stops routing traffic to data-starved gateways.

**Rolling Deployments & Connection Draining**
Since connections are long-lived, deploying a new version of the Gateway means terminating thousands of connections. The architecture should specify a **graceful shutdown** sequence:
1. Gateway fails the LB health check (stops accepting new connections).
2. Gateway slowly disconnects existing clients over a randomized window (e.g., 1-2 minutes) to prevent a massive reconnect surge.
3. Gateway exits once empty.

## 3. Failure Scenarios

> [!IMPORTANT]
> **Stale Data Risk on Redis Outage**
> The design states: *Redis unavailable -> Connections remain active. Clients stay connected. Data distribution pauses.*
> For a **financial market data system**, this is extremely dangerous. If data distribution pauses silently, clients will look at a frozen order book and might execute trades based on stale prices. 
> **Recommendation:** If a gateway detects a Redis outage longer than a few seconds, it MUST either broadcast a "System Degraded/Stale Data" control message to all connected clients or forcibly disconnect them so their UI shows a disconnected state.

**Slow Consumers**
The design handles slow consumers well by mentioning "Per-Client Buffers, Bounded Queues, Disconnect Policy". This is the correct approach. If a client's buffer fills up, the gateway must drop the connection rather than blocking the topic manager.

## 4. Gateway Coordination

> [!NOTE]
> **Lack of Global Rate Limiting**
> The architecture explicitly embraces "Stateless Gateways" with no cross-gateway coordination. This is excellent for horizontal scaling but introduces a gap: **Global Limits**. 
> If a malicious or buggy client tries to open 10,000 connections, a single gateway might limit them to 100. However, the client can bypass this by connecting to all 10 gateways behind the LB, achieving 1,000 total connections. If strict per-user connection limits are required, you will need an external state store (like Redis) to track active sessions across the cluster.

---

## Summary Recommendations

1. **Active Health Checks**: Gateways must fail LB health checks if their upstream data source (Redis) is disconnected.
2. **Stale Data Protection**: Gateways must notify clients or drop connections if market data stops flowing.
3. **Thundering Herd Mitigation**: Implement connection rate limiting and randomized reconnect jitter in the client libraries.
4. **Graceful Draining**: Add logic to slowly drain WebSocket connections during deployments rather than severing them all at once.
