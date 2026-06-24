# Sticky Session Design Review

This document provides a technical review of the `STICKY_SESSION_DESIGN.md` architecture, focusing on WebSocket compatibility, scaling concerns, and failover implications.

## 1. WebSocket Compatibility

> [!NOTE]
> **Inherent Stickiness**
> The design correctly identifies that WebSockets are inherently "sticky" because they are long-lived TCP connections. Once the initial HTTP Upgrade request is routed by the Load Balancer, the connection is pinned to that gateway until it drops. 
> Therefore, "Sticky Sessions" (like IP Hash or Cookies) at the LB level *only* apply to the reconnect behavior, not the active traffic flow.

## 2. Scaling Concerns

> [!WARNING]
> **Uneven Load Distribution**
> Implementing strict sticky sessions (e.g., Consistent Hashing or IP Affinity) actively fights against horizontal scaling. Over time, as clients disconnect and reconnect, traffic can clump onto specific gateways. When you add new gateways to the cluster, existing "sticky" clients won't rebalance to the new nodes, leaving your new hardware underutilized.
> **The recommendation to drop sticky sessions in favor of "Least Connections" is absolutely correct.**

> [!TIP]
> **Managing "Externalized State"**
> The design recommends a "Distributed State Layer" to track client subscriptions so that they can reconnect to any gateway. However, querying an external Redis cluster for user subscriptions on *every* reconnect can create a massive database bottleneck during a thundering herd reconnect event.
> **Better Approach (Fat Client):** Design the frontend client libraries to remember their own active subscriptions. When the WebSocket reconnects to a random new gateway, the client simply re-sends a batch `SUBSCRIBE` payload. This makes the gateways truly 100% stateless and removes the need for a distributed state database entirely.

## 3. Failover Implications

> [!IMPORTANT]
> **Failover Speed**
> In a real-time market data system, recovery time is critical. If sticky sessions are enabled (e.g., via cookies), and Gateway 3 crashes, the LB might route the reconnect attempt back to Gateway 3 (which is dead) or hold the connection waiting for it to recover. 
> Without sticky sessions, the LB immediately routes the reconnect to a healthy node (e.g., Gateway 4), minimizing downtime to milliseconds.

**The "Cold Start" Penalty**
When a client fails over to a new gateway without sticky sessions, there is a minor "cold start" penalty: the new gateway might not yet be subscribed to the Redis Pub/Sub channels for that client's symbols (if using Topic-Based channels). The gateway will need to quickly subscribe to the upstream Redis channel before data starts flowing to the client. This latency is usually negligible but should be accounted for in latency-sensitive applications.

---

## Conclusion & Recommendations

The final recommendation in the design document—to **abandon sticky sessions in favor of stateless gateways and Least Connections load balancing**—is the industry-standard approach for massive WebSocket deployments.

**Action Items to Implement:**
1. **Client-Side State:** Ensure the WebSocket client (JS/Mobile) holds the "source of truth" for subscriptions and automatically re-sends them on reconnect.
2. **Remove Stickiness:** Explicitly disable sticky sessions/session affinity on your Load Balancer (e.g., ALB, HAProxy, NGINX).
3. **Optimize Upstream Subscriptions:** Ensure the Gateway's local Topic Manager can rapidly subscribe to upstream Redis channels when a reconnected client requests a symbol that the gateway isn't currently tracking.
