# Service Discovery Implementation Review

This document provides a technical review of the Redis-based service discovery mechanism implemented in `internal/discovery/registry.go`, focusing on stale registrations, heartbeat failures, and scalability limits.

## 1. Scalability Limits

> [!WARNING]
> **O(N) `SCAN` Operation in a Shared Database**
> The `List(ctx)` method uses `r.client.Scan(..., "rtmds:gateways:*", ...)` to find all registered gateways. The `SCAN` command iterates over the *entire* Redis keyspace. If this Redis instance is shared with other services or high-churn market data (e.g., millions of active keys), the `SCAN` operation will take O(N) time relative to the *total keys in the database*, not the number of gateways.
> This will cause severe latency spikes when calling `List()`, potentially bringing down any routing logic that relies on it. 
> **Recommendation:** Instead of `SCAN`, maintain a Redis Set (e.g., `SADD rtmds:gateways:active {id}`) to track active gateways, or use a dedicated Redis logical database (e.g., DB 1) exclusively for service discovery so `SCAN` only traverses gateway keys.

**Large `MGet` Payloads**
After `SCAN`, the implementation does a single `MGet` for all accumulated keys. While Redis can handle an `MGet` of 100-200 keys easily, if the cluster scales to thousands of microservices/gateways, a massive `MGet` could block the single-threaded Redis event loop.

## 2. Heartbeat Failures

> [!TIP]
> **Resilient Re-Registration**
> The `heartbeatLoop` uses `r.client.Set(..., data, r.ttl)` rather than `Expire`. This is an excellent design choice. If a gateway loses connection to Redis for >30 seconds (causing its TTL to expire and drop it from the registry), the heartbeat loop will automatically recreate the key and re-register the gateway the moment the network partition heals.

**Silent Zombies**
The heartbeat loop runs in a background goroutine and only checks `ctx.Done()`. However, it doesn't verify if the actual WebSocket or market data pipelines are healthy. If the gateway's main application loop deadlocks, but the runtime keeps the `heartbeatLoop` goroutine alive, the gateway will remain registered as "healthy" even though it's effectively a zombie. 
**Recommendation:** The heartbeat loop should check a local `isHealthy()` function before sending the heartbeat to Redis. If local health fails, it should intentionally skip the `Set` command to allow the TTL to expire.

## 3. Stale Registrations

> [!NOTE]
> **Graceful Expiration Window**
> The 30s TTL with a 10s heartbeat is a standard and robust ratio. It allows for up to two missed heartbeats (due to network jitter or GC pauses) without prematurely deregistering a healthy node.

**Race Condition in `List()`**
The code handles the scan-then-fetch race condition correctly:
```go
vals, err := r.client.MGet(ctx, keys...).Result()
// ...
if val == nil { continue } // expired between scan and mget
```
This guarantees that the implementation won't panic or error out if a gateway's TTL expires in the milliseconds between the `SCAN` and `MGet` commands.

---

## Conclusion & Action Items

The Redis TTL-based registry is a solid "Phase 2" implementation as per the design doc, but the use of `SCAN` is a ticking time bomb if this Redis cluster stores other market data.

**Immediate Fixes Required:**
1. **Remove `SCAN`:** Switch to a Redis `SET` to track gateway IDs, or isolate the registry to a dedicated Redis database where `SCAN` is safe.
2. **Deep Health Checks:** Ensure `heartbeatLoop` assesses the actual health of the gateway's critical paths (e.g., active WebSocket listeners) before writing the heartbeat to Redis.
