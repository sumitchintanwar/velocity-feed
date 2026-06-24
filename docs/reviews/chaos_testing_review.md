# Chaos Testing Results Analysis

**Reviewer perspective:** Principal Distributed Systems Engineer  
**Target:** Recent Chaos Test Run (`docs/reviews/chaos_results/chaos_results_20260624_121257.md`)

I have reviewed the actual outputs from the latest chaos test execution. Despite the idealized success described in the `CHAOS_TESTING_RESULTS.md` summary document, the raw logs in `chaos_results_20260624_121257.md` reveal severe architectural flaws under failure conditions.

---

## 1. Resilience Gaps

### Nginx Upstream Depletion (Cascading Failure)
In **Scenario 1**, killing a single active gateway (Gateway 1) dropped the active gateway count to 0, completely breaking traffic routing (`Traffic routing: ❌ Broken`). 

**The Gap:** Nginx is failing to route traffic to the surviving nodes. This typically happens when Nginx's `max_fails` and `fail_timeout` parameters are misconfigured. If a node goes down, Nginx might aggressively mark the entire upstream pool as dead, or sticky sessions (implied by `docker-compose.sticky.yml`) are strictly enforcing routing to the dead node without a fallback mechanism.

### Single Node Restarts Break the Cluster
In **Scenario 4 (Rolling Restart)**, restarting a *single* gateway caused the entire system health to report `⚠️ 000000`. 

**The Gap:** The cluster behaves as a monolithic failure domain. A rolling restart—designed specifically to allow 2/3 of the cluster to remain online—took down traffic routing across the board.

---

## 2. Recovery Weaknesses

### Liveness Probe Anti-Pattern (The Redis Dependency)
In **Scenario 3**, stopping Redis caused the Gateway's `/health` endpoint to fail (`000000`). Looking at `internal/transport/router.go`, the `/health` endpoint explicitly checks downstream dependencies:

```go
// handleHealth returns a 200 OK liveness probe, or 503 if any critical
// component (e.g. Redis) is unhealthy.
```

**The Weakness:** This is a severe anti-pattern. `/health` is typically used as a **Liveness Probe**. If a Liveness probe fails, orchestrators (like Kubernetes or Docker) will **kill and restart the container**. 
Because `/health` checks Redis, a brief Redis outage will cause the orchestrator to simultaneously `SIGKILL` all 3 Gateways. The system will destroy itself trying to "recover" from a downstream outage. 

Downstream checks (Redis, DBs) must *only* be performed in the `/ready` (Readiness) probe so that the load balancer stops sending new traffic, but the container remains alive to preserve existing WebSocket connections.

---

## 3. Operational Risks

### Zero-Downtime Deployments Are Impossible
Because a rolling restart (Scenario 4) causes cluster-wide health check failures and breaks routing, you cannot deploy new versions of the Gateway during market hours. Any deployment will result in a hard outage for all connected clients.

### "Fat Client" Reconnection Storms
When Redis goes down, data flow pauses. If the Liveness probe kills the gateways (as described above), all 10,000 WebSocket clients will be violently disconnected. When the gateways restart, 10,000 clients will immediately attempt to reconnect and rebuild their subscriptions simultaneously. Given our previous load testing analysis (where connection establishment caps at ~25 conns/sec), a Redis blip will result in a **4+ minute catastrophic recovery window** while the system thrashes under the reconnection storm.

---

## Recommendations for Remediation

1. **Decouple Liveness and Readiness:** 
   Modify `internal/transport/router.go`. The `/health` endpoint must always return 200 OK as long as the HTTP server is running. Move the Redis health check to `/ready`.
2. **Fix Nginx Upstream Configuration:** 
   Review the `nginx.conf` to ensure `max_fails` is isolated per backend, and `proxy_next_upstream` is configured to retry on `error timeout http_502 http_503 http_504`.
3. **Sticky Session Fallback:** 
   If `ip_hash` or sticky cookies are used, ensure Nginx is configured to silently failover to the next healthy node when the stuck node dies, rather than dropping the connection.
