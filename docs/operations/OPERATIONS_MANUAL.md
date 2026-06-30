# RTMDS Operations Manual

This Operations Manual provides a centralized reference for routine operational tasks related to the Real-Time Market Data System (RTMDS). For emergency incident response, please refer to the specific runbooks located in `docs/runbooks/`.

## 1. Service Startup and Shutdown

### Startup Sequence
RTMDS components must be started in the correct order to avoid connection backoff loops, although the service is designed to gracefully retry dependencies.

1. **Infrastructure Tier:** Start PostgreSQL (if using snapshot/replay) and Redis (Pub/Sub).
2. **Metrics & Tracing Tier:** Start Prometheus, Grafana, and Jaeger/OpenTelemetry Collectors.
3. **Application Tier:** Start the RTMDS Gateways and Publisher Services.

*Kubernetes Deployment:* Ensure `initContainers` are used to verify Redis/Postgres readiness before launching the RTMDS pods.

### Graceful Shutdown
RTMDS supports graceful shutdown by listening for `SIGTERM` and `SIGINT`.
- Upon receiving a termination signal, the Gateway stops accepting new WebSocket connections.
- It flushes in-flight messages from internal ring buffers to connected clients.
- It proactively sends a WebSocket Close frame (`1001 Going Away`) to all active clients, allowing them to reconnect to a different healthy Gateway instance.
- Finally, it closes Redis connections and exits.

## 2. Scaling the System

### Scaling Gateways (Horizontal)
The RTMDS Gateway is entirely stateless. You can scale it horizontally to handle increased client connection counts.
- **Command:** `kubectl scale deployment rtmds-gateway --replicas=5`
- **Impact:** Load balancers will immediately start routing new WebSocket upgrades to the new pods.

### Scaling Publishers (Horizontal)
Market Data Publishers (feed adapters) can be scaled, but require care to avoid publishing duplicate ticks to the Redis Pub/Sub channels.
- Ensure only one publisher per feed (e.g., one for NASDAQ, one for NYSE) is designated as the active writer, utilizing a Redis distributed lock for leader election if running multiple instances for high availability.

## 3. Routine Maintenance

### Configuration Reloads
Currently, RTMDS requires a service restart to pick up changes in `config.yaml` or environment variables. Zero-downtime updates are achieved via Kubernetes Rolling Updates.

### Redis Maintenance
If Redis requires maintenance (e.g., version upgrade, scaling):
1. RTMDS relies on Redis for cross-node topic routing.
2. If Redis goes down, Gateways will buffer a limited number of outbound messages, but cross-node publishing will fail.
3. RTMDS will continuously attempt to reconnect to Redis using exponential backoff.
4. **Action:** Schedule Redis maintenance during low-volume periods (e.g., weekends or outside market hours).

## 4. Monitoring & Observability

- **Metrics:** Core SLIs (P99 Latency, Throughput, Connected Clients, Dropped Messages) are exposed at the `/metrics` endpoint.
- **Dashboards:** Use the pre-built Grafana dashboards (`Platform Operations` and `Market Data Operations`) to monitor system health.
- **Alerts:** Refer to `docs/design/Production Alert Rules & Operational Runbooks Design.md` for a comprehensive list of configured Prometheus alerts.

## 5. Capacity Planning

When provisioning infrastructure for RTMDS, consider the following baselines:
- **Memory:** Each WebSocket connection consumes approximately 40-50KB of memory. 100,000 clients require ~5GB of RAM exclusively for connection state.
- **CPU:** JSON serialization and network I/O are the primary CPU drivers. A standard 4-core instance can typically handle 25,000 - 50,000 messages per second depending on payload size.
- **Network:** Ensure sufficient egress bandwidth. 10,000 clients receiving 1KB messages at 1,000 ticks/sec equates to 10 GB/sec of outbound bandwidth.
