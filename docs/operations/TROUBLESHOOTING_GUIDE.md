# Troubleshooting Guide

**Purpose:** A symptom-driven diagnostic guide to resolving production incidents.
**Intended Audience:** On-call SREs.
**Maintenance Strategy:** Update this document during post-mortems if a new failure mode is discovered.

---

## Symptom: Clients reporting missing ticks (Sequence Gaps)

### Diagnostic Steps
1. **Check the Publisher:** Is the `Publisher` service actually receiving ticks from the exchange? Check the `rtmds_events_published_total` rate in Grafana.
2. **Check Redis:** Are there network drops between the Publisher and Redis? Check the `rtmds_redis_connection_errors_total` metric.
3. **Trace the packet:** Open Jaeger and search for the specific instrument (e.g., `tag: instrument=AAPL`). Did the Publisher emit the span, but the Gateway did not receive it? If so, Redis dropped the message.
   - *LogQL Debugging:* If you have a specific `trace_id` from a client complaint, search Loki to track exactly which microservices processed it:
     ```logql
     {app=~"rtmds-.*"} |= "trace_id=8a3c9f2b"
     ```

### Resolution
- Instruct the impacted clients to query the `Replay API` for the specific missing sequence bounds.
- If Redis is evicting keys due to memory pressure, vertically scale the Redis cluster or adjust the eviction policy (`maxmemory-policy`).

---

## Symptom: High Gateway CPU Usage (Starvation)

### Diagnostic Steps
1. **Check Connections:** Has there been a sudden surge in WebSocket clients? Check `rtmds_websocket_connections_active`.
2. **Profile the CPU:** If the CPU is pegged at 100% but connections are normal, extract a pprof trace.
   ```bash
   curl -H "Authorization: Bearer admin-token" http://localhost:8081/diagnostics/debug/pprof/profile > cpu.prof
   go tool pprof cpu.prof
   ```

### Resolution
- The Kubernetes HPA should automatically provision more Gateway pods. If it fails, manually scale the deployment:
  ```bash
  kubectl scale deployment rtmds-gateway --replicas=10 -n rtmds-prod
  ```

---

## Symptom: Gateway Panics / OOMKills

### Diagnostic Steps
1. **Identify Slow Consumers:** If a client reads from the WebSocket too slowly, the Gateway's internal channel buffer will fill up, causing memory exhaustion.
2. **Review Logs:** Search Loki for `level=WARN msg="slow consumer detected"`.

### Resolution
- Ensure the `WriteTimeout` and buffer sizes are correctly configured in `gateway.yaml`. The system should aggressively disconnect slow clients rather than sacrificing the server's memory.
