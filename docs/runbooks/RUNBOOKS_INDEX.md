# Operational Runbooks

**Purpose:** To define standard operating procedures (SOPs) for routine platform maintenance.
**Intended Audience:** SREs, NOC (Network Operations Center).
**Maintenance Strategy:** Must be updated whenever new Administrative API commands are added.

---

## 1. Enable Maintenance Mode
When a severe downstream issue requires halting all live market data broadcasts temporarily, engage Maintenance Mode.

```bash
# Execute via the Admin API (Requires Operator Role)
curl -X POST http://localhost:8081/operations/maintenance/enable \
     -H "Authorization: Bearer operator-token"
```
**Verification:** Ensure the `rtmds_maintenance_mode_active` metric in Grafana reads `1`.

## 2. Pause Publisher Ingestion
If the upstream Feed Generator goes rogue and begins spamming corrupted ticks, isolate the platform by pausing the Publisher. This blocks ingestion but keeps existing Gateway WebSocket connections alive.

```bash
curl -X POST http://localhost:8081/operations/publisher/pause \
     -H "Authorization: Bearer operator-token"
```

## 3. Dynamic Log Level Debugging
To diagnose a complex race condition in production without restarting the pod and losing state, elevate the `zap` log level to `DEBUG`.

```bash
curl -X POST http://localhost:8081/operations/configuration/log-level \
     -H "Authorization: Bearer operator-token" \
     -H "Content-Type: application/json" \
     -d '{"level": "debug"}'
```
**Warning:** This will cause a massive spike in disk I/O and Loki indexing costs. Revert to `info` immediately after capturing the necessary forensics.
