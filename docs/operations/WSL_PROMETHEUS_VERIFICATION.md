# Verifying Prometheus Metrics from WSL

This document captures the process and pitfalls encountered when running the
server and verifying `curl localhost:9090/metrics` from a WSL 2 environment.

---

## Problem Summary

WSL 2 does **not** have Go installed natively. The project ships prebuilt
Windows binaries (`rtmds-server.exe`, `rtmds-server-linux`) that run via WSL's
Windows interop. Because these are Windows PE executables, they bind to the
**Windows network stack**, which is invisible to WSL's Linux networking tools
(`ss`, `netstat`, `curl`).

---

## Key Findings

### 1. `rtmds-server-linux` is a Windows binary

Despite its name, this file is a Windows PE executable:

```
$ file rtmds-server-linux
PE32+ executable (console) x86-64, for MS Windows, 16 sections
```

It runs through WSL interop but listens on Windows ports, not WSL ports.

### 2. WSL `ss` / `netstat` cannot see Windows-bound ports

When the server starts via a Windows binary and logs `addr: 0.0.0.0:8080`,
running `ss -tlnp` from WSL shows **no port 8080 listener**. The port exists
only on the Windows side.

### 3. WSL `curl localhost` cannot reach Windows-bound ports

`curl http://localhost:9090/metrics` from WSL fails with "Connection refused"
even when the server is running on Windows port 9090. This is because WSL 2
has its own network namespace and `localhost` resolves to WSL's loopback, not
Windows'.

### 4. Cross-compilation via Windows Go did not work cleanly

Running `GOOS=linux GOARCH=amd64 go build` from the Windows Go binary either
produced no output or produced binaries that still behaved as Windows
executables. Cross-compilation from WSL using the Windows Go toolchain is
unreliable.

---

## Working Solution

Since the server runs on the Windows network stack, the only reliable way to
verify the `/metrics` endpoint from WSL is to **curl from the Windows side**
using `powershell.exe`.

### Step 1 — Confirm the server is running on Windows

```bash
# Check which Windows process owns the port
powershell.exe -NoProfile -Command \
  "Get-NetTCPConnection -LocalPort 9090 -ErrorAction SilentlyContinue |
   Select-Object OwningProcess,LocalPort,State"
```

### Step 2 — Curl `/metrics` from Windows

```bash
powershell.exe -NoProfile -Command \
  "(Invoke-WebRequest -Uri 'http://localhost:9090/metrics' \
    -TimeoutSec 5 -UseBasicParsing).Content"
```

### Step 3 — Filter for application metrics only

```bash
powershell.exe -NoProfile -Command \
  "(Invoke-WebRequest -Uri 'http://localhost:9090/metrics' \
    -TimeoutSec 5 -UseBasicParsing).Content" 2>&1 |
  grep -E "^(rtmds_|# HELP rtmds|# TYPE rtmds)"
```

---

## What the Verified Output Looks Like

All three metric types plus the `/metrics` endpoint were confirmed working:

### Counters

```
rtmds_feed_messages_received_total{provider="simulator",symbol="AAPL"} 700
rtmds_feed_messages_received_total{provider="simulator",symbol="AMZN"} 700
rtmds_feed_messages_received_total{provider="simulator",symbol="GOOG"} 700
rtmds_feed_messages_received_total{provider="simulator",symbol="MSFT"} 700
rtmds_feed_messages_received_total{provider="simulator",symbol="TSLA"} 700
rtmds_http_requests_total{method="GET",route="/metrics",status="200"} 1
rtmds_websocket_connections_opened_total 0
rtmds_websocket_messages_written_total 0
rtmds_distribution_broadcasts_total 0
rtmds_topic_publish_operations_total 0
```

### Gauges

```
rtmds_websocket_connections_active 0
rtmds_websocket_active_subscriptions 0
rtmds_websocket_slow_consumers 0
rtmds_distribution_subscribers_active 0
```

### Histograms

```
rtmds_http_request_duration_seconds_bucket{method="GET",route="/metrics",le="0.005"} 1
rtmds_http_request_duration_seconds_count{method="GET",route="/metrics"} 1
rtmds_websocket_delivery_latency_seconds_bucket{le="0.0001"} 0
rtmds_topic_publish_latency_seconds_bucket{le="+Inf"} 0
```

### `/metrics` endpoint

- Served at `/metrics` on port 9090 (default).
- Powered by `promhttp.HandlerFor(gatherer)` with a custom `prometheus.Registry`.
- Enabled by default (`metrics.enabled: true` in config).
- Includes Go runtime collectors (`go_gc_*`, `go_memstats_*`, `process_*`).

---

## Quick Reference: Server Ports

| Context | Default Port | Config Override |
|---------|-------------|-----------------|
| Go code default | 9090 | `RTMDS_SERVER_PORT` env var |
| Docker Compose | 8080 | `RTMDS_SERVER_PORT: "8080"` |
| Prebuilt Windows binary | 8080 | Hardcoded in that build |

---

## Lessons Learned

1. **Never trust the filename** — always run `file` on prebuilt binaries.
2. **WSL interop does not bridge network namespaces** — Windows processes bind
   on Windows sockets; WSL processes bind on WSL sockets. They are isolated.
3. **Use `powershell.exe` from WSL** to reach Windows-hosted HTTP servers.
4. **If `ss` shows no listener but the server says "ready"**, the port is on
   the other side of the WSL boundary.
