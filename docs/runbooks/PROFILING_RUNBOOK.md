# Production Profiling Runbook

This guide explains how to properly collect and analyze profiles from the Real-Time Market Data System (RTMDS) using the built-in authenticated admin API.

## 1. Enabling Runtime Profiling (Mutex & Block)

By default, to avoid performance overhead in production, Mutex and Block profiling are disabled. When you need to profile synchronization contention or goroutine blocking, you can enable them dynamically via the Admin API **without restarting the application**.

**Command to enable dynamically:**
```bash
curl -X POST -H "Authorization: Bearer $RTMDS_ADMIN_TOKEN" \
     -H "Content-Type: application/json" \
     -d '{"mutex_fraction": 10, "block_rate": 10}' \
     "http://localhost:9090/admin/operations/configuration/profiling"
```

> [!TIP]
> You can also enable these permanently at startup using environment variables (`RTMDS_PROFILING_ENABLED=true`, `RTMDS_PROFILING_MUTEX_FRACTION=10`, `RTMDS_PROFILING_BLOCK_RATE=10`), but dynamic activation is preferred during live incidents to prevent WebSocket disconnection storms caused by restarts.

## 2. Collecting Profiles

Profiling endpoints require the Administrator role. You must provide a valid JWT bearer token via the `RTMDS_ADMIN_TOKEN` environment variable.

We provide a script to collect all profiles safely (and sequentially where necessary to avoid observer skew):

```bash
cd scripts/
RTMDS_ADMIN_TOKEN="my-token" ./collect_profiles.sh localhost:9090 30
```

This script will output a directory containing:
- `cpu.pprof`
- `heap.pprof`
- `allocs.pprof`
- `goroutine.pprof`
- `mutex.pprof`
- `block.pprof`
- `threadcreate.pprof`
- `trace.pprof`

## 3. Manual Profile Collection via cURL

If you prefer to grab a single profile manually:

> [!CAUTION]
> Triggering `/pprof/heap` forces a Stop-The-World (STW) Garbage Collection cycle. In systems with large in-memory caches, this will cause a brief latency spike for connected WebSocket clients. Do not run heap dumps in tight loops.

**CPU Profile (30s):**
```bash
curl -H "Authorization: Bearer $RTMDS_ADMIN_TOKEN" -o cpu.pprof "http://localhost:9090/admin/diagnostics/debug/pprof/profile?seconds=30"
```

**Heap Profile:**
```bash
curl -H "Authorization: Bearer $RTMDS_ADMIN_TOKEN" -o heap.pprof "http://localhost:9090/admin/diagnostics/debug/pprof/heap"
```

**Execution Trace (10s):**
```bash
curl -H "Authorization: Bearer $RTMDS_ADMIN_TOKEN" -o trace.out "http://localhost:9090/admin/diagnostics/debug/pprof/trace?seconds=10"
```

## 4. Analyzing Profiles

Use the built-in Go pprof tool to analyze the output.

### Web Interface (Recommended)
```bash
go tool pprof -http=:8080 cpu.pprof
```

### Command Line
```bash
go tool pprof cpu.pprof
(pprof) top
(pprof) list myFunctionName
```

### Execution Trace
```bash
go tool trace trace.out
```

## 5. Identifying Bottlenecks

1. **High CPU:** Check `cpu.pprof` using the flamegraph view.
2. **High Memory / OOM:** Check `heap.pprof`. Switch between `inuse_space` and `inuse_objects`.
3. **High GC Activity:** Check `allocs.pprof`. Look for excessive short-lived object generation in hot paths.
4. **Poor Scalability (Idling CPUs under load):** Check `mutex.pprof` and `block.pprof` for contention.
5. **Leaking Goroutines:** Check `goroutine.pprof` for unexpected accumulation of idle workers.
