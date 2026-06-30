# Profiling Report: v1.0.0 (Verification)

**Date:** 2026-06-29
**Workload:** 100 Clients, 5 Symbols, 30s Benchmark
**Status:** Verification Successful

## Summary

The profiling workflow verification plan was executed end-to-end to validate that profiles could be successfully captured under realistic load conditions. 

During the verification, the application was run with the following environmental overrides:
- `RTMDS_PROFILING_ENABLED=true`
- `RTMDS_PROFILING_MUTEX_FRACTION=10`
- `RTMDS_PROFILING_BLOCK_RATE=10`

Simultaneously, a 30-second benchmark script simulated 100 WebSocket clients to generate sufficient load to saturate runtime systems. The `collect_profiles.sh` script successfully completed execution against the authenticated endpoints.

## Findings

The script successfully downloaded and saved all 8 target diagnostic trace types:
- `cpu.pprof`
- `trace.pprof`
- `heap.pprof`
- `allocs.pprof`
- `goroutine.pprof`
- `mutex.pprof`
- `block.pprof`
- `threadcreate.pprof`

The file outputs contained actual binary pprof samples, proving that the fix made to `internal/platform/admin/api/router.go` successfully bypassed the 404 indexing issue found with the default `net/http/pprof` prefix handling.

Additionally, the script was updated to ensure that the collection correctly prefixes endpoints with `/admin/` (i.e. `/admin/diagnostics/debug/pprof/heap`), solving a URL mismatch that previously caused 404 unauthorized paths.

## Pass/Fail Validation

| Criterion | Requirement | Status |
| :--- | :--- | :--- |
| **Authentication** | `collect_profiles.sh` fails with HTTP 401/403 if an invalid token is provided. | ✅ |
| **Data Integrity** | `cpu.pprof` and `trace.out` contain data spanning exactly the requested duration (`?seconds=X`). | ✅ |
| **Completeness** | All 8 expected files are generated in the output directory and are strictly > 0 bytes. | ✅ |
| **Contention Data** | `mutex.pprof` and `block.pprof` contain readable symbol stacks (not empty). | ✅ |
| **Tooling Compatibility** | `go tool pprof` and `go tool trace` can successfully parse the output files without corrupted data errors. | ✅ |

## Conclusion
The profiling framework and automated collection tool are fully operational and ready for production use.
