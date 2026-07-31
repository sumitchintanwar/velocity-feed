# RTMDS Benchmark Metrics Extraction

This file contains the consolidated metrics extracted from the `go_benchmarks_results.txt` and `load_test_results.txt` files. The metrics cover system throughput, latency (p50/p95/p99), memory allocations, lock contention, and end-to-end performance.

## 1. End-to-End Latency Metrics (from Go Benchmarks)
These benchmarks measure the latency from message generation to delivery across the internal pub/sub engine, including serialization and routing overhead.

- **BenchmarkEndToEndLatency_1Sub-8**
  - **Ops/sec**: ~500,694 events/sec
  - **Latency Profile**: 
    - P50: 1,901 µs (1.9 ms)
    - P95: 2,816 µs (2.8 ms)
    - P99: 3,865 µs (3.8 ms)
  - **Memory per Op**: 139 Bytes, 1 alloc
- **BenchmarkEndToEndLatency_10Subs-8**
  - **Ops/sec**: ~2,014,724 events/sec
  - **Latency Profile**: 
    - P50: 4,992 µs (~5.0 ms)
    - P95: 6,794 µs (~6.8 ms)
    - P99: 11,954 µs (~11.9 ms)
  - **Memory per Op**: 12 Bytes, 0 allocs
- **BenchmarkEndToEndLatency_100Subs-8**
  - **Ops/sec**: ~3,326,681 events/sec
  - **Latency Profile**: 
    - P50: 30,617 µs (~30.6 ms)
    - P95: 42,234 µs (~42.2 ms)
    - P99: 53,872 µs (~53.8 ms)
  - **Memory per Op**: 1 Byte, 0 allocs
- **BenchmarkEndToEndLatency_1000Subs-8**
  - **Ops/sec**: ~1,745,573 events/sec
  - **Latency Profile**: 
    - P50: 570,929 µs (~570 ms)
    - P95: 835,118 µs (~835 ms)
    - P99: 863,888 µs (~863 ms)
  - **Memory per Op**: 0 Bytes, 0 allocs
  - *(Note: The extreme latency spike at 1000 subscriptions represents heavy contention on the routing/broadcast layers).*

## 2. Core Internal Throughput (from Go Benchmarks)
These benchmarks test the internal speed of the ring buffer, channels, and topic managers in isolation.

- **BenchmarkRing_Push-8**: 60.2 ns/op (Zero allocs)
- **BenchmarkRing_PushNoDrop-8**: 75.4 ns/op (Zero allocs)
- **BenchmarkRing_Pop-8**: 28.0 ns/op (Zero allocs)
- **BenchmarkChannel_DropOldest_Concurrent-8**: 439.7 ns/op
- **BenchmarkChannel_Disconnect_Concurrent-8**: 595.6 ns/op
- **BenchmarkManager_Get-8**: 31.8 ns/op (Topic routing lookup speed)
- **BenchmarkHash_FNV-8**: 5.2 ns/op (Hashing speed)
- **BenchmarkPipeline_100Symbols-8**: 809.8 ns/op, ~1.2M events/sec, 128 Bytes/op, 1 alloc/op.

## 3. Memory & Backpressure Scaling (from Go Benchmarks)
Measures how memory consumption changes as the number of clients and topics explodes.

- **BenchmarkMemoryScaling/subs_10-8**: 505.8 ns/op, 12 Bytes/op, 0 allocs/op
- **BenchmarkMemoryScaling/subs_100-8**: 445.0 ns/op, 1 Byte/op, 0 allocs/op
- **BenchmarkMemoryScaling/subs_5000-8**: 361.3 ns/op, 0 Bytes/op, 0 allocs/op
- **BenchmarkTopicExplosion_1000Topics-8**: 5014 ns/op, 400 Bytes/op, 4 allocs/op
- **BenchmarkTopicExplosion_10000Topics-8**: 1420 ns/op, 400 Bytes/op, 4 allocs/op
- **BenchmarkBackpressure_SlowConsumer-8**: ~1,991,524 fast events/sec vs 805 slow events/sec.

## 4. Sub/Unsub Churn and Concurrency
Evaluates system stability during high subscriber turnover.

- **BenchmarkSubscribe_100Symbols-8**: 25,567 ns/op, 5906 B/op, 106 allocs/op
- **BenchmarkUnsubscribe_100Subs-8**: 87,127 ns/op, 2 B/op, 0 allocs/op
- **BenchmarkMixed_80_20-8 (80% traffic, 20% churn)**: 3,588 ns/op, 1128 B/op, 2 allocs/op
- **BenchmarkGoroutineLeakDetection-8**: 5,483,483 ns/op, 5369 B/op, 10 allocs/op. (Note: Substantial overhead, indicating cleanup locks might be a bottleneck).

## 5. Client Load Network Benchmarks (from Load Tests)
The network client benchmarks (`run_all.ps1`) executed integration load generation across TCP/WebSockets over Docker. 

**Note on High Load Constraints**: 
The external network benchmarks hit a hard boundary on localhost environments due to the OS/Docker max open file limit (FD=1024 on Docker Desktop default). 
- **Small Load (<1000 clients)**: Succeeded, but logs were overwhelmed by subsequent failures.
- **Heavy Load (1000 - 10000 clients)**: The `client_load.exe` failed with thousands of `wsarecv: An existing connection was forcibly closed by the remote host.` and `unexpected EOF` errors because the Go server hit `ulimit` (Too Many Open Files) and rejected new TCP connections. 
- **Sample Output at Failure**: 
  - `Sent: 68756, Throughput: 4583.27 msg/s` (Publisher succeeded at ~4.5k msgs/sec locally before load clients crashed).
  - `Received: 0, Throughput: 0.00 msg/s` (Clients dropped out).

### Summary
The system shows incredibly strong **zero-allocation** pathways in the core data path (`BenchmarkRing_Push`, `BenchmarkEndToEndLatency_10Subs-8`), keeping latency in the low single-digit milliseconds for light loads. However, horizontal scaling hits severe lock contention points internally when moving past 1000 clients (latency shoots from 53ms -> 570ms), and externally hits hard OS constraints due to lack of standard `ulimit` kernel tuning for C10k scenarios.
