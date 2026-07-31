# Routing Benchmark Results & Interpretation

The routing and rebalancing system was benchmarked on an `11th Gen Intel(R) Core(TM) i5-1135G7 @ 2.40GHz`. We measured latency, throughput, and memory allocations for the entire consistent-hashing pipeline.

## 1. Raw Performance Metrics

```text
BenchmarkHashThroughput-8          72811114       15.63 ns/op       0 B/op     0 allocs/op
BenchmarkPartitionLookup-8         69042098       18.94 ns/op       0 B/op     0 allocs/op
BenchmarkGatewayLookup-8           12133042      100.0  ns/op       0 B/op     0 allocs/op
BenchmarkFullLookupLatency-8        5204530      249.0  ns/op      24 B/op     1 allocs/op
```

### Interpretation: The Hot Path
The "Hot Path" is the time it takes for a Gateway to decide if it owns a Symbol, and if not, who to redirect the client to.
1. **Hash Throughput (15.6 ns)**: `xxhash` provides immense string-hashing throughput.
2. **Partition Lookup (18.9 ns)**: Mapping a string symbol to a `uint32` partition adds virtually zero overhead.
3. **Gateway Lookup (100.0 ns)**: Resolving the partition ID to a physical Gateway ID (via binary search over 10,000 vnodes) is highly optimized.
4. **Full Lookup Latency (249.0 ns)**: The entire `RedirectTarget` pipeline. It currently incurs **1 allocation (24 Bytes)** which occurs strictly when concatenating the `ws://{address}/ws` string for redirection.

**Conclusion**: The router can process **~4 million routing decisions per second, per core** on modest laptop hardware. At the network edge, 250ns is statistically invisible compared to network ping (which is typically measured in milliseconds).

---

## 2. Rebalancing Metrics

```text
BenchmarkNodeAdd-8                     1842    3411309 ns/op    11578 B/op     4 allocs/op
BenchmarkNodeRemove-8                168823       8180 ns/op        0 B/op     0 allocs/op
BenchmarkRebalanceTime-8              43066      28538 ns/op    28101 B/op    17 allocs/op
```

### Interpretation: Topology Sync
The daemon continuously synchronizes state with the Registry.
1. **Node Add (3.4 ms)**: Adding a node is the most "expensive" operation because the system must generate 100 new `vnode` hashes and `sort.Slice` the ring array. However, 3.4ms is far below our timeout thresholds, and this only happens when a physical server boots up.
2. **Node Remove (8.1 µs)**: Node removals (crashes) are extremely fast. The system simply filters the keys array and deletes the mappings without requiring a re-sort.
3. **Rebalance Time / Sync Topology (28.5 µs)**: A periodic, full comparison of the 100-node registry against the local topology state takes 28 microseconds. 

**Conclusion**: We can run the background `syncTopology` polling loop as fast as every `10ms` without putting any meaningful load on the CPU. The node discovery phase is effectively real-time.

---

## 3. Distribution Quality

To measure the effectiveness of the Consistent Hash Ring, we simulated **10,000,000 symbol lookups** across a cluster of **50 Gateways** (using 100 virtual nodes per gateway).

### Interpretation: Load Balancing
- **Mean symbols per gateway**: 200,000
- **Standard Deviation**: 22,835 (11.42%)

**Conclusion**: The hashing distribution guarantees that no single gateway becomes a hotspot simply due to bad math. With a standard deviation of `~11.4%`, the traffic is extremely well balanced. If a specific symbol (e.g. `AAPL`) generates significantly more traffic than others, the previously implemented `PartitionMetrics` and `PartitionManager.MovePartition()` manual overrides allow for granular correction without breaking the mathematical ring.
