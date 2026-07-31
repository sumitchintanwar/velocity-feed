# Hashing Benchmark Results

We evaluated three hash algorithms (xxHash, CRC32, and FNV) to determine the best foundational hash for the Consistent Hashing Router.

## Benchmark Output
```
goos: windows
goarch: amd64
pkg: github.com/sumit/rtmds/internal/hashing
cpu: 11th Gen Intel(R) Core(TM) i5-1135G7 @ 2.40GHz
BenchmarkHash_xxHash-8   	100000000	        11.14 ns/op	       0 B/op	       0 allocs/op
BenchmarkHash_CRC32-8    	 14837308	        97.94 ns/op	      16 B/op	       1 allocs/op
BenchmarkHash_FNV-8      	208689954	         5.58 ns/op	       0 B/op	       0 allocs/op
```

## Analysis

1. **CRC32**: The slowest (`97.94 ns/op`). It also forces an allocation (`16 B/op`) because the standard library `crc32.ChecksumIEEE` expects a byte slice, requiring a cast from a string that forces a heap escape. Unsuitable for millions of operations per second.
2. **FNV (Fowler–Noll–Vo)**: The fastest for very short strings (`5.58 ns/op`). FNV has practically zero initialization overhead, making it incredibly fast for 4-10 character strings like `AAPL`. 
3. **xxHash**: Extremely fast (`11.14 ns/op`) and 100% zero-allocation (`Sum64String` uses `unsafe` pointers under the hood to read the string directly). 

## Decision: xxHash
While FNV beat xxHash by ~5 nanoseconds for a short string, **we have chosen `xxHash`**. 

> [!IMPORTANT]
> In Distributed Systems and Consistent Hashing, **Uniformity (Avalanche effect)** is infinitely more important than saving 5 nanoseconds. FNV is known to have poor dispersion for highly similar strings (e.g., `AAPL_OPT_2026` vs `AAPL_OPT_2027`). If the hash isn't perfectly uniform, Virtual Nodes will clump together on the ring, causing catastrophic "hot spots" on specific gateways. 
> 
> `xxHash` guarantees pristine distribution and is the industry standard used by Kafka, Cassandra, and Hadoop for partition hashing.
