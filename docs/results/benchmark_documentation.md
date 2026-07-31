# Protocol Serialization Benchmarks

This document outlines the performance characteristics of the Real-Time Market Data System (RTMDS) supported serialization formats. These benchmarks were measured on a local Intel Core i5-1135G7 using Go 1.22.

## Raw Throughput

Benchmarks measured the raw speed of serialization and deserialization in memory for 1 Million standard `Quote` events.

| Format | Operation | Latency (ns/op) | Throughput (msgs/sec) | Memory Traffic | Allocs/op |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **FlatBuffers** | **Deserialize** | **127 ns** | **7.9 Million** | **131 MB/s** | **2** |
| FlatBuffers | Serialize | 267 ns | 3.7 Million | 132 MB/s | 1 |
| Protobuf | Deserialize | 431 ns | 2.3 Million | 322 MB/s | 5 |
| Protobuf | Serialize | 454 ns | 2.2 Million | 79 MB/s | 2 |
| JSON | Serialize | 845 ns | 1.1 Million | 636 MB/s | 7 |
| JSON | Deserialize | 1,587 ns | 630k | 484 MB/s | 20 |

> [!TIP]
> **Performance Winner:** FlatBuffers leads in raw speed by a massive margin. Its zero-copy deserialization is **12.5x faster** than JSON and **3.4x faster** than Protobuf.

## Wire Size & Bandwidth

Message payload sizes heavily influence bandwidth usage and network saturation when fanning out to 10,000+ WebSocket clients.

| Format | Average `Quote` Size | Reduction vs JSON |
| :--- | :--- | :--- |
| **Protobuf** | **47 Bytes** | **74% smaller** |
| FlatBuffers | 112 Bytes | 40% smaller |
| JSON | 187 Bytes | Baseline |

> [!TIP]
> **Bandwidth Winner:** Protobuf's variable-length integer encoding (varint) strips zero-value fields entirely, making it incredibly compact for network transit.

## Recommendations for Clients

Based on these benchmarks, clients should negotiate the following formats based on their environment constraints:

1. **High-Frequency Trading (HFT) / C++ Bots**: Request `v1.flatbuffers.rtmds`. You will achieve sub-microsecond latency parsing events.
2. **Mobile Clients / Cellular Connections**: Request `v1.protobuf.rtmds`. It uses 74% less bandwidth than JSON, preserving mobile data and battery life.
3. **Web Dashboards / Browser UIs**: Request `v1.json.rtmds`. While slower in Go, the browser's native `JSON.parse` is highly optimized, and the RTMDS gateway pre-encodes JSON once to eliminate fan-out penalties.
