# Persistent Replay Architecture Review
*By: Staff Engineer, Confluent*

After reviewing the persistent WAL and replay engine implementation, I have compiled a critique of the architecture, focusing on durability, scalability, latency, correctness, and maintainability. 

While the functional transition from replay-to-live is working in test environments, the architecture requires significant hardening before it can safely handle production-grade market data workloads (millions of msgs/sec).

Below is the ranked list of issues and suggested improvements.

---

## 1. CRITICAL: Data Loss & Durability Guarantees
### Issue: Missing `fsync` Policy
In `wal.Publisher`, the publisher calls `LogWriter.Append(msg)` but **never calls `LogWriter.Sync()`**. The data is buffered purely in userspace by `bufio.Writer(64KB)`. If the OS crashes, power is lost, or the process panics, up to 64KB of the most recent market events will be permanently lost. 
### Fix
Implement a configurable flush policy (similar to Kafka's `flush.ms` and `flush.messages`). Create a background goroutine in the WAL that calls `Sync()` on a ticker (e.g., every 50ms), or force a sync when the buffer reaches a certain threshold.

## 2. CRITICAL: Security & Correctness
### Issue: Missing Topic Authorization / Filtering during Replay
When a client sends `{ "action": "reconnect", "resumeFrom": 3 }`, the replay engine scans the WAL and blindly forwards all messages `Sequence >= 3`. The WAL is heavily multiplexed, containing events for all symbols (AAPL, MSFT, TSLA). The client will receive the entire firehose of the exchange regardless of what they actually subscribed to, violating data access and overwhelming the client socket.
### Fix
The `Replay()` API must accept a list of `topics []string` representing the client's current subscriptions. The `LogReader` must filter `msg.Topic` against this list before yielding messages back to the WebSocket buffer.

## 3. HIGH: Scalability & Architecture
### Issue: Unbounded Single-File WAL
The WAL writes indefinitely to a single file (`os.O_APPEND`). There is no log segmenting (`0000.log`, `0001.log`), no sparse indexing, and no retention/compaction policy. 
This will cause two fatal issues:
1. The disk will eventually fill up and crash the gateway.
2. The `LogReader` performs an `O(N)` sequential scan from the beginning of the file to find `resumeFrom: X`. As the file grows to terabytes, client reconnects will take hours and instantly time out.
### Fix
- **Log Segments**: Roll the file every `1GB` or `1 hour`.
- **Indexing**: Maintain a sparse index (`.index`) that maps Sequence Numbers to physical file bytes/offsets. `ResumeFrom` must perform a binary search `O(log N)` on the index, open the correct segment, and seek directly to the byte offset.

## 4. HIGH: Race Conditions & System Stability
### Issue: Unbounded Client Deduplication Buffer (OOM Risk)
During a historical replay, live messages are appended to a slice `reconnectBuffer` in the `writePump`. If a client reconnects and requests 1 million historical messages, the live stream will continue pushing events into this buffer at wire speed for several seconds (or minutes). A single slow client can cause the Gateway to allocate massive slices and crash via Out-Of-Memory (OOM).
### Fix
Enforce a hard capacity limit on `reconnectBuffer` (e.g., `max_items = 10,000`). If the buffer fills before replay completes, the client is too far behind to seamlessly merge. The server must abort the replay, drop the client with an error, and force them to reconnect.

## 5. HIGH: Latency Bottlenecks
### Issue: Global Mutex & Synchronous Hashing in the Hot Path
`LogWriter.Append()` locks a global `sync.Mutex` for all topics. Under high throughput, this mutex will severely contend. Worse, the CRC32 checksum calculation and binary encoding are performed *inside* the critical section, holding the lock while doing heavy CPU work.
### Fix
1. Pre-compute the CRC32 and binary header outside the mutex.
2. Adopt a Disruptor or Channel-based batching pattern (like Kafka's `RecordAccumulator`). The publisher should write to a lock-free ring buffer, and a single dedicated background thread should drain the buffer and write to the filesystem, completely removing the disk I/O and mutex from the network thread's critical path.

## 6. MEDIUM: Maintainability
### Issue: JSON Serialization Duplication
The `wal.Publisher` synchronously calls `json.Marshal(ev)` in the live hot path, which allocates memory and adds latency. Later, `client.go` has to manually manipulate strings (`{"type":"quote","payload":...}`) to trick the WebSocket envelope into matching the live stream format. This tightly couples the WAL storage format to the network envelope protocol.
### Fix
The WAL should store raw binary structs (using protobuf or gob), NOT JSON. JSON serialization should only happen at the edge (in `TopicManager` and `client.go`). When replaying, the server should deserialize the binary WAL struct into a domain `marketdata.Quote` and route it through the exact same WebSocket serializer as the live feed.
