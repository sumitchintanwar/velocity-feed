# Load Test Results Analysis

**Reviewer perspective:** Principal Distributed Systems Engineer  
**Target:** Recent Load Test Runs (`docs/results/LOAD_TEST_2026-06-23_17-*.md`)

I have reviewed the load testing results for the 1,000, 5,000, and 10,000 client scenarios. The data reveals **four catastrophic systemic failures** that validate the architectural concerns we identified in previous code reviews. 

The system currently fails to meet real-time production requirements at any scale.

---

## 1. Connection Establishment Serialization

**The Data:**
- 1,000 clients: 38.8 seconds to connect (**~25 conns/sec**)
- 5,000 clients: 2 minutes 37 seconds to connect (**~31 conns/sec**)
- 10,000 clients: 4 minutes 1.5 seconds to connect (**~24 conns/sec**)

**The Bottleneck:**
There is a hard, serialized bottleneck limiting connection establishment to ~25-30 connections per second. This scales linearly, indicating that client upgrades are acquiring a global lock, or the HTTP server is configured with an artificially low `MaxConnsPerIP`/`Accept` limit. 

At a typical market open, a system might need to handle 5,000 concurrent reconnects in under 5 seconds (1,000 conn/sec). The current architecture would take almost 3 minutes to let everyone in.

## 2. Catastrophic Latency Accumulation

**The Data:**
- 1,000 clients: P99 Latency = **45 seconds**
- 5,000 clients: P99 Latency = **2 minutes 10 seconds**
- 10,000 clients: P99 Latency = **4 minutes 3 seconds**

*(Note: The test duration was only 10s, but latency is measured in minutes!)*

**The Bottleneck (Queue Failure):**
This proves that the "Backpressure/Per-Subscriber Buffering" design is fundamentally broken. A real-time market data system should *never* deliver a price update that is 4 minutes old—it should drop the message instead. 

Because the connection phase takes minutes (see #1), messages generated during those minutes are being queued indefinitely rather than dropped. When the test finally enters the 10s reading phase, clients receive prices generated at the very beginning of the test. The `PolicyDropOldest` ring buffer is either oversized, being bypassed, or the "Hot-Loop Ring Drain" bug we identified earlier is causing messages to accumulate in unbounded channel queues rather than the ring buffer.

## 3. Throughput Collapse (O(N) Fan-Out Degradation)

**The Data:**
- 1,000 clients: 22,224 msg/sec
- 5,000 clients: 10,200 msg/sec
- 10,000 clients: 3,912 msg/sec

**The Bottleneck:**
This is the most alarming metric. In a healthy pub/sub system, total delivered throughput should increase linearly with the number of clients, or at least remain steady until network saturation. 

Here, total throughput **collapses** as client count increases. 10,000 clients receive 80% *fewer* total messages per second than 1,000 clients. 

This confirms the **O(N) JSON Serialization Cliff** we identified in the Gateway Review. Because `writePump` JSON-encodes every message independently inside the client's goroutine, adding more clients exponentially increases CPU load. At 10,000 clients, the CPU is entirely consumed by GC thrashing and redundant JSON serialization, starving the network stack.

## 4. Connection Failures & Gateway Unresponsiveness

**The Data:**
- 1,000 clients: 0.5% failure rate
- 5,000 clients: 4.5% failure rate
- 10,000 clients: **42.2% failure rate** (4,226 failures)

**The Bottleneck:**
The system suffers from complete resource exhaustion before reaching 10k connections. The failures are likely HTTP `503 Service Unavailable` due to exceeding `MaxConcurrentConns`, TCP `SYN` queue overflow, or connection timeouts because the Gateway is completely locked up. The global `sync.Map` lock in the Topic Manager fan-out path combined with the massive CPU load from serialization creates a "death spiral" where the server cannot even process TCP handshakes.

---

## Architectural Conclusion & Next Steps

The system is currently functioning as a **highly inefficient, unbounded FIFO queue** rather than a real-time stream processor. 

To make this production-ready, we must immediately implement the fixes we planned:

1. **Pre-Encode JSON (Fix Throughput):** The Publisher or TopicManager must serialize messages to JSON `[]byte` *once*, and the Gateway must pass the raw bytes to clients.
2. **Fix Ring Buffer Backpressure (Fix Latency):** Enforce strict fixed-size ring buffers and ensure slow clients drop data rather than accumulating minutes of latency.
3. **Remove Global Locks (Fix Connection/Failures):** Shard the `sync.Map` in the Topic Manager and remove lock contention from the connect/disconnect path.
