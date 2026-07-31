# RTMDS Protocol Layer Review
**Reviewer:** Staff Engineer, Google Cloud
**Date:** July 2026
**Subject:** Multi-format Serialization & Protocol Layer Architecture

Overall, the foundational architecture of the new protocol layer is solid and hits the right priorities: minimizing allocations, supporting zero-copy (FlatBuffers), and keeping bandwidth tight (Protobuf). The WebSocket negotiation via `Sec-WebSocket-Protocol` is idiomatic and clean. 

However, if we scale this to **millions of messages per second across thousands of clients**, there are several critical architectural bottlenecks that will cause tail-latency spikes, CPU saturation, and client-side parsing failures.

Here is my severity-ranked critique and recommendations inspired by production systems like gRPC, Kafka, and NATS.

---

## 🔴 High Severity (Immediate Action Required)

### 1. O(N) Serialization on the Fan-Out Hot Path
**Critique:** 
Currently, when an event is published, the `TopicManager` pre-encodes the JSON payload into a `CachedEvent`. However, the binary formats (Protobuf and FlatBuffers) are serialized *dynamically inside the per-client `writePump`*. 
If 10,000 clients are subscribed to `AAPL` via Protobuf, the server pays the CPU cost of `serializer.Serialize()` **10,000 times for the exact same message**.
**Why this breaks at scale:** The `writePump` is already bottlenecked by network I/O. Injecting CPU-bound serialization into the network write loop will cause slow-client buildup and trigger aggressive message dropping.
**Improvement (NATS / Kafka Pattern):**
Pre-serialize the message into *all* actively subscribed formats **once** at the publisher/router layer before fanning out. Pass an immutable, reference-counted byte array (or `protocol.PreSerializedMessage`) to the client queues. The `writePump` should do absolutely nothing except `conn.WriteMessage(msgType, bytes)`.

### 2. Mixed Protocol Control Messages
**Critique:** 
We allow clients to negotiate binary protocols (e.g., `v1.flatbuffers.rtmds`), but the Gateway still sends control messages (errors, subscription acks) as standard JSON text frames. 
**Why this breaks at scale:** A strict C++ FlatBuffers client now has to implement a dual-parser: checking if the WebSocket frame is `MessageText` (invoke JSON parser) or `MessageBinary` (invoke FlatBuffers parser). This defeats the performance purpose of choosing FlatBuffers.
**Improvement (gRPC Pattern):**
Establish a unified binary envelope. If a client negotiates Protobuf, *all* messages—including errors and acks—must be Protobuf. Define a generic `ServerMessage` Protobuf/FlatBuffers schema that contains a `oneof` (or union) representing either a `MarketEvent`, a `ControlAck`, or a `SystemError`.

---

## 🟠 Medium Severity (Address Before General Availability)

### 3. Hardcoded Type Routing & Interface Boxing
**Critique:**
In `JSONSerializer` and the Gateway, we are using hardcoded string comparisons (`"quote"`, `"trade"`) and `switch typStr` logic, along with boxing `MarketEvent` inside `interface{}`/`any` fields.
**Why this breaks at scale:** Go interface boxing forces heap allocations. Type assertions inside the hot loop add overhead. Adding a new event type (e.g., `OrderBookSnapshot`) requires modifying the `JSONSerializer`, the `Gateway`, and the `TopicManager`.
**Improvement:**
Use an integer-based Message Type ID prefix (e.g., `0x01` for Quote, `0x02` for Bar) in the binary payloads. Maintain a registry of decoders. This avoids string allocations and makes the system pluggable without modifying the core routing logic.

### 4. Lack of Schema Versioning in Payloads
**Critique:**
While the WebSocket subprotocol negotiates `v1.protobuf.rtmds`, there is no mechanism for an individual message to announce its schema version if we do an out-of-band schema migration that isn't perfectly backward compatible.
**Why this breaks at scale:** If we add a field that changes semantic meaning (e.g., changing `price` from `float64` to `int64` basis points), old clients will read garbage data without failing.
**Improvement (Confluent Schema Registry Pattern):**
Prefix every binary payload with a 5-byte header: `[1-byte Magic Marker] [4-byte Schema ID]`. Clients can extract the Schema ID, realize they are outdated, and either dynamically fetch the new schema or explicitly log a version mismatch error rather than silently corrupting financial data.

---

## 🟡 Low Severity (Technical Debt / Optimizations)

### 5. Client Reconnect Storms
**Critique:**
The client SDK implements exponential backoff for reconnects, which is great. However, if the RTMDS cluster restarts, 10,000 clients will all attempt to reconnect simultaneously, overwhelming the WebSocket negotiation and `TopicManager` subscription locks.
**Improvement (Aeron / Redis Pattern):**
Introduce **jitter** to the exponential backoff. Add a random `+/- 20%` variance to the backoff timer so that reconnect storms are smeared across a wider time window, saving the gateway from a thundering herd.

### 6. Subprotocol String Parsing
**Critique:**
The client SDK allocates strings repeatedly during connection loops to append subprotocols: `append(dialOpts.Subprotocols, protocol.FormatToSubprotocol(f))`.
**Improvement:**
Since the subprotocols are statically known (`v1.json.rtmds`, etc.), define them as static slices at the package level and reuse them rather than allocating new arrays/strings on every dial attempt.

---

## Summary Verdict

The architecture **can** support millions of messages per second and thousands of clients, **provided you fix the O(N) serialization bottleneck (Severity 1)**. 

To achieve production-grade Aeron-level performance, the pipeline must strictly separate:
1. **The Ingest Phase**: Validate, sequence.
2. **The Serialization Phase**: Encode to JSON, Protobuf, FlatBuffers exactly *once*.
3. **The Network Phase**: Blindly copy bytes from memory to the socket buffer. 

If you make these architectural tweaks, this layer will be rock solid.
