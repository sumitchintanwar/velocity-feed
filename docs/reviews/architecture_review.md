# End-to-End Architecture Review
**Reviewer perspective:** Principal Engineer  
**Target:** Feed Generator → Publisher → Topic Manager → WebSocket Gateway  
**Goal:** Define boundaries, lifecycles, and identify cross-component design mistakes.

---

## 1. Responsibilities & Ownership Boundaries

The architecture represents a classic fan-out event distribution system. To scale to 10k events/sec and 5k+ concurrent connections, boundaries must be strictly enforced.

| Component | Responsibility | Ownership | What it MUST NOT do |
| :--- | :--- | :--- | :--- |
| **Feed Generator** | Ingest external data or generate synthetic data. Format into standard `MarketEvent`. | **Data Ingestion** | MUST NOT block if downstream is slow. MUST NOT know about topics or subscriptions. |
| **Publisher Service** | Accept events from Feed Generator. Route to appropriate Topic Manager shards. | **Event Routing** | MUST NOT manage client connections. MUST NOT hold business logic related to client state. |
| **Topic Manager** | Maintain the Topic ↔ Subscriber mapping. Execute the actual fan-out by pushing to subscriber queues. | **Subscription State** | MUST NOT know about WebSockets, TCP, or network buffers. MUST NOT block publishers. |
| **WebSocket Gateway** | Terminate TLS/TCP. Parse JSON commands. Push data from internal queues to network sockets. | **Network Transport** | MUST NOT maintain its own topic mapping. MUST NOT parse or inspect the `MarketEvent` payload. |

### The Contract Boundary
The defining boundary in this system is the **`Subscriber` interface** between the Topic Manager and the WebSocket Gateway. The Gateway implements `Subscriber` (providing an `OutboundQueue` or `channel`), and the Topic Manager writes to it. The Topic Manager knows nothing of sockets; the Gateway knows nothing of topics (only its own subscriptions).

---

## 2. Startup Sequence

In a distributed or concurrent system, the order of initialization dictates resilience. A component cannot start accepting work until its dependencies are ready.

**Correct Initialization Order:**
1. **Platform Layer:** Logger, Metrics Registry, Config.
2. **Topic Manager:** Initialize shards, allocate locks, prepare empty subscriber registries.
3. **Publisher Service:** Initialize, inject the Topic Manager as its primary target.
4. **WebSocket Gateway:** Initialize, inject the Topic Manager. Start HTTP server, open `/ws` endpoint. *(Clients can now connect and subscribe, but no data is flowing yet).*
5. **Feed Generator:** Start the feed. Inject the Publisher.

**Why this matters:** If the Feed Generator starts before the WebSocket Gateway is ready, data is dropped (or worse, buffered in memory). If the Gateway starts before the Topic Manager is ready, early client subscriptions will panic or fail.

---

## 3. Shutdown Sequence

Shutdown must be graceful. The order is exactly the **reverse** of startup to prevent cascading failures and data loss.

**Correct Shutdown Order:**
1. **Feed Generator:** Stop generating/ingesting new data. Flush any internal buffers to the Publisher. Wait for flush to complete.
2. **Publisher Service:** Stop accepting new events. Flush its internal queues to the Topic Manager.
3. **Topic Manager:** Refuse new subscriptions. Wait for all in-flight fan-outs to complete.
4. **WebSocket Gateway:**
   - Stop accepting *new* connections (close the listener).
   - Send `CloseGoingAway` (1001) WebSocket frames to all active clients.
   - Wait (with a timeout, e.g., 5s) for clients to acknowledge the close.
   - Force close any remaining sockets.
5. **Platform Layer:** Flush logs, push final metrics.

**Why this matters:** If you shut down the Gateway first, you drop active connections while the Feed is still pushing data, causing massive write errors. If you shut down the Topic Manager first, the Publisher panics trying to write to a closed system.

---

## 4. Architectural Design Mistakes

Looking at the system holistically across the four design documents, several critical flaws emerge when these components interact.

### Mistake 1: The "God" Publisher Bottleneck
The `PUBLISHER_SERVICE.md` defines a central "Event Bus" that receives all data and pushes to "Subscribers" (which include the Topic Manager, Redis, Persistence, etc.). 

**The Flaw:** By putting a single Publisher service in front of everything, you create a single-threaded bottleneck. If the Persistence Consumer is slow, does it block the WebSocket Consumer? 
**The Fix:** The Publisher should not exist as a central bus. The Feed Generator should push directly to a ring buffer or a Go channel. Specialized workers (one for Topic Manager, one for Persistence) read from that buffer. This decouples the latency of writing to disk from the latency of pushing to a socket.

### Mistake 2: Undefined Backpressure Strategy (OOM Cascades)
The documents mention "dropping messages" for slow consumers, but there is no holistic backpressure strategy.
* Feed → Publisher: What happens if Publisher is slow?
* Publisher → Topic Manager: What happens if Topic Manager is locked?
* Topic Manager → Gateway Queue: Drop policy is mentioned but not enforced.

**The Flaw:** In Go, unbounded channels or undefined drop policies lead to Out-Of-Memory (OOM) crashes under load. If the Feed generates 10k msgs/sec and the Gateway can only write 5k msgs/sec, the 5k/sec difference accumulates in RAM.
**The Fix:** Every boundary must have a explicitly sized buffer and a defined policy: **Drop Head** (for market data, the newest price is all that matters; drop the old ones).

### Mistake 3: Conflating Domain Topics with Transport Routing
`WEBSOCKET_GATEWAY_DESIGN.md` implies the Gateway asks the Topic Manager to subscribe a client. 

**The Flaw:** If the Topic Manager maps `AAPL -> Client_1234`, the Topic Manager is doing routing work. At 10k connections, fan-out takes `O(N)`. If a WebSocket drops, the Gateway has to tell the Topic Manager to remove `Client_1234` from 50 different topics.
**The Fix:** The Topic Manager should manage *topics*, not *connections*. The Gateway should own the connection multiplexing. 
* Topic Manager maps `AAPL -> Gateway Broadcast Channel`.
* Gateway maps `Gateway Broadcast Channel -> Client_1234`.
This prevents the Topic Manager from having to lock and unlock its structures every time a network socket drops.

### Mistake 4: Missing System-Wide Cancellation Context
None of the designs specify a global `context.Context` for lifecycle management.

**The Flaw:** If the Feed Generator crashes, the system stays up but does nothing. If an admin requests a shutdown, there is no way to signal all components simultaneously.
**The Fix:** The `main()` function must create a `ctx, cancel := signal.NotifyContext(...)`. This single context must be passed down to the Feed Generator, Publisher, Topic Manager, and Gateway. When cancelled, all components initiate their graceful shutdown sequence simultaneously.
