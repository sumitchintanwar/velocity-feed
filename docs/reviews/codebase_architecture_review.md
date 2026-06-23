# Codebase Architecture Review
**Reviewer perspective:** Principal Engineer  
**Focus:** Maintainability, Coupling, Extensibility, Production Readiness  
**Target:** The Go implementation (cmd, internal, pkg)

---

## 1. Maintainability

**Assessment:** *Good, but the composition root is becoming a monolith.*

**Strengths:**
*   **Domain-Driven Layout:** The package structure (`marketdata`, `pubsub`, `topicmanager`, `transport`, `websocket`) strictly isolates concerns. You can read a package and understand its purpose without knowing the whole system.
*   **Idiomatic Go:** Concurrency primitives, context propagation, and interface definitions are well-written. The use of `select` with `ctx.Done()` is correctly applied throughout most of the pipeline.

**Weaknesses:**
*   **Overloaded `app.go`:** The `internal/app/app.go` file acts as the Dependency Injection container, but it is taking on too much responsibility. It defines ad-hoc adapters (`topicPublisher`), manages complex ordered shutdown logic manually via a custom `component` struct, and contains HTTP server initialization. As the system grows, this file will become an unmaintainable god-object.
*   **Dead Code & Confusing State:** `websocket.Client` defines a `send: make(chan marketdata.MarketEvent, sendQueueSize)` channel but *never uses it*. Instead, `writePump` reads directly from the TopicManager's `h.C()` channel. This creates a misleading "Outbound Queue" that developers will try to debug, not realizing the actual queue is inside the TopicManager.

---

## 2. Coupling

**Assessment:** *Clean domain boundaries, but the Transport layer violates the abstraction.*

**Strengths:**
*   **Feed to Publisher:** The `feed.Pipeline` takes a generic `marketdata.Feed` and `pubsub.Publisher`. The pipeline has zero knowledge of whether the publisher is an in-memory topic manager, Redis, or Kafka. This is textbook Dependency Inversion.

**Weaknesses:**
*   **Gateway to TopicManager:** The `WEBSOCKET_GATEWAY_DESIGN.md` explicitly states the gateway should not know about "Topics" and should rely on an internal Event Bus. However, `websocket.Gateway` directly imports and requires a `topicmanager.Manager`. 
    *   *Why this is bad:* The Gateway is tightly coupled to the in-memory topic abstraction. If you introduce Redis, the Gateway must either be rewritten to use a `RedisSubscriber` interface, or the `TopicManager` must become an awkward, distributed facade over Redis. The Gateway should be handed a generic subscription stream, not the routing manager itself.

---

## 3. Extensibility

**Assessment:** *Strong vertical extensibility; weak horizontal extensibility.*

**Strengths:**
*   **Adding New Feeds:** Implementing a new feed (e.g., Binance, Polygon) is trivial. You just implement the `marketdata.Feed` interface and inject it into the pipeline.
*   **Sharded State:** `MemoryManager` in `topicmanager` uses `hash/maphash` to shard the topic registry. This scales linearly. To support 100k topics, you simply increase `defaultShardCount`.

**Weaknesses:**
*   **Distributed Pub/Sub Bridge:** The `topicmanager.Handle` interface exposes a Go channel (`C() <-chan marketdata.MarketEvent`). Channels cannot be sent over the network. If you try to extend this system to use Kafka/NATS, bridging a distributed message broker into a local Go channel interface per-client will introduce massive impedance mismatch and goroutine overhead. Interfaces intended for network extensibility should use callback functions or stream abstractions, not raw channels.

---

## 4. Production Readiness

**Assessment:** *Not ready for production. Contains systemic flaws that will cause catastrophic failure under load.*

**Strengths:**
*   **Zero-Allocation Hot Path:** The `MemoryManager.Publish` implementation is a masterclass in Go performance. By using `atomic.Pointer` and immutable `subList` (Copy-on-Write), the publish hot-path performs 1 atomic load, 0 allocations, and requires 0 locks. It will easily handle millions of messages per second.
*   **Topic Tombstoning:** The implementation correctly deletes empty topics (`delete(s.topics, t)`), resolving the `MG1` memory growth flaw identified in earlier design reviews.

**Critical Bugs (Must Fix Before Deploy):**

1.  **The 10-Second Connection Death (Critical):**
    In `app.go:194`, the HTTP Server is configured with `WriteTimeout: a.cfg.Server.WriteTimeout` (default 10s). In `gateway.go:87`, the client context is derived from `r.Context()`. 
    *Impact:* The HTTP server cancels `r.Context()` when the `WriteTimeout` fires. This means **every WebSocket connection is forcefully terminated after 10 seconds**, regardless of activity.

2.  **Marshalling on the Consumer Thread (High):**
    `websocket.writePump` reads from `h.C()` and immediately calls `c.writeJSON(env)`. `WriteJSON` uses reflection-based `json.Marshal`. 
    *Impact:* This makes the consumer incredibly slow. Because `topicmanager.Publish` uses a non-blocking send (`select { case sub.ch <- event: default: }`), the slow `writePump` will cause `sub.ch` to fill up instantly under load, and the TopicManager will silently drop the majority of market events for that client.

3.  **Map Memory Leak (Medium):**
    `Gateway.clients` map deletes disconnected clients but never shrinks its underlying hash table. A burst of 10,000 connections that immediately disconnects leaves a permanently bloated map in memory.

4.  **Double-Close Graceful Shutdown Race (Medium):**
    Both `readPump` and `writePump` call `c.conn.Close()` in `defer`. When `Shutdown` cancels the context, `writePump` exits and closes the TCP socket before `readPump` has a chance to send the WebSocket `CloseGoingAway` frame. Clients receive an abrupt network error instead of a protocol-compliant disconnect.

---

## Summary Recommendations

1.  **Fix the Context Leak:** Clear the HTTP write deadline for hijacked WebSocket connections using `http.ResponseController`.
2.  **Decouple the Gateway:** Change `websocket.Gateway` to accept a `pubsub.Subscriber` factory rather than the `topicmanager.Manager` directly.
3.  **Optimize the Write Pump:** Pre-allocate a `json.Encoder` in the `writePump` and remove the unused `send` channel from the `Client` struct to clarify the queue architecture.
4.  **Extract App Wiring:** Move the lifecycle management out of `app.go` into a dedicated `lifecycle` package, and use a standard library like `uber-go/fx` if the DI graph grows any larger.
