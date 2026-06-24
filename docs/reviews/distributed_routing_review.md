# Distributed Topic Routing Review

This document provides a technical review of the distributed topic routing architecture and its implementation in `internal/topicmanager/router.go` and `internal/topicmanager/memory.go`, focusing on message duplication, subscription consistency, and scaling bottlenecks.

## 1. Scaling Bottlenecks

> [!WARNING]
> **Global Mutex Contention (`symbolMu`)**
> The `MemoryManager` (`memory.go`) goes to great lengths to avoid lock contention by using sharded locks (`shards` and `subShards`) and Copy-on-Write lists. This allows high-throughput, concurrent subscriptions.
> However, `DistributedRouter` (`router.go`) completely negates these performance gains. In `DistributedRouter.Subscribe`, it acquires a single, global mutex (`r.symbolMu.Lock()`) for *every* symbol a client subscribes to. 
> If 50,000 clients connect and subscribe to 100 symbols each during a thundering herd event, that single global mutex will be locked 5,000,000 times sequentially, causing a severe connection bottleneck.
> **Recommendation:** Shard the `symbolSubs` map in `DistributedRouter` exactly like the `MemoryManager` does.

> [!TIP]
> **Double Locking in `TrackedRouter`**
> `TrackedRouter` introduces another global lock (`subsMu sync.RWMutex`). A client un/subscribing will hit `subsMu`, then `symbolMu`, then the sharded locks in `MemoryManager`. This layered locking is dangerous and heavily degrades horizontal scaling of connection handling.

## 2. Subscription Consistency

> [!IMPORTANT]
> **No Retry on `onChange` Failure**
> When the first local client subscribes to a symbol, `DistributedRouter` sets `st.redisSub.Store(true)` and calls `r.onChange(topic, SubscribeRequested)`. 
> If the `onChange` callback fails (e.g., Redis network timeout), the router assumes the subscription succeeded. Because `st.redisSub` is now `true`, subsequent clients subscribing to that symbol will *not* trigger another `onChange` attempt. The gateway will be stuck in an inconsistent state where it thinks it is subscribed to Redis, but it actually isn't. Those clients will silently receive no data.
> **Recommendation:** Implement a reconciliation loop or a retry mechanism that ensures the physical Redis subscription matches the logical `st.redisSub` state.

## 3. Message Duplication

> [!NOTE]
> **Topic Group Mapping Overlap**
> The design document recommends "Topic Group Routing" (Option 3, e.g., `market:equities`). However, the implementation in `router.go` simply uses the exact symbol name (e.g., `AAPL`) and passes it to `onChange` to become a Redis channel (e.g., `market:AAPL` if using a prefix). 
> If you implement Topic Group routing upstream, but the Gateway still requests subscriptions by symbol, you will either fail to receive data or accidentally subscribe to overlapping channels (if publishers double-publish). 
> Furthermore, `DistributedRouter.Publish` contains no deduplication logic. If a gateway accidentally subscribes to both `market:equities` and `market:AAPL` (due to evolving mapping rules), any events published to both will be fanned out to local clients twice. 
> **Recommendation:** Clarify the translation layer between Client Topic (e.g., "AAPL") and Redis Channel (e.g., "market:equities"). Ensure clients cannot trigger overlapping Redis channel subscriptions.

---

## Conclusion

The `MemoryManager` is well-optimized for the publish hot-path, but the `DistributedRouter` wrapper creates significant bottlenecks for the *subscribe/unsubscribe* paths. To achieve the target of 50,000 concurrent connections, the global locks in `router.go` must be removed or heavily sharded.
