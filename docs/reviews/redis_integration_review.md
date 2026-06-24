# Redis Pub/Sub Integration Review

This document provides a technical review of the Redis Pub/Sub integration for the real-time market data system, focusing on message loss, reconnect behavior, shutdown safety, and scalability limits under production load.

## 1. Scalability Limits

> [!WARNING]
> **Single Global Channel Bottleneck**
> The design document recommends "Topic-Based Channels" (Option 3), but the implementation uses a single global `DefaultChannel` ("market:data"). Every gateway instance receives, deserializes, and processes *every* market event, even if no local clients are subscribed to those symbols. Under production load (e.g., thousands of updates per second), this will cause severe CPU and network saturation on all gateways.

> [!TIP]
> **JSON Serialization Overhead**
> The hot path in `Publisher.Publish` and `Subscriber.handleMessage` relies on `encoding/json`. `Publisher` marshals the event and then marshals an envelope. `Subscriber` unmarshals the envelope and then unmarshals the event. This dual-JSON parsing is highly CPU-intensive. Consider moving to Protobuf, FlatBuffers, or at minimum, a faster JSON library (e.g., `jsoniter` or `ffjson`).

> [!CAUTION]
> **Blocking Publisher**
> The `Publisher.Publish` documentation states "The contract is non-blocking", but `p.client.Publish(...)` is a synchronous network call. If the network degrades or Redis experiences high latency, the `Publish` call will block, propagating backpressure to the feed generator and potentially stalling the entire data ingestion pipeline.

## 2. Shutdown Safety

> [!IMPORTANT]
> **Data Loss on Publisher Shutdown**
> `Publisher.Close()` sets `p.closed = true` but does not wait for in-flight Redis `Publish` calls to complete. If the application exits immediately after `Close()`, any events currently being transmitted will be abruptly terminated and lost.

**Context Cancellation in Subscriber**
`Subscriber.Stop()` cancels the context, which causes the `listen` loop to return. However, this same context is passed down into `s.tm.Publish(ctx, event)`. If `Stop()` is called while a message is being processed, the local topic manager might receive a canceled context, potentially aborting the fan-out midway and leaving clients without updates.

## 3. Reconnect Behavior

**Go-Redis Implicit Reconnection**
The subscriber relies entirely on the `go-redis` library's automatic reconnection. While `go-redis` handles temporary network drops by attempting to resubscribe, the application is unaware of the connection state. 
- If Redis is down for a prolonged period, the subscriber silently waits.
- The system lacks a circuit breaker or health-check exposure for the subscriber. Gateways should ideally report their Redis connection status to a load balancer so traffic isn't routed to a gateway that isn't receiving market data.

## 4. Message Loss

> [!NOTE]
> **Fire-and-Forget Architecture**
> As outlined in the design document, Redis Pub/Sub provides no persistence or replay. Any messages published while a gateway is disconnected or restarting will be permanently lost for that gateway's clients.

**Rate Limiting / Dropped Metrics**
The publisher increments a `p.dropped` counter when `Publish` fails. However, because it's a blocking call, "failures" mostly represent closed connections or timeouts rather than an active load-shedding mechanism. Under extreme load, the system will experience latency before it experiences dropped messages, which is worse for real-time systems.

---

## Recommendations

1. **Implement Topic-Based Channels**: Update the subscriber to dynamically subscribe/unsubscribe to Redis channels based on local client demand, rather than listening to a single firehose.
2. **Asynchronous Publisher**: Introduce a buffered channel and worker goroutines inside `Publisher` to truly decouple `Publish()` from the network latency of Redis. If the buffer fills, *then* drop the message.
3. **Graceful Shutdown**: Add a `sync.WaitGroup` in the Publisher to drain the worker queue and wait for pending publishes before exiting.
4. **Binary Serialization**: Replace `encoding/json` with a more efficient serialization format to reduce CPU burn on the gateways.
