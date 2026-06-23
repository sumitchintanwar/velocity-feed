# WebSocket Gateway — Code Review
**Reviewer perspective:** Distributed Systems Engineer  
**Files:** `gateway.go`, `client.go`, `WEBSOCKET_GATEWAY_DESIGN.md`  
**Target:** 5,000 concurrent connections

---

## Summary

The overall architecture is sound: dedicated read/write goroutines per connection, a `done` channel coupling their lifetimes, `sync.Once` protecting subscription cancellation, and a `sync.WaitGroup`-based graceful shutdown. These are the right patterns. The problems are in the **edge cases**: specific goroutine leak paths that only surface on shutdown or rapid reconnect, a memory footprint that is 5× larger than the design estimates, and several places where the design doc's stated guarantees aren't actually upheld by the code.

---

## GOROUTINE LEAKS

### GL1 — writePump goroutine leaked when Shutdown races with active readPump

**`gateway.go:135–163`, `client.go:265–285`**

`Shutdown` does the following:

```go
// gateway.go:135
g.mu.RLock()
snapshot := make([]*Client, 0, len(g.clients))
for _, c := range g.clients { snapshot = append(snapshot, c) }
g.mu.RUnlock()

for _, c := range snapshot {
    go func(c *Client) { c.shutdown(ctx) }(c)
}
```

`c.shutdown()` does:
```go
// client.go:265
c.cancelCtx()                         // cancels the client context
_ = c.conn.WriteControl(CloseMessage) // sends close frame
select {
case <-c.done:                        // waits for writePump to exit
case <-ctx.Done():                    // or gives up on shutdown timeout
}
```

`readPump` exits when `conn.Read()` returns an error — which it will after receiving the close frame or after the context is cancelled. When `readPump` exits, it calls `close(c.done)`, which unblocks `writePump`.

**The leak path**: `shutdown` cancels `cancelCtx`, which cancels `ctx`. `writePump` selects on `<-ctx.Done()` and returns. But `readPump` may still be blocked in `conn.ReadMessage()` — Gorilla WebSocket's `ReadMessage` does not respect a context; it only returns on a deadline or error. `cancelCtx()` does not set a read deadline. So `readPump` can remain blocked for up to `pongWait = 60 seconds` after shutdown begins.

During those 60 seconds: `writePump` has exited (via ctx.Done), but `readPump` is still alive, holding the connection open. `close(c.done)` has NOT fired yet (readPump fires it). If the shutdown context expires before readPump unblocks, `c.shutdown` returns, the Handler returns, `unregister` runs, `cancelAll` runs — but `readPump` goroutine is still alive with a reference to the now-deregistered `Client`.

**Fix**: After `cancelCtx()`, explicitly set a read deadline on the connection to force `ReadMessage` to return:
```go
c.cancelCtx()
_ = c.conn.SetReadDeadline(time.Now().Add(writeWait)) // forces ReadMessage to return
```

---

### GL2 — Shutdown's wg.Wait goroutine leaks when context expires

**`gateway.go:153–162`**

```go
done := make(chan struct{})
go func() {
    wg.Wait()
    close(done)
}()

select {
case <-done:
case <-ctx.Done():  // ← returns here, leaving the goroutine alive
}
```

If `ctx` expires before all clients drain, `Shutdown` returns but the anonymous goroutine continues running, holding a reference to `wg` and transitively to all `*Client` values in the snapshot slice. This goroutine runs until all write pumps finish — which may never happen if any client's write pump is itself leaked (GL1). In the worst case this goroutine lives for the entire process lifetime after a failed graceful shutdown.

**Fix**: This is unavoidable with `sync.WaitGroup` — it has no cancellation. Replace with a context-aware wait:
```go
// Use a semaphore channel instead of WaitGroup:
sem := make(chan struct{}, len(snapshot))
for _, c := range snapshot {
    go func(c *Client) { c.shutdown(ctx); sem <- struct{}{} }(c)
}
for i := 0; i < len(snapshot); i++ {
    select {
    case <-sem:
    case <-ctx.Done():
        return
    }
}
```

---

### GL3 — Double conn.Close: resource contention, not a leak but a correctness issue

**`client.go:77–82` and `client.go:161–164`**

```go
// readPump defer:
defer func() {
    close(c.done)
    _ = c.conn.Close()   // ← Close #1
}()

// writePump defer:
defer func() {
    ticker.Stop()
    _ = c.conn.Close()   // ← Close #2
}()
```

Both goroutines call `conn.Close()`. Gorilla's `Close` is safe to call multiple times (it's idempotent). However, if `writePump` exits first (via `ctx.Done()`), it closes the connection, then `readPump`'s blocked `conn.ReadMessage()` returns an error, `readPump` returns, and also calls `Close`. The second `Close` on an already-closed connection sends a FIN on a closed socket — OS behavior is undefined (commonly `EBADF` or `EPIPE`). The errors are discarded (`_ =`), so this is silent.

More importantly: if `writePump` closes the connection before `readPump` has sent a close frame, the client receives an abrupt close rather than a graceful WebSocket close handshake. Section 9 of the RFC requires a close frame before TCP close.

**Fix**: Only `writePump` should close the connection (it owns writes). `readPump` should signal via `done` only. Add a `sendClose` call inside `writePump`'s defer.

---

## SLOW CONSUMERS

### SC1 — sendQueueSize = 256 is Underspecified: 250MB Reserved at Scale

**`gateway.go:45`, `client.go:63`**

```go
sendQueueSize = 256 // items
send: make(chan marketdata.MarketEvent, sendQueueSize)
```

The design doc (Section 12) states "Careful queue sizing is critical." The implementation fixes it at 256. At 5,000 connections:

| Metric | Value |
|---|---|
| Queue slots per client | 256 |
| MarketEvent size (estimated) | ~200 bytes |
| Memory per client queue | ~51 KB |
| Total at 5,000 clients | **~250 MB** |

The design analysis (Section 14) says "Very manageable in memory" — that estimate assumed the queue was small. 250MB of pre-allocated channel memory is not small. The queue channel itself is allocated at `make` time, so all 250MB is claimed at peak connection count, even if queues are empty.

Additionally, the drop policy is implicit — the code in `handleMessage` sends to `h.C()` (the topic manager handle's channel), and the topic manager drops on full. The gateway's `send` channel is then read by `writePump` via `eventC`. But there's **no drop counter** and **no auto-disconnect threshold** for a consistently full queue. A slow client stays connected indefinitely, holding its slot.

**Fix**: Add a drop counter per client. After N consecutive drops (configurable, e.g., 100), log a warning and call `c.cancelCtx()` to force disconnect.

---

### SC2 — Control Channel Drop Silently Loses Subscription Confirmations

**`client.go:243–250`**

```go
func (c *Client) sendControl(msg ServerMessage) {
    select {
    case c.control <- msg:
    default:
        // Control channel full — drop (client is too slow).
    }
}
```

`control` has capacity 16. If `writePump` is blocked on a slow socket write (common during TCP congestion), and the client sends 17 subscription requests rapidly, the 17th subscription confirmation is silently dropped. The client is subscribed in the Topic Manager but receives no `{"type":"subscribed"}` confirmation. From the client's perspective, the subscription silently failed. It may retry, causing duplicate subscriptions.

The control channel should not use the same drop-on-full policy as market data. Control messages are low-volume and must be delivered reliably. They should be queued with a backlog warning, not dropped.

---

### SC3 — No Write Deadline on json.Marshal + WriteJSON Hot Path

**`client.go:238–241`**

```go
func (c *Client) writeJSON(v any) error {
    _ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
    return c.conn.WriteJSON(v)
}
```

This correctly sets a 10-second write deadline before each write — good. But `conn.WriteJSON` internally calls `json.Marshal`, which is a heap-allocating reflection-based operation. At 10k events/sec per client, `json.Marshal(ServerMessage{Type: ..., Payload: ev})` produces:
- 1 allocation for the output `[]byte`  
- 1 allocation boxing `ev` into `any` (the `Payload` field)
- Internal encoder state allocations

For 5,000 clients × 10k events/sec = **50 million JSON serializations/sec** across the gateway. This is the primary GC load source. A pre-allocated `json.Encoder` writing to a `sync.Pool`-recycled `bytes.Buffer` reduces this to near zero.

---

## MEMORY LEAKS

### ML1 — clients Map Retains Peak Capacity After Disconnect Storm

**`gateway.go:63,112–123`**

```go
clients map[string]*Client // id → client

func (g *Gateway) unregister(id string) {
    g.mu.Lock()
    delete(g.clients, id)
    g.mu.Unlock()
    // ...
}
```

Go maps **never shrink** their underlying hash table after deletions. If 5,000 clients connect and disconnect simultaneously (e.g., market open burst + market close disconnect storm), the `clients` map allocates capacity for 5,000 entries. After all disconnections, the map is empty but still holds the hash table for 5,000 entries — approximately `5000 × (8 bytes key pointer + 8 bytes value pointer + bucket overhead) ≈ 640 KB` permanently resident.

Over a trading day with multiple connection waves, this can reach several MB. More importantly, the map's iteration cost (used in `Shutdown`) scales with capacity, not current size — a 5,000-capacity empty map iterates as slowly as a full one.

**Fix**: Periodically replace the map with a fresh copy of its current entries (rebuild under write lock). Or use a more memory-efficient concurrent map structure.

---

### ML2 — ServerMessage.Payload any Boxes Every MarketEvent on the Heap

**`client.go:200`, `messages.go:12`**

```go
// messages.go
type ServerMessage struct {
    Type    string `json:"type"`
    Payload any    `json:"payload"`  // ← interface type
}

// client.go
env := ServerMessage{Type: ev.EventType(), Payload: ev}
```

Assigning a concrete value to `any` (interface) boxes it onto the heap if the value is larger than a pointer (which `MarketEvent` implementations certainly are). Every single message delivery creates a heap allocation for the boxed event value. At 50 million deliveries/sec across 5,000 clients, this is 50 million heap allocations/sec from this line alone.

**Fix**: `Payload` should be typed per message kind, or `ServerMessage` should be replaced with purpose-built per-event types that implement `json.Marshaler` directly.

---

### ML3 — cancelOnce Holds handle Reference After Subscription Cancelled

**`client.go:252–261`**

```go
func (c *Client) cancelAll() {
    c.cancelOnce.Do(func() {
        c.handleMu.Lock()
        h := c.handle
        c.handleMu.Unlock()
        if h != nil {
            h.Cancel()
        }
    })
}
```

`cancelOnce` is a `sync.Once` — its closure captures `c` (the whole client). After `cancelAll()` runs, the `Once`'s internal function value retains the captured closure (which holds `c`). In Go, `sync.Once` keeps a reference to the executed function internally until the `Once` itself is collected. Since `c.cancelOnce` is a field of `c`, this is self-referential — not a leak per se, but it means `h` (the handle) is kept alive for the full lifetime of `c`, even after `h.Cancel()` has been called.

More importantly: if `cancelAll` is called multiple times (by both `unregister` and a concurrent disconnect path), `sync.Once` correctly gates it. But the first call reads `c.handle` under `handleMu.Lock()` while `readPump` might simultaneously be writing `c.handle` under `handleMu.Lock()` in `handleMessage`. The `sync.Once` does not prevent this — it only prevents `cancelAll`'s body from running twice, not concurrent access during the first run. The `handleMu.Lock()` inside is correct, but the Lock/Unlock without modifying `c.handle` while `cancelAll` runs is a subtle gap: another goroutine can set `c.handle` to a new value between `cancelAll` reading `h` and the old `h.Cancel()` executing — the new handle is never cancelled.

---

## RESOURCE CLEANUP

### RC1 — cancelAll Called After writePump May Still Be Running

**`gateway.go:96–122`**

```go
go c.writePump(ctx)
c.readPump(ctx)          // blocks

g.unregister(id)         // calls cancelAll → h.Cancel()
```

The sequence on normal disconnect:

1. `readPump` exits → fires `close(c.done)`
2. `writePump` sees `<-c.done`, returns
3. Handler function returns from `c.readPump(ctx)` call
4. `g.unregister(id)` → `c.cancelAll()` → `h.Cancel()`

This is **correct** on the happy path. But step 2 and step 3 are on different goroutines. After `close(c.done)`, the Go scheduler may not immediately run `writePump` to process `<-c.done`. The Handler function returns to step 4 before `writePump` has actually exited.

`unregister` calls `cancelAll` which calls `h.Cancel()`, which in the Topic Manager closes the subscription channel. If `writePump` is simultaneously reading `<-eventC` from that channel, it sees channel closed (ok=false), sets `eventC = nil`, and continues the select loop. This is safe.

However, if `writePump` is blocked in `c.writeJSON(env)` (a kernel write call in progress), and `cancelAll` fires, `h.Cancel()` closes the subscription channel. `writePump` finishes its write, loops back to select, sees `eventC` is closed, stops receiving — but then selects on `<-c.done` which is already closed, and returns. This is ultimately safe but leaves a window where subscription entries in the Topic Manager are cancelled while writePump is still reading from the channel that feeds those entries.

**Real issue**: `unregister` does NOT wait for `writePump` to exit. It cannot, because there's no handle to wait on. This means the client object is deregistered (removed from the clients map, subscription cancelled) while `writePump` is still alive. Any metrics or logging that `writePump` performs after this point (after `cancelAll`) operate on a client that the Gateway no longer considers active.

**Fix**: Track writePump exit explicitly with a WaitGroup at the client level. `unregister` should wait for `<-c.done` (which is closed by `readPump`, which only returns after `writePump` has been signaled) before calling `cancelAll`. The current flow actually does this implicitly since `unregister` is called after `readPump` returns — but the guarantee isn't obvious from the code and can break if the call order changes.

---

### RC2 — No Connection Limit Enforced at Handler Entry

**`gateway.go:78–101`**

```go
func (g *Gateway) Handler() http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        conn, err := upgrader.Upgrade(w, r, nil)
        // ...
        g.register(c)
        // ...
    }
}
```

There is no check of `g.ClientCount()` before `upgrader.Upgrade`. If 10,000 clients connect simultaneously, all 10,000 are accepted. The design target is 5,000 concurrent connections. Exceeding this doubles goroutine count (to 20,000), doubles queue memory (to 500MB), and may hit OS file descriptor limits.

At Linux defaults (65,536 open files per process), 5,000 WebSocket connections consume:
- 5,000 TCP sockets (file descriptors)
- Plus other fds (log files, Prometheus, Redis)

At 10,000 connections, the process is close to the default fd limit. Connection 10,001 fails with `EMFILE`, which the upgrader returns as an HTTP 500 rather than a graceful 503.

**Fix**:
```go
if g.ClientCount() >= maxConnections {
    http.Error(w, "connection limit reached", http.StatusServiceUnavailable)
    return
}
```

---

### RC3 — r.Context() as Parent Context Couples HTTP Timeout to WebSocket Lifetime

**`gateway.go:87`**

```go
ctx, cancel := context.WithCancel(r.Context())
```

`r.Context()` is the HTTP request context. For a standard `net/http` server, this context is cancelled when:
1. The client closes the connection
2. The handler returns
3. The server calls `Shutdown`

For (1): the HTTP layer considers the WebSocket handshake complete once the connection is upgraded. The request context is NOT cancelled when the WebSocket client disconnects — only when the TCP connection is closed at the HTTP layer, which happens AFTER the Handler function returns. So this is safe for the connection lifetime.

However, if the `http.Server` has a `WriteTimeout` (which `app.go` sets to 10 seconds via `cfg.Server.WriteTimeout`), the HTTP framework may cancel `r.Context()` after the write timeout fires. A WebSocket connection can be alive for hours. A 10-second `WriteTimeout` on the HTTP server **will cancel `r.Context()` and kill all active WebSocket connections after 10 seconds**.

**Severity: HIGH.** The configuration in `app.go` sets `WriteTimeout: cfg.Server.WriteTimeout` (default 10s). This means every WebSocket connection is killed after 10 seconds at idle or after the first write takes longer than 10 seconds.

**Fix**: WebSocket handlers must use `http.NewResponseController(w).SetWriteDeadline(time.Time{})` to clear the HTTP write timeout, or construct the context from `context.Background()` rather than `r.Context()`.

---

## CONNECTION HANDLING

### CH1 — gorilla/websocket vs nhooyr.io/websocket Mismatch

**`gateway.go:9`, `client.go:9`**

The implementation imports `github.com/gorilla/websocket`. The earlier `client.go` (from the initial scaffold) used `nhooyr.io/websocket`. The current implementation has switched to Gorilla.

This is fine architecturally, but Gorilla has one important behavioral difference: **ping/pong must be handled manually**. The pong handler in `readPump` is correct:

```go
c.conn.SetPongHandler(func(string) error {
    _ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
    return nil
})
```

But the read deadline is set to `pongWait = 60s` at startup (line 85), then reset on each pong. If the client never sends a pong (malicious or broken client), the read deadline fires after 60 seconds and `ReadMessage` returns a timeout error. This correctly triggers cleanup. ✓

However: the ping is sent from `writePump` via the ticker. If `writePump` is blocked in `c.writeJSON()` for a long time (e.g., 10s write deadline during network congestion), the ping timer fires late. The total effective liveness window is `pongWait + writeWait = 70 seconds` — not the stated 60 seconds. This is acceptable but undocumented.

---

### CH2 — handleMessage Processes Unlimited Symbols Per Subscribe

**`client.go:114–134`**

```go
case "subscribe":
    if len(cm.Symbols) == 0 {
        c.sendControl(ServerMessage{Type: "error", Payload: "no symbols provided"})
        return
    }
    c.handle = c.tm.Subscribe(c.id, cm.Symbols...)
```

There is no upper bound on `len(cm.Symbols)`. A client can send:
```json
{"action": "subscribe", "symbols": ["SYM1", "SYM2", ..., "SYM100000"]}
```

This creates 100,000 topic subscriptions in the Topic Manager for a single client. The Topic Manager's reverse index (`Subscriber → Topics`) grows to 100,000 entries for this client. At disconnect, cleanup must remove 100,000 entries. This is both a memory and CPU DoS vector.

**Fix**: Enforce a per-client subscription limit (e.g., 100 symbols):
```go
if len(cm.Symbols) > maxSymbolsPerSubscription {
    c.sendControl(ServerMessage{Type: "error", Payload: "too many symbols"})
    return
}
```

---

## Summary Table

| Category | Issue | File | Severity |
|---|---|---|---|
| Goroutine Leak | readPump not unblocked after cancelCtx; stays alive up to 60s post-shutdown | `gateway.go:267` | **High** |
| Goroutine Leak | wg.Wait goroutine in Shutdown leaks on context expiry | `gateway.go:154` | **Medium** |
| Goroutine Leak | Double conn.Close breaks graceful close handshake | `client.go:81,163` | **Medium** |
| Slow Consumer | sendQueueSize=256 × 5000 clients = 250MB reserved queue memory | `gateway.go:45` | **High** |
| Slow Consumer | Control channel drops subscription confirmations silently | `client.go:244` | **High** |
| Slow Consumer | No consecutive-drop counter → no auto-disconnect for slow clients | `client.go` | **Medium** |
| Memory Leak | clients map never shrinks after peak | `gateway.go:63` | **Low** |
| Memory Leak | ServerMessage.Payload any boxes MarketEvent every message | `messages.go:12` | **High** |
| Memory Leak | json.Marshal via WriteJSON: 50M allocs/sec at scale | `client.go:240` | **High** |
| Resource Cleanup | No connection limit before upgrade → OOM on connection storm | `gateway.go:78` | **High** |
| Resource Cleanup | r.Context() parent: HTTP WriteTimeout (10s) kills WebSocket connections | `gateway.go:87` | **Critical** |
| Connection Handling | Unlimited symbols per subscribe → Topic Manager DoS | `client.go:116` | **High** |

---

## The One Critical Fix

The `r.Context()` parent with a 10-second HTTP `WriteTimeout` will make the system appear to work in development (short connections) but **every production WebSocket connection will be killed after 10 seconds**. This must be fixed before any other work:

```go
// gateway.go — replace:
ctx, cancel := context.WithCancel(r.Context())

// with:
ctx, cancel := context.WithCancel(context.Background())
// and separately clear the HTTP write deadline:
rc := http.NewResponseController(w)
_ = rc.SetWriteDeadline(time.Time{}) // disable HTTP timeout for this connection
```
