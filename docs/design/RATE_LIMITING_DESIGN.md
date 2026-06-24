# Rate Limiting Design

## WebSocket Market Data Gateway

### Goal

Design a rate limiting strategy for a real-time market data gateway that protects:

* Gateway resources
* Topic Manager resources
* Subscription infrastructure
* Other clients

while maintaining low latency and predictable behavior.

Target environment:

```text id="qk8gxw"
5000+ Concurrent Clients

10000+ Market Updates/sec

Heavy Read Workload
```

Rate limiting applies to:

```text id="4v8bbh"
Subscribe Requests

Unsubscribe Requests

Control Messages

Connection Establishment
```

Market data delivery itself should generally not be rate limited.

---

# 1. Why Rate Limiting Exists

Without protection:

```text id="7d08rf"
Client
      ↓

Subscribe(AAPL)

Subscribe(MSFT)

Subscribe(GOOG)

Subscribe(...)
```

can generate thousands of requests per second.

Consequences:

```text id="8uytfz"
CPU Exhaustion

Lock Contention

Registry Growth

Memory Pressure
```

A single abusive client can degrade service for all users.

---

# 2. Threat Model

## Scenario 1

Subscription Spam

```text id="o2apv4"
10000 Subscribe Requests/sec
```

against the Topic Manager.

Impact:

```text id="ab83cc"
Registry Contention

Increased Latency
```

---

## Scenario 2

Connection Churn

```text id="v2pk8v"
Connect
Disconnect
Reconnect
```

thousands of times per second.

Impact:

```text id="plz5wp"
Connection Management Overhead
```

---

## Scenario 3

Malformed Clients

Example:

```text id="h15mzs"
Infinite Retry Loop
```

from buggy software.

Impact:

```text id="4pnijv"
Resource Waste
```

---

## Scenario 4

Intentional Abuse

Example:

```text id="7f4jzo"
Bot Network
```

attempting to overwhelm the gateway.

Impact:

```text id="4jcv2m"
Denial Of Service
```

---

# 3. Rate Limiting Scope

## Recommended Limits

### Connection Creation

```text id="if34yh"
Connections Per Minute
```

---

### Subscribe Requests

```text id="jox1d8"
Subscribe Requests Per Second
```

---

### Unsubscribe Requests

```text id="slg7v2"
Unsubscribe Requests Per Second
```

---

### Control Messages

```text id="c9wgln"
Administrative Messages
```

---

## Not Recommended

Do not rate limit:

```text id="kz6q9z"
Market Data Delivery
```

The platform exists to distribute market data.

Limiting delivery defeats its purpose.

---

# 4. Fixed Window Algorithm

## Concept

Time divided into fixed intervals.

Example:

```text id="e4eovc"
Window
=
1 Second
```

Limit:

```text id="skp6zi"
100 Requests
```

per window.

---

## Example

```text id="3wntkq"
12:00:00
100 Requests
```

Allowed.

```text id="4nj61h"
12:00:01
Counter Resets
```

Client receives another:

```text id="1t1p6f"
100 Requests
```

immediately.

---

# Advantages

Simple.

```text id="5g06xl"
Low Memory

Low CPU

Easy To Implement
```

---

# Disadvantages

Boundary problem.

Example:

```text id="1knc6x"
100 Requests
At End Of Window

100 Requests
At Beginning Of Next Window
```

Effective burst:

```text id="4a49qa"
200 Requests
```

despite limit of:

```text id="htz3zt"
100
```

Produces uneven traffic.

---

# 5. Leaky Bucket Algorithm

## Concept

Requests enter a bucket.

Bucket drains at a fixed rate.

Architecture:

```text id="dcjlwm"
Incoming Requests
        ↓

     Bucket
        ↓

 Fixed Output Rate
```

---

## Behavior

Traffic becomes smooth.

Example:

```text id="g2wuws"
Burst
100 Requests
```

is transformed into:

```text id="8a1ddx"
10/sec

10/sec

10/sec
```

---

# Advantages

Excellent traffic smoothing.

Predictable load.

Stable downstream systems.

---

# Disadvantages

Punishes legitimate bursts.

Example:

```text id="u77vgm"
Client Reconnects

Subscribes To
50 Symbols
```

Leaky bucket may delay valid requests.

Not ideal for market data clients.

---

# 6. Token Bucket Algorithm

## Concept

Tokens accumulate over time.

Example:

```text id="pn83fu"
Rate
=
10 Tokens/sec
```

Maximum capacity:

```text id="0k8vhv"
100 Tokens
```

---

## Operation

Each request consumes a token.

Example:

```text id="s6jc18"
Subscribe Request
      ↓
Consume Token
```

If tokens exist:

```text id="wvv7wv"
Allowed
```

Otherwise:

```text id="vjib9m"
Rejected
```

---

## Burst Support

Client idle for some time:

```text id="g1q0rz"
Tokens Accumulate
```

Example:

```text id="o7t8gc"
100 Available Tokens
```

Client reconnects:

```text id="rzt15r"
50 Subscribe Requests
```

Processed immediately.

This is desirable.

---

# Advantages

Supports bursts.

Fair.

Predictable.

Widely used in production systems.

---

# Disadvantages

Slightly more complex.

Requires token accounting.

Still very manageable.

---

# 7. Algorithm Comparison

| Property                | Fixed Window | Leaky Bucket | Token Bucket |
| ----------------------- | ------------ | ------------ | ------------ |
| Simplicity              | Excellent    | Good         | Good         |
| Burst Handling          | Poor         | Poor         | Excellent    |
| Fairness                | Moderate     | Good         | Excellent    |
| Resource Protection     | Moderate     | Excellent    | Excellent    |
| Market Data Suitability | Moderate     | Moderate     | Excellent    |
| Production Adoption     | High         | High         | Very High    |

---

# 8. Market Data Specific Considerations

Real trading clients behave differently from REST APIs.

Example:

```text id="0x6j9d"
Client Connects
```

Immediately:

```text id="q2v90j"
Subscribe AAPL

Subscribe MSFT

Subscribe GOOG

Subscribe TSLA

Subscribe NVDA
```

A short burst is normal.

---

## Fixed Window Problem

May allow:

```text id="e4h4kh"
Huge Window Boundary Bursts
```

creating load spikes.

---

## Leaky Bucket Problem

May artificially delay:

```text id="i4w3yl"
Legitimate Startup Activity
```

creating unnecessary latency.

---

## Token Bucket Benefit

Supports:

```text id="6aj8nt"
Short Bursts
```

while preventing:

```text id="d9xnwx"
Sustained Abuse
```

This matches market data behavior well.

---

# 9. Multi-Layer Rate Limiting

Recommended architecture:

```text id="sd6g4v"
Connection Limit
        ↓

Client Rate Limit
        ↓

Subscription Limit
        ↓

Topic Manager
```

---

## Layer 1

Connection Protection

Example:

```text id="pm0x2j"
10 New Connections/sec
```

per client identity.

Protects gateway resources.

---

## Layer 2

Subscription Requests

Example:

```text id="qxb5jy"
50 Subscribe Requests/sec
```

per connection.

Protects Topic Manager.

---

## Layer 3

Maximum Active Subscriptions

Example:

```text id="tzf5s4"
1000 Symbols
```

per client.

Protects memory usage.

---

# 10. Failure Handling

When limits are exceeded:

```text id="xsk2e8"
Warning
      ↓
Throttle
      ↓
Disconnect
```

depending on severity.

---

## Recommended Policy

Occasional violation:

```text id="8cbog5"
Reject Request
```

Repeated violation:

```text id="m3bwj7"
Temporary Ban
```

Persistent abuse:

```text id="0zpdjt"
Disconnect
```

---

# 11. Observability

Track:

```text id="hm52k5"
rate_limit_hits

requests_rejected

active_tokens

subscription_requests
```

---

## Abuse Metrics

```text id="mof1wr"
top_clients

violations

disconnects
```

These become extremely useful during incidents.

---

# 12. Recommended Production Design

For a WebSocket market data gateway:

### Algorithm

```text id="qzshpb"
Token Bucket
```

---

### Limits

```text id="jbk15u"
Connection Creation

Subscribe Requests

Unsubscribe Requests
```

---

### Additional Protection

```text id="8e31ee"
Maximum Active Subscriptions
```

per client.

---

### Enforcement

```text id="3j31ni"
Reject
Throttle
Disconnect
```

depending on severity.

---

# Final Recommendation

For a market data platform serving:

```text id="gvvt6u"
5000+ Concurrent Clients
```

the best choice is:

```text id="wt3ts5"
Token Bucket
```

because it provides:

```text id="k9yr9l"
Burst Tolerance

Fairness

Abuse Prevention

Low Overhead

Excellent User Experience
```

while matching the real behavior of trading applications that frequently perform short bursts of subscription activity during connection establishment and recovery.

If this were a quantitative trading system, I would implement:

```text id="ps0qdl"
Per-Client Token Bucket
+
Connection Rate Limits
+
Subscription Caps
+
Observability Metrics
```

which provides the best balance of operational safety, scalability, and client experience.
