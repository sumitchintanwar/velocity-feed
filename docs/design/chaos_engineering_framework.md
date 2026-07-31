# Chaos Engineering Framework Design

## 1. Objective
Design a production-grade Chaos Engineering Framework to validate the resilience, failover, and fault-tolerance of the Real-Time Market Data System (RTMDS). This framework verifies that our state machine, heartbeat protocols, automatic reconnects, and partition routing work exactly as expected under extreme, unpredictable failure scenarios.

## 2. Failure Scenarios and Injection

### 2.1. Gateway Failures

#### Kill Gateway
*   **Description**: Hard-kill the gateway process (`SIGKILL`) without a graceful shutdown sequence.
*   **Injection**: `kill -9 <pid>` or Docker API `docker kill`.
*   **Expected Recovery**: The load balancer immediately removes the node. Clients experience connection drops (abnormal closures). Client SDKs engage exponential backoff and route to a surviving gateway. The surviving gateway fetches the last sequence from the WAL and replays missed messages.

#### Pause Gateway
*   **Description**: Freeze the gateway process to simulate extreme CPU starvation or GC pauses.
*   **Injection**: Send `SIGSTOP` to the gateway process, hold for $N$ seconds, then send `SIGCONT`.
*   **Expected Recovery**: Heartbeats to clients will stop. Clients detect a heartbeat timeout, close the connection, and initiate the reconnect flow to a different node.

#### Gateway Restart (Graceful)
*   **Description**: Execute a graceful restart.
*   **Injection**: Send `SIGTERM` or invoke the `/shutdown` HTTP endpoint.
*   **Expected Recovery**: The Gateway transitions to `Draining` state. New connections are rejected (HTTP 503). Existing clients receive a `goaway` message, gracefully drop their active connection, and migrate. Active replays finish before final closure.

### 2.2. Network Failures
*Using `tc` (Traffic Control) or `toxiproxy` / `pumba` for Docker-based injection.*

#### Network Latency
*   **Description**: Introduce high RTT latency (e.g., 500ms - 2000ms) on gateway ports.
*   **Injection**: `tc qdisc add dev eth0 root netem delay 500ms`.
*   **Expected Recovery**: The system must sustain backpressure. TCP buffers fill up, and the Gateway's non-blocking `writePump` eventually reaches its buffer limits, leading to dropped messages (`WSSlowConsumers`). The client might miss a heartbeat and reconnect.

#### Packet Loss
*   **Description**: Drop a percentage of network packets.
*   **Injection**: `tc qdisc add dev eth0 root netem loss 10%`.
*   **Expected Recovery**: TCP retransmission handles minor loss. Severe loss triggers heartbeat timeouts on either the client or the server. The server registers a dead client, cleans up subscriptions, and the client reconnects.

#### Packet Duplication
*   **Description**: Duplicate a percentage of TCP packets.
*   **Injection**: `tc qdisc add dev eth0 root netem duplicate 5%`.
*   **Expected Recovery**: TCP inherently handles deduplication at the protocol level. At the application level, if the client receives a duplicated sequence number during a replay race condition, it must idempotently discard it based on the `seq` number tracking.

#### Temporary Partition
*   **Description**: Completely blackhole traffic between a Gateway node and the PostgreSQL WAL / PubSub backbone, or between the Gateway and clients.
*   **Injection**: `iptables -A INPUT -p tcp --dport 5432 -j DROP`.
*   **Expected Recovery**: The Gateway loses the ability to fetch new events or persist state. Its health monitor detects partition disconnection and transitions to `Offline` or `Warning`. Load balancers fail the health check and route clients elsewhere.

### 2.3. Client Failures

#### Client Disconnect
*   **Description**: Client abruptly closes the TCP socket without sending a WebSocket Close frame.
*   **Injection**: A chaos test client hard-closes the underlying `net.Conn`.
*   **Expected Recovery**: The Gateway `readPump` detects `unexpected EOF`. The client is unregistered, and `Gateway.activeCount` decrements. No goroutine leaks occur.

## 3. Metrics and Observability

To validate chaos experiments, we rely on Prometheus metrics:

1.  **State Transitions**: `rtmds_gateway_state_transitions_total{from="Healthy", to="Draining"}` validates that a kill/restart initiates the correct lifecycle.
2.  **Heartbeat Timeouts**: `rtmds_client_heartbeat_timeouts_total` spikes when network latency or pause scenarios are injected.
3.  **Reconnect Success Rate**: `rtmds_client_reconnects_total` and `rtmds_client_resume_success_total`. High reconnects with 100% resume success means the FSM is resilient.
4.  **Dropped Connections**: `rtmds_ws_read_errors_total` tracks abnormal closures during network partitions.

## 4. Production Tradeoffs

1.  **Detection Time vs. False Positives**: Setting strict heartbeat timeouts (e.g., 5 seconds) detects dead nodes instantly but triggers massive thundering herds (false positives) during temporary GC pauses. Our 30s-90s ping/pong window sacrifices immediate detection for stability.
2.  **Resource Limits during Reconnect Storms**: When 5,000 clients migrate simultaneously (Kill Gateway), the surviving nodes face CPU/Memory spikes for WAL fetching. The FSM limits concurrent reconnects (`connRateLimiter`) to protect the cluster, trading longer recovery time for cluster survival.
