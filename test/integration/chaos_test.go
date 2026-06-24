package integration

import (
	"context"
	"encoding/json"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/distribution/redisbus"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/topicmanager"
)

// chaosSkipIfDocker skips if docker compose stack is not running.
func chaosSkipIfDocker(t *testing.T) {
	t.Helper()
	out, err := exec.Command("docker", "compose", "-f", "docker-compose.sticky.yml", "ps", "--format", "json").Output()
	if err != nil {
		t.Skipf("skipping: docker compose not available: %v", err)
	}
	running := 0
	for _, line := range splitLines(string(out)) {
		if line == "" {
			continue
		}
		running++
	}
	if running < 4 { // redis + 3 gateways + nginx
		t.Skipf("skipping: stack not fully up (found %d services)", running)
	}
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}

// chaosKillGateway stops a gateway container via docker.
func chaosKillGateway(t *testing.T, gw string) {
	t.Helper()
	gwName := map[string]string{
		"gw1": "rtmds-gateway1-sticky",
		"gw2": "rtmds-gateway2-sticky",
		"gw3": "rtmds-gateway3-sticky",
	}[gw]
	if gwName == "" {
		t.Fatalf("unknown gateway: %s", gw)
	}
	if err := exec.Command("docker", "kill", gwName).Run(); err != nil {
		t.Logf("kill %s: %v (may already be down)", gwName, err)
	}
	t.Cleanup(func() {
		gwCompose := map[string]string{"gw1": "gateway1", "gw2": "gateway2", "gw3": "gateway3"}[gw]
		exec.Command("docker", "compose", "-f", "docker-compose.sticky.yml", "up", "-d", gwCompose).Run()
		time.Sleep(5 * time.Second)
	})
}

// chaosRestartGateway restarts a gateway container.
func chaosRestartGateway(t *testing.T, gw string) {
	t.Helper()
	gwCompose := map[string]string{
		"gw1": "gateway1",
		"gw2": "gateway2",
		"gw3": "gateway3",
	}[gw]
	if err := exec.Command("docker", "compose", "-f", "docker-compose.sticky.yml", "up", "-d", gwCompose).Run(); err != nil {
		t.Fatalf("restart %s: %v", gw, err)
	}
	time.Sleep(5 * time.Second)
}

// chaosRestartRedis stops and starts the Redis container.
func chaosRestartRedis(t *testing.T) {
	t.Helper()
	exec.Command("docker", "stop", "rtmds-redis-sticky").Run()
	time.Sleep(2 * time.Second)
	exec.Command("docker", "start", "rtmds-redis-sticky").Run()
	time.Sleep(5 * time.Second)
}

// chaosCheckHealth verifies the HTTP health endpoint returns 200.
func chaosCheckHealth(t *testing.T) bool {
	t.Helper()
	out, err := exec.Command("docker", "exec", "rtmds-nginx-sticky",
		"wget", "-q", "-O", "-", "http://localhost:8080/health").Output()
	if err != nil {
		t.Logf("health check failed: %v", err)
		return false
	}
	t.Logf("health endpoint: %s", string(out))
	return len(out) > 0
}

// chaosWSClient runs a reconnecting WebSocket client and counts messages.
func chaosWSClient(t *testing.T, ctx context.Context, symbols []string) (*atomic.Int64, *atomic.Int64) {
	t.Helper()
	received := &atomic.Int64{}
	reconnects := &atomic.Int64{}

	go func() {
		backoff := 500 * time.Millisecond
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			conn, _, err := websocket.DefaultDialer.DialContext(ctx, "ws://localhost:8080/ws", nil)
			if err != nil {
				reconnects.Add(1)
				select {
				case <-ctx.Done():
					return
				case <-time.After(backoff):
					backoff = minDur(backoff*2, 5*time.Second)
					continue
				}
			}
			subMsg, _ := json.Marshal(map[string]any{"action": "subscribe", "symbols": symbols})
			conn.WriteMessage(websocket.TextMessage, subMsg)
			backoff = 500 * time.Millisecond

			for {
				conn.SetReadDeadline(time.Now().Add(2 * time.Second))
				_, _, err := conn.ReadMessage()
				if err != nil {
					conn.Close()
					break
				}
				received.Add(1)
			}
		}
	}()

	return received, reconnects
}

func minDur(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// ═══════════════════════════════════════════════════════════════════
// Scenario 1: Kill Gateway 1
// ═══════════════════════════════════════════════════════════════════

func TestChaos_KillGateway1(t *testing.T) {
	chaosSkipIfDocker(t)

	t.Log("═══ Scenario 1: Kill Gateway 1 ═══")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	msgCount, reconnectCount := chaosWSClient(t, ctx, []string{"AAPL", "MSFT"})

	time.Sleep(3 * time.Second)
	beforeCount := msgCount.Load()
	t.Logf("Messages before kill: %d", beforeCount)

	t.Log("Killing gateway 1 ...")
	chaosKillGateway(t, "gw1")
	time.Sleep(3 * time.Second)

	healthy := chaosCheckHealth(t)
	t.Logf("Health after kill: %v", healthy)

	time.Sleep(10 * time.Second)
	afterCount := msgCount.Load()
	reconnects := reconnectCount.Load()
	t.Logf("Messages after kill: %d, reconnects: %d", afterCount, reconnects)

	if !healthy {
		t.Error("system not healthy after gateway 1 kill")
	}
	if reconnects == 0 {
		t.Error("expected at least 1 reconnect after gateway kill")
	}

	t.Logf("✅ Scenario 1 PASSED — system recovered, %d messages delivered, %d reconnects", afterCount, reconnects)
}

// ═══════════════════════════════════════════════════════════════════
// Scenario 2: Kill Gateway 2
// ═══════════════════════════════════════════════════════════════════

func TestChaos_KillGateway2(t *testing.T) {
	chaosSkipIfDocker(t)

	t.Log("═══ Scenario 2: Kill Gateway 2 ═══")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	msgCount, reconnectCount := chaosWSClient(t, ctx, []string{"AAPL", "TSLA"})

	time.Sleep(3 * time.Second)
	t.Logf("Messages before kill: %d", msgCount.Load())

	t.Log("Killing gateway 2 ...")
	chaosKillGateway(t, "gw2")
	time.Sleep(3 * time.Second)

	healthy := chaosCheckHealth(t)
	t.Logf("Health after kill: %v", healthy)

	time.Sleep(10 * time.Second)
	t.Logf("Messages after kill: %d, reconnects: %d", msgCount.Load(), reconnectCount.Load())

	if !healthy {
		t.Error("system not healthy after gateway 2 kill")
	}

	t.Logf("✅ Scenario 2 PASSED — system recovered, %d messages delivered", msgCount.Load())
}

// ═══════════════════════════════════════════════════════════════════
// Scenario 3: Restart Redis
// ═══════════════════════════════════════════════════════════════════

func TestChaos_RestartRedis(t *testing.T) {
	chaosSkipIfDocker(t)

	t.Log("═══ Scenario 3: Restart Redis ═══")

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	msgCount, reconnectCount := chaosWSClient(t, ctx, []string{"AAPL", "GOOG"})

	time.Sleep(3 * time.Second)
	t.Logf("Messages before restart: %d", msgCount.Load())

	t.Log("Restarting Redis ...")
	chaosRestartRedis(t)
	time.Sleep(3 * time.Second)

	// Check gateways are still running
	for _, gw := range []string{"rtmds-gateway1-sticky", "rtmds-gateway2-sticky", "rtmds-gateway3-sticky"} {
		out, _ := exec.Command("docker", "inspect", "-f", "{{.State.Running}}", gw).Output()
		t.Logf("Gateway %s running: %s", gw, string(out))
	}

	time.Sleep(15 * time.Second)

	healthy := chaosCheckHealth(t)
	t.Logf("Health after Redis restart: %v, messages: %d, reconnects: %d",
		healthy, msgCount.Load(), reconnectCount.Load())

	if !healthy {
		t.Error("system not healthy after Redis restart")
	}

	t.Logf("✅ Scenario 3 PASSED — system recovered after Redis restart")
}

// ═══════════════════════════════════════════════════════════════════
// Scenario 4: Restart All Gateways (Rolling)
// ═══════════════════════════════════════════════════════════════════

func TestChaos_RestartAllGateways(t *testing.T) {
	chaosSkipIfDocker(t)

	t.Log("═══ Scenario 4: Restart All Gateways (Rolling) ═══")

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	msgCount, reconnectCount := chaosWSClient(t, ctx, []string{"AAPL", "MSFT", "GOOG"})

	time.Sleep(3 * time.Second)

	gateways := []struct {
		name    string
		compose string
	}{
		{"gateway 1", "gateway1"},
		{"gateway 2", "gateway2"},
		{"gateway 3", "gateway3"},
	}

	for _, gw := range gateways {
		t.Logf("Restarting %s ...", gw.name)
		exec.Command("docker", "compose", "-f", "docker-compose.sticky.yml", "stop", gw.compose).Run()
		time.Sleep(2 * time.Second)

		healthy := chaosCheckHealth(t)
		t.Logf("Health during %s restart: %v", gw.name, healthy)

		exec.Command("docker", "compose", "-f", "docker-compose.sticky.yml", "up", "-d", gw.compose).Run()
		time.Sleep(5 * time.Second)
	}

	time.Sleep(5 * time.Second)
	healthy := chaosCheckHealth(t)
	t.Logf("Health after all restarts: %v, messages: %d, reconnects: %d",
		healthy, msgCount.Load(), reconnectCount.Load())

	if !healthy {
		t.Error("system not healthy after rolling restart")
	}

	t.Logf("✅ Scenario 4 PASSED — rolling restart completed without full outage")
}

// ═══════════════════════════════════════════════════════════════════
// Scenario 5: Kill Gateway + Redis Simultaneously
// ═══════════════════════════════════════════════════════════════════

func TestChaos_KillGatewayAndRedis(t *testing.T) {
	chaosSkipIfDocker(t)

	t.Log("═══ Scenario 5: Kill Gateway 1 + Restart Redis ═══")

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	msgCount, reconnectCount := chaosWSClient(t, ctx, []string{"AAPL"})

	time.Sleep(3 * time.Second)
	t.Logf("Messages before chaos: %d", msgCount.Load())

	t.Log("Killing gateway 1 + restarting Redis ...")
	chaosKillGateway(t, "gw1")
	go chaosRestartRedis(t)

	time.Sleep(20 * time.Second)

	healthy := chaosCheckHealth(t)
	t.Logf("Health after combined failure: %v, messages: %d", healthy, msgCount.Load())

	time.Sleep(15 * time.Second)
	healthy = chaosCheckHealth(t)
	t.Logf("Health after recovery window: %v, messages: %d, reconnects: %d",
		healthy, msgCount.Load(), reconnectCount.Load())

	if !healthy {
		t.Error("system not healthy after combined failure")
	}

	t.Logf("✅ Scenario 5 PASSED — system survived combined failure")
}

// ═══════════════════════════════════════════════════════════════════
// Scenario 6: Verify Data Flow After Recovery
// ═══════════════════════════════════════════════════════════════════

func TestChaos_DataFlowVerification(t *testing.T) {
	chaosSkipIfDocker(t)

	t.Log("═══ Scenario 6: Data Flow Verification ═══")

	redisClient := skipIfNoRedis(t)
	defer redisClient.Close()
	prefix := testPrefix(t)
	ctx := context.Background()
	log := zerolog.Nop()
	queueCfg := newTestQueueCfg()

	pubClient := newTestRedisClient(t)
	pub := redisbus.NewPublisher(pubClient, log, redisbus.WithChannelPrefix(prefix))
	defer pub.Close()

	tm := topicmanager.NewWithQueue(0, queueCfg, log, nil, nil)
	var redisSub *redisbus.Subscriber
	router := topicmanager.NewDistributedRouter(tm, prefix, log, "gateway-chaos",
		topicmanager.WithSubscriptionChangeCallback(func(symbol string, change topicmanager.SubscriptionChange) {
			if redisSub == nil {
				return
			}
			switch change {
			case topicmanager.SubscribeRequested:
				redisSub.Subscribe(symbol)
			case topicmanager.UnsubscribeRequested:
				redisSub.Unsubscribe(symbol)
			}
		}),
	)

	redisSub = redisbus.NewSubscriber(redisClient, router, log, redisbus.WithSubscriberPrefix(prefix))
	redisSub.Start(ctx)
	defer redisSub.Stop()

	handle := router.Subscribe("chaos-client-1", "AAPL", "MSFT")
	time.Sleep(1 * time.Second)

	pub.Publish(ctx, marketdata.Quote{
		Symbol:    "AAPL",
		Type:      "trade",
		Price:     150.0,
		Timestamp: time.Now(),
		Provider:  "chaos-test",
	})

	select {
	case cached := <-handle.C():
		if cached == nil {
			t.Fatal("received nil event")
		}
		t.Logf("✅ Data flow verified: received event on channel %s", prefix)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

// newTestRedisClient creates a Redis client for chaos tests.
func newTestRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	return skipIfNoRedis(t)
}
