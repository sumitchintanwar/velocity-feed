package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/sumit/rtmds/internal/config"
)

// setupMockRedis starts a miniredis instance and returns its address.
func setupMockRedis(t *testing.T) *miniredis.Miniredis {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	return mr
}

// buildGatewayConfig creates a config for a gateway node.
func buildGatewayConfig(id, redisAddr, port string) *config.Config {
	cfg, _ := config.LoadEnv()
	p, _ := strconv.Atoi(port)
	cfg.Server.Port = p
	cfg.Server.Host = "127.0.0.1"
	// Set an environment variable or field if the config supports ID natively
	// The codebase uses a hack for GatewayID. We assume Server.Host:Server.Port is the ID
	// internally unless overriden.
	cfg.Redis.Enabled = true
	cfg.Redis.Addr = redisAddr
	cfg.Discovery.Enabled = true
	cfg.Discovery.TTL = 5 * time.Second
	cfg.Discovery.HeartbeatInterval = 1 * time.Second

	// We only need the subscriber/discovery portion for this test
	cfg.Feed.Enabled = false
	cfg.Database.Enabled = false
	cfg.Snapshot.Enabled = false

	return cfg
}

func startApp(t *testing.T, cfg *config.Config, id string) (*App, context.CancelFunc) {
	app, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create app %s: %v", id, err)
	}

	// Override ID for the test since we injected it via cfg
	// The routing engine gets its ID from `a.cfg.Server.GetGatewayID()`.
	app.cfg.Server.Host = "127.0.0.1"

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = app.Run(ctx)
	}()

	// Wait for server to be ready
	url := fmt.Sprintf("http://127.0.0.1:%d/health", cfg.Server.Port)
	for i := 0; i < 50; i++ {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}

	return app, cancel
}

func TestRoutingIntegration_Redirect(t *testing.T) {
	// 1. Setup Redis
	mr := setupMockRedis(t)

	// 2. Setup Gateway A (Port 8081)
	cfgA := buildGatewayConfig("gateway-A", mr.Addr(), "8081")
	appA, cancelA := startApp(t, cfgA, "gateway-A")
	defer cancelA()

	// 3. Setup Gateway B (Port 8082)
	cfgB := buildGatewayConfig("gateway-B", mr.Addr(), "8082")
	appB, cancelB := startApp(t, cfgB, "gateway-B")
	defer cancelB()

	// Wait for topology to sync. Both nodes should discover each other.
	time.Sleep(3 * time.Second)

	// Find a symbol that Gateway B owns.
	// Since both nodes are in the ring, roughly 50% of symbols belong to B.
	var targetSymbol string
	for i := 0; i < 100; i++ {
		sym := fmt.Sprintf("SYM_%d", i)
		owner := appA.partitionMgr.GatewayForSymbol(sym)
		if owner == appB.cfg.Server.GetGatewayID() {
			targetSymbol = sym
			break
		}
	}

	if targetSymbol == "" {
		t.Fatalf("could not find a symbol owned by Gateway B")
	}

	t.Logf("Symbol %s is owned by %s", targetSymbol, appB.cfg.Server.GetGatewayID())

	// 4. Connect client to Gateway A
	urlA := fmt.Sprintf("ws://127.0.0.1:8081/ws")
	dialer := websocket.DefaultDialer
	conn, resp, err := dialer.Dial(urlA, nil)
	if err != nil {
		t.Fatalf("failed to connect to gateway A: %v", err)
	}
	defer conn.Close()

	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected 101, got %d", resp.StatusCode)
	}

	// 5. Send subscription request for targetSymbol
	req := map[string]interface{}{
		"action":  "subscribe",
		"symbols": []string{targetSymbol},
	}
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("failed to write subscribe request: %v", err)
	}

	// 6. Expect Redirect Control Frame
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	msgType, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	if msgType != websocket.TextMessage {
		t.Fatalf("expected text message, got %v", msgType)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if response["type"] != "redirect" {
		t.Fatalf("expected redirect control frame, got: %v", string(payload))
	}

	payloadMap, ok := response["payload"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected payload to be a map, got: %v", response["payload"])
	}

	url, ok := payloadMap["url"].(string)
	if !ok || !strings.Contains(url, "8082") {
		t.Fatalf("expected redirect URL to point to Gateway B (8082), got: %v", url)
	}

	t.Logf("Successfully received redirect to: %s", url)
}

// TestRoutingIntegration_Stress concurrent lookups during topology changes
func TestRoutingIntegration_Stress(t *testing.T) {
	mr := setupMockRedis(t)

	cfgA := buildGatewayConfig("gateway-A", mr.Addr(), "8083")
	_, cancelA := startApp(t, cfgA, "gateway-A")
	defer cancelA()

	// Connect 100 websocket clients to Gateway A
	var conns []*websocket.Conn
	urlA := fmt.Sprintf("ws://127.0.0.1:8083/ws")
	for i := 0; i < 100; i++ {
		conn, _, err := websocket.DefaultDialer.Dial(urlA, nil)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		conns = append(conns, conn)
	}

	// Fire and forget subscriptions in parallel
	for i, conn := range conns {
		go func(c *websocket.Conn, idx int) {
			req := map[string]interface{}{
				"action":  "subscribe",
				"symbols": []string{fmt.Sprintf("SYM_%d", idx)},
			}
			_ = c.WriteJSON(req)
			// Drain responses
			for {
				_, _, err := c.ReadMessage()
				if err != nil {
					return
				}
			}
		}(conn, i)
	}

	// Mid-stress, start Gateway B
	cfgB := buildGatewayConfig("gateway-B", mr.Addr(), "8084")
	_, cancelB := startApp(t, cfgB, "gateway-B")

	// Wait a moment for B to register and Engine to rebalance
	time.Sleep(2 * time.Second)

	cancelB()

	// Clean up
	for _, c := range conns {
		_ = c.Close()
	}
}
