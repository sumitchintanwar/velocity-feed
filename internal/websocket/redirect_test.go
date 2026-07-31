package websocket

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type mockRedirector struct {
	routes map[string]string
}

func (m *mockRedirector) RedirectTarget(symbol string) string {
	return m.routes[symbol] // Returns empty string if not found, meaning local
}

// TestGateway_Redirect sends a subscribe and verifies the gateway responds with a redirect control message.
func TestGateway_Redirect(t *testing.T) {
	gw, _ := setupTestGateway(t)

	// Set up the redirector: AAPL goes to another server, MSFT stays local.
	redir := &mockRedirector{
		routes: map[string]string{
			"AAPL": "ws://gateway-2/ws",
		},
	}
	gw.SetRedirector(redir)

	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	conn := dialWS(t, wsURL)
	defer conn.Close()

	// Subscribe to AAPL (redirected) and MSFT (local)
	req := ClientMessage{Action: "subscribe", Symbols: []string{"AAPL", "MSFT"}}
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("write: %v", err)
	}

	// We expect two messages: one redirect for AAPL, one subscribed for MSFT.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	var receivedRedirect bool
	var receivedSubscribed bool

	for i := 0; i < 2; i++ {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}

		var smsg ServerMessage
		if err := json.Unmarshal(msg, &smsg); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}

		if smsg.Type == "redirect" {
			receivedRedirect = true
			payloadBytes, _ := json.Marshal(smsg.Payload)
			var payload RedirectPayload
			json.Unmarshal(payloadBytes, &payload)

			if payload.Symbol != "AAPL" {
				t.Errorf("expected redirect for AAPL, got %s", payload.Symbol)
			}
			if payload.URL != "ws://gateway-2/ws" {
				t.Errorf("expected redirect to ws://gateway-2/ws, got %s", payload.URL)
			}
		} else if smsg.Type == "subscribed" {
			receivedSubscribed = true
			payloadBytes, _ := json.Marshal(smsg.Payload)
			var symbols []string
			json.Unmarshal(payloadBytes, &symbols)

			if len(symbols) != 1 || symbols[0] != "MSFT" {
				t.Errorf("expected subscribed only to MSFT, got %v", symbols)
			}
		}
	}

	if !receivedRedirect || !receivedSubscribed {
		t.Errorf("Missing expected messages: redirect=%v, subscribed=%v", receivedRedirect, receivedSubscribed)
	}
}

// TestGateway_AutoReconnectClient simulates a thick client SDK that automatically
// follows redirects across multiple gateways.
func TestGateway_AutoReconnectClient(t *testing.T) {
	// Gateway 1 (Entry point)
	gw1, _ := setupTestGateway(t)
	// Gateway 2 (Intermediate)
	gw2, _ := setupTestGateway(t)
	// Gateway 3 (Final Destination)
	gw3, tm3 := setupTestGateway(t)

	ts1 := httptest.NewServer(gw1.Handler())
	defer ts1.Close()
	ts2 := httptest.NewServer(gw2.Handler())
	defer ts2.Close()
	ts3 := httptest.NewServer(gw3.Handler())
	defer ts3.Close()

	wsURL1 := "ws" + strings.TrimPrefix(ts1.URL, "http") + "/ws"
	wsURL2 := "ws" + strings.TrimPrefix(ts2.URL, "http") + "/ws"
	wsURL3 := "ws" + strings.TrimPrefix(ts3.URL, "http") + "/ws"

	// Routing setup:
	// Client asks GW1 for AAPL -> GW1 redirects to GW2
	// Client asks GW2 for AAPL -> GW2 redirects to GW3
	// Client asks GW3 for AAPL -> GW3 accepts.
	gw1.SetRedirector(&mockRedirector{routes: map[string]string{"AAPL": wsURL2}})
	gw2.SetRedirector(&mockRedirector{routes: map[string]string{"AAPL": wsURL3}})
	gw3.SetRedirector(&mockRedirector{routes: map[string]string{}}) // accepts AAPL

	// Auto-reconnecting client implementation
	connectWithRedirects := func(startURL string, symbol string, maxRedirects int) (*websocket.Conn, error) {
		currentURL := startURL

		for attempt := 0; attempt <= maxRedirects; attempt++ {
			conn := dialWS(t, currentURL)

			req := ClientMessage{Action: "subscribe", Symbols: []string{symbol}}
			_ = conn.WriteJSON(req)

			_ = conn.SetReadDeadline(time.Now().Add(time.Second))
			_, msg, err := conn.ReadMessage()
			if err != nil {
				conn.Close()
				return nil, err
			}

			var smsg ServerMessage
			json.Unmarshal(msg, &smsg)

			if smsg.Type == "redirect" {
				payloadBytes, _ := json.Marshal(smsg.Payload)
				var payload RedirectPayload
				json.Unmarshal(payloadBytes, &payload)

				conn.Close()             // Disconnect from current gateway
				currentURL = payload.URL // Follow redirect
				continue
			} else if smsg.Type == "subscribed" {
				return conn, nil
			}
		}
		t.Fatalf("Exceeded max redirects")
		return nil, nil
	}

	// Start connection process at GW1
	conn, err := connectWithRedirects(wsURL1, "AAPL", 5)
	if err != nil {
		t.Fatalf("auto-reconnect client failed: %v", err)
	}
	defer conn.Close()

	// Verify the final connection landed on GW3
	if n := tm3.SubscriberCount("AAPL"); n != 1 {
		t.Fatalf("expected 1 subscriber on GW3, got %d", n)
	}
}
