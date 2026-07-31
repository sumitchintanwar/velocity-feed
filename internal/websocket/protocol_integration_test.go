package websocket

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sumit/rtmds/pkg/client"
	"github.com/sumit/rtmds/pkg/marketdata"
	"github.com/sumit/rtmds/pkg/protocol"
)

// TestGateway_FullProtocolIntegration verifies end-to-end routing and serialization
// across all three supported formats (JSON, Protocol Buffers, FlatBuffers).
func TestGateway_FullProtocolIntegration(t *testing.T) {
	// 1. Setup Gateway
	gw, tm := setupTestGateway(t)

	// Ensure all serializers are registered for the test gateway
	// (Normally done in NewGateway, but setupTestGateway uses NewGateway which we just updated)

	ts := httptest.NewServer(gw.Handler())
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"

	opts := client.Options{
		DialTimeout: 2 * time.Second,
		Reconnect:   false,
	}

	// 2. Connect three clients, each enforcing a specific protocol format

	// Client 1: JSON
	cJSON, err := client.Connect(wsURL, opts, protocol.FormatJSON)
	if err != nil {
		t.Fatalf("JSON client connect failed: %v", err)
	}
	defer cJSON.Close()

	// Client 2: Protobuf
	cProto, err := client.Connect(wsURL, opts, protocol.FormatProtobuf)
	if err != nil {
		t.Fatalf("Protobuf client connect failed: %v", err)
	}
	defer cProto.Close()

	// Client 3: FlatBuffers
	cFB, err := client.Connect(wsURL, opts, protocol.FormatFlatBuffers)
	if err != nil {
		t.Fatalf("FlatBuffers client connect failed: %v", err)
	}
	defer cFB.Close()

	// Give clients time to connect
	if !waitForClientCount(gw, 3, 2*time.Second) {
		t.Fatalf("expected 3 clients connected, got %d", gw.ClientCount())
	}

	// 3. Subscribe all clients to "AAPL"
	symbol := "AAPL"
	if err := cJSON.Subscribe(symbol); err != nil {
		t.Fatalf("JSON subscribe failed: %v", err)
	}
	if err := cProto.Subscribe(symbol); err != nil {
		t.Fatalf("Protobuf subscribe failed: %v", err)
	}
	if err := cFB.Subscribe(symbol); err != nil {
		t.Fatalf("FlatBuffers subscribe failed: %v", err)
	}

	// Give subscriptions time to propagate
	time.Sleep(100 * time.Millisecond)

	// 4. Publish a test Quote to the Gateway via TopicManager
	now := time.Now().Truncate(time.Millisecond) // Flatbuffers truncates nanoseconds by default if not set
	quote := &marketdata.Quote{
		Symbol:    symbol,
		Type:      marketdata.QuoteTypeQuote,
		Price:     150.25,
		Volume:    1000,
		Provider:  "integration_test",
		Timestamp: now,
	}

	tm.Publish(context.Background(), quote)

	// 5. Verify all clients receive and deserialize the quote correctly

	verifyClientReceive := func(name string, c *client.Client) {
		t.Helper()
		select {
		case ev := <-c.Receive():
			q, ok := ev.(*marketdata.Quote)
			if !ok {
				t.Fatalf("%s client: expected Quote, got %T", name, ev)
			}
			if q.Symbol != symbol {
				t.Errorf("%s client: expected symbol %q, got %q", name, symbol, q.Symbol)
			}
			if q.Price != 150.25 {
				t.Errorf("%s client: expected price 150.25, got %v", name, q.Price)
			}
			if q.Volume != 1000 {
				t.Errorf("%s client: expected volume 1000, got %v", name, q.Volume)
			}
			if q.Provider != "integration_test" {
				t.Errorf("%s client: expected provider 'integration_test', got %q", name, q.Provider)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s client: timeout waiting for quote", name)
		}
	}

	verifyClientReceive("JSON", cJSON)
	verifyClientReceive("Protobuf", cProto)
	verifyClientReceive("FlatBuffers", cFB)
}
