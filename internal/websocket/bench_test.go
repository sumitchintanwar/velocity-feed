package websocket

import (
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/topicmanager"
)

var (
	sharedGateway  *Gateway
	sharedTM       topicmanager.Manager
	sharedURL      string
	sharedServer   *httptest.Server
)

func TestMain(m *testing.M) {
	metrics, _ := platform.NewMetrics("bench_ws")
	sharedTM = topicmanager.New(0)
	sharedGateway = NewGateway(sharedTM, zerolog.Nop(), metrics)
	sharedServer = httptest.NewServer(sharedGateway.Handler())
	sharedURL = "ws" + strings.TrimPrefix(sharedServer.URL, "http") + "/ws"
	
	code := m.Run()
	sharedServer.Close()
	os.Exit(code)
}

func benchSetup(b *testing.B) (*Gateway, topicmanager.Manager, string) {
	b.Helper()
	return sharedGateway, sharedTM, sharedURL
}

func benchDial(b *testing.B, url string) *websocket.Conn {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	return conn
}

func benchClose(conn *websocket.Conn) {
	_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	_ = conn.Close()
}

func benchSubscribeSync(conn *websocket.Conn, symbols ...string) {
	req := ClientMessage{Action: "subscribe", Symbols: symbols}
	_ = conn.WriteJSON(req)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, _ = conn.ReadMessage() // subscribed confirmation
}

// ---------- Connect throughput ----------

func BenchmarkGateway_Connect(b *testing.B) {
	_, _, wsURL := benchSetup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn := benchDial(b, wsURL)
		benchClose(conn)
	}
}

func BenchmarkGateway_ConnectParallel(b *testing.B) {
	_, _, wsURL := benchSetup(b)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn := benchDial(b, wsURL)
			benchClose(conn)
		}
	})
}

// ---------- Publish throughput (events delivered to N connected clients) ----------

func BenchmarkGateway_Publish_1Client(b *testing.B) {
	benchGatewayPublish(b, 1)
}

func BenchmarkGateway_Publish_10Clients(b *testing.B) {
	benchGatewayPublish(b, 10)
}

func BenchmarkGateway_Publish_100Clients(b *testing.B) {
	benchGatewayPublish(b, 100)
}

func benchGatewayPublish(b *testing.B, numClients int) {
	_, tm, wsURL := benchSetup(b)

	conns := make([]*websocket.Conn, numClients)
	for i := range conns {
		conns[i] = benchDial(b, wsURL)
		benchSubscribeSync(conns[i], "AAPL")
	}
	defer func() {
		for _, c := range conns {
			benchClose(c)
		}
	}()

	ev := marketdata.Quote{
		Symbol: "AAPL", Type: marketdata.QuoteTypeTrade,
		Price: 150.0, Timestamp: time.Now(),
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm.Publish(ctx, ev)
	}
}

// ---------- Subscribe/Unsubscribe throughput ----------

func BenchmarkGateway_SubscribeUnsubscribe(b *testing.B) {
	_, _, wsURL := benchSetup(b)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn := benchDial(b, wsURL)
		benchSubscribeSync(conn, "AAPL")
		benchClose(conn)
	}
}

// ---------- Mixed workload (80% publish, 20% connect/subscribe) ----------

func BenchmarkGateway_Mixed80_20(b *testing.B) {
	_, tm, wsURL := benchSetup(b)

	// Pre-connect 50 clients subscribed to AAPL.
	permanent := make([]*websocket.Conn, 50)
	for i := range permanent {
		permanent[i] = benchDial(b, wsURL)
		benchSubscribeSync(permanent[i], "AAPL")
	}
	defer func() {
		for _, c := range permanent {
			benchClose(c)
		}
	}()

	ev := marketdata.Quote{
		Symbol: "AAPL", Type: marketdata.QuoteTypeTrade,
		Price: 150.0, Timestamp: time.Now(),
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%5 == 0 {
			// 20% subscribe
			conn := benchDial(b, wsURL)
			benchSubscribeSync(conn, "AAPL")
			benchClose(conn)
		} else {
			// 80% publish
			tm.Publish(ctx, ev)
		}
	}
}

// ---------- Concurrent publish stress ----------

func BenchmarkGateway_ConcurrentPublish_100Clients(b *testing.B) {
	_, tm, wsURL := benchSetup(b)

	const numClients = 100
	conns := make([]*websocket.Conn, numClients)
	for i := range conns {
		conns[i] = benchDial(b, wsURL)
		benchSubscribeSync(conns[i], "AAPL")
	}
	defer func() {
		for _, c := range conns {
			benchClose(c)
		}
	}()

	ev := marketdata.Quote{
		Symbol: "AAPL", Type: marketdata.QuoteTypeTrade,
		Price: 150.0, Timestamp: time.Now(),
	}
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tm.Publish(ctx, ev)
		}
	})
}

// ---------- Connect/disconnect churn ----------

func BenchmarkGateway_ConnectChurn(b *testing.B) {
	_, _, wsURL := benchSetup(b)

	// Keep 10 persistent connections while churning through others.
	persistent := make([]*websocket.Conn, 10)
	for i := range persistent {
		persistent[i] = benchDial(b, wsURL)
		benchSubscribeSync(persistent[i], "AAPL")
	}
	defer func() {
		for _, c := range persistent {
			benchClose(c)
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn := benchDial(b, wsURL)
		benchSubscribeSync(conn, "AAPL")
		benchClose(conn)
	}
}

// ---------- Full end-to-end latency ----------

func BenchmarkGateway_EndToEnd_100Clients(b *testing.B) {
	_, tm, wsURL := benchSetup(b)

	const numClients = 100
	conns := make([]*websocket.Conn, numClients)
	for i := range conns {
		conns[i] = benchDial(b, wsURL)
		benchSubscribeSync(conns[i], "AAPL")
	}
	defer func() {
		for _, c := range conns {
			benchClose(c)
		}
	}()

	ev := marketdata.Quote{
		Symbol: "AAPL", Type: marketdata.QuoteTypeTrade,
		Price: 150.0, Timestamp: time.Now(),
	}
	ctx := context.Background()

	// Drain goroutines to avoid interference.
	var drainWg sync.WaitGroup
	for _, conn := range conns {
		drainWg.Add(1)
		go func(c *websocket.Conn) {
			defer drainWg.Done()
			for {
				_, _, err := c.ReadMessage()
				if err != nil {
					return
				}
			}
		}(conn)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm.Publish(ctx, ev)
	}
	b.StopTimer()

	for _, c := range conns {
		benchClose(c)
	}
	drainWg.Wait()
}

// ---------- Publish scaling with varying subscriber counts ----------

func BenchmarkGateway_PublishScaling(b *testing.B) {
	for _, numSubs := range []int{1, 10, 50, 100, 500} {
		b.Run(fmt.Sprintf("subs_%d", numSubs), func(b *testing.B) {
			benchGatewayPublish(b, numSubs)
		})
	}
}
