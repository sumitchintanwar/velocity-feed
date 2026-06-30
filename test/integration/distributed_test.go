package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"github.com/sumit/rtmds/internal/clientqueue"
	"github.com/sumit/rtmds/internal/distribution/redisbus"
	"github.com/sumit/rtmds/internal/log"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/platform"
	"github.com/sumit/rtmds/internal/topicmanager"
)

var testPrefixCounter atomic.Int64

func skipIfNoRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := "localhost:6379"
	if v := os.Getenv("REDIS_ADDR"); v != "" {
		addr = v
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		c.Close()
		t.Skipf("skipping: Redis not available at %s: %v", addr, err)
	}
	return c
}

func testPrefix(t *testing.T) string {
	t.Helper()
	id := testPrefixCounter.Add(1)
	return fmt.Sprintf("test:%s:%d:", uuid.New().String()[:8], id)
}

func newTestQueueCfg() *clientqueue.Config {
	cfg := clientqueue.DefaultConfig()
	cfg.QueueSize = 64
	cfg.MaxAge = 100 * time.Millisecond
	return &cfg
}

func TestRedisSubscriberWithRouter(t *testing.T) {
	redisClient := skipIfNoRedis(t)
	defer redisClient.Close()
	prefix := testPrefix(t)
	ctx := context.Background()
	log := log.New(nil, "test")
	queueCfg := newTestQueueCfg()

	pubClient := redis.NewClient(&redis.Options{Addr: redisClient.Options().Addr})
	defer pubClient.Close()
	pub := redisbus.NewPublisher(pubClient, log, redisbus.WithChannelPrefix(prefix))
	defer pub.Close()

	tm := topicmanager.NewWithQueue(0, queueCfg, log, nil, nil)

	var redisSub *redisbus.Subscriber

	router := topicmanager.NewDistributedRouter(tm, prefix, log, "gateway-test",
		topicmanager.WithSubscriptionChangeCallback(func(symbol string, change topicmanager.SubscriptionChange) {
			t.Logf("callback: %s %v", symbol, change)
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

	handle := router.Subscribe("client-1", "AAPL")
	t.Logf("handle: %v", handle)

	time.Sleep(1 * time.Second)
	t.Logf("subscribed symbols: %v", redisSub.SubscribedSymbols())

	pub.Publish(ctx, marketdata.Quote{
		Symbol:    "AAPL",
		Type:      "trade",
		Price:     200.00,
		Timestamp: time.Now(),
		Provider:  "test",
	})

	select {
	case cached := <-handle.C():
		if cached == nil {
			t.Fatal("received nil event")
		}
		q, ok := cached.Event.(marketdata.Quote)
		if !ok {
			t.Fatalf("expected Quote, got %T", cached.Event)
		}
		t.Logf("SUCCESS: received %s @ %.2f", q.Symbol, q.Price)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out")
	}

	t.Logf("events routed: %d", router.EventsRouted())
	t.Logf("received: %d", redisSub.Received())
}

func TestCrossGatewayDelivery(t *testing.T) {
	redisClient := skipIfNoRedis(t)
	defer redisClient.Close()
	prefix := testPrefix(t)
	ctx := context.Background()
	log := log.New(nil, "test")
	queueCfg := newTestQueueCfg()

	pubClient := redis.NewClient(&redis.Options{Addr: redisClient.Options().Addr})
	defer pubClient.Close()
	pub := redisbus.NewPublisher(pubClient, log, redisbus.WithChannelPrefix(prefix))
	defer pub.Close()

	tmB := topicmanager.NewWithQueue(0, queueCfg, log, nil, nil)

	var redisSub *redisbus.Subscriber
	routerB := topicmanager.NewDistributedRouter(tmB, prefix, log, "gateway-B",
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

	redisSub = redisbus.NewSubscriber(redisClient, routerB, log, redisbus.WithSubscriberPrefix(prefix))
	redisSub.Start(ctx)
	defer redisSub.Stop()

	handle := routerB.Subscribe("client-B1", "AAPL")

	time.Sleep(1 * time.Second)

	if routerB.ActiveRedisSubscriptions() != 1 {
		t.Fatalf("expected 1 active Redis sub, got %d", routerB.ActiveRedisSubscriptions())
	}

	pub.Publish(ctx, marketdata.Quote{
		Symbol:    "AAPL",
		Type:      "trade",
		Price:     150.25,
		Bid:       150.20,
		Ask:       150.30,
		Volume:    1000,
		Timestamp: time.Now(),
		Provider:  "test",
	})

	select {
	case cached := <-handle.C():
		if cached == nil {
			t.Fatal("received nil event")
		}
		q, ok := cached.Event.(marketdata.Quote)
		if !ok {
			t.Fatalf("expected Quote, got %T", cached.Event)
		}
		if q.Symbol != "AAPL" {
			t.Fatalf("expected AAPL, got %s", q.Symbol)
		}
		if q.Price != 150.25 {
			t.Fatalf("expected price 150.25, got %f", q.Price)
		}
		t.Logf("SUCCESS: client on Gateway B received AAPL via Redis: price=%.2f", q.Price)

	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for event from Redis")
	}

	if routerB.EventsRouted() != 1 {
		t.Fatalf("expected 1 event routed, got %d", routerB.EventsRouted())
	}
}

func TestCrossGatewayMultipleClients(t *testing.T) {
	redisClient := skipIfNoRedis(t)
	defer redisClient.Close()
	prefix := testPrefix(t)
	ctx := context.Background()
	log := log.New(nil, "test")
	queueCfg := newTestQueueCfg()

	pubClient := redis.NewClient(&redis.Options{Addr: redisClient.Options().Addr})
	defer pubClient.Close()
	pub := redisbus.NewPublisher(pubClient, log, redisbus.WithChannelPrefix(prefix))
	defer pub.Close()

	tmB := topicmanager.NewWithQueue(0, queueCfg, log, nil, nil)
	var subB *redisbus.Subscriber
	routerB := topicmanager.NewDistributedRouter(tmB, prefix, log, "gateway-B",
		topicmanager.WithSubscriptionChangeCallback(func(symbol string, change topicmanager.SubscriptionChange) {
			if subB == nil {
				return
			}
			switch change {
			case topicmanager.SubscribeRequested:
				subB.Subscribe(symbol)
			case topicmanager.UnsubscribeRequested:
				subB.Unsubscribe(symbol)
			}
		}),
	)
	subB = redisbus.NewSubscriber(redisClient, routerB, log, redisbus.WithSubscriberPrefix(prefix))
	subB.Start(ctx)
	defer subB.Stop()

	tmC := topicmanager.NewWithQueue(0, queueCfg, log, nil, nil)
	var subC *redisbus.Subscriber
	routerC := topicmanager.NewDistributedRouter(tmC, prefix, log, "gateway-C",
		topicmanager.WithSubscriptionChangeCallback(func(symbol string, change topicmanager.SubscriptionChange) {
			if subC == nil {
				return
			}
			switch change {
			case topicmanager.SubscribeRequested:
				subC.Subscribe(symbol)
			case topicmanager.UnsubscribeRequested:
				subC.Unsubscribe(symbol)
			}
		}),
	)
	subC = redisbus.NewSubscriber(redisClient, routerC, log, redisbus.WithSubscriberPrefix(prefix))
	subC.Start(ctx)
	defer subC.Stop()

	handleB := routerB.Subscribe("client-B1", "AAPL", "MSFT")
	handleC := routerC.Subscribe("client-C1", "AAPL")

	time.Sleep(1 * time.Second)

	pub.Publish(ctx, marketdata.Quote{Symbol: "AAPL", Price: 150.0, Timestamp: time.Now(), Provider: "sim"})
	pub.Publish(ctx, marketdata.Quote{Symbol: "MSFT", Price: 300.0, Timestamp: time.Now(), Provider: "sim"})

	received := 0
	timeout := time.After(10 * time.Second)
	for received < 2 {
		select {
		case cached := <-handleB.C():
			if cached != nil {
				q := cached.Event.(marketdata.Quote)
				t.Logf("gateway-B received: %s @ %.2f", q.Symbol, q.Price)
				received++
			}
		case <-timeout:
			t.Fatalf("gateway-B: expected 2 events, got %d", received)
		}
	}

	select {
	case cached := <-handleC.C():
		if cached != nil {
			q := cached.Event.(marketdata.Quote)
			if q.Symbol != "AAPL" {
				t.Fatalf("expected AAPL, got %s", q.Symbol)
			}
			t.Logf("gateway-C received: %s @ %.2f", q.Symbol, q.Price)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("gateway-C: timed out waiting for AAPL event")
	}

	select {
	case cached := <-handleC.C():
		if cached != nil {
			q := cached.Event.(marketdata.Quote)
			t.Fatalf("gateway-C should NOT have received MSFT, got %s", q.Symbol)
		}
	case <-time.After(500 * time.Millisecond):
	}
}

func TestWebSocketCrossGateway(t *testing.T) {
	redisClient := skipIfNoRedis(t)
	defer redisClient.Close()
	prefix := testPrefix(t)
	ctx := context.Background()
	log := log.New(nil, "test")
	queueCfg := newTestQueueCfg()
	_, _ = platform.NewMetrics("rtmds_test")

	pubClient := redis.NewClient(&redis.Options{Addr: redisClient.Options().Addr})
	defer pubClient.Close()
	pub := redisbus.NewPublisher(pubClient, log, redisbus.WithChannelPrefix(prefix))
	defer pub.Close()

	tm := topicmanager.NewWithQueue(0, queueCfg, log, nil, nil)

	var wsSub *redisbus.Subscriber
	router := topicmanager.NewDistributedRouter(tm, prefix, log, "gateway-ws",
		topicmanager.WithSubscriptionChangeCallback(func(symbol string, change topicmanager.SubscriptionChange) {
			if wsSub == nil {
				return
			}
			switch change {
			case topicmanager.SubscribeRequested:
				wsSub.Subscribe(symbol)
			case topicmanager.UnsubscribeRequested:
				wsSub.Unsubscribe(symbol)
			}
		}),
	)

	wsSub = redisbus.NewSubscriber(redisClient, router, log, redisbus.WithSubscriberPrefix(prefix))
	wsSub.Start(ctx)
	defer wsSub.Stop()

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var cm struct {
			Action  string   `json:"action"`
			Symbols []string `json:"symbols"`
		}
		json.Unmarshal(msg, &cm)

		if cm.Action == "subscribe" {
			h := router.Subscribe("ws-client", cm.Symbols...)

			go func() {
				for {
					select {
					case ev := <-h.C():
						if ev == nil {
							return
						}
						_ = conn.WriteMessage(websocket.TextMessage, ev.EncodedMsg)
					case <-h.Done():
						return
					}
				}
			}()
		}

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				break
			}
		}
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	go http.Serve(listener, mux)
	time.Sleep(100 * time.Millisecond)

	wsURL := "ws://" + listener.Addr().String() + "/ws"
	ws, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer ws.Close()

	subMsg, _ := json.Marshal(map[string]any{
		"action":  "subscribe",
		"symbols": []string{"AAPL"},
	})
	if err := ws.WriteMessage(websocket.TextMessage, subMsg); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}

	time.Sleep(1 * time.Second)

	pub.Publish(ctx, marketdata.Quote{
		Symbol:    "AAPL",
		Type:      "trade",
		Price:     175.50,
		Timestamp: time.Now(),
		Provider:  "test",
	})

	ws.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, msg, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	var envelope struct {
		Type    string           `json:"type"`
		Payload marketdata.Quote `json:"payload"`
	}
	if err := json.Unmarshal(msg, &envelope); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	q := envelope.Payload

	if q.Symbol != "AAPL" || q.Price != 175.50 {
		t.Fatalf("unexpected event: %+v", q)
	}
	t.Logf("SUCCESS: WebSocket client received AAPL trade via Redis: price=%.2f", q.Price)
}

func TestDynamicSubscriptionLifecycle(t *testing.T) {
	_ = skipIfNoRedis(t)
	log := log.New(nil, "test")
	queueCfg := newTestQueueCfg()

	tm := topicmanager.NewWithQueue(0, queueCfg, log, nil, nil)

	var subCount, unsubCount int
	router := topicmanager.NewDistributedRouter(tm, "market:", log, "gateway-test",
		topicmanager.WithSubscriptionChangeCallback(func(symbol string, change topicmanager.SubscriptionChange) {
			switch change {
			case topicmanager.SubscribeRequested:
				subCount++
			case topicmanager.UnsubscribeRequested:
				unsubCount++
			}
		}),
	)

	router.Subscribe("c1", "AAPL", "MSFT")
	if subCount != 2 {
		t.Fatalf("expected 2 subscribes, got %d", subCount)
	}

	router.Subscribe("c2", "AAPL")
	if subCount != 2 {
		t.Fatalf("expected 2 subscribes (no new for AAPL), got %d", subCount)
	}

	router.Unsubscribe("c1")
	if unsubCount != 1 {
		t.Fatalf("expected 1 unsubscribe (MSFT), got %d", unsubCount)
	}

	router.Unsubscribe("c2")
	if unsubCount != 2 {
		t.Fatalf("expected 2 total unsubscribes, got %d", unsubCount)
	}

	if router.ActiveRedisSubscriptions() != 0 {
		t.Fatalf("expected 0 active Redis subs, got %d", router.ActiveRedisSubscriptions())
	}
}
