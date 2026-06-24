package redisbus

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
	"github.com/sumit/rtmds/internal/topicmanager"
)

// testRedisAddr returns the Redis address from the environment or a default.
func testRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "redis:6379"
}

// skipIfNoRedis skips the test if Redis is not available.
func skipIfNoRedis(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available at %s: %v", testRedisAddr(), err)
	}
}

// newTestTopicManager creates a minimal TopicManager for testing.
func newTestTopicManager(t *testing.T) topicmanager.Manager {
	t.Helper()
	return topicmanager.New(0)
}

func TestPublisher_Publish(t *testing.T) {
	skipIfNoRedis(t)

	ctx := context.Background()
	log := zerolog.New(zerolog.NewTestWriter(t)).Output(zerolog.ConsoleWriter{Out: os.Stderr})

	client := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}

	pub := NewPublisher(client, log, WithWorkers(1), WithQueueSize(64), WithChannelPrefix("test:publish:"))
	defer pub.Close()

	event := marketdata.Quote{
		Symbol:    "AAPL",
		Type:      marketdata.QuoteTypeTrade,
		Price:     203.41,
		Bid:       203.40,
		Ask:       203.42,
		Volume:    100,
		Timestamp: time.Now(),
		Provider:  "test",
	}

	pub.Publish(ctx, event)

	// Wait for async worker to process.
	time.Sleep(200 * time.Millisecond)

	if dropped := pub.Dropped(); dropped != 0 {
		t.Errorf("expected 0 drops, got %d", dropped)
	}
}

func TestSubscriber_ReceivesMessage(t *testing.T) {
	skipIfNoRedis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log := zerolog.New(zerolog.NewTestWriter(t)).Output(zerolog.ConsoleWriter{Out: os.Stderr})

	client := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	defer client.Close()

	tm := newTestTopicManager(t)
	sub := NewSubscriber(client, tm, log, WithSubscriberPrefix("test:sub1:"))
	sub.Start(ctx)
	defer sub.Stop()

	// Subscribe to AAPL on the subscriber.
	sub.Subscribe("AAPL")

	// Give subscriber time to connect to Redis.
	time.Sleep(500 * time.Millisecond)

	// Create a publisher and publish an event.
	pub := NewPublisher(client, log, WithWorkers(1), WithQueueSize(64), WithChannelPrefix("test:sub1:"))
	defer pub.Close()

	event := marketdata.Quote{
		Symbol:    "AAPL",
		Type:      marketdata.QuoteTypeTrade,
		Price:     420.50,
		Timestamp: time.Now(),
		Provider:  "test",
	}
	pub.Publish(ctx, event)

	// Wait for async delivery through Redis.
	time.Sleep(1 * time.Second)

	if received := sub.Received(); received != 1 {
		t.Errorf("expected 1 received, got %d", received)
	}
}

func TestMultiGateway_AllReceiveMessage(t *testing.T) {
	skipIfNoRedis(t)

	const numGateways = 3
	const numEvents = 5

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	log := zerolog.New(zerolog.NewTestWriter(t)).Output(zerolog.ConsoleWriter{Out: os.Stderr})

	// Use a unique channel prefix to isolate from other tests.
	const testPrefix = "test:multi:"

	// Create N gateway instances, each with its own TopicManager + Subscriber.
	type gateway struct {
		tm     topicmanager.Manager
		sub    *Subscriber
		handle topicmanager.Handle
	}

	gateways := make([]*gateway, numGateways)
	for i := 0; i < numGateways; i++ {
		gwClient := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
		defer gwClient.Close()

		tm := newTestTopicManager(t)
		sub := NewSubscriber(gwClient, tm, log, WithSubscriberPrefix(testPrefix))

		gateways[i] = &gateway{tm: tm, sub: sub}

		// Subscribe to AAPL on the subscriber.
		sub.Subscribe("AAPL")

		// Subscribe to AAPL on the local TopicManager.
		gateways[i].handle = tm.Subscribe("gw-"+string(rune('0'+i)), "AAPL")

		sub.Start(ctx)
	}

	// Give all subscribers time to connect to Redis.
	time.Sleep(1 * time.Second)

	// Create a single publisher.
	pubClient := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	defer pubClient.Close()

	pub := NewPublisher(pubClient, log, WithWorkers(2), WithQueueSize(256), WithChannelPrefix(testPrefix))
	defer pub.Close()

	// Publish events.
	for j := 0; j < numEvents; j++ {
		event := marketdata.Quote{
			Symbol:    "AAPL",
			Type:      marketdata.QuoteTypeTrade,
			Price:     float64(200 + j),
			Timestamp: time.Now(),
			Provider:  "test",
		}
		pub.Publish(ctx, event)
		time.Sleep(50 * time.Millisecond)
	}

	// Wait for async delivery through Redis.
	time.Sleep(3 * time.Second)

	// Verify each gateway received all events.
	for i, gw := range gateways {
		received := gw.sub.Received()
		if received != numEvents {
			t.Errorf("gateway %d: expected %d events, got %d", i, numEvents, received)
		}
	}
}

func TestPublisher_Close(t *testing.T) {
	skipIfNoRedis(t)

	ctx := context.Background()
	log := zerolog.New(zerolog.NewTestWriter(t)).Output(zerolog.ConsoleWriter{Out: os.Stderr})

	client := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	defer client.Close()

	pub := NewPublisher(client, log, WithWorkers(2), WithQueueSize(64))

	// Publish some events.
	for i := 0; i < 10; i++ {
		event := marketdata.Quote{
			Symbol:    "AAPL",
			Type:      marketdata.QuoteTypeTrade,
			Price:     float64(200 + i),
			Timestamp: time.Now(),
			Provider:  "test",
		}
		pub.Publish(ctx, event)
	}

	// Close waits for workers to drain.
	pub.Close()

	// After close, Publish silently drops.
	event := marketdata.Quote{
		Symbol:    "AAPL",
		Type:      marketdata.QuoteTypeTrade,
		Price:     999,
		Timestamp: time.Now(),
		Provider:  "test",
	}
	pub.Publish(ctx, event)

	// Verify Done channel is closed.
	select {
	case <-pub.Done():
	case <-time.After(5 * time.Second):
		t.Error("Done channel not closed after Close()")
	}
}

func TestSubscriber_Stop(t *testing.T) {
	skipIfNoRedis(t)

	ctx := context.Background()
	log := zerolog.New(zerolog.NewTestWriter(t)).Output(zerolog.ConsoleWriter{Out: os.Stderr})

	client := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	defer client.Close()

	tm := newTestTopicManager(t)
	sub := NewSubscriber(client, tm, log)
	sub.Start(ctx)

	time.Sleep(200 * time.Millisecond)
	sub.Stop()

	select {
	case <-sub.Done():
	case <-time.After(5 * time.Second):
		t.Error("subscriber did not stop within timeout")
	}
}

func TestSubscriber_SubscribeUnsubscribe(t *testing.T) {
	skipIfNoRedis(t)

	ctx := context.Background()
	log := zerolog.New(zerolog.NewTestWriter(t)).Output(zerolog.ConsoleWriter{Out: os.Stderr})

	client := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	defer client.Close()

	tm := newTestTopicManager(t)
	sub := NewSubscriber(client, tm, log)
	sub.Start(ctx)
	defer sub.Stop()

	time.Sleep(200 * time.Millisecond)

	// Subscribe to symbols.
	sub.Subscribe("AAPL")
	sub.Subscribe("MSFT")
	sub.Subscribe("GOOG")

	syms := sub.SubscribedSymbols()
	if len(syms) != 3 {
		t.Errorf("expected 3 subscribed symbols, got %d: %v", len(syms), syms)
	}

	// Unsubscribe from one.
	sub.Unsubscribe("MSFT")

	syms = sub.SubscribedSymbols()
	if len(syms) != 2 {
		t.Errorf("expected 2 subscribed symbols after unsubscribe, got %d: %v", len(syms), syms)
	}
}

func TestPublisher_TopicChannel(t *testing.T) {
	pub := &Publisher{prefix: "market:"}

	ch := pub.ChannelForSymbol("AAPL")
	if ch != "market:AAPL" {
		t.Errorf("expected 'market:AAPL', got %q", ch)
	}

	ch = pub.ChannelForSymbol("BTCUSD")
	if ch != "market:BTCUSD" {
		t.Errorf("expected 'market:BTCUSD', got %q", ch)
	}
}

func TestPublisher_QueueFull(t *testing.T) {
	skipIfNoRedis(t)

	ctx := context.Background()
	log := zerolog.New(zerolog.NewTestWriter(t)).Output(zerolog.ConsoleWriter{Out: os.Stderr})

	client := redis.NewClient(&redis.Options{Addr: testRedisAddr()})
	defer client.Close()

	// Tiny queue with no workers to process it.
	pub := NewPublisher(client, log, WithWorkers(0), WithQueueSize(2))

	// Fill the queue (workers=0 means nothing reads from it).
	// Use a goroutine to avoid blocking.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			event := marketdata.Quote{
				Symbol:    "AAPL",
				Type:      marketdata.QuoteTypeTrade,
				Price:     200,
				Timestamp: time.Now(),
				Provider:  "test",
			}
			pub.Publish(ctx, event)
		}()
	}
	wg.Wait()

	if dropped := pub.Dropped(); dropped == 0 {
		t.Error("expected some drops with zero workers, got 0")
	}

	pub.Close()
}

func TestNewClient(t *testing.T) {
	client := NewClient("localhost:6379", "", 0)
	if client == nil {
		t.Fatal("expected non-nil client")
	}
	defer client.Close()
}

func TestPing(t *testing.T) {
	skipIfNoRedis(t)

	ctx := context.Background()
	client := NewClient(testRedisAddr(), "", 0)
	defer client.Close()

	if err := Ping(ctx, client); err != nil {
		t.Errorf("expected ping to succeed: %v", err)
	}
}

func TestChannelConstants(t *testing.T) {
	if ChannelPrefix != "market:" {
		t.Errorf("expected ChannelPrefix to be 'market:', got %q", ChannelPrefix)
	}
}
