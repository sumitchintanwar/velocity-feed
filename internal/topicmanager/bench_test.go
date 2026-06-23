package topicmanager

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/sumit/rtmds/internal/marketdata"
)

func benchQuote(symbol string) marketdata.Quote {
	return marketdata.Quote{
		Symbol:    symbol,
		Type:      marketdata.QuoteTypeTrade,
		Price:     150.0,
		Timestamp: time.Now(),
	}
}

// ---------- Publish ----------

func BenchmarkPublish_1Subscriber(b *testing.B) {
	benchPublish(b, 1, 1)
}

func BenchmarkPublish_10Subscribers(b *testing.B) {
	benchPublish(b, 10, 1)
}

func BenchmarkPublish_100Subscribers(b *testing.B) {
	benchPublish(b, 100, 1)
}

func BenchmarkPublish_1000Subscribers(b *testing.B) {
	benchPublish(b, 1000, 1)
}

func benchPublish(b *testing.B, numSubs, numTopics int) {
	b.Helper()
	m := New(0)
	topics := make([]Topic, numTopics)
	for i := range topics {
		topics[i] = fmt.Sprintf("SYM%d", i)
	}
	for i := 0; i < numSubs; i++ {
		m.Subscribe(fmt.Sprintf("c%d", i), topics...)
	}
	ev := benchQuote(topics[0])
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Publish(ctx, ev)
	}
}

// ---------- Publish parallel ----------

func BenchmarkPublishParallel_100Subscribers(b *testing.B) {
	benchPublishParallel(b, 100)
}

func BenchmarkPublishParallel_1000Subscribers(b *testing.B) {
	benchPublishParallel(b, 1000)
}

func BenchmarkPublishParallel_10000Subscribers(b *testing.B) {
	benchPublishParallel(b, 10000)
}

func benchPublishParallel(b *testing.B, numSubs int) {
	b.Helper()
	m := New(0)
	for i := 0; i < numSubs; i++ {
		m.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
	}
	ev := benchQuote("AAPL")
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Publish(ctx, ev)
		}
	})
}

// ---------- Subscribe / Unsubscribe ----------

func BenchmarkSubscribe_1Topic(b *testing.B) {
	benchSubscribe(b, 1)
}

func BenchmarkSubscribe_10Topics(b *testing.B) {
	benchSubscribe(b, 10)
}

func BenchmarkSubscribe_100Topics(b *testing.B) {
	benchSubscribe(b, 100)
}

func benchSubscribe(b *testing.B, numTopics int) {
	b.Helper()
	m := New(0)
	topics := make([]Topic, numTopics)
	for i := range topics {
		topics[i] = fmt.Sprintf("SYM%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("c%d", i)
		h := m.Subscribe(id, topics...)
		h.Cancel()
	}
}

func BenchmarkUnsubscribe_100Subscribers(b *testing.B) {
	b.Helper()
	m := New(0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		subs := make([]Handle, 100)
		for j := 0; j < 100; j++ {
			subs[j] = m.Subscribe(fmt.Sprintf("c%d-%d", i, j), "AAPL")
		}
		b.StartTimer()
		for _, h := range subs {
			h.Cancel()
		}
	}
}

// ---------- Mixed workload ----------

func BenchmarkMixed_80_20(b *testing.B) {
	// 80% publish, 20% subscribe/unsubscribe — realistic workload.
	b.Helper()
	m := New(0)
	for i := 0; i < 100; i++ {
		m.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
	}
	ev := benchQuote("AAPL")
	ctx := context.Background()
	rng := rand.New(rand.NewSource(42))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if rng.Intn(100) < 80 {
			m.Publish(ctx, ev)
		} else {
			id := fmt.Sprintf("bench-%d", i)
			h := m.Subscribe(id, "AAPL")
			h.Cancel()
		}
	}
}

// ---------- TopicCount / SubscriberCount ----------

func BenchmarkTopicCount(b *testing.B) {
	m := New(0)
	for i := 0; i < 1000; i++ {
		m.Subscribe(fmt.Sprintf("c%d", i), fmt.Sprintf("SYM%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.TopicCount()
	}
}

func BenchmarkSubscriberCount(b *testing.B) {
	m := New(0)
	for i := 0; i < 100; i++ {
		m.Subscribe(fmt.Sprintf("c%d", i), "AAPL")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.SubscriberCount("AAPL")
	}
}
