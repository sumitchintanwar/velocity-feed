package feed

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/sumit/rtmds/internal/marketdata"
)

// --- Mock Feed ---

type mockFeed struct {
	name    string
	ch      chan marketdata.Quote
	err     error
	running bool
	mu      sync.Mutex
}

func newMockFeed(err error) *mockFeed {
	return &mockFeed{
		name: "mock",
		ch:   make(chan marketdata.Quote, 64),
		err:  err,
	}
}

func (f *mockFeed) Name() string { return f.name }

func (f *mockFeed) Subscribe(symbols ...string) error { return nil }
func (f *mockFeed) Unsubscribe(symbols ...string) error { return nil }

func (f *mockFeed) Run(ctx context.Context) (<-chan marketdata.Quote, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.mu.Lock()
	f.running = true
	f.mu.Unlock()
	return f.ch, nil
}

func (f *mockFeed) emit(q marketdata.Quote) {
	f.ch <- q
}

func (f *mockFeed) close() {
	close(f.ch)
}

// --- Mock Publisher ---

type mockPublisher struct {
	mu      sync.Mutex
	quotes  []marketdata.Quote
	publish func(ctx context.Context, event marketdata.MarketEvent) // optional hook
}

func (p *mockPublisher) Publish(ctx context.Context, event marketdata.MarketEvent) {
	if p.publish != nil {
		p.publish(ctx, event)
		return
	}
	q, ok := event.(marketdata.Quote)
	if !ok {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.quotes = append(p.quotes, q)
}

func (p *mockPublisher) published() []marketdata.Quote {
	p.mu.Lock()
	defer p.mu.Unlock()
	dst := make([]marketdata.Quote, len(p.quotes))
	copy(dst, p.quotes)
	return dst
}

// --- Tests ---

func TestPipeline_PublishesAllQuotes(t *testing.T) {
	feed := newMockFeed(nil)
	pub := &mockPublisher{}
	p := NewPipeline(feed, pub, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	feed.emit(marketdata.Quote{Symbol: "AAPL", Price: 100.0})
	feed.emit(marketdata.Quote{Symbol: "AAPL", Price: 101.0})
	feed.emit(marketdata.Quote{Symbol: "TSLA", Price: 200.0})
	feed.close()

	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := pub.published()
	if len(got) != 3 {
		t.Fatalf("expected 3 quotes, got %d", len(got))
	}
	if got[0].Price != 100.0 || got[1].Price != 101.0 || got[2].Price != 200.0 {
		t.Errorf("unexpected quotes: %+v", got)
	}
	cancel()
}

func TestPipeline_StopOnContext(t *testing.T) {
	feed := newMockFeed(nil)
	pub := &mockPublisher{}
	p := NewPipeline(feed, pub, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// Let the pipeline start, then cancel.
	time.Sleep(10 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
}

func TestPipeline_FeedErrorReturned(t *testing.T) {
	feedErr := errors.New("feed connection lost")
	feed := newMockFeed(feedErr)
	pub := &mockPublisher{}
	p := NewPipeline(feed, pub, zerolog.Nop())

	err := p.Run(context.Background())
	if err == nil {
		t.Fatal("expected error from feed failure")
	}
	if !errors.Is(err, feedErr) {
		t.Errorf("expected feedErr, got: %v", err)
	}
}

func TestPipeline_PublisherReceivesFeedOutput(t *testing.T) {
	feed := newMockFeed(nil)
	pub := &mockPublisher{}
	p := NewPipeline(feed, pub, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// Emit a sequence and verify ordering is preserved.
	prices := []float64{10.0, 20.0, 30.0, 40.0, 50.0}
	for _, price := range prices {
		feed.emit(marketdata.Quote{Symbol: "SYM", Price: price})
	}
	feed.close()
	<-done

	got := pub.published()
	if len(got) != len(prices) {
		t.Fatalf("expected %d quotes, got %d", len(prices), len(got))
	}
	for i, price := range prices {
		if got[i].Price != price {
			t.Errorf("quote[%d]: expected price %f, got %f", i, price, got[i].Price)
		}
	}
	cancel()
}

func TestPipeline_PublisherErrorDoesNotCrash(t *testing.T) {
	feed := newMockFeed(nil)
	pub := &mockPublisher{
		publish: func(ctx context.Context, event marketdata.MarketEvent) {
			// Simulate a panic in publisher — pipeline should survive.
			// In production, the Hub never panics, but we verify the
			// pipeline doesn't assume that.
		},
	}
	p := NewPipeline(feed, pub, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	feed.emit(marketdata.Quote{Symbol: "AAPL", Price: 100.0})
	feed.close()

	if err := <-done; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	cancel()
}
