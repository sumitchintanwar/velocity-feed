package ratelimit

import (
	"sync"
	"testing"
	"time"
)

// ---------- Bucket ----------

func TestBucket_Allow_Basic(t *testing.T) {
	b := NewBucket(10, 10) // 10/sec, burst 10

	// Should allow up to burst.
	for i := 0; i < 10; i++ {
		if !b.Allow() {
			t.Fatalf("Allow() = false on request %d, want true", i+1)
		}
	}

	// 11th should be rejected.
	if b.Allow() {
		t.Fatal("Allow() = true after exhausting tokens, want false")
	}
}

func TestBucket_Refill(t *testing.T) {
	b := NewBucket(10, 10)

	// Exhaust all tokens.
	for i := 0; i < 10; i++ {
		b.Allow()
	}

	// Wait for refill.
	time.Sleep(200 * time.Millisecond)

	// Should have ~2 tokens refilled (200ms × 10/sec = 2).
	if !b.Allow() {
		t.Fatal("Allow() = false after refill, want true")
	}
	if !b.Allow() {
		t.Fatal("Allow() = false after refill, want true")
	}
	// 3rd should fail (only ~2 tokens refilled).
	if b.Allow() {
		t.Fatal("Allow() = true with insufficient tokens, want false")
	}
}

func TestBucket_AllowN(t *testing.T) {
	b := NewBucket(10, 10)

	// AllowN(5) should succeed.
	if !b.AllowN(5) {
		t.Fatal("AllowN(5) = false, want true")
	}

	// AllowN(6) should fail (only 5 remaining).
	if b.AllowN(6) {
		t.Fatal("AllowN(6) = true, want false")
	}

	// AllowN(5) should succeed.
	if !b.AllowN(5) {
		t.Fatal("AllowN(5) = false, want true")
	}
}

func TestBucket_AllowN_Zero(t *testing.T) {
	b := NewBucket(10, 10)
	if !b.AllowN(0) {
		t.Fatal("AllowN(0) = false, want true")
	}
	if !b.AllowN(-1) {
		t.Fatal("AllowN(-1) = false, want true")
	}
}

func TestBucket_Tokens(t *testing.T) {
	b := NewBucket(10, 10)

	tokens := b.Tokens()
	if tokens != 10.0 {
		t.Fatalf("Tokens() = %v, want 10", tokens)
	}

	b.Allow()
	b.Allow()

	tokens = b.Tokens()
	if tokens != 8.0 {
		t.Fatalf("Tokens() = %v after 2 allows, want 8", tokens)
	}
}

func TestBucket_BurstSize(t *testing.T) {
	b := NewBucket(50, 200)
	if b.BurstSize() != 200 {
		t.Fatalf("BurstSize() = %d, want 200", b.BurstSize())
	}
}

func TestBucket_TokensPerSec(t *testing.T) {
	b := NewBucket(50, 200)
	if b.TokensPerSec() != 50 {
		t.Fatalf("TokensPerSec() = %v, want 50", b.TokensPerSec())
	}
}

func TestBucket_DoesNotExceedBurst(t *testing.T) {
	b := NewBucket(1000, 5) // high rate, low burst

	// Wait to accumulate tokens — should not exceed burst.
	time.Sleep(100 * time.Millisecond)

	tokens := b.Tokens()
	if tokens > 5.0 {
		t.Fatalf("Tokens() = %v, exceeds burst size 5", tokens)
	}
}

func TestBucket_Concurrent(t *testing.T) {
	b := NewBucket(1000, 1000)

	var wg sync.WaitGroup
	const goroutines = 100
	const opsPerGoroutine = 100

	allowed := make([]int64, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				if b.Allow() {
					allowed[id]++
				}
			}
		}(i)
	}
	wg.Wait()

	total := int64(0)
	for _, a := range allowed {
		total += a
	}

	// Total allowed should be <= burst + refilled tokens.
	// With 1000 tokens/sec and ~1ms total, we get at most ~1000 + burst.
	if total > 2000 {
		t.Fatalf("total allowed = %d, expected <= ~2000", total)
	}
	t.Logf("total allowed: %d / %d attempts", total, goroutines*opsPerGoroutine)
}

func TestBucket_ConcurrentNoDataRace(t *testing.T) {
	b := NewBucket(100, 100)

	var wg sync.WaitGroup
	wg.Add(200)
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			b.Allow()
		}()
		go func() {
			defer wg.Done()
			b.Tokens()
		}()
	}
	wg.Wait()
}

// ---------- Limiter ----------

func TestLimiter_AllowSubscribe(t *testing.T) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 5, Burst: 5},
		Unsubscribe: RateLimit{Rate: 2, Burst: 2},
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("c1")

	// Should allow up to burst.
	for i := 0; i < 5; i++ {
		if !l.AllowSubscribe("c1") {
			t.Fatalf("AllowSubscribe() = false on request %d, want true", i+1)
		}
	}

	// 6th should be rejected.
	if l.AllowSubscribe("c1") {
		t.Fatal("AllowSubscribe() = true after exhausting tokens, want false")
	}
}

func TestLimiter_AllowUnsubscribe(t *testing.T) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 5, Burst: 5},
		Unsubscribe: RateLimit{Rate: 2, Burst: 2},
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("c1")

	// Should allow up to burst.
	for i := 0; i < 2; i++ {
		if !l.AllowUnsubscribe("c1") {
			t.Fatalf("AllowUnsubscribe() = false on request %d, want true", i+1)
		}
	}

	// 3rd should be rejected.
	if l.AllowUnsubscribe("c1") {
		t.Fatal("AllowUnsubscribe() = true after exhausting tokens, want false")
	}
}

func TestLimiter_PerClientIndependence(t *testing.T) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 5, Burst: 5},
		Unsubscribe: RateLimit{Rate: 2, Burst: 2},
	}
	l := NewLimiter(cfg, nil)
	defer func() {
		l.RemoveClient("c1")
		l.RemoveClient("c2")
	}()

	// Exhaust c1's tokens.
	for i := 0; i < 5; i++ {
		l.AllowSubscribe("c1")
	}

	// c2 should still have full tokens.
	if !l.AllowSubscribe("c2") {
		t.Fatal("AllowSubscribe() = false for c2, want true — clients should be independent")
	}
}

func TestLimiter_AllowSubscription_MaxSubscriptions(t *testing.T) {
	cfg := Config{
		Subscribe:        RateLimit{Rate: 100, Burst: 100},
		MaxSubscriptions: 3,
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("c1")

	// Should allow up to max.
	for i := 0; i < 3; i++ {
		if !l.AllowSubscription("c1") {
			t.Fatalf("AllowSubscription() = false on request %d, want true", i+1)
		}
	}

	// 4th should be rejected.
	if l.AllowSubscription("c1") {
		t.Fatal("AllowSubscription() = true at max, want false")
	}

	// Release one and try again.
	l.ReleaseSubscription("c1")
	if !l.AllowSubscription("c1") {
		t.Fatal("AllowSubscription() = false after release, want true")
	}
}

func TestLimiter_AllowSubscription_Unlimited(t *testing.T) {
	cfg := Config{
		Subscribe:        RateLimit{Rate: 100, Burst: 100},
		MaxSubscriptions: 0, // unlimited
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("c1")

	for i := 0; i < 1000; i++ {
		if !l.AllowSubscription("c1") {
			t.Fatalf("AllowSubscription() = false at request %d with unlimited, want true", i+1)
		}
	}
}

func TestLimiter_RemoveClient(t *testing.T) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 5, Burst: 5},
		Unsubscribe: RateLimit{Rate: 2, Burst: 2},
	}
	l := NewLimiter(cfg, nil)

	l.AllowSubscribe("c1")
	if l.ClientCount() != 1 {
		t.Fatalf("ClientCount() = %d, want 1", l.ClientCount())
	}

	l.RemoveClient("c1")
	if l.ClientCount() != 0 {
		t.Fatalf("ClientCount() = %d after RemoveClient, want 0", l.ClientCount())
	}
}

func TestLimiter_ClientCount(t *testing.T) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 5, Burst: 5},
		Unsubscribe: RateLimit{Rate: 2, Burst: 2},
	}
	l := NewLimiter(cfg, nil)
	defer func() {
		l.RemoveClient("c1")
		l.RemoveClient("c2")
		l.RemoveClient("c3")
	}()

	l.AllowSubscribe("c1")
	l.AllowSubscribe("c2")
	l.AllowSubscribe("c3")

	if l.ClientCount() != 3 {
		t.Fatalf("ClientCount() = %d, want 3", l.ClientCount())
	}
}

func TestLimiter_Refill(t *testing.T) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 50, Burst: 5},
		Unsubscribe: RateLimit{Rate: 20, Burst: 5},
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("c1")

	// Exhaust tokens.
	for i := 0; i < 5; i++ {
		l.AllowSubscribe("c1")
	}

	// Wait for refill.
	time.Sleep(100 * time.Millisecond)

	// Should have ~5 tokens (100ms × 50/sec = 5).
	if !l.AllowSubscribe("c1") {
		t.Fatal("AllowSubscribe() = false after refill, want true")
	}
}

func TestLimiter_Concurrent(t *testing.T) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 1000, Burst: 1000},
		Unsubscribe: RateLimit{Rate: 500, Burst: 500},
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("c1")

	var wg sync.WaitGroup
	const goroutines = 100
	const opsPerGoroutine = 100

	allowed := make([]int64, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				if l.AllowSubscribe("c1") {
					allowed[id]++
				}
			}
		}(i)
	}
	wg.Wait()

	total := int64(0)
	for _, a := range allowed {
		total += a
	}
	t.Logf("total allowed: %d / %d attempts", total, goroutines*opsPerGoroutine)
}

// ---------- Config ----------

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Connect.Rate != 10 {
		t.Errorf("Connect.Rate = %v, want 10", cfg.Connect.Rate)
	}
	if cfg.Connect.Burst != 20 {
		t.Errorf("Connect.Burst = %v, want 20", cfg.Connect.Burst)
	}
	if cfg.Subscribe.Rate != 50 {
		t.Errorf("Subscribe.Rate = %v, want 50", cfg.Subscribe.Rate)
	}
	if cfg.Subscribe.Burst != 100 {
		t.Errorf("Subscribe.Burst = %v, want 100", cfg.Subscribe.Burst)
	}
	if cfg.Unsubscribe.Rate != 20 {
		t.Errorf("Unsubscribe.Rate = %v, want 20", cfg.Unsubscribe.Rate)
	}
	if cfg.Unsubscribe.Burst != 50 {
		t.Errorf("Unsubscribe.Burst = %v, want 50", cfg.Unsubscribe.Burst)
	}
	if cfg.MaxSubscriptions != 1000 {
		t.Errorf("MaxSubscriptions = %v, want 1000", cfg.MaxSubscriptions)
	}
	if cfg.ClientTTL != 5*time.Minute {
		t.Errorf("ClientTTL = %v, want 5m", cfg.ClientTTL)
	}
}

func TestNoLimits(t *testing.T) {
	cfg := NoLimits()
	if cfg.Subscribe.Rate != 1e6 {
		t.Errorf("NoLimits().Subscribe.Rate = %v, want 1e6", cfg.Subscribe.Rate)
	}
}

// ---------- Connect ----------

func TestLimiter_AllowConnect(t *testing.T) {
	cfg := Config{
		Connect:   RateLimit{Rate: 3, Burst: 3},
		Subscribe: RateLimit{Rate: 100, Burst: 100},
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("c1")

	for i := 0; i < 3; i++ {
		if !l.AllowConnect("c1") {
			t.Fatalf("AllowConnect() = false on request %d, want true", i+1)
		}
	}
	if l.AllowConnect("c1") {
		t.Fatal("AllowConnect() = true after exhausting tokens, want false")
	}
}

// ---------- AllowAndSubscribe ----------

func TestLimiter_AllowAndSubscribe(t *testing.T) {
	cfg := Config{
		Subscribe:        RateLimit{Rate: 100, Burst: 100},
		MaxSubscriptions: 3,
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("c1")

	// Should allow up to max.
	for i := 0; i < 3; i++ {
		if !l.AllowAndSubscribe("c1") {
			t.Fatalf("AllowAndSubscribe() = false on request %d, want true", i+1)
		}
	}

	// 4th should be rejected.
	if l.AllowAndSubscribe("c1") {
		t.Fatal("AllowAndSubscribe() = true at max, want false")
	}
}

func TestLimiter_AllowAndSubscribe_RefundsToken(t *testing.T) {
	cfg := Config{
		Subscribe:        RateLimit{Rate: 100, Burst: 5},
		MaxSubscriptions: 2,
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("c1")

	// Allow 2 subscriptions.
	if !l.AllowAndSubscribe("c1") {
		t.Fatal("AllowAndSubscribe() = false, want true")
	}
	if !l.AllowAndSubscribe("c1") {
		t.Fatal("AllowAndSubscribe() = false, want true")
	}

	// 3rd rejected (cap exceeded) — token should be refunded.
	if l.AllowAndSubscribe("c1") {
		t.Fatal("AllowAndSubscribe() = true at max, want false")
	}

	// Release one subscription.
	l.ReleaseSubscription("c1")

	// Should succeed — token was refunded, so rate limit not exhausted.
	if !l.AllowAndSubscribe("c1") {
		t.Fatal("AllowAndSubscribe() = false after release + refund, want true")
	}
}

// ---------- TTL Eviction ----------

func TestLimiter_TTLEviction(t *testing.T) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 100, Burst: 100},
		Unsubscribe: RateLimit{Rate: 50, Burst: 50},
		ClientTTL:   100 * time.Millisecond,
	}
	l := NewLimiter(cfg, nil)
	defer l.Stop()

	l.AllowSubscribe("c1")
	if l.ClientCount() != 1 {
		t.Fatalf("ClientCount() = %d, want 1", l.ClientCount())
	}

	// Wait for TTL + evictor interval (1 minute ticker).
	// Manually trigger eviction instead of waiting.
	l.evictStale()

	// Should still be there — lastAccess was just set.
	if l.ClientCount() != 1 {
		t.Fatalf("ClientCount() = %d after immediate eviction, want 1", l.ClientCount())
	}

	// Simulate old access time.
	sh := l.shard("c1")
	sh.mu.RLock()
	cl := sh.clients["c1"]
	sh.mu.RUnlock()
	cl.lastAccess.Store(time.Now().Add(-1 * time.Minute).UnixNano())

	l.evictStale()
	if l.ClientCount() != 0 {
		t.Fatalf("ClientCount() = %d after TTL eviction, want 0", l.ClientCount())
	}
}

// ---------- Concurrent AllowSubscription (B1 fix) ----------

func TestLimiter_ConcurrentAllowSubscription(t *testing.T) {
	cfg := Config{
		Subscribe:        RateLimit{Rate: 10000, Burst: 10000},
		MaxSubscriptions: 100,
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("c1")

	var wg sync.WaitGroup
	const goroutines = 50
	const opsPerGoroutine = 10

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				l.AllowSubscription("c1")
			}
		}()
	}
	wg.Wait()

	// Should not exceed MaxSubscriptions.
	cl := l.shard("c1").clients["c1"]
	if cl.activeSubs.Load() > int64(cfg.MaxSubscriptions) {
		t.Errorf("activeSubs = %d, want <= %d", cl.activeSubs.Load(), cfg.MaxSubscriptions)
	}
}

// ---------- Spam Tests ----------

func TestSpam_SubscribeBurstLimit(t *testing.T) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 1, Burst: 10}, // low rate to minimize refill during test
		Unsubscribe: RateLimit{Rate: 1, Burst: 5},
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("spammer")

	const spamCount = 1000
	var allowed, rejected int

	for i := 0; i < spamCount; i++ {
		if l.AllowSubscribe("spammer") {
			allowed++
		} else {
			rejected++
		}
	}

	t.Logf("Spam result: %d allowed, %d rejected out of %d", allowed, rejected, spamCount)

	// Burst is 10, rate is 1/sec. In <1sec, at most 10 + 1 refilled.
	if allowed > 12 {
		t.Errorf("allowed = %d, want <= 12 (burst 10 + minor refill)", allowed)
	}
	if allowed < 10 {
		t.Errorf("allowed = %d, want >= 10 (burst size)", allowed)
	}
}

func TestSpam_RefillAllowsMore(t *testing.T) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 100, Burst: 10},
		Unsubscribe: RateLimit{Rate: 20, Burst: 5},
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("spammer")

	// Exhaust burst.
	for i := 0; i < 10; i++ {
		if !l.AllowSubscribe("spammer") {
			t.Fatalf("burst AllowSubscribe() = false on request %d", i+1)
		}
	}

	// Spam should all be rejected now.
	rejected := 0
	for i := 0; i < 100; i++ {
		if !l.AllowSubscribe("spammer") {
			rejected++
		}
	}
	if rejected != 100 {
		t.Errorf("rejected = %d after exhaust, want 100", rejected)
	}

	// Wait for refill: 500ms × 100/sec = 50 tokens, but capped at burst (10).
	time.Sleep(500 * time.Millisecond)

	// Should be able to allow up to burst (10).
	allowed := 0
	for i := 0; i < 10; i++ {
		if l.AllowSubscribe("spammer") {
			allowed++
		}
	}

	t.Logf("After refill: %d allowed out of 10 attempts", allowed)
	if allowed < 8 {
		t.Errorf("allowed = %d after refill, want >= 8", allowed)
	}
}

func TestSpam_PerClientIsolation(t *testing.T) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 10, Burst: 10},
		Unsubscribe: RateLimit{Rate: 5, Burst: 5},
	}
	l := NewLimiter(cfg, nil)
	defer func() {
		l.RemoveClient("spammer")
		l.RemoveClient("legit")
	}()

	// Spammer exhausts their tokens.
	for i := 0; i < 100; i++ {
		l.AllowSubscribe("spammer")
	}

	// Legitimate client should still have full burst.
	allowed := 0
	for i := 0; i < 10; i++ {
		if l.AllowSubscribe("legit") {
			allowed++
		}
	}
	if allowed != 10 {
		t.Errorf("legit allowed = %d, want 10 — spammer should not affect other clients", allowed)
	}
}

func TestSpam_MaxSubscriptionCap(t *testing.T) {
	cfg := Config{
		Subscribe:        RateLimit{Rate: 10000, Burst: 10000},
		MaxSubscriptions: 5,
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("spammer")

	// Spam subscription attempts.
	var active, rejected int
	for i := 0; i < 1000; i++ {
		if l.AllowSubscription("spammer") {
			active++
		} else {
			rejected++
		}
	}

	t.Logf("Subscription cap: %d active, %d rejected out of 1000", active, rejected)

	if active != 5 {
		t.Errorf("active = %d, want 5 (MaxSubscriptions)", active)
	}
	if rejected != 995 {
		t.Errorf("rejected = %d, want 995", rejected)
	}

	// Release one, should allow one more.
	l.ReleaseSubscription("spammer")
	if !l.AllowSubscription("spammer") {
		t.Error("AllowSubscription() = false after release, want true")
	}
}

func TestSpam_ConcurrentSpamMultipleClients(t *testing.T) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 100, Burst: 50},
		Unsubscribe: RateLimit{Rate: 50, Burst: 25},
	}
	l := NewLimiter(cfg, nil)
	defer func() {
		for i := 0; i < 10; i++ {
			l.RemoveClient(string(rune('A' + i)))
		}
	}()

	const clients = 10
	const spamPerClient = 1000

	var mu sync.Mutex
	results := make(map[string]int) // client → allowed count

	var wg sync.WaitGroup
	wg.Add(clients)
	for c := 0; c < clients; c++ {
		go func(cid int) {
			defer wg.Done()
			clientID := string(rune('A' + cid))
			allowed := 0
			for i := 0; i < spamPerClient; i++ {
				if l.AllowSubscribe(clientID) {
					allowed++
				}
			}
			mu.Lock()
			results[clientID] = allowed
			mu.Unlock()
		}(c)
	}
	wg.Wait()

	// Each client should be limited to burst + refilled tokens.
	// With 100/sec and ~1ms total, each gets at most ~50 + 1 = 51.
	totalAllowed := 0
	for id, allowed := range results {
		t.Logf("Client %s: %d allowed out of %d", id, allowed, spamPerClient)
		if allowed > 60 {
			t.Errorf("client %s: allowed = %d, want <= 60", id, allowed)
		}
		totalAllowed += allowed
	}

	t.Logf("Total allowed across %d clients: %d / %d", clients, totalAllowed, clients*spamPerClient)
}

func TestSpam_UnsubscribeBurstLimit(t *testing.T) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 100, Burst: 100},
		Unsubscribe: RateLimit{Rate: 10, Burst: 5},
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("spammer")

	const spamCount = 1000
	var allowed, rejected int

	for i := 0; i < spamCount; i++ {
		if l.AllowUnsubscribe("spammer") {
			allowed++
		} else {
			rejected++
		}
	}

	t.Logf("Unsubscribe spam: %d allowed, %d rejected out of %d", allowed, rejected, spamCount)

	if allowed != 5 {
		t.Errorf("allowed = %d, want 5 (burst size)", allowed)
	}
}

func TestSpam_BurstThenSteadyState(t *testing.T) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 10, Burst: 20},
		Unsubscribe: RateLimit{Rate: 5, Burst: 5},
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("spammer")

	// Phase 1: Burst — should allow 20.
	burstAllowed := 0
	for i := 0; i < 20; i++ {
		if l.AllowSubscribe("spammer") {
			burstAllowed++
		}
	}
	if burstAllowed != 20 {
		t.Errorf("burst allowed = %d, want 20", burstAllowed)
	}

	// Phase 2: Spam immediately after burst — all rejected.
	spamRejected := 0
	for i := 0; i < 100; i++ {
		if !l.AllowSubscribe("spammer") {
			spamRejected++
		}
	}
	if spamRejected != 100 {
		t.Errorf("spam rejected = %d, want 100", spamRejected)
	}

	// Phase 3: Wait 1 second — 10 tokens refilled.
	time.Sleep(time.Second)

	steadyAllowed := 0
	for i := 0; i < 10; i++ {
		if l.AllowSubscribe("spammer") {
			steadyAllowed++
		}
	}
	t.Logf("Steady state after 1s: %d allowed out of 10", steadyAllowed)
	if steadyAllowed < 8 {
		t.Errorf("steady allowed = %d, want >= 8", steadyAllowed)
	}
}

func TestSpam_AllowAndSubscribeCap(t *testing.T) {
	cfg := Config{
		Subscribe:        RateLimit{Rate: 100, Burst: 100},
		MaxSubscriptions: 5,
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("spammer")

	var active, rateRejected, capRejected int
	for i := 0; i < 1000; i++ {
		if l.AllowAndSubscribe("spammer") {
			active++
		} else {
			// Distinguish rate vs cap rejection.
			if active >= 5 {
				capRejected++
			} else {
				rateRejected++
			}
		}
	}

	t.Logf("AllowAndSubscribe spam: %d active, %d cap-rejected, %d rate-rejected",
		active, capRejected, rateRejected)

	if active != 5 {
		t.Errorf("active = %d, want 5 (MaxSubscriptions)", active)
	}
	// All rejections should be cap, not rate (rate burst is 100).
	if rateRejected != 0 {
		t.Errorf("rate-rejected = %d, want 0 (tokens should be refunded)", rateRejected)
	}
}

func TestSpam_ConnectBurstLimit(t *testing.T) {
	cfg := Config{
		Connect:   RateLimit{Rate: 5, Burst: 5},
		Subscribe: RateLimit{Rate: 100, Burst: 100},
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("spammer")

	var allowed, rejected int
	for i := 0; i < 1000; i++ {
		if l.AllowConnect("spammer") {
			allowed++
		} else {
			rejected++
		}
	}

	if allowed != 5 {
		t.Errorf("allowed = %d, want 5 (burst size)", allowed)
	}
	if rejected != 995 {
		t.Errorf("rejected = %d, want 995", rejected)
	}
}

// ---------- Benchmarks ----------

func BenchmarkBucket_Allow(b *testing.B) {
	bucket := NewBucket(10000, 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bucket.Allow()
	}
}

func BenchmarkBucket_AllowN(b *testing.B) {
	bucket := NewBucket(10000, 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bucket.AllowN(1)
	}
}

func BenchmarkLimiter_AllowSubscribe(b *testing.B) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 10000, Burst: 10000},
		Unsubscribe: RateLimit{Rate: 5000, Burst: 5000},
	}
	l := NewLimiter(cfg, nil)
	defer l.RemoveClient("bench")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.AllowSubscribe("bench")
	}
}

func BenchmarkLimiter_MultiClient(b *testing.B) {
	cfg := Config{
		Subscribe:   RateLimit{Rate: 10000, Burst: 10000},
		Unsubscribe: RateLimit{Rate: 5000, Burst: 5000},
	}
	l := NewLimiter(cfg, nil)
	defer func() {
		for i := 0; i < 100; i++ {
			l.RemoveClient(string(rune('A' + i%26)))
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			l.AllowSubscribe(string(rune('A' + i%26)))
			i++
		}
	})
}
