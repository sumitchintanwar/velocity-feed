package sequencer

import (
	"sync"
	"testing"
)

// ---------------------------------------------------------------------------
// Sequencer tests
// ---------------------------------------------------------------------------

func TestSequencerNext(t *testing.T) {
	s := New()

	got := s.Next("AAPL")
	if got != 1 {
		t.Fatalf("first Next(AAPL) = %d, want 1", got)
	}
	got = s.Next("AAPL")
	if got != 2 {
		t.Fatalf("second Next(AAPL) = %d, want 2", got)
	}
	got = s.Next("AAPL")
	if got != 3 {
		t.Fatalf("third Next(AAPL) = %d, want 3", got)
	}
}

func TestSequencerPerSymbolIndependence(t *testing.T) {
	s := New()

	aapl := s.Next("AAPL")
	msft := s.Next("MSFT")
	if aapl != 1 || msft != 1 {
		t.Fatalf("first sequences should both be 1, got AAPL=%d MSFT=%d", aapl, msft)
	}

	aapl2 := s.Next("AAPL")
	msft2 := s.Next("MSFT")
	if aapl2 != 2 || msft2 != 2 {
		t.Fatalf("second sequences should both be 2, got AAPL=%d MSFT=%d", aapl2, msft2)
	}
}

func TestSequencerCurrent(t *testing.T) {
	s := New()

	if got := s.Current("AAPL"); got != 0 {
		t.Fatalf("Current before any Next should be 0, got %d", got)
	}

	s.Next("AAPL")
	s.Next("AAPL")

	if got := s.Current("AAPL"); got != 2 {
		t.Fatalf("Current after 2 Next calls should be 2, got %d", got)
	}
}

func TestSequencerReset(t *testing.T) {
	s := New()
	s.Next("AAPL")
	s.Next("AAPL")
	s.Reset()

	if got := s.Current("AAPL"); got != 0 {
		t.Fatalf("Current after Reset should be 0, got %d", got)
	}
}

func TestSequencerConcurrency(t *testing.T) {
	s := New()
	const goroutines = 100
	const perGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	seen := make(chan int64, goroutines*perGoroutine)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				seen <- s.Next("CONCURRENT")
			}
		}()
	}

	wg.Wait()
	close(seen)

	m := make(map[int64]bool, goroutines*perGoroutine)
	for seq := range seen {
		if m[seq] {
			t.Fatalf("duplicate sequence number %d", seq)
		}
		m[seq] = true
	}

	if len(m) != goroutines*perGoroutine {
		t.Fatalf("expected %d unique sequences, got %d", goroutines*perGoroutine, len(m))
	}
}

// ---------------------------------------------------------------------------
// Validator tests
// ---------------------------------------------------------------------------

func TestValidatorInOrder(t *testing.T) {
	v := NewValidator()

	for seq := int64(1); seq <= 4; seq++ {
		result, gap := v.Validate("AAPL", seq)
		if result != Ok {
			t.Fatalf("seq=%d: result=%s, want ok", seq, result)
		}
		if gap != nil {
			t.Fatalf("seq=%d: unexpected gap %+v", seq, gap)
		}
	}
}

func TestValidatorNoDisorder(t *testing.T) {
	v := NewValidator()

	// Simulate the user's exact test: 1, 2, 3, 4
	seqs := []int64{1, 2, 3, 4}
	for _, seq := range seqs {
		result, gap := v.Validate("AAPL", seq)
		if result != Ok {
			t.Fatalf("seq=%d: result=%s, want ok", seq, result)
		}
		if gap != nil {
			t.Fatalf("seq=%d: unexpected gap %+v", seq, gap)
		}
	}

	stats := v.StatsFor("AAPL")
	if stats.TotalReceived != 4 {
		t.Fatalf("TotalReceived=%d, want 4", stats.TotalReceived)
	}
	if stats.GapsDetected != 0 {
		t.Fatalf("GapsDetected=%d, want 0", stats.GapsDetected)
	}
	if stats.OutOfOrderCount != 0 {
		t.Fatalf("OutOfOrderCount=%d, want 0", stats.OutOfOrderCount)
	}
	if stats.Duplicates != 0 {
		t.Fatalf("Duplicates=%d, want 0", stats.Duplicates)
	}
	if stats.LatestSeq != 4 {
		t.Fatalf("LatestSeq=%d, want 4", stats.LatestSeq)
	}
}

func TestValidatorGapDetection(t *testing.T) {
	v := NewValidator()

	// 1, 2, 4 → gap at 3
	result, gap := v.Validate("AAPL", 1)
	if result != Ok || gap != nil {
		t.Fatalf("seq=1: result=%s gap=%v", result, gap)
	}
	result, gap = v.Validate("AAPL", 2)
	if result != Ok || gap != nil {
		t.Fatalf("seq=2: result=%s gap=%v", result, gap)
	}
	result, gap = v.Validate("AAPL", 4)
	if result != Ok {
		t.Fatalf("seq=4: result=%s, want ok", result)
	}
	if gap == nil {
		t.Fatal("seq=4: expected gap, got nil")
	}
	if gap.Expected != 3 {
		t.Fatalf("gap.Expected=%d, want 3", gap.Expected)
	}
	if gap.Received != 4 {
		t.Fatalf("gap.Received=%d, want 4", gap.Received)
	}
	if gap.Missing != 1 {
		t.Fatalf("gap.Missing=%d, want 1", gap.Missing)
	}

	stats := v.StatsFor("AAPL")
	if stats.GapsDetected != 1 {
		t.Fatalf("GapsDetected=%d, want 1", stats.GapsDetected)
	}
}

func TestValidatorMultiGap(t *testing.T) {
	v := NewValidator()

	// 1, 5 → gap of 3 (missing 2,3,4)
	v.Validate("AAPL", 1)
	_, gap := v.Validate("AAPL", 5)
	if gap == nil {
		t.Fatal("expected gap, got nil")
	}
	if gap.Missing != 3 {
		t.Fatalf("gap.Missing=%d, want 3", gap.Missing)
	}

	stats := v.StatsFor("AAPL")
	if stats.GapsDetected != 1 {
		t.Fatalf("GapsDetected=%d, want 1", stats.GapsDetected)
	}
}

func TestValidatorOutOfOrder(t *testing.T) {
	v := NewValidator()

	v.Validate("AAPL", 1)
	v.Validate("AAPL", 2)
	v.Validate("AAPL", 3)

	// seq=2 arrives after seq=3
	result, _ := v.Validate("AAPL", 2)
	if result != OutOfOrder {
		t.Fatalf("result=%s, want out_of_order", result)
	}

	stats := v.StatsFor("AAPL")
	if stats.OutOfOrderCount != 1 {
		t.Fatalf("OutOfOrderCount=%d, want 1", stats.OutOfOrderCount)
	}
}

func TestValidatorDuplicate(t *testing.T) {
	v := NewValidator()

	v.Validate("AAPL", 1)
	v.Validate("AAPL", 2)

	// Duplicate of seq=2
	result, _ := v.Validate("AAPL", 2)
	if result != Duplicate {
		t.Fatalf("result=%s, want duplicate", result)
	}

	stats := v.StatsFor("AAPL")
	if stats.Duplicates != 1 {
		t.Fatalf("Duplicates=%d, want 1", stats.Duplicates)
	}
	if stats.TotalReceived != 3 {
		t.Fatalf("TotalReceived=%d, want 3", stats.TotalReceived)
	}
}

func TestValidatorPerSymbolIndependence(t *testing.T) {
	v := NewValidator()

	v.Validate("AAPL", 1)
	v.Validate("AAPL", 2)
	v.Validate("MSFT", 1)

	aaplStats := v.StatsFor("AAPL")
	msftStats := v.StatsFor("MSFT")

	if aaplStats.LatestSeq != 2 {
		t.Fatalf("AAPL LatestSeq=%d, want 2", aaplStats.LatestSeq)
	}
	if msftStats.LatestSeq != 1 {
		t.Fatalf("MSFT LatestSeq=%d, want 1", msftStats.LatestSeq)
	}
}

func TestValidatorReset(t *testing.T) {
	v := NewValidator()
	v.Validate("AAPL", 1)
	v.Validate("AAPL", 2)

	v.Reset()

	stats := v.StatsFor("AAPL")
	if stats.TotalReceived != 0 {
		t.Fatalf("TotalReceived after Reset=%d, want 0", stats.TotalReceived)
	}
}

func TestValidatorStatsForUnknownSymbol(t *testing.T) {
	v := NewValidator()
	stats := v.StatsFor("UNKNOWN")
	if stats.TotalReceived != 0 {
		t.Fatalf("StatsFor unknown symbol should be zero, got %+v", stats)
	}
}

func TestValidationResultString(t *testing.T) {
	tests := []struct {
		r    ValidationResult
		want string
	}{
		{Ok, "ok"},
		{Gap, "gap"},
		{OutOfOrder, "out_of_order"},
		{Duplicate, "duplicate"},
		{ValidationResult(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.r.String(); got != tt.want {
			t.Errorf("ValidationResult(%d).String() = %q, want %q", tt.r, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Integration: sequencer + validator
// ---------------------------------------------------------------------------

func TestSequencerValidatorIntegration(t *testing.T) {
	seq := New()
	val := NewValidator()

	symbols := []string{"AAPL", "MSFT", "NVDA"}

	// Simulate 100 ticks per symbol.
	for tick := 0; tick < 100; tick++ {
		for _, sym := range symbols {
			s := seq.Next(sym)
			result, gap := val.Validate(sym, s)
			if result != Ok {
				t.Fatalf("symbol=%s seq=%d result=%s", sym, s, result)
			}
			if gap != nil {
				t.Fatalf("symbol=%s seq=%d unexpected gap=%+v", sym, s, gap)
			}
		}
	}

	for _, sym := range symbols {
		stats := val.StatsFor(sym)
		if stats.LatestSeq != 100 {
			t.Fatalf("%s LatestSeq=%d, want 100", sym, stats.LatestSeq)
		}
		if stats.GapsDetected != 0 {
			t.Fatalf("%s GapsDetected=%d, want 0", sym, stats.GapsDetected)
		}
		if stats.OutOfOrderCount != 0 {
			t.Fatalf("%s OutOfOrderCount=%d, want 0", sym, stats.OutOfOrderCount)
		}
	}
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

func BenchmarkSequencerNext(b *testing.B) {
	s := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Next("BENCH")
	}
}

func BenchmarkValidatorValidate(b *testing.B) {
	v := NewValidator()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.Validate("BENCH", int64(i+1))
	}
}

func BenchmarkSequencerNextParallel(b *testing.B) {
	s := New()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s.Next("BENCH")
		}
	})
}
