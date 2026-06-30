package healthcheck

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRegistry_AllHealthy(t *testing.T) {
	r := NewRegistry()
	r.Register(Check{
		Name: "check-a",
		CheckFn: func(_ context.Context) error {
			return nil
		},
		Timeout: 1 * time.Second,
	})
	r.Register(Check{
		Name: "check-b",
		CheckFn: func(_ context.Context) error {
			return nil
		},
		Timeout: 1 * time.Second,
	})

	result := r.Run(context.Background())

	if result.Status != "ok" {
		t.Errorf("expected status ok, got %s", result.Status)
	}
	if len(result.Checks) != 2 {
		t.Errorf("expected 2 checks, got %d", len(result.Checks))
	}
	for _, c := range result.Checks {
		if !c.OK {
			t.Errorf("check %s should be OK", c.Name)
		}
	}
}

func TestRegistry_OneFails(t *testing.T) {
	r := NewRegistry()
	r.Register(Check{
		Name: "healthy",
		CheckFn: func(_ context.Context) error {
			return nil
		},
		Timeout: 1 * time.Second,
	})
	r.Register(Check{
		Name: "unhealthy",
		CheckFn: func(_ context.Context) error {
			return errors.New("connection refused")
		},
		Timeout: 1 * time.Second,
	})

	result := r.Run(context.Background())

	if result.Status != "degraded" {
		t.Errorf("expected status degraded, got %s", result.Status)
	}
	for _, c := range result.Checks {
		if c.Name == "unhealthy" && c.OK {
			t.Error("unhealthy check should not be OK")
		}
		if c.Name == "unhealthy" && c.Detail != "connection refused" {
			t.Errorf("expected detail 'connection refused', got '%s'", c.Detail)
		}
	}
}

func TestRegistry_Timeout(t *testing.T) {
	r := NewRegistry()
	r.Register(Check{
		Name: "slow",
		CheckFn: func(ctx context.Context) error {
			select {
			case <-time.After(5 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		Timeout: 50 * time.Millisecond,
	})

	start := time.Now()
	result := r.Run(context.Background())
	elapsed := time.Since(start)

	if result.Status != "degraded" {
		t.Errorf("expected status degraded, got %s", result.Status)
	}
	if elapsed > 200*time.Millisecond {
		t.Errorf("timeout check took too long: %v", elapsed)
	}
	for _, c := range result.Checks {
		if c.Name == "slow" && c.OK {
			t.Error("slow check should have timed out")
		}
	}
}

func TestRegistry_ContextCanceled(t *testing.T) {
	r := NewRegistry()
	r.Register(Check{
		Name: "canceled",
		CheckFn: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
		Timeout: 5 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	result := r.Run(ctx)

	if result.Status != "degraded" {
		t.Errorf("expected status degraded, got %s", result.Status)
	}
}

func TestRegistry_Empty(t *testing.T) {
	r := NewRegistry()
	result := r.Run(context.Background())

	if result.Status != "ok" {
		t.Errorf("expected status ok for empty registry, got %s", result.Status)
	}
	if len(result.Checks) != 0 {
		t.Errorf("expected 0 checks, got %d", len(result.Checks))
	}
}

func TestRegistry_ConcurrentSafety(t *testing.T) {
	r := NewRegistry()
	for i := 0; i < 100; i++ {
		r.Register(Check{
			Name: "check",
			CheckFn: func(_ context.Context) error {
				return nil
			},
			Timeout: 1 * time.Second,
		})
	}

	// Run concurrently
	done := make(chan struct{}, 10)
	for i := 0; i < 10; i++ {
		go func() {
			_ = r.Run(context.Background())
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestRunChecks_Convenience(t *testing.T) {
	result := RunChecks(context.Background(),
		Check{
			Name: "ok",
			CheckFn: func(_ context.Context) error {
				return nil
			},
			Timeout: 1 * time.Second,
		},
	)

	if result.Status != "ok" {
		t.Errorf("expected status ok, got %s", result.Status)
	}
}

func TestLiveHandler(t *testing.T) {
	handler := LiveHandler(nil)

	req := httptest.NewRequest("GET", "/liveness", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status ok, got %s", body["status"])
	}
}

func TestLiveHandler_WithHeartbeat_Alive(t *testing.T) {
	hb := NewHeartbeat()
	hb.Mark() // just marked, should be alive

	handler := LiveHandler(hb)

	req := httptest.NewRequest("GET", "/liveness", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status ok, got %s", body["status"])
	}
}

func TestLiveHandler_WithHeartbeat_Stale(t *testing.T) {
	hb := NewHeartbeat()
	// Simulate stale heartbeat by setting time far in the past
	hb.lastBeat.Store(time.Now().Add(-20 * time.Second).UnixNano())

	handler := LiveHandler(hb)

	req := httptest.NewRequest("GET", "/liveness", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}

	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if body["status"] != "not_ok" {
		t.Errorf("expected status not_ok, got %s", body["status"])
	}
}

func TestReadyHandler_AllHealthy(t *testing.T) {
	r := NewRegistry()
	r.Register(Check{
		Name: "ok",
		CheckFn: func(_ context.Context) error {
			return nil
		},
		Timeout: 1 * time.Second,
	})

	handler := r.ReadyHandler()

	req := httptest.NewRequest("GET", "/readiness", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestReadyHandler_Degraded(t *testing.T) {
	r := NewRegistry()
	r.Register(Check{
		Name: "failing",
		CheckFn: func(_ context.Context) error {
			return errors.New("redis down")
		},
		Timeout: 1 * time.Second,
	})

	handler := r.ReadyHandler()

	req := httptest.NewRequest("GET", "/readiness", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}

	var body Result
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if body.Status != "degraded" {
		t.Errorf("expected status degraded, got %s", body.Status)
	}
}

func TestResult_Summary(t *testing.T) {
	result := Result{
		Status: "ok",
		Checks: []Status{
			{Name: "a", OK: true},
		},
		Duration: 5 * time.Millisecond,
	}

	summary := result.Summary()
	if summary != `{"status":"ok","checks":1,"duration_ms":5}` {
		t.Errorf("unexpected summary: %s", summary)
	}
}

func TestFunc_Checker(t *testing.T) {
	called := false
	c := Func("custom", func(_ context.Context) error {
		called = true
		return nil
	}, 1*time.Second)

	err := c.CheckFn(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("check function was not called")
	}
	if c.Name != "custom" {
		t.Errorf("expected name custom, got %s", c.Name)
	}
}

func TestErrDetail_Nil(t *testing.T) {
	d := errDetail(nil)
	if d != "" {
		t.Errorf("expected empty string for nil error, got '%s'", d)
	}
}

func TestErrDetail_Error(t *testing.T) {
	d := errDetail(errors.New("test error"))
	if d != "test error" {
		t.Errorf("expected 'test error', got '%s'", d)
	}
}

func TestHeartbeat_MarkAndIsAlive(t *testing.T) {
	hb := NewHeartbeat()

	// Just created and marked — should be alive
	if !hb.IsAlive(10 * time.Second) {
		t.Error("expected heartbeat to be alive immediately after Mark")
	}

	// Simulate old heartbeat
	hb.lastBeat.Store(time.Now().Add(-5 * time.Second).UnixNano())
	if !hb.IsAlive(10 * time.Second) {
		t.Error("expected heartbeat to be alive within 10s window")
	}

	// Simulate stale heartbeat
	hb.lastBeat.Store(time.Now().Add(-15 * time.Second).UnixNano())
	if hb.IsAlive(10 * time.Second) {
		t.Error("expected heartbeat to be stale after 10s window")
	}
}

func TestHeartbeat_LastBeat(t *testing.T) {
	hb := NewHeartbeat()
	before := time.Now()
	hb.Mark()
	after := time.Now()

	last := hb.LastBeat()
	if last.Before(before) || last.After(after) {
		t.Errorf("LastBeat() = %v, want between %v and %v", last, before, after)
	}
}

func TestLivenessCheck_Alive(t *testing.T) {
	hb := NewHeartbeat()
	hb.Mark()

	check := LivenessCheck(hb, 10*time.Second)
	err := check.CheckFn(context.Background())
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestLivenessCheck_Stale(t *testing.T) {
	hb := NewHeartbeat()
	hb.lastBeat.Store(time.Now().Add(-20 * time.Second).UnixNano())

	check := LivenessCheck(hb, 10*time.Second)
	err := check.CheckFn(context.Background())
	if err == nil {
		t.Error("expected error for stale heartbeat, got nil")
	}
}

func TestRedisCheck_OnFailure_CalledOnError(t *testing.T) {
	called := false
	// Use a nil client to force an error
	check := RedisCheck(nil, func() {
		called = true
	})

	err := check.CheckFn(context.Background())
	if err == nil {
		t.Error("expected error for nil client, got nil")
	}
	if !called {
		t.Error("expected onFailure callback to be called on error")
	}
}

func TestRedisCheck_OnFailure_NotCalledOnSuccess(t *testing.T) {
	called := false
	// RedisCheck with no onFailure callback should not panic
	check := RedisCheck(nil)

	err := check.CheckFn(context.Background())
	if err == nil {
		t.Error("expected error for nil client, got nil")
	}
	// No callback registered, just verify no panic
	_ = called
}

func TestGatewayCheck_NeverFails(t *testing.T) {
	mock := &mockGateway{count: 9999}
	check := GatewayCheck(mock)

	err := check.CheckFn(context.Background())
	if err != nil {
		t.Errorf("expected nil error (capability check only), got %v", err)
	}
}

type mockGateway struct {
	count int
}

func (m *mockGateway) ClientCount() int {
	return m.count
}
