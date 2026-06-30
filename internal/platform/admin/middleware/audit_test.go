package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sumit/rtmds/internal/platform/admin/audit"
)

// mockAuditLogger captures the last recorded event
type mockAuditLogger struct {
	lastEvent audit.Event
}

func (m *mockAuditLogger) Record(ctx context.Context, event audit.Event) {
	m.lastEvent = event
}

func TestAuditMiddleware_Success(t *testing.T) {
	logger := &mockAuditLogger{}
	actionName := "PausePublisher"
	targetService := "publisher"

	handler := Audit(logger, actionName, targetService)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("POST", "/operations/pause", nil)
	// Inject user identity into context
	ctx := context.WithValue(req.Context(), userContextKey, "test-user")
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if logger.lastEvent.Outcome != audit.OutcomeSuccess {
		t.Errorf("expected outcome SUCCESS, got %v", logger.lastEvent.Outcome)
	}
	if logger.lastEvent.Action != actionName {
		t.Errorf("expected action %s, got %s", actionName, logger.lastEvent.Action)
	}
	if logger.lastEvent.UserIdentity != "test-user" {
		t.Errorf("expected user test-user, got %s", logger.lastEvent.UserIdentity)
	}
	if logger.lastEvent.Duration < 0 {
		t.Errorf("expected non-negative duration, got %v", logger.lastEvent.Duration)
	}
}

func TestAuditMiddleware_Failure(t *testing.T) {
	logger := &mockAuditLogger{}

	handler := Audit(logger, "FailAction", "testSvc")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))

	req := httptest.NewRequest("POST", "/operations/fail", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if logger.lastEvent.Outcome != audit.OutcomeFailure {
		t.Errorf("expected outcome FAILURE, got %v", logger.lastEvent.Outcome)
	}
}
