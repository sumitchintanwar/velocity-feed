package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sumit/rtmds/internal/platform/admin/audit"
	"github.com/sumit/rtmds/internal/platform/admin/commands"
	"github.com/sumit/rtmds/internal/platform/admin/middleware"
	"github.com/sumit/rtmds/internal/platform/lifecycle"
	
	"go.uber.org/zap"
)

// Mock components
type mockComponent struct {
	name string
}

func (m *mockComponent) Name() string {
	return m.name
}

func (m *mockComponent) Start(ctx context.Context) error {
	return nil
}

func (m *mockComponent) Stop(ctx context.Context) error {
	return nil
}

func TestRouter_Integration(t *testing.T) {
	manager := lifecycle.NewManager()
	manager.Register(&mockComponent{name: "mock"})

	auditLogger := audit.NewZapAuditLogger(zap.NewNop())
	bus := commands.NewCommandBus()
	pubCtrl := &commands.MockPublisherController{}
	maintCtrl := &commands.MockMaintenanceController{}

	cfg := RouterConfig{
		Manager:               manager,
		Version:               "1.0.0",
		Authenticator: &middleware.StaticTokenAuthenticator{
			AdminToken:    "admin",
			OperatorToken: "operator",
			ViewerToken:   "viewer",
		},
		AuditLogger:           auditLogger,
		CommandBus:            bus,
		PublisherController:   pubCtrl,
		MaintenanceController: maintCtrl,
		AtomicLogLevel:        zap.NewAtomicLevelAt(zap.InfoLevel),
	}

	router := NewRouter(cfg)

	// Test 1: Missing Auth
	req := httptest.NewRequest("GET", "/inspection/runtime", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", rr.Code)
	}

	// Test 2: Valid Auth (Viewer) - Inspection Success
	req = httptest.NewRequest("GET", "/inspection/runtime", nil)
	req.Header.Set("Authorization", "Bearer viewer")
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rr.Code)
	}

	// Test 3: Insufficient Role (Viewer trying to access pprof)
	req = httptest.NewRequest("GET", "/diagnostics/debug/pprof/", nil)
	req.Header.Set("Authorization", "Bearer viewer")
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for pprof, got %d", rr.Code)
	}

	// Test 4: Valid Admin accessing pprof
	req = httptest.NewRequest("GET", "/diagnostics/debug/pprof/", nil)
	req.Header.Set("Authorization", "Bearer admin")
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 OK for admin on pprof, got %d", rr.Code)
	}

	// Test 5: Insufficient Role (Viewer trying to Pause Publisher)
	req = httptest.NewRequest("POST", "/operations/publisher/pause", nil)
	req.Header.Set("Authorization", "Bearer viewer")
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden, got %d", rr.Code)
	}

	// Test 6: Valid Role (Operator pausing Publisher via Generic Dispatch)
	req = httptest.NewRequest("POST", "/operations/publisher/pause", nil)
	req.Header.Set("Authorization", "Bearer operator")
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rr.Code)
	}

	// Verify Publisher actually paused
	if !pubCtrl.IsPaused() {
		t.Error("expected publisher to be paused after command execution")
	}
}
