package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sumit/rtmds/internal/platform/admin/api"
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
func (m *mockComponent) Name() string { return m.name }
func (m *mockComponent) Start(ctx context.Context) error { return nil }
func (m *mockComponent) Stop(ctx context.Context) error { return nil }

func setupQAServer() (*http.ServeMux, *commands.MockPublisherController, *commands.MockMaintenanceController) {
	manager := lifecycle.NewManager()
	manager.Register(&mockComponent{name: "mock"})

	auditLogger := audit.NewZapAuditLogger(zap.NewNop())
	bus := commands.NewCommandBus()
	pubCtrl := &commands.MockPublisherController{}
	maintCtrl := &commands.MockMaintenanceController{}

	cfg := api.RouterConfig{
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
	}

	return api.NewRouter(cfg), pubCtrl, maintCtrl
}

// 2.1 Inspection (Read-Only)
func TestQA_Inspection(t *testing.T) {
	router, _, _ := setupQAServer()

	// Missing token -> 401
	req := httptest.NewRequest("GET", "/inspection/runtime", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rr.Code)
	}

	// Viewer token -> 200
	req = httptest.NewRequest("GET", "/inspection/runtime", nil)
	req.Header.Set("Authorization", "Bearer viewer")
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

// 2.2 Diagnostics
func TestQA_Diagnostics(t *testing.T) {
	router, _, _ := setupQAServer()
	
	req := httptest.NewRequest("GET", "/diagnostics/goroutines", nil)
	req.Header.Set("Authorization", "Bearer viewer")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rr.Code)
	}
}

// 2.3 Operations & Idempotency
func TestQA_OperationsIdempotency(t *testing.T) {
	router, _, _ := setupQAServer()

	// 1. Pause Publisher -> 200
	req := httptest.NewRequest("POST", "/operations/publisher/pause", nil)
	req.Header.Set("Authorization", "Bearer operator")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 on first pause, got %d", rr.Code)
	}

	// 2. Pause Publisher again -> 409 Conflict (or error)
	req2 := httptest.NewRequest("POST", "/operations/publisher/pause", nil)
	req2.Header.Set("Authorization", "Bearer operator")
	rr2 := httptest.NewRecorder()
	router.ServeHTTP(rr2, req2)
	// Currently our operations handler returns 409 on error!
	if rr2.Code != http.StatusConflict {
		t.Errorf("expected 409 on second pause, got %d", rr2.Code)
	}
}

// 2.4 Concurrency & Security
func TestQA_Concurrency(t *testing.T) {
	router, _, maintCtrl := setupQAServer()

	// Verify Viewer gets 403
	req := httptest.NewRequest("POST", "/operations/maintenance/enable", nil)
	req.Header.Set("Authorization", "Bearer viewer")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Errorf("expected 403, got %d", rr.Code)
	}

	// Concurrent maintenance enablement (50 requests at exactly same time)
	var wg sync.WaitGroup
	var successCount int32
	var conflictCount int32

	requests := 50
	wg.Add(requests)

	// Launch them all
	for i := 0; i < requests; i++ {
		go func() {
			defer wg.Done()
			
			creq := httptest.NewRequest("POST", "/operations/maintenance/enable", nil)
			creq.Header.Set("Authorization", "Bearer operator")
			crr := httptest.NewRecorder()
			
			router.ServeHTTP(crr, creq)

			if crr.Code == http.StatusOK {
				atomic.AddInt32(&successCount, 1)
			} else if crr.Code == http.StatusConflict {
				atomic.AddInt32(&conflictCount, 1)
			}
		}()
	}

	wg.Wait()

	if successCount != 1 {
		t.Errorf("expected exactly 1 success, got %d", successCount)
	}
	
	if conflictCount != int32(requests-1) {
		t.Errorf("expected exactly %d conflicts, got %d", requests-1, conflictCount)
	}

	if !maintCtrl.IsMaintenance() {
		t.Errorf("expected maintenance mode to be true")
	}
}
