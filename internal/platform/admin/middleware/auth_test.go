package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAuthenticate(t *testing.T) {
	auth := &StaticTokenAuthenticator{
		AdminToken:    "admin-secret",
		OperatorToken: "operator-secret",
		ViewerToken:   "viewer-secret",
	}

	handler := Authenticate(auth)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity := GetUserIdentity(r.Context())
		if identity == "anonymous" {
			t.Errorf("expected identity to be injected into context")
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}
}

func TestRequireRole(t *testing.T) {
	auth := &StaticTokenAuthenticator{
		AdminToken:    "admin-secret",
		ViewerToken:   "viewer-secret",
	}

	// Route that requires Operator level
	handler := Authenticate(auth)(RequireRole(RoleOperator)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))

	tests := []struct {
		name       string
		token      string
		wantStatus int
	}{
		{"Admin Access", "Bearer admin-secret", http.StatusOK}, // Admin > Operator
		{"Viewer Denied", "Bearer viewer-secret", http.StatusForbidden}, // Viewer < Operator
		{"Missing Auth", "", http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			if tt.token != "" {
				req.Header.Set("Authorization", tt.token)
			}
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Errorf("got status %v, want %v", rr.Code, tt.wantStatus)
			}
		})
	}
}
