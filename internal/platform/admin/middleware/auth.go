package middleware

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

// Role defines the operational capabilities of the caller.
type Role string

const (
	RoleViewer        Role = "viewer"
	RoleOperator      Role = "operator"
	RoleAdministrator Role = "administrator"
)

// Context keys
type contextKey string
const (
	userContextKey contextKey = "user_identity"
	roleContextKey contextKey = "user_role"
)

// Authenticator validates a bearer token and resolves the user's identity and role.
type Authenticator interface {
	AuthenticateToken(token string) (identity string, role Role, err error)
}

// StaticTokenAuthenticator is an MVP implementation that uses static configuration tokens.
type StaticTokenAuthenticator struct {
	AdminToken    string
	OperatorToken string
	ViewerToken   string
}

func (s *StaticTokenAuthenticator) AuthenticateToken(token string) (string, Role, error) {
	switch token {
	case s.AdminToken:
		return "admin-user", RoleAdministrator, nil
	case s.OperatorToken:
		return "operator-user", RoleOperator, nil
	case s.ViewerToken:
		return "viewer-user", RoleViewer, nil
	default:
		return "", "", errors.New("invalid token")
	}
}

// Authenticate verifies the Authorization Bearer token using the provided Authenticator.
func Authenticate(auth Authenticator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
				http.Error(w, `{"error": "unauthorized"}`, http.StatusUnauthorized)
				return
			}

			token := strings.TrimPrefix(authHeader, "Bearer ")
			identity, role, err := auth.AuthenticateToken(token)
			if err != nil {
				http.Error(w, `{"error": "invalid token"}`, http.StatusUnauthorized)
				return
			}

			ctx := context.WithValue(r.Context(), userContextKey, identity)
			ctx = context.WithValue(ctx, roleContextKey, role)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RequireRole ensures the caller has at least the required role to execute the handler.
func RequireRole(required Role) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			roleVal := r.Context().Value(roleContextKey)
			if roleVal == nil {
				http.Error(w, `{"error": "unauthorized"}`, http.StatusUnauthorized)
				return
			}

			actualRole := roleVal.(Role)
			
			if !isAuthorized(actualRole, required) {
				http.Error(w, `{"error": "forbidden: insufficient permissions"}`, http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func isAuthorized(actual, required Role) bool {
	if required == RoleViewer {
		return actual == RoleViewer || actual == RoleOperator || actual == RoleAdministrator
	}
	if required == RoleOperator {
		return actual == RoleOperator || actual == RoleAdministrator
	}
	if required == RoleAdministrator {
		return actual == RoleAdministrator
	}
	return false
}

func GetUserIdentity(ctx context.Context) string {
	if val := ctx.Value(userContextKey); val != nil {
		return val.(string)
	}
	return "anonymous"
}
