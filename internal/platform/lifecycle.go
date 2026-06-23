// Package platform provides cross-cutting concerns: logging, metrics,
// and lifecycle management for application components.
package platform

import "context"

// Component represents a named application component with lifecycle management.
type Component interface {
	// Name returns the component's name for logging and health checks.
	Name() string
}

// Startable is implemented by components that need initialization work.
// Start is called during the startup sequence. The component must not
// begin accepting work until Start returns successfully.
type Startable interface {
	Start(ctx context.Context) error
}

// Stoppable is implemented by components that need cleanup work.
// Stop is called during the shutdown sequence in reverse startup order.
// Stop must complete within the deadline imposed by ctx.
type Stoppable interface {
	Stop(ctx context.Context) error
}

// HealthChecker is implemented by components that can report their health.
type HealthChecker interface {
	HealthCheck(ctx context.Context) HealthStatus
}

// HealthStatus represents the health of a component.
type HealthStatus struct {
	OK     bool   `json:"ok"`
	Detail string `json:"detail,omitempty"`
}

// OK creates a healthy status.
func OK() HealthStatus {
	return HealthStatus{OK: true}
}

// Degraded creates a degraded (warning) status.
func Degraded(detail string) HealthStatus {
	return HealthStatus{OK: true, Detail: detail}
}

// Unhealthy creates an unhealthy status.
func Unhealthy(detail string) HealthStatus {
	return HealthStatus{OK: false, Detail: detail}
}
