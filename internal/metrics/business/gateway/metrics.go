// Package gateway provides client experience and connection metrics for the Gateway cluster.
package gateway

import (
	"fmt"

	"github.com/sumit/rtmds/internal/metrics/factory"
	"github.com/sumit/rtmds/internal/metrics/interfaces"
	"io"
	"net"
	"strings"
)

// Metrics encapsulates all client-facing business metrics for the Gateway.
type Metrics struct {
	ActiveConnections    interfaces.Gauge
	ActiveSubscriptions  interfaces.Gauge
	ReconnectsTotal      interfaces.Counter
	DisconnectsTotal     interfaces.Counter
}

// NewMetrics instantiates and registers the Gateway metric group.
func NewMetrics(f *factory.Factory) (*Metrics, error) {
	connections, err := f.NewGauge(
		"marketdata_gateway_connections",
		"Current number of active WebSocket connections",
		[]string{"protocol"}, // e.g., "ws", "tcp"
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway_connections: %w", err)
	}

	subscriptions, err := f.NewGauge(
		"marketdata_gateway_subscriptions",
		"Current number of active topic subscriptions across all clients",
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway_subscriptions: %w", err)
	}

	reconnects, err := f.NewCounter(
		"marketdata_gateway_reconnects_total",
		"Total number of client reconnections observed",
		[]string{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway_reconnects_total: %w", err)
	}
	
	disconnects, err := f.NewCounter(
		"marketdata_gateway_disconnects_total",
		"Total number of client disconnections (clean or dropped)",
		[]string{"reason"}, // e.g., "clean", "timeout", "error"
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register gateway_disconnects_total: %w", err)
	}

	return &Metrics{
		ActiveConnections:   connections,
		ActiveSubscriptions: subscriptions,
		ReconnectsTotal:     reconnects,
		DisconnectsTotal:    disconnects,
	}, nil
}

// MapDisconnectReason sanitizes raw Go network errors into safe, bounded Prometheus enums
// to prevent cardinality explosions from dynamic error strings (e.g., dynamic IPs).
func MapDisconnectReason(err error) string {
	if err == nil {
		return "clean"
	}
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return "eof"
	}

	var netErr net.Error
	if ok := func() bool {
		// Go 1.20+ way or type assertion
		if e, ok := err.(net.Error); ok {
			netErr = e
			return true
		}
		return false
	}(); ok && netErr.Timeout() {
		return "timeout"
	}

	errStr := strings.ToLower(err.Error())
	if strings.Contains(errStr, "connection reset") {
		return "connection_reset"
	}
	if strings.Contains(errStr, "broken pipe") {
		return "broken_pipe"
	}
	if strings.Contains(errStr, "close 1006") {
		return "abnormal_closure" // typical websocket abrupt close
	}

	return "internal_error"
}
