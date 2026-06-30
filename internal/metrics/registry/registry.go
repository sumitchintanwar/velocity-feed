// Package registry manages the centralized Prometheus registry.
//
// Purpose:
// Every process should own exactly one Prometheus registry to avoid the notorious
// "duplicate metric registration" panics during application boot.
//
// Architecture & Design Decisions:
// - Encapsulates prometheus.Registry.
// - Provides safe Registration methods.
// - Abstracts away the global prometheus.DefaultRegisterer, forcing dependency injection.
package registry

import (
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Registry manages the registration and collection of Prometheus metrics.
type Registry struct {
	promRegistry *prometheus.Registry
	mu           sync.Mutex
	registered   map[string]prometheus.Collector
}

// New constructs a new isolated metrics registry.
// We explicitly avoid using the global prometheus.DefaultRegistry to ensure
// clean testability and isolation.
func New() *Registry {
	return &Registry{
		promRegistry: prometheus.NewRegistry(),
		registered:   make(map[string]prometheus.Collector),
	}
}

// Register safely registers a new collector (metric).
// If a metric with the same name is already registered, it returns the existing one,
// avoiding Prometheus panics on duplicate registration.
func (r *Registry) Register(name string, c prometheus.Collector) (prometheus.Collector, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, exists := r.registered[name]; exists {
		return existing, nil
	}

	if err := r.promRegistry.Register(c); err != nil {
		return nil, fmt.Errorf("failed to register metric %s: %w", name, err)
	}

	r.registered[name] = c
	return c, nil
}

// Gatherer returns the underlying prometheus Gatherer interface,
// which is required by HTTP exporters to scrape the metrics.
func (r *Registry) Gatherer() prometheus.Gatherer {
	return r.promRegistry
}
