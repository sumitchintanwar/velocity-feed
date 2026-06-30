// Package exporter provides HTTP handlers for exposing metrics to Prometheus.
package exporter

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sumit/rtmds/internal/metrics/registry"
)

// MountMetrics attaches the Prometheus scrape endpoint to the provided ServeMux.
// It leverages the specific isolated registry to prevent exposing unwanted global metrics
// (e.g., standard go metrics) unless explicitly registered.
func MountMetrics(mux *http.ServeMux, path string, reg *registry.Registry) {
	handler := promhttp.HandlerFor(reg.Gatherer(), promhttp.HandlerOpts{})
	mux.Handle(path, handler)
}
