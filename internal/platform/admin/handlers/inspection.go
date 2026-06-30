package handlers

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/sumit/rtmds/internal/platform/lifecycle"
)

// InspectionHandler provides read-only visibility into the platform's runtime state.
type InspectionHandler struct {
	Manager *lifecycle.Manager
	Version string
	Started time.Time
}

// NewInspectionHandler creates a new InspectionHandler.
func NewInspectionHandler(manager *lifecycle.Manager, version string) *InspectionHandler {
	return &InspectionHandler{
		Manager: manager,
		Version: version,
		Started: time.Now(),
	}
}

func (h *InspectionHandler) HandleRuntime(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"version": h.Version,
		"uptime":  time.Since(h.Started).String(),
		"state":   "running",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *InspectionHandler) HandleConfiguration(w http.ResponseWriter, r *http.Request) {
	// Redact sensitive secrets before returning
	response := map[string]interface{}{
		"environment": "production",
		"log_level":   "info",
		// In a real implementation, this would dump safe config structs
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
