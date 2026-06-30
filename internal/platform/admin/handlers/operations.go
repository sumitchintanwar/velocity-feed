package handlers

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/sumit/rtmds/internal/platform/admin/commands"
)

type OperationsHandler struct {
	bus *commands.CommandBus
}

func NewOperationsHandler(bus *commands.CommandBus) *OperationsHandler {
	return &OperationsHandler{bus: bus}
}

// HandleDispatch dynamically routes the HTTP request to the corresponding command in the bus.
// It expects the path to be parameterized as `/operations/{service}/{action}` (or similarly deeply nested).
func (h *OperationsHandler) HandleDispatch(w http.ResponseWriter, r *http.Request) {
	// Extract the path suffix after "/operations/"
	path := strings.TrimPrefix(r.URL.Path, "/operations/")
	path = strings.Trim(path, "/")

	// Decode payload if present (e.g. for log level setting)
	var payload map[string]interface{}
	if r.Body != nil && r.ContentLength > 0 {
		_ = json.NewDecoder(r.Body).Decode(&payload)
	}

	cmdName, err := h.bus.DispatchByPath(r.Context(), path, payload)
	
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		if strings.Contains(err.Error(), "no command registered") {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusConflict) // Or 500/400
		}
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "success", "command": cmdName})
}
