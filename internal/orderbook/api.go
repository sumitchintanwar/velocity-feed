package orderbook

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// API provides HTTP endpoints for Order Book operations, primarily snapshots.
type API struct {
	manager *Manager
}

// NewAPI creates a new API instance.
func NewAPI(manager *Manager) *API {
	return &API{
		manager: manager,
	}
}

// RegisterRoutes registers the L2 routes with the provided chi router.
func (a *API) RegisterRoutes(r chi.Router) {
	r.Route("/api/v1/l2", func(r chi.Router) {
		r.Get("/{symbol}/snapshot", a.handleGetSnapshot)
	})
}

func (a *API) handleGetSnapshot(w http.ResponseWriter, r *http.Request) {
	symbol := chi.URLParam(r, "symbol")

	snap, err := a.manager.GetSnapshot(symbol)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(snap); err != nil {
		http.Error(w, "Failed to encode snapshot", http.StatusInternalServerError)
	}
}
