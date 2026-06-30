package metadata

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"
)

// API provides REST endpoints for the Symbol Metadata Service.
type API struct {
	service *Service
}

// NewAPI creates a new API handler.
func NewAPI(service *Service) *API {
	return &API{service: service}
}

// Routes returns the chi router with metadata endpoints.
func (a *API) Routes() chi.Router {
	r := chi.NewRouter()

	r.Get("/symbols/{symbol}", a.handleGetSymbol)
	r.Get("/exchanges/{exchange}", a.handleGetExchange)
	r.Get("/asset-classes/{class}", a.handleGetAssetClass)
	r.Post("/refresh", a.handleRefresh)

	return r
}

func (a *API) handleGetSymbol(w http.ResponseWriter, r *http.Request) {
	symbol := chi.URLParam(r, "symbol")
	inst, err := a.service.GetInstrument(symbol)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	respondJSON(w, inst)
}

func (a *API) handleGetExchange(w http.ResponseWriter, r *http.Request) {
	exchange := chi.URLParam(r, "exchange")
	insts := a.service.GetInstrumentsByExchange(exchange)
	respondPaginatedJSON(w, r, insts)
}

func (a *API) handleGetAssetClass(w http.ResponseWriter, r *http.Request) {
	class := chi.URLParam(r, "class")
	insts := a.service.GetInstrumentsByAssetClass(AssetClass(class))
	respondPaginatedJSON(w, r, insts)
}

func (a *API) handleRefresh(w http.ResponseWriter, r *http.Request) {
	// Execute refresh asynchronously to prevent blocking the HTTP client
	go func() {
		// Create a background context since the request context will be cancelled
		_ = a.service.LoadCache(context.Background())
	}()
	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte(`{"status":"accepted","message":"cache refresh initiated"}`))
}

func respondJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}

func respondPaginatedJSON(w http.ResponseWriter, r *http.Request, insts []*Instrument) {
	w.Header().Set("Content-Type", "application/json")
	
	// Basic string-based pagination fallback (in production, use standard query params)
	limitStr := r.URL.Query().Get("limit")
	offsetStr := r.URL.Query().Get("offset")
	
	limit := 100
	offset := 0
	
	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
		limit = l
	}
	if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
		offset = o
	}
	
	if offset >= len(insts) {
		_ = json.NewEncoder(w).Encode([]*Instrument{})
		return
	}
	
	end := offset + limit
	if end > len(insts) {
		end = len(insts)
	}
	
	_ = json.NewEncoder(w).Encode(insts[offset:end])
}
