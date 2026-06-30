package handlers

import (
	"encoding/json"
	"net/http"
	"runtime"
	"sync"
	"time"
)

// DiagnosticsHandler provides insight into Go runtime metrics.
type DiagnosticsHandler struct {
	mu           sync.Mutex
	lastMemCheck time.Time
	cachedMem    *runtime.MemStats
}

func NewDiagnosticsHandler() *DiagnosticsHandler {
	return &DiagnosticsHandler{}
}

func (h *DiagnosticsHandler) HandleGoroutines(w http.ResponseWriter, r *http.Request) {
	count := runtime.NumGoroutine()
	response := map[string]interface{}{
		"goroutines": count,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (h *DiagnosticsHandler) HandleMemory(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	now := time.Now()
	
	// Cache for 1 second to prevent "stop-the-world" spam from degrading system performance
	if h.cachedMem == nil || now.Sub(h.lastMemCheck) > time.Second {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		h.cachedMem = &m
		h.lastMemCheck = now
	}
	m := h.cachedMem
	h.mu.Unlock()

	response := map[string]interface{}{
		"alloc_bytes":       m.Alloc,
		"total_alloc_bytes": m.TotalAlloc,
		"sys_bytes":         m.Sys,
		"num_gc":            m.NumGC,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
