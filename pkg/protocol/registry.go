package protocol

import (
	"fmt"
	"sync"
)

// Registry manages a collection of serializers keyed by their Format.
type Registry struct {
	mu          sync.RWMutex
	serializers map[Format]Serializer
}

// NewRegistry creates a new empty serializer registry.
func NewRegistry() *Registry {
	return &Registry{
		serializers: make(map[Format]Serializer),
	}
}

// Register adds a new serializer to the registry.
func (r *Registry) Register(s Serializer) {
	if s == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.serializers[s.Format()] = s
}

// Lookup returns the serializer for the given format, or an error if not found.
func (r *Registry) Lookup(f Format) (Serializer, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.serializers[f]
	if !ok {
		return nil, fmt.Errorf("serializer for format %s not found", f)
	}
	return s, nil
}
