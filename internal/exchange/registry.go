package exchange

import (
	"fmt"
	"sync"
)

// Purpose: Centralized registry for adapter factories.
// Architecture: Implements the Registry pattern to support Dependency Injection and plugin-style loading.
// Design Decisions: Concurrent-safe map ensures thread-safe registration during init().

var (
	registryMu sync.RWMutex
	factories  = make(map[string]AdapterFactory)
)

// Register records an adapter factory by name. Panics if registered twice.
func Register(name string, factory AdapterFactory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := factories[name]; exists {
		panic(fmt.Sprintf("exchange: adapter %s already registered", name))
	}
	factories[name] = factory
}

// GetFactory returns the factory for a given adapter name.
func GetFactory(name string) (AdapterFactory, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	factory, exists := factories[name]
	if !exists {
		return nil, fmt.Errorf("exchange: adapter %s not found", name)
	}
	return factory, nil
}
