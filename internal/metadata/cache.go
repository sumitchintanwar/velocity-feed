package metadata

import (
	"fmt"
	"sync/atomic"
)

// cacheData holds the internal maps for the cache.
type cacheData struct {
	bySymbol     map[string]*Instrument
	byExchange   map[string][]*Instrument
	byAssetClass map[AssetClass][]*Instrument
}

// InstrumentCache is a highly concurrent, lock-free read-optimized cache for metadata lookups.
type InstrumentCache struct {
	value atomic.Value // holds *cacheData
}

// NewInstrumentCache creates a new empty cache.
func NewInstrumentCache() *InstrumentCache {
	c := &InstrumentCache{}
	// Initialize with empty maps
	c.value.Store(&cacheData{
		bySymbol:     make(map[string]*Instrument),
		byExchange:   make(map[string][]*Instrument),
		byAssetClass: make(map[AssetClass][]*Instrument),
	})
	return c
}

// Replace atomically swaps the entire cache contents with the new dataset.
// Pre-allocates map capacities based on the old data to reduce GC pressure.
func (c *InstrumentCache) Replace(instruments []*Instrument) {
	oldData := c.value.Load().(*cacheData)
	
	newBySymbol := make(map[string]*Instrument, len(oldData.bySymbol))
	newByExchange := make(map[string][]*Instrument, len(oldData.byExchange))
	newByAssetClass := make(map[AssetClass][]*Instrument, len(oldData.byAssetClass))

	for _, inst := range instruments {
		newBySymbol[inst.CanonicalSymbol] = inst
		newByExchange[inst.Exchange] = append(newByExchange[inst.Exchange], inst)
		newByAssetClass[inst.AssetClass] = append(newByAssetClass[inst.AssetClass], inst)
	}

	c.value.Store(&cacheData{
		bySymbol:     newBySymbol,
		byExchange:   newByExchange,
		byAssetClass: newByAssetClass,
	})
}

// GetBySymbol performs an O(1) lock-free lookup for a specific canonical symbol.
func (c *InstrumentCache) GetBySymbol(symbol string) (*Instrument, error) {
	data := c.value.Load().(*cacheData)
	inst, ok := data.bySymbol[symbol]
	if !ok {
		return nil, fmt.Errorf("symbol not found: %s", symbol)
	}
	return inst, nil
}

// GetByExchange returns all instruments listed on a specific exchange.
func (c *InstrumentCache) GetByExchange(exchange string) []*Instrument {
	data := c.value.Load().(*cacheData)
	return data.byExchange[exchange]
}

// GetByAssetClass returns all instruments of a specific asset class.
func (c *InstrumentCache) GetByAssetClass(class AssetClass) []*Instrument {
	data := c.value.Load().(*cacheData)
	return data.byAssetClass[class]
}
