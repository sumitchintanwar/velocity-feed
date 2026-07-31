package protocol

import "github.com/sumit/rtmds/pkg/marketdata"

// Serializer defines the contract for serializing and deserializing
// internal market data events. It is used both at the gateway (for
// broadcasting) and potentially at the client edge (for parsing).
type Serializer interface {
	// Format returns the format this serializer produces.
	Format() Format

	// Serialize encodes the market event into a byte slice.
	// To minimize allocations, implementations may return byte slices
	// backed by a sync.Pool. It is the caller's responsibility to handle
	// lifecycle if required.
	Serialize(event marketdata.MarketEvent) ([]byte, error)

	// Deserialize decodes the byte slice into a concrete domain event.
	Deserialize(data []byte) (marketdata.MarketEvent, error)
}
