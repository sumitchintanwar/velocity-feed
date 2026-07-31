package protocol

import (
	"bytes"

	"github.com/sumit/rtmds/pkg/marketdata"
)

// Format represents a wire protocol used to encode messages.
type Format uint8

const (
	// FormatJSON is the standard JSON representation.
	FormatJSON Format = iota
	// FormatProtobuf is the Protocol Buffers representation.
	FormatProtobuf
	// FormatFlatBuffers is the FlatBuffers representation.
	FormatFlatBuffers
)

func (f Format) String() string {
	switch f {
	case FormatJSON:
		return "json"
	case FormatProtobuf:
		return "protobuf"
	case FormatFlatBuffers:
		return "flatbuffers"
	default:
		return "unknown"
	}
}

// PreSerializedMessage holds pre-computed byte slices for different formats
// allowing a single domain event to be serialized once and broadcast to many clients.
type PreSerializedMessage interface {
	// Payload returns the raw bytes for the requested format.
	// Returns an error if the payload was not pre-computed for this format.
	Payload(format Format) ([]byte, error)

	// Retain increments the reference count of the underlying buffers.
	// Must be called if the message will be used asynchronously.
	Retain()

	// Release decrements the reference count and returns memory to the pool
	// when it hits zero. Must be called when finished with the message.
	Release()
}

// Codec defines the contract for serializing internal market data events.
type Codec interface {
	// Format returns the format this codec produces.
	Format() Format

	// Encode serializes the market event into the provided buffer.
	Encode(event marketdata.MarketEvent, buf *bytes.Buffer) error
}
