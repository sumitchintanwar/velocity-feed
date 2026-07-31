package protocol

import (
	"fmt"
	"strings"
)

const (
	SubprotocolJSON        = "v1.json.rtmds"
	SubprotocolProtobuf    = "v1.protobuf.rtmds"
	SubprotocolFlatBuffers = "v1.flatbuffers.rtmds"
)

// Negotiator handles the mapping between WebSocket subprotocols and internal Formats.
type Negotiator struct {
	supported map[string]Format
}

// NewNegotiator creates a Negotiator with a defined set of supported subprotocols.
func NewNegotiator() *Negotiator {
	return &Negotiator{
		supported: map[string]Format{
			SubprotocolJSON:        FormatJSON,
			SubprotocolProtobuf:    FormatProtobuf,
			SubprotocolFlatBuffers: FormatFlatBuffers,
		},
	}
}

// SelectProtocol parses the Sec-WebSocket-Protocol header (which may contain
// a comma-separated list of protocols) and returns the most preferred Format,
// the matched subprotocol string, and an error if none match.
// Preference is given in the order the client specifies them, prioritizing
// binary formats if the client provides equal preference.
func (n *Negotiator) SelectProtocol(clientProtocols []string) (Format, string, error) {
	if len(clientProtocols) == 0 {
		return FormatJSON, SubprotocolJSON, nil
	}

	for _, p := range clientProtocols {
		p = strings.TrimSpace(p)
		if f, ok := n.supported[p]; ok {
			return f, p, nil
		}
	}

	return 0, "", fmt.Errorf("no supported subprotocols found in %v", clientProtocols)
}

// FormatToSubprotocol converts an internal Format back to its standard string representation.
func FormatToSubprotocol(f Format) string {
	switch f {
	case FormatJSON:
		return SubprotocolJSON
	case FormatProtobuf:
		return SubprotocolProtobuf
	case FormatFlatBuffers:
		return SubprotocolFlatBuffers
	default:
		return ""
	}
}
