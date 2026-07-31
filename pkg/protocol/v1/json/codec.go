package json

import (
	"bytes"
	stdjson "encoding/json"

	"github.com/sumit/rtmds/pkg/marketdata"
	"github.com/sumit/rtmds/pkg/protocol"
)

// Codec implements the JSON format encoder for market data events.
// Currently wraps encoding/json, but is structurally isolated so it can
// be seamlessly upgraded to a zero-reflection encoder like easyjson or ffjson.
type Codec struct{}

// Format returns the format identifier.
func (c *Codec) Format() protocol.Format {
	return protocol.FormatJSON
}

// Encode serializes the market event into the provided buffer.
func (c *Codec) Encode(event marketdata.MarketEvent, buf *bytes.Buffer) error {
	encoder := stdjson.NewEncoder(buf)
	// Disable HTML escaping (e.g. replacing < with \u003c) for pure API payloads
	encoder.SetEscapeHTML(false)
	return encoder.Encode(event)
}
