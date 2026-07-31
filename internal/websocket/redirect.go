package websocket

// Redirector determines if a symbol belongs to another gateway.
type Redirector interface {
	// RedirectTarget returns the full websocket URL (e.g., "ws://10.0.0.5:8080/ws")
	// of the gateway that owns the given symbol.
	// If the current gateway owns the symbol, it returns an empty string.
	RedirectTarget(symbol string) string
}

// RedirectPayload is sent to the client when they subscribe to a symbol
// that is owned by a different gateway in the consistent hash ring.
type RedirectPayload struct {
	Symbol string `json:"symbol"`
	URL    string `json:"url"`
}
