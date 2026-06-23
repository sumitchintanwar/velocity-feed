package websocket

// ClientMessage is the JSON envelope sent by clients to the gateway.
type ClientMessage struct {
	Action  string   `json:"action"`            // "subscribe" | "unsubscribe"
	Symbols []string `json:"symbols,omitempty"` // e.g. ["AAPL", "TSLA"]
}

// ServerMessage is the JSON envelope sent by the gateway to clients.
type ServerMessage struct {
	Type    string `json:"type"`    // "trade" | "quote" | "bar" | "error" | "subscribed" | "unsubscribed"
	Payload any    `json:"payload"` // MarketEvent or error string
}
