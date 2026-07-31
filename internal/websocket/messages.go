package websocket

// ClientMessage is the JSON envelope sent by clients to the gateway.
type ClientMessage struct {
	Action  string   `json:"action"`            // "subscribe" | "unsubscribe" | "replay" | "replay_pause" | "replay_resume" | "replay_stop" | "reconnect"
	Symbols []string `json:"symbols,omitempty"` // e.g. ["AAPL", "TSLA"]

	// Replay fields (used when action == "replay").
	Symbol string  `json:"symbol,omitempty"` // single symbol for replay
	Start  string  `json:"start,omitempty"`  // RFC3339 start time
	End    string  `json:"end,omitempty"`    // RFC3339 end time
	Speed  float64 `json:"speed,omitempty"`  // replay speed multiplier (0 = max speed)

	// Reconnect fields (used when action == "reconnect").
	ResumeSeq      map[string]uint64 `json:"resume_seq,omitempty"`
	ReconnectStart int64             `json:"reconnect_start,omitempty"`

	// Heartbeat fields (used when action == "pong").
	Timestamp int64 `json:"timestamp,omitempty"`
}

// ServerMessage is the JSON envelope sent by the gateway to clients.
type ServerMessage struct {
	Type    string `json:"type"`    // "trade" | "quote" | "bar" | "error" | "subscribed" | "unsubscribed" | "replay_started" | "replay_completed" | "replay_stopped"
	Payload any    `json:"payload"` // MarketEvent or error string
}
