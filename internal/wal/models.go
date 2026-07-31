package wal

// Message represents a single market data event in the Write Ahead Log.
type Message struct {
	Sequence  uint64
	Timestamp int64
	Topic     string
	Type      string
	Payload   []byte
}
