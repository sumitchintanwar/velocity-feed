package protocol

import (
	"errors"
	"testing"

	"github.com/sumit/rtmds/pkg/marketdata"
)

type mockCodec struct {
	format      Format
	encodeError error
	called      bool
}

func (m *mockCodec) Format() Format { return m.format }
func (m *mockCodec) Serialize(event marketdata.MarketEvent) ([]byte, error) {
	m.called = true
	if m.encodeError != nil {
		return nil, m.encodeError
	}
	return []byte("mock data"), nil
}

func (m *mockCodec) Deserialize(data []byte) (marketdata.MarketEvent, error) {
	return nil, nil
}

// mockEvent implements marketdata.MarketEvent
type mockEvent struct{}

func (m *mockEvent) EventSymbol() string { return "MOCK" }
func (m *mockEvent) EventType() string   { return "mock_event" }
func (m *mockEvent) GetTimestamp() int64 { return 0 }

func TestManager_SerializeLazy(t *testing.T) {
	mgr := NewManager()

	jsonCodec := &mockCodec{format: FormatJSON}
	pbCodec := &mockCodec{format: FormatProtobuf}

	mgr.RegisterCodec(jsonCodec)
	mgr.RegisterCodec(pbCodec)

	// No clients tracked yet, serialization should fail
	_, err := mgr.Serialize(&mockEvent{})
	if err == nil {
		t.Error("expected error when no clients are active")
	}
	if jsonCodec.called || pbCodec.called {
		t.Error("codecs should not be called if there are no active clients")
	}

	// Track one JSON client
	mgr.TrackClient(FormatJSON)

	msg, err := mgr.Serialize(&mockEvent{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !jsonCodec.called {
		t.Error("JSON codec should have been called")
	}
	if pbCodec.called {
		t.Error("Protobuf codec should NOT have been called")
	}

	// Verify the payload exists for JSON and not Protobuf
	if _, err := msg.Payload(FormatJSON); err != nil {
		t.Errorf("expected JSON payload, got error: %v", err)
	}
	if _, err := msg.Payload(FormatProtobuf); err == nil {
		t.Error("expected error for missing Protobuf payload")
	}

	// Cleanup
	msg.Release() // Since ref count is 0, this will immediately return buffers to the pool.
}

func TestManager_SerializeError(t *testing.T) {
	mgr := NewManager()

	failingCodec := &mockCodec{
		format:      FormatJSON,
		encodeError: errors.New("encode failed"),
	}

	mgr.RegisterCodec(failingCodec)
	mgr.TrackClient(FormatJSON)

	_, err := mgr.Serialize(&mockEvent{})
	if err == nil {
		t.Fatal("expected serialization error, got nil")
	}
	if err.Error() != "failed to encode format json: encode failed" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func BenchmarkSerialize_SingleFormat(b *testing.B) {
	mgr := NewManager()
	jsonCodec := &mockCodec{format: FormatJSON}
	mgr.RegisterCodec(jsonCodec)
	mgr.TrackClient(FormatJSON)

	ev := &mockEvent{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg, _ := mgr.Serialize(ev)
		msg.Release()
	}
}

func BenchmarkSerialize_MultiFormat(b *testing.B) {
	mgr := NewManager()
	mgr.RegisterCodec(&mockCodec{format: FormatJSON})
	mgr.RegisterCodec(&mockCodec{format: FormatProtobuf})
	mgr.RegisterCodec(&mockCodec{format: FormatFlatBuffers})

	mgr.TrackClient(FormatJSON)
	mgr.TrackClient(FormatProtobuf)
	mgr.TrackClient(FormatFlatBuffers)

	ev := &mockEvent{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg, _ := mgr.Serialize(ev)
		msg.Release()
	}
}
