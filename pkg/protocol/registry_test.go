package protocol

import (
	"testing"
)

func TestRegistry_RegisterAndLookup(t *testing.T) {
	r := NewRegistry()

	// Create mock serializer
	jsonSer := NewJSONSerializer()
	r.Register(jsonSer)

	// Lookup JSON
	s, err := r.Lookup(FormatJSON)
	if err != nil {
		t.Fatalf("expected to find JSON serializer, got error: %v", err)
	}
	if s.Format() != FormatJSON {
		t.Errorf("expected format %v, got %v", FormatJSON, s.Format())
	}

	// Lookup missing
	_, err = r.Lookup(FormatProtobuf)
	if err == nil {
		t.Fatalf("expected error for missing serializer, got nil")
	}
}

func TestRegistry_RegisterNil(t *testing.T) {
	r := NewRegistry()
	r.Register(nil)

	_, err := r.Lookup(FormatJSON)
	if err == nil {
		t.Errorf("expected error after registering nil, got nil")
	}
}
