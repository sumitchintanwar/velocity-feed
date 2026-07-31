package protocol

import (
	"testing"
)

func TestNegotiator_SelectProtocol(t *testing.T) {
	n := NewNegotiator()

	tests := []struct {
		name            string
		clientProtocols []string
		wantFormat      Format
		wantStr         string
		wantErr         bool
	}{
		{
			name:            "default fallback when empty",
			clientProtocols: nil,
			wantFormat:      FormatJSON,
			wantStr:         SubprotocolJSON,
			wantErr:         false,
		},
		{
			name:            "exact match json",
			clientProtocols: []string{"v1.json.rtmds"},
			wantFormat:      FormatJSON,
			wantStr:         SubprotocolJSON,
			wantErr:         false,
		},
		{
			name:            "exact match protobuf",
			clientProtocols: []string{"v1.protobuf.rtmds"},
			wantFormat:      FormatProtobuf,
			wantStr:         SubprotocolProtobuf,
			wantErr:         false,
		},
		{
			name:            "unsupported protocol",
			clientProtocols: []string{"v2.xml.rtmds"},
			wantErr:         true,
		},
		{
			name:            "multiple protocols, prefers first match",
			clientProtocols: []string{"v2.xml.rtmds", "v1.protobuf.rtmds", "v1.json.rtmds"},
			wantFormat:      FormatProtobuf,
			wantStr:         SubprotocolProtobuf,
			wantErr:         false,
		},
		{
			name:            "whitespace handling",
			clientProtocols: []string{" v1.json.rtmds "},
			wantFormat:      FormatJSON,
			wantStr:         SubprotocolJSON,
			wantErr:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFmt, gotStr, err := n.SelectProtocol(tt.clientProtocols)
			if (err != nil) != tt.wantErr {
				t.Errorf("SelectProtocol() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if gotFmt != tt.wantFormat {
					t.Errorf("SelectProtocol() gotFmt = %v, want %v", gotFmt, tt.wantFormat)
				}
				if gotStr != tt.wantStr {
					t.Errorf("SelectProtocol() gotStr = %v, want %v", gotStr, tt.wantStr)
				}
			}
		})
	}
}

func BenchmarkSelectProtocol(b *testing.B) {
	n := NewNegotiator()
	protocols := []string{"v2.xml.rtmds", "v1.protobuf.rtmds"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n.SelectProtocol(protocols)
	}
}
