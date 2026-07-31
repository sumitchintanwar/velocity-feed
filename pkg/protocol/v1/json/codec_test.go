package json

import (
	"bytes"
	"strings"
	"testing"

	"github.com/sumit/rtmds/pkg/protocol"
)

type dummyEvent struct {
	Sym string `json:"symbol"`
	Typ string `json:"type"`
	Ts  int64  `json:"timestamp"`
}

func (d *dummyEvent) EventSymbol() string { return d.Sym }
func (d *dummyEvent) EventType() string   { return d.Typ }
func (d *dummyEvent) GetTimestamp() int64 { return d.Ts }

func TestCodec_Encode(t *testing.T) {
	c := &Codec{}

	if c.Format() != protocol.FormatJSON {
		t.Errorf("expected FormatJSON, got %v", c.Format())
	}

	buf := new(bytes.Buffer)
	ev := &dummyEvent{
		Sym: "AAPL",
		Typ: "trade",
		Ts:  1620000000000,
	}

	err := c.Encode(ev, buf)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, `"symbol":"AAPL"`) {
		t.Errorf("expected output to contain symbol, got: %s", out)
	}
	if !strings.Contains(out, `"type":"trade"`) {
		t.Errorf("expected output to contain type, got: %s", out)
	}
	if !strings.Contains(out, `"timestamp":1620000000000`) {
		t.Errorf("expected output to contain timestamp, got: %s", out)
	}
}

func BenchmarkJSONEncode(b *testing.B) {
	c := &Codec{}
	buf := new(bytes.Buffer)
	ev := &dummyEvent{
		Sym: "AAPL",
		Typ: "trade",
		Ts:  1620000000000,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		c.Encode(ev, buf)
	}
}
