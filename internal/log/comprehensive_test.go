package log

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// --- Schema validation ---

func TestSchema_MandatoryFields(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{
		Level:      "info",
		Format:     "json",
		Service:    "test-svc",
		InstanceID: "inst-1",
		Writer:     &buf,
	}
	l := NewFromConfig(cfg)

	Info(context.Background(), l).
		Str("event", "test_event").
		Str("component", "test").
		Msg("schema check")

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}

	for _, field := range RequiredFields {
		if _, ok := entry[field]; !ok {
			t.Errorf("missing mandatory field: %s", field)
		}
	}
}

func TestSchema_EnvironmentFields(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{
		Level:       "info",
		Format:      "json",
		Service:     "svc",
		Environment: "prod",
		Version:     "1.0.0",
		Region:      "us-east-1",
		InstanceID:  "pod-1",
		Writer:      &buf,
	}
	l := NewFromConfig(cfg)

	Info(context.Background(), l).
		Str("event", "test_event").
		Msg("env check")

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}

	assertField := func(key, want string) {
		t.Helper()
		got, ok := entry[key]
		if !ok {
			t.Errorf("missing field %s", key)
		} else if got != want {
			t.Errorf("%s = %v, want %v", key, got, want)
		}
	}

	assertField("environment", "prod")
	assertField("version", "1.0.0")
	assertField("region", "us-east-1")
	assertField("instance_id", "pod-1")
}

// --- Correlation ID propagation ---

func TestCorrelationID_PropagatesThroughContext(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test-svc")
	ctx := context.Background()
	ctx = SetCorrelationID(ctx, "trace-abc-123")
	ctx = IntoContext(ctx, l)

	Info(ctx, l).
		Str("event", "propagation_test").
		Msg("with correlation")

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}

	if entry["correlation_id"] != "trace-abc-123" {
		t.Errorf("correlation_id = %v, want trace-abc-123", entry["correlation_id"])
	}
}

func TestCorrelationID_PropagatesToComponentLogger(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test-svc")
	fl := FeedGenerator(l, "sim")

	ctx := SetCorrelationID(context.Background(), "comp-xyz")
	Info(ctx, fl).
		Str("event", "generator_started").
		Msg("component test")

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}

	if entry["correlation_id"] != "comp-xyz" {
		t.Errorf("correlation_id = %v, want comp-xyz", entry["correlation_id"])
	}
	if entry["component"] != "feed_generator" {
		t.Errorf("component = %v, want feed_generator", entry["component"])
	}
}

// --- Sanitization ---

func TestSanitization_PasswordRedacted(t *testing.T) {
	input := `{"password":"hunter2","user":"admin"}`
	got := SanitizeString(input)
	if strings.Contains(got, "hunter2") {
		t.Errorf("password not redacted: %s", got)
	}
	if !strings.Contains(got, "[REDACTED]") {
		t.Errorf("expected [REDACTED]: %s", got)
	}
}

func TestSanitization_TokenRedacted(t *testing.T) {
	input := `{"authorization":"Bearer sk_live_abc123"}`
	got := SanitizeString(input)
	if strings.Contains(got, "sk_live_abc123") {
		t.Errorf("token not redacted: %s", got)
	}
}

func TestSanitization_NonSensitiveUnchanged(t *testing.T) {
	input := `{"event":"client_connected","level":"info","message":"ok"}`
	got := SanitizeString(input)
	if got != input {
		t.Errorf("non-sensitive data modified: got %s, want %s", got, input)
	}
}

// --- Cardinality / Event naming ---

var validEventNames = []string{
	"gateway_started",
	"client_connected",
	"replay_completed",
	"snapshot_loaded",
	"topic_created",
	"redis_connected",
	"publish_failed",
	"heartbeat_timeout",
	"subscription_added",
	"checkpoint_saved",
}

var invalidEventNames = []string{
	"Started",
	"gateway_started_v2",
	"gatewayStarted",
	"GATEWAY_STARTED",
	"started",
	"gateway_started_extra_parts",
	"123_started",
	"gateway_",
	"_started",
	"gateway__started",
	"gateway started",
}

func TestEventName_Valid(t *testing.T) {
	for _, name := range validEventNames {
		t.Run(name, func(t *testing.T) {
			if err := ValidateEventName(name); err != nil {
				t.Errorf("ValidateEventName(%q) returned error: %v", name, err)
			}
		})
	}
}

func TestEventName_Invalid(t *testing.T) {
	for _, name := range invalidEventNames {
		t.Run(name, func(t *testing.T) {
			if err := ValidateEventName(name); err == nil {
				t.Errorf("ValidateEventName(%q) should have returned error", name)
			}
		})
	}
}

func TestEventName_RegexPattern(t *testing.T) {
	pattern := regexp.MustCompile(`^[a-z][a-z0-9]*_[a-z][a-z0-9]*$`)

	for _, name := range validEventNames {
		if !pattern.MatchString(name) {
			t.Errorf("valid event %q does not match regex", name)
		}
	}
	for _, name := range invalidEventNames {
		if pattern.MatchString(name) {
			t.Errorf("invalid event %q matches regex", name)
		}
	}
}

// --- Truncation ---

func TestTruncateString_ShortValue(t *testing.T) {
	got := TruncateString("hello", 1024)
	if got != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}
}

func TestTruncateString_LongValue(t *testing.T) {
	long := strings.Repeat("x", 2000)
	got := TruncateString(long, 1024)
	if len(got) > 1024 {
		t.Errorf("truncated length = %d, want <= 1024", len(got))
	}
	if !strings.HasSuffix(got, "…[truncated]") {
		t.Errorf("missing truncation suffix: %s", got)
	}
}

func TestTruncateString_ExactLimit(t *testing.T) {
	s := strings.Repeat("a", 1024)
	got := TruncateString(s, 1024)
	if got != s {
		t.Error("exact limit should not truncate")
	}
}

func TestTruncateBytes(t *testing.T) {
	long := make([]byte, 2000)
	got := TruncateBytes(long, 1024)
	if len(got) != 1024 {
		t.Errorf("len = %d, want 1024", len(got))
	}
}

// --- Interfaces ---

func TestDefaultContextExtractor(t *testing.T) {
	extractor := DefaultContextExtractor()
	ctx := SetCorrelationID(context.Background(), "ext-123")

	got := extractor.ExtractCorrelationID(ctx)
	if got != "ext-123" {
		t.Errorf("ExtractCorrelationID = %v, want ext-123", got)
	}
}

func TestDefaultContextExtractor_Empty(t *testing.T) {
	extractor := DefaultContextExtractor()
	got := extractor.ExtractCorrelationID(context.Background())
	if got != "" {
		t.Errorf("ExtractCorrelationID on empty ctx = %v, want empty", got)
	}
}

func TestDefaultSanitizer(t *testing.T) {
	sanitizer := DefaultSanitizer()
	input := `{"password":"secret123"}`
	got := sanitizer.Sanitize(input)
	if strings.Contains(got, "secret123") {
		t.Errorf("password not redacted: %s", got)
	}
}

// --- Panic recovery ---

func TestRecovery_CatchesPanic(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test-svc")

	handler := Recovery(l)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic value")
	}))

	req := httptest.NewRequest("GET", "/test", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != 500 {
		t.Errorf("status = %d, want 500", rec.Code)
	}

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}

	if entry["level"] != "error" {
		t.Errorf("level = %v, want error", entry["level"])
	}
	if entry["event"] != "panic_recovered" {
		t.Errorf("event = %v, want panic_recovered", entry["event"])
	}
	if _, ok := entry["stack"]; !ok {
		t.Error("missing stack trace in log entry")
	}
}

func TestRecoverWithContext_CatchesPanic(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test-svc")
	ctx := SetCorrelationID(context.Background(), "panic-ctx")

	func() {
		defer RecoverWithContext(ctx, l)
		panic("goroutine panic")
	}()

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}

	if entry["event"] != "panic_recovered" {
		t.Errorf("event = %v, want panic_recovered", entry["event"])
	}
	if entry["correlation_id"] != "panic-ctx" {
		t.Errorf("correlation_id = %v, want panic-ctx", entry["correlation_id"])
	}
}

// --- Level-specific context helpers ---

func TestLevelHelpers_AllLevels(t *testing.T) {
	tests := []struct {
		name  string
		fn    func(ctx context.Context, l *Logger) *zerolog.Event
		level string
	}{
		{"Debug", Debug, "debug"},
		{"Info", Info, "info"},
		{"Warn", Warn, "warn"},
		{"Error", Error, "error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			var l *Logger
			if tt.level == "debug" {
				lvl, _ := zerolog.ParseLevel("debug")
				zl := zerolog.New(&buf).Level(lvl).With().Timestamp().Str("service", "test").Logger()
				l = &Logger{z: zl, service: "test"}
			} else {
				l = New(&buf, "test")
			}
			ctx := IntoContext(context.Background(), l)
			ctx = SetCorrelationID(ctx, "level-test")

			tt.fn(ctx, l).Str("event", "level_test").Msg("level test")

			var entry map[string]interface{}
			json.Unmarshal(buf.Bytes(), &entry)

			if entry["level"] != tt.level {
				t.Errorf("level = %v, want %v", entry["level"], tt.level)
			}
			if entry["correlation_id"] != "level-test" {
				t.Errorf("correlation_id not propagated for %s", tt.name)
			}
		})
	}
}

// --- Benchmarks ---

func BenchmarkLoggerInfo_WithCorrelation(b *testing.B) {
	var buf bytes.Buffer
	l := New(&buf, "bench")
	ctx := IntoContext(context.Background(), l)
	ctx = SetCorrelationID(ctx, "bench-id")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Info(ctx, l).
			Str("event", "bench_event").
			Str("symbol", "AAPL").
			Msg("benchmark message")
		buf.Reset()
	}
}

func BenchmarkLoggerInfo_WithoutCorrelation(b *testing.B) {
	var buf bytes.Buffer
	l := New(&buf, "bench")
	ctx := IntoContext(context.Background(), l)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Info(ctx, l).
			Str("event", "bench_event").
			Msg("benchmark message")
		buf.Reset()
	}
}

func BenchmarkTruncateString(b *testing.B) {
	long := strings.Repeat("x", 2000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = TruncateString(long, 1024)
	}
}

func BenchmarkSanitizeString(b *testing.B) {
	input := `{"event":"test","password":"secret","token":"abc123","level":"info"}`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SanitizeString(input)
	}
}

// --- Component-specific tests ---

func TestComponentLoggers_AllComponents(t *testing.T) {
	tests := []struct {
		name      string
		component string
		build     func(l *Logger) *Logger
	}{
		{"FeedGenerator", "feed_generator", func(l *Logger) *Logger { return FeedGenerator(l, "sim") }},
		{"Publisher", "publisher", func(l *Logger) *Logger { return Publisher(l) }},
		{"RedisLayer", "redis", func(l *Logger) *Logger { return RedisLayer(l, "localhost:6379") }},
		{"TopicManager", "topic_manager", func(l *Logger) *Logger { return TopicManager(l) }},
		{"WebSocketGateway", "gateway", func(l *Logger) *Logger { return WebSocketGateway(l, "gw-1") }},
		{"ReplayAPI", "replay_api", func(l *Logger) *Logger { return ReplayAPI(l) }},
		{"SnapshotService", "snapshot", func(l *Logger) *Logger { return SnapshotService(l) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			l := New(&buf, "test")
			cl := tt.build(l)

			cl.Underlying().Info().Str("event", "component_test").Msg("ok")

			var entry map[string]interface{}
			if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
				t.Fatalf("not valid JSON: %v", err)
			}

			if entry["component"] != tt.component {
				t.Errorf("component = %v, want %v", entry["component"], tt.component)
			}
		})
	}
}

// --- InvalidEventError ---

func TestInvalidEventError_Error(t *testing.T) {
	err := &InvalidEventError{Name: "BadName"}
	msg := err.Error()
	if !strings.Contains(msg, "BadName") {
		t.Errorf("error message should contain name: %s", msg)
	}
	if !strings.Contains(msg, "noun_action") {
		t.Errorf("error message should mention format: %s", msg)
	}
}

// --- Edge cases ---

func TestLogger_NilWriter(t *testing.T) {
	l := New(nil, "test")
	l.Underlying().Info().Msg("ok")
}

func TestLogger_EmptyService(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "")
	l.Underlying().Info().Msg("empty service")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	if entry["service"] != "" {
		t.Errorf("service = %v, want empty", entry["service"])
	}
}

func TestCorrelationID_EmptyContext(t *testing.T) {
	id := GetCorrelationID(context.Background())
	if id != "" {
		t.Errorf("GetCorrelationID on empty ctx = %v, want empty", id)
	}
}

func TestFromContext_NilLogger(t *testing.T) {
	l := FromContext(context.Background())
	if l == nil {
		t.Fatal("FromContext returned nil")
	}
	l.Underlying().Info().Msg("nop")
}
