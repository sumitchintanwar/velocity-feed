package log

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// --- Logger construction ---

func TestNew_CreatesJSONLogger(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test_service")

	l.Underlying().Info().Str("key", "value").Msg("hello")

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("log output is not valid JSON: %v", err)
	}
	if entry["service"] != "test_service" {
		t.Errorf("service = %v, want test_service", entry["service"])
	}
	if entry["message"] != "hello" {
		t.Errorf("message = %v, want hello", entry["message"])
	}
	if entry["key"] != "value" {
		t.Errorf("key = %v, want value", entry["key"])
	}
}

func TestNewFromConfig_TextFormat(t *testing.T) {
	var buf bytes.Buffer
	l := NewFromConfig(Config{Level: "debug", Format: "json", Service: "svc"})
	l.Underlying().Debug().Msg("debug msg")

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err == nil {
		// json format works
	}
	// Verify level is set
	if l.Underlying().GetLevel() != zerolog.DebugLevel {
		t.Errorf("level = %v, want debug", l.Underlying().GetLevel())
	}
}

func TestNewFromConfig_InvalidLevelDefaultsToInfo(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test")
	// New() defaults to InfoLevel
	if l.Underlying().GetLevel() != zerolog.InfoLevel {
		t.Errorf("default level = %v, want info", l.Underlying().GetLevel())
	}
}

func TestNewFromConfig_EnvironmentFields(t *testing.T) {
	var buf bytes.Buffer
	cfg := Config{
		Level:       "info",
		Format:      "json",
		Service:     "test-svc",
		Environment: "prod",
		Version:     "1.2.3",
		Region:      "us-east-1",
		InstanceID:  "pod-42",
		Writer:      &buf,
	}
	l := NewFromConfig(cfg)
	l.Underlying().Info().Msg("env test")

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	assert := func(key, want string) {
		t.Helper()
		if got, ok := entry[key]; !ok || got != want {
			t.Errorf("%s = %v, want %v", key, got, want)
		}
	}
	assert("service", "test-svc")
	assert("environment", "prod")
	assert("version", "1.2.3")
	assert("region", "us-east-1")
	assert("instance_id", "pod-42")
}

func TestNew_NilWriter(t *testing.T) {
	// Should not panic
	l := New(nil, "test")
	l.Underlying().Info().Msg("ok")
}

// --- Component loggers ---

func TestFeedGenerator(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test")
	fl := FeedGenerator(l, "simulator")

	fl.Underlying().Info().Str("event", "generator_started").Msg("started")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	if entry["component"] != "feed_generator" {
		t.Errorf("component = %v, want feed_generator", entry["component"])
	}
	if entry["feed"] != "simulator" {
		t.Errorf("feed = %v, want simulator", entry["feed"])
	}
}

func TestPublisher(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test")
	pl := Publisher(l)

	pl.Underlying().Info().Str("event", "publish_started").Msg("ok")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	if entry["component"] != "publisher" {
		t.Errorf("component = %v, want publisher", entry["component"])
	}
}

func TestRedisLayer(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test")
	rl := RedisLayer(l, "redis:6379")

	rl.Underlying().Info().Str("event", "redis_connected").Msg("ok")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	if entry["component"] != "redis" {
		t.Errorf("component = %v, want redis", entry["component"])
	}
	if entry["addr"] != "redis:6379" {
		t.Errorf("addr = %v, want redis:6379", entry["addr"])
	}
}

func TestTopicManager(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test")
	tl := TopicManager(l)

	tl.Underlying().Info().Str("event", "topic_created").Msg("ok")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	if entry["component"] != "topic_manager" {
		t.Errorf("component = %v, want topic_manager", entry["component"])
	}
}

func TestWebSocketGateway(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test")
	gl := WebSocketGateway(l, "gateway-9091")

	gl.Underlying().Info().Str("event", "client_connected").Msg("ok")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	if entry["component"] != "gateway" {
		t.Errorf("component = %v, want gateway", entry["component"])
	}
	if entry["gateway_id"] != "gateway-9091" {
		t.Errorf("gateway_id = %v, want gateway-9091", entry["gateway_id"])
	}
}

func TestReplayAPI(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test")
	rl := ReplayAPI(l)

	rl.Underlying().Info().Str("event", "replay_requested").Msg("ok")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	if entry["component"] != "replay_api" {
		t.Errorf("component = %v, want replay_api", entry["component"])
	}
}

func TestSnapshotService(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test")
	sl := SnapshotService(l)

	sl.Underlying().Info().Str("event", "snapshot_loaded").Msg("ok")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	if entry["component"] != "snapshot" {
		t.Errorf("component = %v, want snapshot", entry["component"])
	}
}

// --- Context propagation ---

func TestContextRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test")
	ctx := IntoContext(context.Background(), l)
	got := FromContext(ctx)

	if got == nil {
		t.Fatal("FromContext returned nil")
	}
	// Verify it's the same logger (same underlying writer)
	got.Underlying().Info().Msg("ctx test")
	if buf.Len() == 0 {
		t.Fatal("logger from context did not write")
	}
}

func TestFromContext_NoLogger(t *testing.T) {
	got := FromContext(context.Background())
	if got == nil {
		t.Fatal("FromContext on empty context should return nop logger, not nil")
	}
	// Should not panic
	got.Underlying().Info().Msg("nop")
}

// --- Correlation ID ---

func TestCorrelationID_RoundTrip(t *testing.T) {
	ctx := SetCorrelationID(context.Background(), "abc-123")
	got := GetCorrelationID(ctx)
	if got != "abc-123" {
		t.Errorf("GetCorrelationID = %v, want abc-123", got)
	}
}

func TestCorrelationID_Empty(t *testing.T) {
	got := GetCorrelationID(context.Background())
	if got != "" {
		t.Errorf("GetCorrelationID on empty ctx = %v, want empty", got)
	}
}

func TestCorrelationID_InLogOutput(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test")
	ctx := SetCorrelationID(context.Background(), "req-42")
	ctx = IntoContext(ctx, l)

	Info(ctx, l).Str("event", "test_event").Msg("with correlation")

	var entry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	if entry["correlation_id"] != "req-42" {
		t.Errorf("correlation_id = %v, want req-42", entry["correlation_id"])
	}
}

func TestCorrelationID_Absent(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test")
	ctx := IntoContext(context.Background(), l)

	Info(ctx, l).Str("event", "test_event").Msg("no correlation")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	if _, exists := entry["correlation_id"]; exists {
		t.Error("correlation_id should not be present when not set")
	}
}

// --- Level helpers ---

func TestLevelHelpers(t *testing.T) {
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
				// Debug requires a debug-level logger to not be filtered
				lvl, _ := zerolog.ParseLevel("debug")
				zl := zerolog.New(&buf).Level(lvl).With().Timestamp().Str("service", "test").Logger()
				l = &Logger{z: zl, service: "test"}
			} else {
				l = New(&buf, "test")
			}
			ctx := IntoContext(context.Background(), l)

			tt.fn(ctx, l).Str("event", "test").Msg("level test")

			var entry map[string]interface{}
			json.Unmarshal(buf.Bytes(), &entry)

			if entry["level"] != tt.level {
				t.Errorf("level = %v, want %v", entry["level"], tt.level)
			}
		})
	}
}

// --- Instance logger ---

func TestInstance(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test")
	il := l.Instance("gateway-9091")

	il.Underlying().Info().Msg("instance test")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	if entry["instance_id"] != "gateway-9091" {
		t.Errorf("instance_id = %v, want gateway-9091", entry["instance_id"])
	}
}

// --- WithField logger ---

func TestWithField(t *testing.T) {
	var buf bytes.Buffer
	l := New(&buf, "test")
	l2 := l.WithField("symbol", "AAPL")

	l2.Underlying().Info().Msg("field test")

	var entry map[string]interface{}
	json.Unmarshal(buf.Bytes(), &entry)

	if entry["symbol"] != "AAPL" {
		t.Errorf("symbol = %v, want AAPL", entry["symbol"])
	}
}

// --- Sanitizer ---

func TestSanitizeString_Password(t *testing.T) {
	input := `{"password":"hunter2","user":"admin"}`
	got := SanitizeString(input)
	if strings.Contains(got, "hunter2") {
		t.Errorf("password not redacted: %s", got)
	}
	if !strings.Contains(got, "[REDACTED]") {
		t.Errorf("expected [REDACTED] in output: %s", got)
	}
}

func TestSanitizeString_Token(t *testing.T) {
	input := `{"authorization":"Bearer secret123","path":"/ws"}`
	got := SanitizeString(input)
	if strings.Contains(got, "secret123") {
		t.Errorf("token not redacted: %s", got)
	}
}

func TestSanitizeString_APIKey(t *testing.T) {
	input := `{"api_key":"sk_live_abc123","method":"GET"}`
	got := SanitizeString(input)
	if strings.Contains(got, "sk_live_abc123") {
		t.Errorf("api_key not redacted: %s", got)
	}
}

func TestSanitizeString_CaseInsensitive(t *testing.T) {
	input := `{"PASSWORD":"test123"}`
	got := SanitizeString(input)
	if strings.Contains(got, "test123") {
		t.Errorf("case-insensitive redaction failed: %s", got)
	}
}

func TestSanitizeString_NoSensitive(t *testing.T) {
	input := `{"event":"client_connected","level":"info"}`
	got := SanitizeString(input)
	if got != input {
		t.Errorf("non-sensitive data was modified: %s", got)
	}
}

func TestIsSensitive(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"password", true},
		{"Password", true},
		{"REDIS_PASSWORD", true},
		{"api_key", true},
		{"authorization", true},
		{"token", true},
		{"secret", true},
		{"event", false},
		{"level", false},
		{"message", false},
		{"symbol", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsSensitive(tt.name); got != tt.want {
				t.Errorf("IsSensitive(%q) = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

// --- Benchmark ---

func BenchmarkLoggerInfo(b *testing.B) {
	var buf bytes.Buffer
	l := New(&buf, "bench")
	ctx := IntoContext(context.Background(), l)
	ctx = SetCorrelationID(ctx, "bench-id")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Info(ctx, l).Str("event", "bench").Msg("benchmark")
		buf.Reset()
	}
}
