package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_Defaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Server.Host != "0.0.0.0" {
		t.Errorf("expected server.host=0.0.0.0, got %q", cfg.Server.Host)
	}
	if cfg.Server.Port != 9090 {
		t.Errorf("expected server.port=9090, got %d", cfg.Server.Port)
	}
	if cfg.Redis.Addr != "localhost:6379" {
		t.Errorf("expected redis.addr=localhost:6379, got %q", cfg.Redis.Addr)
	}
	if cfg.Log.Level != "info" {
		t.Errorf("expected log.level=info, got %q", cfg.Log.Level)
	}
	if cfg.Log.Format != "json" {
		t.Errorf("expected log.format=json, got %q", cfg.Log.Format)
	}
	if cfg.Database.Port != 5432 {
		t.Errorf("expected database.port=5432, got %d", cfg.Database.Port)
	}
}

func TestLoad_YAMLFile(t *testing.T) {
	yaml := `
server:
  host: "127.0.0.1"
  port: 8080
  read_timeout: "5s"
  write_timeout: "5s"
  shutdown_timeout: "10s"
redis:
  enabled: true
  addr: "redis:6379"
  db: 2
feed:
  enabled: true
  symbols: ["AAPL", "MSFT"]
  reconnect_delay: "3s"
log:
  level: "debug"
  format: "text"
metrics:
  enabled: false
database:
  enabled: false
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Server.Host != "127.0.0.1" {
		t.Errorf("expected server.host=127.0.0.1, got %q", cfg.Server.Host)
	}
	if cfg.Server.Port != 8080 {
		t.Errorf("expected server.port=8080, got %d", cfg.Server.Port)
	}
	if cfg.Redis.Addr != "redis:6379" {
		t.Errorf("expected redis.addr=redis:6379, got %q", cfg.Redis.Addr)
	}
	if cfg.Redis.DB != 2 {
		t.Errorf("expected redis.db=2, got %d", cfg.Redis.DB)
	}
	if cfg.Log.Level != "debug" {
		t.Errorf("expected log.level=debug, got %q", cfg.Log.Level)
	}
	if cfg.Log.Format != "text" {
		t.Errorf("expected log.format=text, got %q", cfg.Log.Format)
	}
	if len(cfg.Feed.Symbols) != 2 || cfg.Feed.Symbols[0] != "AAPL" {
		t.Errorf("expected feed.symbols=[AAPL MSFT], got %v", cfg.Feed.Symbols)
	}
}

func TestLoad_EnvironmentOverrides(t *testing.T) {
	// Set env vars
	os.Setenv("RTMDS_SERVER_PORT", "3000")
	os.Setenv("RTMDS_LOG_LEVEL", "warn")
	os.Setenv("RTMDS_REDIS_ADDR", "custom-redis:6380")
	defer os.Unsetenv("RTMDS_SERVER_PORT")
	defer os.Unsetenv("RTMDS_LOG_LEVEL")
	defer os.Unsetenv("RTMDS_REDIS_ADDR")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Server.Port != 3000 {
		t.Errorf("expected server.port=3000 from env, got %d", cfg.Server.Port)
	}
	if cfg.Log.Level != "warn" {
		t.Errorf("expected log.level=warn from env, got %q", cfg.Log.Level)
	}
	if cfg.Redis.Addr != "custom-redis:6380" {
		t.Errorf("expected redis.addr=custom-redis:6380 from env, got %q", cfg.Redis.Addr)
	}
}

func TestLoad_EnvOverridesFile(t *testing.T) {
	yaml := `
server:
  port: 8080
log:
  level: "info"
redis:
  enabled: true
  addr: "redis:6379"
database:
  enabled: false
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	os.Setenv("RTMDS_SERVER_PORT", "9999")
	defer os.Unsetenv("RTMDS_SERVER_PORT")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.Server.Port != 9999 {
		t.Errorf("expected env override to take precedence: got port=%d", cfg.Server.Port)
	}
	if cfg.Redis.Addr != "redis:6379" {
		t.Errorf("expected file value for redis.addr, got %q", cfg.Redis.Addr)
	}
}

func TestValidation_InvalidPort(t *testing.T) {
	yaml := `
server:
  port: 99999
redis:
  enabled: false
database:
  enabled: false
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid port")
	}
	if !contains(err.Error(), "server.port") {
		t.Errorf("expected error about server.port, got: %v", err)
	}
}

func TestValidation_InvalidLogLevel(t *testing.T) {
	yaml := `
server:
  port: 9090
redis:
  enabled: false
log:
  level: "verbose"
database:
  enabled: false
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid log level")
	}
	if !contains(err.Error(), "log.level") {
		t.Errorf("expected error about log.level, got: %v", err)
	}
}

func TestValidation_InvalidLogFormat(t *testing.T) {
	yaml := `
server:
  port: 9090
redis:
  enabled: false
log:
  format: "xml"
database:
  enabled: false
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid log format")
	}
	if !contains(err.Error(), "log.format") {
		t.Errorf("expected error about log.format, got: %v", err)
	}
}

func TestValidation_RedisEnabledWithoutAddr(t *testing.T) {
	yaml := `
server:
  port: 9090
redis:
  enabled: true
  addr: ""
database:
  enabled: false
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for empty redis addr when enabled")
	}
}

func TestValidation_DatabaseEnabledWithoutHost(t *testing.T) {
	yaml := `
server:
  port: 9090
redis:
  enabled: false
database:
  enabled: true
  host: ""
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for empty database host when enabled")
	}
}

func TestValidation_DatabaseInvalidSSLMode(t *testing.T) {
	yaml := `
server:
  port: 9090
redis:
  enabled: false
database:
  enabled: true
  host: "localhost"
  sslmode: "invalid"
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid sslmode")
	}
}

func TestValidation_DiscoveryEnabledWithoutRedis(t *testing.T) {
	yaml := `
server:
  port: 9090
redis:
  enabled: false
database:
  enabled: false
snapshot:
  enabled: false
discovery:
  enabled: true
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for discovery without redis")
	}
	if !contains(err.Error(), "discovery.enabled requires redis") {
		t.Errorf("expected discovery/redis error, got: %v", err)
	}
}

func TestValidation_TracingEnabledWithoutEndpoint(t *testing.T) {
	yaml := `
server:
  port: 9090
redis:
  enabled: false
database:
  enabled: false
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: true
  endpoint: ""
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for tracing without endpoint")
	}
}

func TestValidation_TracingInvalidSampleRatio(t *testing.T) {
	yaml := `
server:
  port: 9090
redis:
  enabled: false
database:
  enabled: false
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: true
  endpoint: "localhost:4318"
  sample_ratio: 1.5
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid sample_ratio")
	}
	if !contains(err.Error(), "sample_ratio") {
		t.Errorf("expected error about sample_ratio, got: %v", err)
	}
}

func TestValidation_MultipleErrors(t *testing.T) {
	yaml := `
server:
  port: 99999
redis:
  enabled: true
  addr: ""
database:
  enabled: true
  host: ""
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for multiple validation failures")
	}

	errStr := err.Error()
	// Should contain multiple errors
	if !contains(errStr, "server.port") {
		t.Error("expected error about server.port")
	}
	if !contains(errStr, "redis.addr") {
		t.Error("expected error about redis.addr")
	}
	if !contains(errStr, "database.host") {
		t.Error("expected error about database.host")
	}
}

func TestLoadFile_Convenience(t *testing.T) {
	yaml := `
server:
  port: 7777
redis:
  enabled: false
database:
  enabled: false
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	cfg, err := LoadFile(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Server.Port != 7777 {
		t.Errorf("expected port=7777, got %d", cfg.Server.Port)
	}
}

func TestLoadEnv_NoFile(t *testing.T) {
	os.Setenv("RTMDS_SERVER_PORT", "5555")
	defer os.Unsetenv("RTMDS_SERVER_PORT")

	cfg, err := LoadEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Server.Port != 5555 {
		t.Errorf("expected port=5555, got %d", cfg.Server.Port)
	}
}

func TestSummary_RedactsSecrets(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	summary := cfg.Summary()
	redisSummary := summary["redis"].(map[string]interface{})

	// Should have addr but NOT password
	if _, exists := redisSummary["password"]; exists {
		t.Error("summary should not contain password")
	}
	if _, exists := redisSummary["addr"]; !exists {
		t.Error("summary should contain addr")
	}
}

func TestServerConfig_Addr(t *testing.T) {
	cfg := ServerConfig{Host: "0.0.0.0", Port: 8080}
	if cfg.Addr() != "0.0.0.0:8080" {
		t.Errorf("expected 0.0.0.0:8080, got %q", cfg.Addr())
	}
}

func TestServerConfig_GetGatewayID(t *testing.T) {
	tests := []struct {
		name     string
		cfg      ServerConfig
		expected string
	}{
		{
			name:     "explicit ID",
			cfg:      ServerConfig{Host: "0.0.0.0", Port: 8080, GatewayID: "gw-1"},
			expected: "gw-1",
		},
		{
			name:     "auto-generated from port",
			cfg:      ServerConfig{Host: "0.0.0.0", Port: 8080},
			expected: "gateway-8080",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.GetGatewayID(); got != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, got)
			}
		})
	}
}

func TestValidation_ShutdownTimeoutLessThanReadTimeout(t *testing.T) {
	yaml := `
server:
  port: 9090
  read_timeout: "30s"
  write_timeout: "10s"
  shutdown_timeout: "10s"
redis:
  enabled: false
database:
  enabled: false
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for shutdown_timeout < read_timeout")
	}
	if !contains(err.Error(), "shutdown_timeout") {
		t.Errorf("expected error about shutdown_timeout, got: %v", err)
	}
}

func TestValidation_SnapshotEnabledWithoutPath(t *testing.T) {
	yaml := `
server:
  port: 9090
redis:
  enabled: false
database:
  enabled: false
snapshot:
  enabled: true
  checkpoint_path: ""
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for snapshot without path")
	}
}

func TestValidation_FeedEnabledWithoutSymbols(t *testing.T) {
	yaml := `
server:
  port: 9090
redis:
  enabled: false
feed:
  enabled: true
  symbols: []
database:
  enabled: false
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for feed without symbols")
	}
}

func TestValidation_MetricsEnabledWithoutPath(t *testing.T) {
	yaml := `
server:
  port: 9090
redis:
  enabled: false
database:
  enabled: false
snapshot:
  enabled: false
metrics:
  enabled: true
  path: ""
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for metrics without path")
	}
}

func TestValidation_RedisInvalidDB(t *testing.T) {
	yaml := `
server:
  port: 9090
redis:
  enabled: true
  addr: "localhost:6379"
  db: 16
database:
  enabled: false
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for redis db > 15")
	}
}

func TestValidation_DatabaseInvalidPort(t *testing.T) {
	yaml := `
server:
  port: 9090
redis:
  enabled: false
database:
  enabled: true
  host: "localhost"
  port: 0
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for database port 0")
	}
}

func TestValidation_DatabaseIdleConnsExceedsOpenConns(t *testing.T) {
	yaml := `
server:
  port: 9090
redis:
  enabled: false
database:
  enabled: true
  host: "localhost"
  port: 5432
  max_open_conns: 5
  max_idle_conns: 10
snapshot:
  enabled: false
discovery:
  enabled: false
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for max_idle_conns > max_open_conns")
	}
}

func TestValidation_DiscoveryHeartbeatExceedsTTL(t *testing.T) {
	yaml := `
server:
  port: 9090
redis:
  enabled: true
  addr: "localhost:6379"
database:
  enabled: false
snapshot:
  enabled: false
discovery:
  enabled: true
  ttl: "10s"
  heartbeat_interval: "15s"
tracing:
  enabled: false
`
	path := filepath.Join(t.TempDir(), "test.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for heartbeat_interval >= ttl")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
