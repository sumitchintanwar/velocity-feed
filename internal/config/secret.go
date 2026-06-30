// Package config provides secret-safe types for configuration values
// that should never appear in logs.
package config

import (
	"encoding/json"
	"fmt"
)

// SecretString is a string type that always redacts its value in logs.
// Use this type for passwords, API keys, tokens, and other sensitive
// configuration values. This makes accidental secret leakage physically
// impossible, even if the entire config struct is printed.
//
// Usage:
//
//	type RedisConfig struct {
//	    Password config.SecretString `mapstructure:"password"`
//	}
//
//	// Logging the config struct will show password=***
//	fmt.Printf("%+v", cfg.Redis)  // password: ***
type SecretString struct {
	value string
}

// NewSecretString creates a SecretString from a plain text value.
func NewSecretString(v string) SecretString {
	return SecretString{value: v}
}

// Value returns the underlying secret value.
// Only call this when you need the actual secret (e.g., connecting to Redis).
// Never log the return value of this method.
func (s SecretString) Value() string {
	return s.value
}

// String implements fmt.Stringer — always returns "***" for safe logging.
func (s SecretString) String() string {
	return "***"
}

// GoString implements fmt.GoStringer — always returns "***".
func (s SecretString) GoString() string {
	return "***"
}

// MarshalJSON implements json.Marshaler — outputs "***" in JSON.
func (s SecretString) MarshalJSON() ([]byte, error) {
	return json.Marshal("***")
}

// UnmarshalJSON implements json.Unmarshaler — reads the actual value.
func (s *SecretString) UnmarshalJSON(data []byte) error {
	var v string
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	s.value = v
	return nil
}

// MarshalText implements encoding.TextMarshaler.
func (s SecretString) MarshalText() ([]byte, error) {
	return []byte("***"), nil
}

// IsEmpty returns true if the secret has no value.
func (s SecretString) IsEmpty() bool {
	return s.value == ""
}

// Redacted returns a redacted representation of the secret.
// Use this when you need to log a hint about the secret without exposing it.
func (s SecretString) Redacted() string {
	if s.IsEmpty() {
		return "(empty)"
	}
	return fmt.Sprintf("(set, length=%d)", len(s.value))
}
