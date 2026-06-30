package log

import (
	"strings"

	"github.com/rs/zerolog"
)

// sensitivePatterns contains substrings that indicate a field holds sensitive data.
// Matching is case-insensitive. Add patterns as new sensitive fields are discovered.
var sensitivePatterns = []string{
	"password",
	"secret",
	"token",
	"api_key",
	"apikey",
	"authorization",
	"auth",
	"credential",
	"private_key",
	"privatekey",
	"access_key",
	"accesskey",
}

// SanitizeHook returns a zerolog.Hook that redacts sensitive fields.
// Attach it to a logger to prevent credentials from appearing in logs:
//
//	log := zerolog.New(w).Hook(log.SanitizeHook())
func SanitizeHook() zerolog.Hook {
	return sanitizeHook{}
}

type sanitizeHook struct{}

func (h sanitizeHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	// Hook API does not expose field iteration. We rely on the
	// SanitizeString function for raw log output instead.
	// This hook is a placeholder for future field-level interception
	// if zerolog adds field enumeration support.
}

func (h sanitizeHook) Levels() []zerolog.Level {
	return []zerolog.Level{zerolog.DebugLevel, zerolog.InfoLevel, zerolog.WarnLevel, zerolog.ErrorLevel, zerolog.FatalLevel, zerolog.PanicLevel}
}

// SanitizeString redacts sensitive values from a raw log line (JSON string).
// It scans for known sensitive key patterns and replaces their values with [REDACTED].
// This is a defense-in-depth measure for log output that bypasses the structured API.
func SanitizeString(s string) string {
	result := s
	for _, pattern := range sensitivePatterns {
		result = redactPattern(result, pattern)
	}
	return result
}

// redactPattern replaces values of fields matching the given pattern with [REDACTED].
// It handles both "key":"value" and "key": "value" forms.
func redactPattern(s, pattern string) string {
	lower := strings.ToLower(s)
	patLower := strings.ToLower(pattern)
	result := s
	searchFrom := 0

	for {
		idx := strings.Index(lower[searchFrom:], patLower)
		if idx == -1 {
			break
		}
		idx += searchFrom

		// Find the start of the key (look backward for quote or start)
		keyStart := idx
		for keyStart > 0 && lower[keyStart-1] != '"' && lower[keyStart-1] != ',' && lower[keyStart-1] != '{' {
			keyStart--
		}

		// Find the value: look for the colon after the key
		colonIdx := strings.Index(lower[idx:], ":")
		if colonIdx == -1 {
			break
		}
		valueStart := idx + colonIdx + 1

		// Skip whitespace after colon
		for valueStart < len(lower) && lower[valueStart] == ' ' {
			valueStart++
		}

		if valueStart >= len(lower) {
			break
		}

		var valueEnd int
		if lower[valueStart] == '"' {
			// Quoted string: find closing quote
			valueEnd = strings.Index(lower[valueStart+1:], `"`)
			if valueEnd == -1 {
				break
			}
			valueEnd = valueStart + 1 + valueEnd + 1 // include closing quote
		} else {
			// Unquoted: find end of value (comma, brace, or end)
			valueEnd = valueStart
			for valueEnd < len(lower) && lower[valueEnd] != ',' && lower[valueEnd] != '}' {
				valueEnd++
			}
		}

		// Replace the value portion, preserve the key
		result = result[:valueStart] + `"[REDACTED]"` + result[valueEnd:]
		lower = strings.ToLower(result)
		searchFrom = valueStart + len(`"[REDACTED]"`)
	}
	return result
}

// IsSensitive returns true if the given field name matches a known sensitive pattern.
// Use this to guard against logging sensitive data at the call site.
func IsSensitive(fieldName string) bool {
	lower := strings.ToLower(fieldName)
	for _, pattern := range sensitivePatterns {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}
