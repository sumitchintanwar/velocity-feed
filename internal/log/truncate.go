package log

const (
	// MaxPayloadBytes is the default maximum byte length for log field values.
	// Values exceeding this are truncated with a "…[truncated]" suffix.
	MaxPayloadBytes = 1024

	// truncationSuffix is appended to truncated values to indicate data loss.
	truncationSuffix = "…[truncated]"
)

// TruncateString truncates s to maxBytes, appending a truncation marker if shortened.
// Returns the original string if it fits within the limit.
func TruncateString(s string, maxBytes int) string {
	if maxBytes <= 0 {
		maxBytes = MaxPayloadBytes
	}
	if len(s) <= maxBytes {
		return s
	}
	// Reserve space for the suffix
	keep := maxBytes - len(truncationSuffix)
	if keep < 0 {
		keep = 0
	}
	return s[:keep] + truncationSuffix
}

// TruncateBytes truncates a byte slice to maxBytes, returning a new slice.
func TruncateBytes(b []byte, maxBytes int) []byte {
	if maxBytes <= 0 {
		maxBytes = MaxPayloadBytes
	}
	if len(b) <= maxBytes {
		return b
	}
	return b[:maxBytes]
}
