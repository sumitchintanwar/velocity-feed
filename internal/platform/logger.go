// Package platform provides shared infrastructure primitives: structured
// logging and Prometheus metrics registration.
package platform

import (
	"os"
	"time"

	"github.com/rs/zerolog"
)

// NewLogger constructs a zerolog.Logger configured for the given level and
// output format. Call once at startup and pass the result through the
// application via context or dependency injection.
func NewLogger(level, format string) zerolog.Logger {
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}

	var base zerolog.Logger
	if format == "text" {
		base = zerolog.New(zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		})
	} else {
		base = zerolog.New(os.Stdout)
	}

	return base.Level(lvl).With().Timestamp().Caller().Logger()
}
