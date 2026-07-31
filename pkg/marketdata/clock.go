package marketdata

import "time"

// Clock abstracts time so that production code uses the real wall clock
// while tests can inject a deterministic one.
type Clock interface {
	Now() time.Time
}

// WallClock returns the real system time. Use as the default in production.
type WallClock struct{}

func (WallClock) Now() time.Time { return time.Now() }
