package marketdata

import "errors"

var (
	// ErrSymbolRequired is returned when a subscribe/unsubscribe request
	// contains no symbols.
	ErrSymbolRequired = errors.New("marketdata: at least one symbol is required")

	// ErrUnknownAction is returned when a client sends an unrecognised action.
	ErrUnknownAction = errors.New("marketdata: unknown action")

	// ErrConnectionClosed is returned when an operation is attempted on a
	// closed client connection.
	ErrConnectionClosed = errors.New("marketdata: connection closed")
)
