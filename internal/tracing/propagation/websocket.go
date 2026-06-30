package propagation

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// ExtractFromUpgrade extracts the W3C trace context from the headers of an incoming
// WebSocket HTTP upgrade request. This context should be saved to the connection
// struct, and used as the parent context for all subsequent child spans generated
// by incoming messages on this socket.
func ExtractFromUpgrade(req *http.Request) context.Context {
	if req == nil {
		return context.Background()
	}
	return otel.GetTextMapPropagator().Extract(req.Context(), propagation.HeaderCarrier(req.Header))
}
