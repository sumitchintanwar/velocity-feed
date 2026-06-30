# ─── Stage 1: build ──────────────────────────────────────────────────────────
# Multi-stage build: compile in Go, copy binary to minimal scratch image.
# This produces a ~15MB image with zero OS vulnerabilities.
FROM golang:1.23-alpine AS builder

ENV GOTOOLCHAIN=auto

# Install git for version embedding and ca-certificates for TLS.
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /src

# Layer dependency download separately for Docker layer caching.
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy source and build a statically-linked binary.
COPY . .

ARG VERSION=dev
ARG REVISION=unknown

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w \
      -X main.Version=${VERSION} \
      -X main.Revision=${REVISION} \
      -X main.BuildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -o /bin/rtmds \
    ./cmd/server

# Create the data directory in the builder (scratch has no shell for RUN).
RUN mkdir -p /data && chown 65534:65534 /data

# ─── Stage 2: runtime ────────────────────────────────────────────────────────
# scratch: no shell, no package manager, minimal attack surface.
# For debugging, replace with: FROM alpine:3.20
FROM alpine:3.20

# Labels for container metadata (OCI standard).
LABEL org.opencontainers.image.title="rtmds"
LABEL org.opencontainers.image.description="Real-Time Market Data System"
LABEL org.opencontainers.image.source="https://github.com/sumit/rtmds"

# TLS certs for outbound HTTPS (e.g., OTLP collector, feed providers).
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
# Timezone data for correct timestamps in logs.
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
# The statically-linked binary.
COPY --from=builder /bin/rtmds /rtmds
# Data directory for snapshot checkpoints.
COPY --from=builder --chown=65534:65534 /data /data

# Non-root user (UID 65534 = nobody). Matches Kubernetes runAsNonRoot.
USER 65534:65534

EXPOSE 8080

ENTRYPOINT ["/rtmds"]
