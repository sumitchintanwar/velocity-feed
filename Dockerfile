# ─── Stage 1: build ────────────────────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

ENV GOTOOLCHAIN=auto

# Install git for go mod download and ca-certificates for TLS.
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /src

# Layer the dependency download separately so it is cached unless go.mod/go.sum
# change (common Docker caching best-practice).
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source tree and build.
COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" \
    -o /bin/rtmds \
    ./cmd/server

# ─── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM scratch

# Pull in TLS certs and timezone data from the builder stage.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Copy the statically-linked binary.
COPY --from=builder /bin/rtmds /rtmds

# Non-root user (numeric UID for compatibility with Kubernetes runAsNonRoot).
USER 65534:65534

EXPOSE 8080

ENTRYPOINT ["/rtmds"]
