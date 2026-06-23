# ══════════════════════════════════════════════════════════════════════════════
# Real-Time Market Data System — Makefile
# ══════════════════════════════════════════════════════════════════════════════

# Project settings
MODULE      := github.com/sumit/rtmds
BINARY      := rtmds
CMD         := ./cmd/server
BUILD_DIR   := ./bin
DOCKER_IMG  := rtmds:local

# Go toolchain flags
GOFLAGS     := -trimpath
LDFLAGS     := -s -w
VERSION     := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME  := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

.PHONY: all build run test test-unit test-integration lint fmt vet tidy \
        docker-build docker-up docker-down clean help

# ── Default ───────────────────────────────────────────────────────────────────
all: build

# ── Build ─────────────────────────────────────────────────────────────────────
## build: Compile the server binary into ./bin/
build:
	@mkdir -p $(BUILD_DIR)
	go build $(GOFLAGS) -ldflags "$(LDFLAGS) -X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME)" \
		-o $(BUILD_DIR)/$(BINARY) $(CMD)
	@echo "✓ Built $(BUILD_DIR)/$(BINARY) (version=$(VERSION))"

## run: Run the server locally (no Docker)
run:
	go run $(CMD) --config config.yaml

# ── Test ──────────────────────────────────────────────────────────────────────
## test: Run all tests with race detector and coverage
test:
	go test -race -count=1 -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out | tail -1

## test-unit: Run only the unit tests
test-unit:
	go test -race -count=1 ./test/unit/...

## test-integration: Run only the integration tests
test-integration:
	go test -race -count=1 -timeout=30s ./test/integration/...

## bench: Run benchmarks
bench:
	go test -bench=. -benchmem ./...

# ── Quality ───────────────────────────────────────────────────────────────────
## lint: Run golangci-lint (must be installed separately)
lint:
	golangci-lint run ./...

## fmt: Format all Go source files
fmt:
	gofmt -w -s .

## vet: Run go vet
vet:
	go vet ./...

## tidy: Tidy and verify the module graph
tidy:
	go mod tidy
	go mod verify

# ── Docker ────────────────────────────────────────────────────────────────────
## docker-build: Build the Docker image
docker-build:
	docker build -t $(DOCKER_IMG) .

## docker-up: Start the full stack (server + Redis + Prometheus + Grafana)
docker-up:
	docker compose up --build -d
	@echo "✓ Stack up — server on :8080, Grafana on :3000, Prometheus on :9090"

## docker-down: Tear down the stack
docker-down:
	docker compose down -v

## docker-logs: Tail server logs
docker-logs:
	docker compose logs -f server

# ── Utilities ─────────────────────────────────────────────────────────────────
## clean: Remove build artifacts
clean:
	rm -rf $(BUILD_DIR) coverage.out

## help: Show this help message
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //' | column -t -s ':'
