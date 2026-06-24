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
        docker-build docker-up docker-down \
        dev-up dev-shell dev-test dev-redis dev-down \
        gw-up gw-down gw-logs gw-health gw-test \
        sticky-up sticky-down sticky-logs sticky-health sticky-test \
        discovery-up discovery-down discovery-logs discovery-gateways clean help


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

# ── Dev Environment (Docker) ─────────────────────────────────────────────────
## dev-up: Start Redis for development
dev-up:
	docker compose -f docker-compose.dev.yml up -d redis
	@echo "✓ Redis running on localhost:6379"

## dev-shell: Open a Go dev shell (Redis available as 'redis:6379')
dev-shell:
	docker compose -f docker-compose.dev.yml run --rm dev sh

## dev-test: Run all tests inside the dev container
dev-test:
	docker compose -f docker-compose.dev.yml run --rm dev go test -race -count=1 ./...

## dev-test-redis: Run only Redis Pub/Sub tests
dev-test-redis:
	docker compose -f docker-compose.dev.yml run --rm dev go test -race -count=1 -v ./internal/distribution/redisbus/...

## dev-build: Build the server inside the dev container
dev-build:
	docker compose -f docker-compose.dev.yml run --rm dev go build -o /app/bin/rtmds ./cmd/server

## dev-run: Run the server inside the dev container
dev-run:
	docker compose -f docker-compose.dev.yml run --rm dev go run ./cmd/server

## dev-redis: Connect to Redis CLI inside the dev container
dev-redis:
	docker compose -f docker-compose.dev.yml exec redis redis-cli

## dev-down: Tear down the dev environment
dev-down:
	docker compose -f docker-compose.dev.yml down -v

# ── Multi-Gateway Deployment ──────────────────────────────────────────────────
## gw-up: Start 3 gateway instances + Redis (multi-gateway mode)
gw-up:
	docker compose -f docker-compose.multigateway.yml up --build -d
	@echo "✓ Multi-gateway stack up"
	@echo "  Gateway 1 (primary):  http://localhost:9091"
	@echo "  Gateway 2 (subscriber): http://localhost:9092"
	@echo "  Gateway 3 (subscriber): http://localhost:9093"
	@echo "  Redis: localhost:6379"

## gw-down: Tear down the multi-gateway stack
gw-down:
	docker compose -f docker-compose.multigateway.yml down -v

## gw-logs: Tail logs from all gateway instances
gw-logs:
	docker compose -f docker-compose.multigateway.yml logs -f

## gw-health: Check health of all gateway instances
gw-health:
	@echo "=== Gateway 1 (9091) ===" && curl -s http://localhost:9091/health && echo
	@echo "=== Gateway 2 (9092) ===" && curl -s http://localhost:9092/health && echo
	@echo "=== Gateway 3 (9093) ===" && curl -s http://localhost:9093/health && echo

## gw-test: Run end-to-end WebSocket test across all gateways
gw-test:
	@echo "Testing WebSocket connectivity on all gateways..."
	@for port in 9091 9092 9093; do \
		echo -n "Gateway $$port: "; \
		curl -s -o /dev/null -w "HTTP %{http_code}" \
			-H "Connection: Upgrade" -H "Upgrade: websocket" \
			-H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: dGVzdA==" \
			http://localhost:$$port/ws; \
		echo; \
	done

# ── Stateless Gateway Deployment (Nginx + Multi-Gateway) ──────────────────────
## sticky-up: Start Nginx + 3 gateways + Redis with least_conn routing
sticky-up:
	docker compose -f docker-compose.sticky.yml up --build -d
	@echo "✓ Stateless gateway stack up"
	@echo "  Nginx load balancer: http://localhost:8080"
	@echo "  Gateway 1 (primary):  internal :9091"
	@echo "  Gateway 2 (subscriber): internal :9092"
	@echo "  Gateway 3 (subscriber): internal :9093"
	@echo "  Redis: localhost:6379"
	@echo ""
	@echo "Test routing:"
	@echo "  curl -s -D - -o /dev/null http://localhost:8080/health"
	@echo "  # Check rtmds-gateway-id header"

## sticky-down: Tear down the gateway stack
sticky-down:
	docker compose -f docker-compose.sticky.yml down -v

## sticky-logs: Tail logs from all services
sticky-logs:
	docker compose -f docker-compose.sticky.yml logs -f

## sticky-health: Check health of Nginx and all gateways
sticky-health:
	@echo "=== Nginx (8080) ===" && curl -s http://localhost:8080/nginx-health && echo
	@echo "=== Through Nginx ===" && curl -s -D - -o /dev/null http://localhost:8080/health | grep -i "rtmds-gateway-id" || echo "(no gateway header)"

## sticky-test: Verify least_conn routing distributes across gateways
sticky-test:
	@echo "Testing least_conn routing..."
	@echo "Sending 10 requests to /health via Nginx:"
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		echo -n "  Request $$i: "; \
		curl -s -D - -o /dev/null http://localhost:8080/health | grep -i "rtmds-gateway-id" | tr -d '\r' || echo "(no header)"; \
	done
	@echo ""
	@echo "With least_conn, requests should distribute across gateways."
	@echo "Clients own subscription state and re-send on reconnect (fat client)."

# ── Service Discovery Deployment ──────────────────────────────────────────────
## discovery-up: Start Nginx + 3 gateways + Redis with service discovery
discovery-up:
	docker compose -f docker-compose.sticky.yml up --build -d
	@echo "✓ Service discovery stack up"
	@echo "  Nginx load balancer: http://localhost:8080"
	@echo "  Gateway 1 (primary):  internal :9091"
	@echo "  Gateway 2 (subscriber): internal :9092"
	@echo "  Gateway 3 (subscriber): internal :9093"
	@echo "  Redis: localhost:6379"
	@echo ""
	@echo "Check registered gateways:"
	@echo "  curl -s http://localhost:8080/gateways | jq ."

## discovery-down: Tear down the service discovery stack
discovery-down:
	docker compose -f docker-compose.sticky.yml down -v

## discovery-logs: Tail logs from all services
discovery-logs:
	docker compose -f docker-compose.sticky.yml logs -f

## discovery-gateways: List all registered gateways via any gateway's /gateways endpoint
discovery-gateways:
	@echo "=== Registered Gateways ==="
	@for port in 9091 9092 9093; do \
		echo "--- Gateway $$port ---"; \
		curl -s http://localhost:$$port/gateways 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "(discovery not available)"; \
	done

# ── Utilities ─────────────────────────────────────────────────────────────────
## clean: Remove build artifacts
clean:
	rm -rf $(BUILD_DIR) coverage.out

## help: Show this help message
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //' | column -t -s ':'
