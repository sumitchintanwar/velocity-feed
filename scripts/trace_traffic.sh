#!/usr/bin/env bash
# trace_traffic.sh — Generate realistic traffic for OpenTelemetry trace verification.
#
# Produces diverse traffic patterns that exercise every traced span:
#   - WebSocket connections (websocket.connect, subscription_request)
#   - Snapshot lookups (snapshot.request, snapshot.lookup)
#   - Market data flow (redis.publish, redis.consume)
#   - Replay API requests (replay.request, db.query_events)
#   - Client disconnects (graceful cleanup)
#
# Prerequisites:
#   make trace-up          # Single gateway + Jaeger
#   # OR
#   make trace-gw-up       # Multi-gateway + Jaeger
#
# Usage:
#   ./scripts/trace_traffic.sh                    # Default: 500 clients, 60s
#   ./scripts/trace_traffic.sh --clients 1000     # Custom client count
#   ./scripts/trace_traffic.sh --duration 120s    # Custom duration
#   ./scripts/trace_traffic.sh --port 9091        # Target specific gateway
#   ./scripts/trace_traffic.sh --replay-only      # Only replay requests
#   ./scripts/trace_traffic.sh --ws-only          # Only WebSocket traffic

set -euo pipefail

# ─── Defaults ────────────────────────────────────────────────────────────────

CLIENTS=500
DURATION="60s"
RAMP_UP="10s"
PORT=8080
MODE="all"           # all, ws-only, replay-only
SYMBOLS="AAPL,MSFT,GOOG,AMZN,TSLA"
RESULTS_DIR="docs/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# ─── Parse Arguments ─────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --clients)   CLIENTS="$2";    shift 2 ;;
        --duration)  DURATION="$2";   shift 2 ;;
        --ramp-up)   RAMP_UP="$2";    shift 2 ;;
        --port)      PORT="$2";       shift 2 ;;
        --mode)      MODE="$2";       shift 2 ;;
        --symbols)   SYMBOLS="$2";    shift 2 ;;
        --replay-only) MODE="replay"; shift ;;
        --ws-only)   MODE="ws";       shift ;;
        -h|--help)
            echo "Usage: $0 [--clients N] [--duration D] [--port P] [--mode all|ws|replay]"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ─── Helpers ─────────────────────────────────────────────────────────────────

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'; BOLD='\033[1m'

log_info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_ok()    { echo -e "${GREEN}[PASS]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_title() { echo -e "\n${BOLD}${CYAN}═══ $* ═══${NC}\n"; }

BASE_URL="http://localhost:${PORT}"
WS_URL="ws://localhost:${PORT}/ws"
JAEGER_URL="http://localhost:16686"

# Wait for service to be ready.
wait_for_service() {
    local url="$1" name="$2" max_wait="${3:-30}"
    local elapsed=0
    while ! curl -sf "$url" > /dev/null 2>&1; do
        sleep 1
        elapsed=$((elapsed + 1))
        if [ "$elapsed" -ge "$max_wait" ]; then
            log_warn "$name not ready after ${max_wait}s — proceeding anyway"
            return 1
        fi
    done
    return 0
}

# ─── Pre-flight Checks ──────────────────────────────────────────────────────

log_title "Pre-flight Checks"

log_info "Checking server at ${BASE_URL}..."
if ! wait_for_service "${BASE_URL}/health" "Server" 30; then
    echo "ERROR: Server not available. Start with: make trace-up"
    exit 1
fi
log_ok "Server is healthy"

log_info "Checking Jaeger at ${JAEGER_URL}..."
if ! wait_for_service "${JAEGER_URL}" "Jaeger" 30; then
    echo "ERROR: Jaeger not available. Start with: make trace-up"
    exit 1
fi
log_ok "Jaeger is available"

# Check initial trace count.
INITIAL_TRACES=$(curl -sf "${JAEGER_URL}/api/services" | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(len(data.get('data', [])))
" 2>/dev/null || echo "0")
log_info "Current services in Jaeger: ${INITIAL_TRACES}"

# ─── Phase 1: WebSocket Traffic ─────────────────────────────────────────────

if [ "$MODE" = "all" ] || [ "$MODE" = "ws" ]; then
    log_title "Phase 1: WebSocket Traffic (${CLIENTS} clients, ${DURATION})"

    log_info "Starting load test..."
    mkdir -p "$RESULTS_DIR"

    go run ./cmd/loadtest \
        -url "$WS_URL" \
        -connections "$CLIENTS" \
        -duration "$DURATION" \
        -ramp-up "$RAMP_UP" \
        -report \
        2>&1 | tee "${RESULTS_DIR}/trace_loadtest_${TIMESTAMP}.log"

    log_ok "Load test complete"
fi

# ─── Phase 2: Replay API Traffic ────────────────────────────────────────────

if [ "$MODE" = "all" ] || [ "$MODE" = "replay" ]; then
    log_title "Phase 2: Replay API Traffic"

    log_info "Generating replay requests..."
    REPLAY_COUNT=0
    REPLAY_ERRORS=0

    for symbol in AAPL MSFT GOOG AMZN TSLA; do
        for limit in 10 50 100; do
            STATUS=$(curl -sf -o /dev/null -w "%{http_code}" \
                "${BASE_URL}/replay?symbol=${symbol}&limit=${limit}" 2>/dev/null || echo "000")
            if [ "$STATUS" = "200" ]; then
                REPLAY_COUNT=$((REPLAY_COUNT + 1))
            else
                REPLAY_ERRORS=$((REPLAY_ERRORS + 1))
            fi
        done
    done

    # Time-range queries
    for symbol in AAPL MSFT; do
        STATUS=$(curl -sf -o /dev/null -w "%{http_code}" \
            "${BASE_URL}/replay?symbol=${symbol}&from=2026-01-01T00:00:00Z&to=2026-12-31T23:59:59Z&limit=100" \
            2>/dev/null || echo "000")
        if [ "$STATUS" = "200" ]; then
            REPLAY_COUNT=$((REPLAY_COUNT + 1))
        else
            REPLAY_ERRORS=$((REPLAY_ERRORS + 1))
        fi
    done

    # Error cases (should return 400)
    for bad_req in "from=invalid" "cursor=invalid" "limit=-1"; do
        STATUS=$(curl -sf -o /dev/null -w "%{http_code}" \
            "${BASE_URL}/replay?${bad_req}" 2>/dev/null || echo "000")
        if [ "$STATUS" = "400" ]; then
            REPLAY_COUNT=$((REPLAY_COUNT + 1))
        else
            REPLAY_ERRORS=$((REPLAY_ERRORS + 1))
        fi
    done

    log_ok "Replay requests: ${REPLAY_COUNT} successful, ${REPLAY_ERRORS} errors"
fi

# ─── Phase 3: Concurrent Replay (rate limiting test) ────────────────────────

if [ "$MODE" = "all" ] || [ "$MODE" = "replay" ]; then
    log_title "Phase 3: Concurrent Replay (rate limiting)"

    log_info "Firing 20 concurrent replay requests..."
    PIDS=()
    for i in $(seq 1 20); do
        curl -sf -o /dev/null "${BASE_URL}/replay?symbol=AAPL&limit=10" &
        PIDS+=($!)
    done
    RATE_LIMITED=0
    for pid in "${PIDS[@]}"; do
        wait "$pid" 2>/dev/null || RATE_LIMITED=$((RATE_LIMITED + 1))
    done
    log_info "Requests completed (${RATE_LIMITED} may have been rate-limited)"
fi

# ─── Phase 4: Wait for Trace Export ─────────────────────────────────────────

log_title "Phase 4: Waiting for Trace Export"

log_info "Waiting 10s for OTel batch export (5s batch timeout + buffer)..."
sleep 10

# ─── Results ─────────────────────────────────────────────────────────────────

log_title "Traffic Generation Complete"

# Count traces in Jaeger.
TRACE_COUNT=$(curl -sf "${JAEGER_URL}/api/services" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for svc in data.get('data', []):
    print(svc)
" 2>/dev/null || echo "(error)")

log_info "Services in Jaeger:"
echo "$TRACE_COUNT" | while read -r svc; do
    [ -n "$svc" ] && echo "  - $svc"
done

echo ""
log_info "Next steps:"
echo "  1. Open Jaeger UI:  ${JAEGER_URL}"
echo "  2. Select service:  rtmds"
echo "  3. Click:           Find Traces"
echo "  4. Verify spans:    websocket.connect, redis.publish, redis.consume, replay.request"
echo ""
log_info "Run verification:  ./scripts/trace_verify.sh"
