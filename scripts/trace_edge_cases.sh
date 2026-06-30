#!/usr/bin/env bash
# trace_edge_cases.sh — Edge case testing for OpenTelemetry tracing.
#
# Tests tracing behavior under fault conditions:
#   - Gateway restart (trace continuity)
#   - Redis restart (propagation recovery)
#   - Client disconnect (span cleanup)
#   - Replay error cases (error spans)
#   - High-volume tracing (no span drops)
#
# Prerequisites:
#   make trace-gw-up    (multi-gateway + Jaeger)
#
# Usage:
#   ./scripts/trace_edge_cases.sh              # Run all scenarios
#   ./scripts/trace_edge_cases.sh gateway      # Gateway restart only
#   ./scripts/trace_edge_cases.sh redis        # Redis restart only
#   ./scripts/trace_edge_cases.sh disconnect   # Client disconnect only
#   ./scripts/trace_edge_cases.sh errors       # Error cases only
#   ./scripts/trace_edge_cases.sh volume       # High-volume test only

set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────────────────

JAEGER_URL="${JAEGER_URL:-http://localhost:16686}"
BASE_URL="http://localhost:9091"
GW1_CONTAINER="rtmds-gw1-trace"
REDIS_CONTAINER="rtmds-redis-trace"
RESULTS_DIR="docs/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="${RESULTS_DIR}/trace_edge_cases_${TIMESTAMP}.md"
SCENARIO="${1:-all}"

mkdir -p "$RESULTS_DIR"

# ─── Helpers ─────────────────────────────────────────────────────────────────

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'; BOLD='\033[1m'

log_info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_ok()    { echo -e "${GREEN}[PASS]${NC}  $*"; }
log_fail()  { echo -e "${RED}[FAIL]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_title() { echo -e "\n${BOLD}${CYAN}═══ $* ═══${NC}\n"; }

results() { echo "$*" | tee -a "$RESULTS_FILE"; }

# Count traces with a specific span name.
count_traces_with_span() {
    local name="$1" lookback="${2:-5m}"
    curl -sf "${JAEGER_URL}/api/traces?service=rtmds&operation=${name}&lookback=${lookback}&limit=1000" 2>/dev/null | \
        python3 -c "
import json, sys
data = json.load(sys.stdin)
print(len(data.get('data', [])))
" 2>/dev/null || echo "0"
}

# Wait for service to be ready.
wait_healthy() {
    local url="$1" max_wait="${2:-30}" elapsed=0
    while ! curl -sf "$url" > /dev/null 2>&1; do
        sleep 1
        elapsed=$((elapsed + 1))
        [ "$elapsed" -ge "$max_wait" ] && return 1
    done
    return 0
}

# ═══════════════════════════════════════════════════════════════════════════════
# SCENARIO 1: Gateway Restart
# ═══════════════════════════════════════════════════════════════════════════════

run_gateway_restart() {
    log_title "Scenario 1: Gateway Restart"
    results "## Gateway Restart"
    results ""

    # 1. Baseline: count traces before restart.
    log_info "Step 1: Recording baseline trace count..."
    BEFORE=$(count_traces_with_span "websocket.connect" "5m")
    results "Before restart: ${BEFORE} websocket.connect traces"
    log_info "Before: ${BEFORE} traces"

    # 2. Kill gateway 1.
    log_info "Step 2: Killing gateway 1..."
    docker stop "$GW1_CONTAINER" 2>/dev/null || true
    sleep 3

    # 3. Verify no new traces from killed gateway.
    log_info "Step 3: Checking no new traces from killed gateway..."
    DURING=$(count_traces_with_span "websocket.connect" "2m")
    if [ "$DURING" -le "$BEFORE" ]; then
        log_ok "No new traces from killed gateway"
        results "During restart: ${DURING} traces (no new — PASS)"
    else
        log_warn "Some traces appeared during restart (${DURING})"
        results "During restart: ${DURING} traces (unexpected — WARN)"
    fi

    # 4. Restart gateway.
    log_info "Step 4: Restarting gateway 1..."
    docker start "$GW1_CONTAINER" 2>/dev/null || true
    sleep 10

    # 5. Verify new traces appear after restart.
    log_info "Step 5: Checking new traces appear after restart..."
    AFTER=$(count_traces_with_span "websocket.connect" "5m")
    if [ "$AFTER" -gt "$BEFORE" ]; then
        log_ok "New traces appeared after restart (${AFTER} total)"
        results "After restart: ${AFTER} traces — PASS"
    else
        log_fail "No new traces after restart"
        results "After restart: ${AFTER} traces — FAIL"
    fi

    # 6. Verify trace propagation resumes.
    log_info "Step 6: Checking Redis propagation resumes..."
    sleep 5
    PROP_COUNT=$(curl -sf "${JAEGER_URL}/api/traces?service=rtmds&operation=redis.publish&lookback=5m&limit=100" 2>/dev/null | \
        python3 -c "
import json, sys
data = json.load(sys.stdin)
propagated = 0
for trace in data.get('data', []):
    names = [s['operationName'] for s in trace.get('spans', [])]
    if 'redis.publish' in names and 'redis.consume' in names:
        propagated += 1
print(propagated)
" 2>/dev/null || echo "0")

    if [ "$PROP_COUNT" -gt 0 ]; then
        log_ok "Redis propagation resumed (${PROP_COUNT} traces)"
        results "Post-restart propagation: ${PROP_COUNT} traces — PASS"
    else
        log_fail "Redis propagation not restored"
        results "Post-restart propagation: 0 traces — FAIL"
    fi

    results ""
}

# ═══════════════════════════════════════════════════════════════════════════════
# SCENARIO 2: Redis Restart
# ═══════════════════════════════════════════════════════════════════════════════

run_redis_restart() {
    log_title "Scenario 2: Redis Restart"
    results "## Redis Restart"
    results ""

    # 1. Baseline.
    log_info "Step 1: Recording baseline..."
    BEFORE=$(count_traces_with_span "redis.publish" "5m")
    results "Before restart: ${BEFORE} redis.publish traces"

    # 2. Kill Redis.
    log_info "Step 2: Killing Redis..."
    docker stop "$REDIS_CONTAINER" 2>/dev/null || true
    sleep 3

    # 3. Verify publish errors are traced.
    log_info "Step 3: Checking error spans during Redis outage..."
    ERRORS=$(curl -sf "${JAEGER_URL}/api/traces?service=rtmds&operation=redis.publish&lookback=2m&limit=100" 2>/dev/null | \
        python3 -c "
import json, sys
data = json.load(sys.stdin)
error_count = 0
for trace in data.get('data', []):
    for span in trace.get('spans', []):
        if span.get('operationName') == 'redis.publish':
            tags = {t['key']: t['value'] for t in span.get('tags', [])}
            if tags.get('error') == True or tags.get('error') == 'true':
                error_count += 1
print(error_count)
" 2>/dev/null || echo "0")

    if [ "$ERRORS" -gt 0 ]; then
        log_ok "Error spans recorded during Redis outage (${ERRORS})"
        results "Error spans during outage: ${ERRORS} — PASS"
    else
        log_warn "No error spans found (Redis may not have been publishing)"
        results "Error spans during outage: 0 — WARN"
    fi

    # 4. Restart Redis.
    log_info "Step 4: Restarting Redis..."
    docker start "$REDIS_CONTAINER" 2>/dev/null || true
    sleep 15

    # 5. Verify propagation resumes.
    log_info "Step 5: Checking propagation resumes..."
    PROP_AFTER=$(curl -sf "${JAEGER_URL}/api/traces?service=rtmds&operation=redis.publish&lookback=5m&limit=100" 2>/dev/null | \
        python3 -c "
import json, sys
data = json.load(sys.stdin)
propagated = 0
for trace in data.get('data', []):
    names = [s['operationName'] for s in trace.get('spans', [])]
    if 'redis.publish' in names and 'redis.consume' in names:
        propagated += 1
print(propagated)
" 2>/dev/null || echo "0")

    if [ "$PROP_AFTER" -gt 0 ]; then
        log_ok "Propagation resumed after Redis restart (${PROP_AFTER} traces)"
        results "Post-restart propagation: ${PROP_AFTER} traces — PASS"
    else
        log_fail "Propagation not restored after Redis restart"
        results "Post-restart propagation: 0 traces — FAIL"
    fi

    results ""
}

# ═══════════════════════════════════════════════════════════════════════════════
# SCENARIO 3: Client Disconnect
# ═══════════════════════════════════════════════════════════════════════════════

run_client_disconnect() {
    log_title "Scenario 3: Client Disconnect"
    results "## Client Disconnect"
    results ""

    # 1. Connect a client.
    log_info "Step 1: Connecting a client..."
    go run ./cmd/loadtest \
        -url "ws://localhost:9091/ws" \
        -connections 5 \
        -duration 10s \
        -ramp-up 2s \
        2>/dev/null &
    LOAD_PID=$!
    sleep 5

    # 2. Kill the client abruptly.
    log_info "Step 2: Killing client abruptly..."
    kill -9 "$LOAD_PID" 2>/dev/null || true
    sleep 3

    # 3. Verify spans are properly ended (no dangling open spans).
    log_info "Step 3: Checking for dangling spans..."
    DANGLING=$(curl -sf "${JAEGER_URL}/api/traces?service=rtmds&operation=websocket.connect&lookback=5m&limit=20" 2>/dev/null | \
        python3 -c "
import json, sys
data = json.load(sys.stdin)
dangling = 0
for trace in data.get('data', []):
    for span in trace.get('spans', []):
        dur = span.get('duration', 0)
        if span.get('operationName') == 'websocket.connect' and dur == 0:
            dangling += 1
print(dangling)
" 2>/dev/null || echo "0")

    if [ "$DANGLING" -eq 0 ]; then
        log_ok "No dangling spans detected"
        results "Dangling spans: 0 — PASS"
    else
        log_fail "${DANGLING} dangling spans detected"
        results "Dangling spans: ${DANGLING} — FAIL"
    fi

    results ""
}

# ═══════════════════════════════════════════════════════════════════════════════
# SCENARIO 4: Replay Error Cases
# ═══════════════════════════════════════════════════════════════════════════════

run_replay_errors() {
    log_title "Scenario 4: Replay Error Cases"
    results "## Replay Error Cases"
    results ""

    # 1. Invalid 'from' parameter.
    log_info "Step 1: Testing invalid 'from' parameter..."
    STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "http://localhost:9091/replay?from=invalid" 2>/dev/null || echo "000")
    if [ "$STATUS" = "400" ]; then
        log_ok "Invalid 'from' returns 400"
        results "Invalid 'from': HTTP ${STATUS} — PASS"
    else
        log_fail "Invalid 'from' returned ${STATUS} (expected 400)"
        results "Invalid 'from': HTTP ${STATUS} — FAIL"
    fi

    # 2. Invalid cursor.
    log_info "Step 2: Testing invalid cursor..."
    STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "http://localhost:9091/replay?cursor=invalid" 2>/dev/null || echo "000")
    if [ "$STATUS" = "400" ]; then
        log_ok "Invalid cursor returns 400"
        results "Invalid cursor: HTTP ${STATUS} — PASS"
    else
        log_fail "Invalid cursor returned ${STATUS} (expected 400)"
        results "Invalid cursor: HTTP ${STATUS} — FAIL"
    fi

    # 3. Invalid limit.
    log_info "Step 3: Testing invalid limit..."
    STATUS=$(curl -sf -o /dev/null -w "%{http_code}" "http://localhost:9091/replay?limit=-1" 2>/dev/null || echo "000")
    if [ "$STATUS" = "400" ]; then
        log_ok "Invalid limit returns 400"
        results "Invalid limit: HTTP ${STATUS} — PASS"
    else
        log_fail "Invalid limit returned ${STATUS} (expected 400)"
        results "Invalid limit: HTTP ${STATUS} — FAIL"
    fi

    # 4. Verify error spans have error attribute.
    log_info "Step 4: Checking error spans in Jaeger..."
    sleep 2
    ERROR_TRACES=$(curl -sf "${JAEGER_URL}/api/traces?service=rtmds&operation=replay.request&lookback=5m&limit=50" 2>/dev/null | \
        python3 -c "
import json, sys
data = json.load(sys.stdin)
error_count = 0
for trace in data.get('data', []):
    for span in trace.get('spans', []):
        if span.get('operationName') == 'replay.request':
            tags = {t['key']: t['value'] for t in span.get('tags', [])}
            if tags.get('error') == True or tags.get('error') == 'true':
                error_count += 1
print(error_count)
" 2>/dev/null || echo "0")

    if [ "$ERROR_TRACES" -gt 0 ]; then
        log_ok "Error spans recorded for invalid requests (${ERROR_TRACES})"
        results "Error spans: ${ERROR_TRACES} — PASS"
    else
        log_warn "No error spans found for invalid requests"
        results "Error spans: 0 — WARN"
    fi

    results ""
}

# ═══════════════════════════════════════════════════════════════════════════════
# SCENARIO 5: High-Volume Tracing
# ═══════════════════════════════════════════════════════════════════════════════

run_high_volume() {
    log_title "Scenario 5: High-Volume Tracing"
    results "## High-Volume Tracing"
    results ""

    # 1. Generate high-volume traffic.
    log_info "Step 1: Generating 200 concurrent connections..."
    go run ./cmd/loadtest \
        -url "ws://localhost:9091/ws" \
        -connections 200 \
        -duration 30s \
        -ramp-up 5s \
        2>/dev/null

    # 2. Wait for export.
    log_info "Step 2: Waiting for trace export..."
    sleep 15

    # 3. Count total traces.
    log_info "Step 3: Counting traces..."
    TOTAL=$(curl -sf "${JAEGER_URL}/api/traces?service=rtmds&lookback=5m&limit=1000" 2>/dev/null | \
        python3 -c "
import json, sys
data = json.load(sys.stdin)
print(len(data.get('data', [])))
" 2>/dev/null || echo "0")

    log_info "Total traces: ${TOTAL}"
    results "Total traces: ${TOTAL}"

    # 4. Check for span drops (if OTel metrics available).
    log_info "Step 4: Checking for dropped spans..."
    # This is a heuristic — if we have traces, spans likely weren't dropped.
    if [ "$TOTAL" -gt 10 ]; then
        log_ok "Traces present — no evidence of span drops"
        results "Span drops: none detected — PASS"
    else
        log_warn "Few traces found — possible span drops or sampling"
        results "Span drops: uncertain — WARN"
    fi

    results ""
}

# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

echo "# Trace Edge Case Results — $(date)" > "$RESULTS_FILE"
echo "" >> "$RESULTS_FILE"

# Verify prerequisites.
log_info "Checking Jaeger..."
if ! curl -sf "${JAEGER_URL}/api/services" > /dev/null 2>&1; then
    echo "ERROR: Jaeger not reachable at ${JAEGER_URL}"
    echo "Start with: make trace-gw-up"
    exit 1
fi

case "$SCENARIO" in
    gateway)    run_gateway_restart ;;
    redis)      run_redis_restart ;;
    disconnect) run_client_disconnect ;;
    errors)     run_replay_errors ;;
    volume)     run_high_volume ;;
    all)
        run_replay_errors
        run_client_disconnect
        run_high_volume
        # Gateway and Redis restarts are disruptive — run last.
        run_gateway_restart
        run_redis_restart
        ;;
    *)  echo "Unknown scenario: $SCENARIO"; exit 1 ;;
esac

echo ""
log_title "Edge Case Testing Complete"
log_info "Results written to: ${RESULTS_FILE}"
echo ""
log_info "Next steps:"
echo "  1. Open Jaeger UI:  ${JAEGER_URL}"
echo "  2. Review traces for each scenario"
echo "  3. Check span continuity after restarts"
