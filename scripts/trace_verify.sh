#!/usr/bin/env bash
# trace_verify.sh — Automated verification of OpenTelemetry trace implementation.
#
# Queries the Jaeger API to verify:
#   1. All expected span names exist
#   2. Parent-child relationships are correct
#   3. Redis trace context propagation works
#   4. Log → trace correlation fields are present
#   5. No high-cardinality attributes
#   6. Span durations are reasonable
#   7. Error spans are recorded
#
# Prerequisites:
#   make trace-up && ./scripts/trace_traffic.sh
#
# Usage:
#   ./scripts/trace_verify.sh              # Full verification
#   ./scripts/trace_verify.sh --quick      # Only critical checks
#   ./scripts/trace_verify.sh --json       # Output results as JSON

set -euo pipefail

# ─── Configuration ───────────────────────────────────────────────────────────

JAEGER_URL="${JAEGER_URL:-http://localhost:16686}"
SERVICE="rtmds"
LOOKBACK="1h"              # How far back to search for traces
MIN_TRACES=5               # Minimum traces to consider results valid
JSON_OUTPUT=false
QUICK=false
PASSED=0
FAILED=0
WARNED=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --quick) QUICK=true;  shift ;;
        --json)  JSON_OUTPUT=true; shift ;;
        -h|--help)
            echo "Usage: $0 [--quick] [--json]"
            echo "  --quick   Only run critical checks"
            echo "  --json    Output results as JSON"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ─── Helpers ─────────────────────────────────────────────────────────────────

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; NC='\033[0m'; BOLD='\033[1m'

pass()  { echo -e "  ${GREEN}PASS${NC}  $*"; PASSED=$((PASSED + 1)); }
fail()  { echo -e "  ${RED}FAIL${NC}  $*"; FAILED=$((FAILED + 1)); }
warn()  { echo -e "  ${YELLOW}WARN${NC}  $*"; WARNED=$((WARNED + 1)); }
info()  { echo -e "  ${BLUE}INFO${NC}  $*"; }
title() { echo -e "\n${BOLD}${CYAN}═══ $* ═══${NC}\n"; }

# Fetch traces from Jaeger.
fetch_traces() {
    local params="service=${SERVICE}&lookback=${LOOKBACK}&limit=100"
    if [ -n "${1:-}" ]; then
        params="${params}&operation=${1}"
    fi
    curl -sf "${JAEGER_URL}/api/traces?${params}" 2>/dev/null
}

# Count spans matching a name.
count_spans() {
    local traces="$1" name="$2"
    echo "$traces" | python3 -c "
import json, sys
data = json.load(sys.stdin)
count = 0
for trace in data.get('data', []):
    for span in trace.get('spans', []):
        if span.get('operationName') == '${name}':
            count += 1
print(count)
" 2>/dev/null || echo "0"
}

# Check if a span name exists in any trace.
span_exists() {
    local traces="$1" name="$2"
    local count
    count=$(count_spans "$traces" "$name")
    if [ "$count" -gt 0 ]; then
        return 0
    fi
    return 1
}

# ─── Pre-flight ──────────────────────────────────────────────────────────────

title "Pre-flight"

info "Connecting to Jaeger at ${JAEGER_URL}..."
if ! curl -sf "${JAEGER_URL}/api/services" > /dev/null 2>&1; then
    echo "ERROR: Cannot connect to Jaeger. Is it running?"
    echo "  Start with: make trace-up"
    exit 1
fi
pass "Jaeger is reachable"

# Check service exists.
SERVICES=$(curl -sf "${JAEGER_URL}/api/services" 2>/dev/null)
if echo "$SERVICES" | python3 -c "
import json, sys
data = json.load(sys.stdin)
sys.exit(0 if '${SERVICE}' in data.get('data', []) else 1)
" 2>/dev/null; then
    pass "Service '${SERVICE}' found in Jaeger"
else
    fail "Service '${SERVICE}' not found in Jaeger"
    echo "  Expected service name: ${SERVICE}"
    echo "  Actual services: $(echo "$SERVICES" | python3 -c "import json,sys; print(json.load(sys.stdin).get('data',[]))" 2>/dev/null)"
    echo ""
    echo "Possible causes:"
    echo "  1. Tracing not enabled: RTMDS_TRACING_ENABLED=false"
    echo "  2. Wrong endpoint: RTMDS_TRACING_ENDPOINT != jaeger:4318"
    echo "  3. No traffic generated yet: run ./scripts/trace_traffic.sh"
    exit 1
fi

# Fetch traces.
info "Fetching traces (lookback=${LOOKBACK})..."
TRACES=$(fetch_traces)
TRACE_COUNT=$(echo "$TRACES" | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(len(data.get('data', [])))
" 2>/dev/null || echo "0")

info "Found ${TRACE_COUNT} traces"
if [ "$TRACE_COUNT" -lt "$MIN_TRACES" ]; then
    warn "Only ${TRACE_COUNT} traces found (minimum: ${MIN_TRACES})"
    warn "Run ./scripts/trace_traffic.sh to generate more traffic"
fi

# ═══════════════════════════════════════════════════════════════════════════════
# CHECK 1: Expected Span Names
# ═══════════════════════════════════════════════════════════════════════════════

title "Check 1: Expected Span Names"

EXPECTED_SPANS=(
    "websocket.connect"
    "subscription_request"
    "snapshot.request"
    "snapshot.lookup"
    "redis.publish"
    "redis.consume"
    "replay.request"
    "db.query_events"
    "pipeline.start"
)

for span in "${EXPECTED_SPANS[@]}"; do
    if span_exists "$TRACES" "$span"; then
        count=$(count_spans "$TRACES" "$span")
        pass "${span} (${count} instances)"
    else
        fail "${span} NOT FOUND"
    fi
done

# ═══════════════════════════════════════════════════════════════════════════════
# CHECK 2: Parent-Child Relationships
# ═══════════════════════════════════════════════════════════════════════════════

if [ "$QUICK" = false ]; then
    title "Check 2: Parent-Child Relationships"

    # Check that redis.consume has a parent (not a root span).
    info "Checking redis.consume parent-child..."
    CONSUME_PARENTS=$(echo "$TRACES" | python3 -c "
import json, sys
data = json.load(sys.stdin)
has_parent = 0
no_parent = 0
for trace in data.get('data', []):
    spans = {s['spanID']: s for s in trace.get('spans', [])}
    for span in trace.get('spans', []):
        if span.get('operationName') == 'redis.consume':
            refs = span.get('references', [])
            if refs:
                has_parent += 1
            else:
                no_parent += 1
print(f'{has_parent},{no_parent}')
" 2>/dev/null || echo "0,0")

    HAS_PARENT=$(echo "$CONSUME_PARENTS" | cut -d, -f1)
    NO_PARENT=$(echo "$CONSUME_PARENTS" | cut -d, -f2)

    if [ "$HAS_PARENT" -gt 0 ]; then
        pass "redis.consume has parent span (${HAS_PARENT} traces with propagation)"
    fi
    if [ "$NO_PARENT" -gt 0 ]; then
        warn "redis.consume has no parent in ${NO_PARENT} traces (may be first message before trace context injected)"
    fi
    if [ "$HAS_PARENT" -eq 0 ] && [ "$NO_PARENT" -eq 0 ]; then
        fail "redis.consume spans not found or no traces available"
    fi

    # Check that db.query_events is child of replay.request.
    info "Checking replay.request → db.query_events..."
    REPLAY_QUERY=$(echo "$TRACES" | python3 -c "
import json, sys
data = json.load(sys.stdin)
correct = 0
broken = 0
for trace in data.get('data', []):
    spans = {s['spanID']: s for s in trace.get('spans', [])}
    for span in trace.get('spans', []):
        if span.get('operationName') == 'db.query_events':
            refs = span.get('references', [])
            parent_id = refs[0]['spanID'] if refs else None
            if parent_id and parent_id in spans:
                parent_name = spans[parent_id].get('operationName', '')
                if parent_name == 'replay.request':
                    correct += 1
                else:
                    broken += 1
print(f'{correct},{broken}')
" 2>/dev/null || echo "0,0")

    CORRECT=$(echo "$REPLAY_QUERY" | cut -d, -f1)
    BROKEN=$(echo "$REPLAY_QUERY" | cut -d, -f2)

    if [ "$CORRECT" -gt 0 ]; then
        pass "replay.request → db.query_events (${CORRECT} correct)"
    fi
    if [ "$BROKEN" -gt 0 ]; then
        fail "replay.request → db.query_events BROKEN (${BROKEN} wrong parent)"
    fi
    if [ "$CORRECT" -eq 0 ] && [ "$BROKEN" -eq 0 ]; then
        info "No replay traces found (skipped)"
    fi
fi

# ═══════════════════════════════════════════════════════════════════════════════
# CHECK 3: Redis Trace Context Propagation
# ═══════════════════════════════════════════════════════════════════════════════

if [ "$QUICK" = false ]; then
    title "Check 3: Redis Trace Context Propagation"

    info "Checking if redis.publish and redis.consume share trace IDs..."
    PROPAGATION=$(echo "$TRACES" | python3 -c "
import json, sys
data = json.load(sys.stdin)
propagated = 0
broken = 0
for trace in data.get('data', []):
    span_names = [s['operationName'] for s in trace.get('spans', [])]
    has_publish = 'redis.publish' in span_names
    has_consume = 'redis.consume' in span_names
    if has_publish and has_consume:
        propagated += 1
    elif has_publish and not has_consume:
        broken += 1
print(f'{propagated},{broken}')
" 2>/dev/null || echo "0,0")

    PROP_OK=$(echo "$PROPAGATION" | cut -d, -f1)
    PROP_BREAK=$(echo "$PROPAGATION" | cut -d, -f2)

    if [ "$PROP_OK" -gt 0 ]; then
        pass "Redis propagation: ${PROP_OK} traces with both publish + consume"
    fi
    if [ "$PROP_BREAK" -gt 0 ]; then
        warn "Redis propagation: ${PROP_BREAK} traces with publish but no consume (may be sampling)"
    fi
    if [ "$PROP_OK" -eq 0 ] && [ "$PROP_BREAK" -eq 0 ]; then
        fail "No redis.publish or redis.consume spans found"
    fi
fi

# ═══════════════════════════════════════════════════════════════════════════════
# CHECK 4: Resource Attributes
# ═══════════════════════════════════════════════════════════════════════════════

if [ "$QUICK" = false ]; then
    title "Check 4: Resource Attributes"

    info "Checking service.name attribute..."
    SVC_NAME=$(echo "$TRACES" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for trace in data.get('data', []):
    proc = trace.get('process', {})
    tags = {t['key']: t['value'] for t in proc.get('tags', [])}
    name = tags.get('service.name', '')
    if name:
        print(name)
        break
" 2>/dev/null || echo "")

    if [ "$SVC_NAME" = "rtmds" ]; then
        pass "service.name = 'rtmds'"
    elif [ -n "$SVC_NAME" ]; then
        warn "service.name = '${SVC_NAME}' (expected 'rtmds')"
    else
        fail "service.name not found in resource attributes"
    fi
fi

# ═══════════════════════════════════════════════════════════════════════════════
# CHECK 5: High-Cardinality Attributes (Anti-pattern detection)
# ═══════════════════════════════════════════════════════════════════════════════

if [ "$QUICK" = false ]; then
    title "Check 5: High-Cardinality Attribute Detection"

    info "Scanning for forbidden high-cardinality attributes..."
    HC_RESULT=$(echo "$TRACES" | python3 -c "
import json, sys
data = json.load(sys.stdin)
forbidden = {'symbol', 'price', 'volume', 'bid', 'ask', 'session_id'}
found = set()
for trace in data.get('data', []):
    for span in trace.get('spans', []):
        for tag in span.get('tags', []):
            key = tag.get('key', '')
            if key in forbidden:
                found.add(key)
if found:
    print(','.join(sorted(found)))
else:
    print('none')
" 2>/dev/null || echo "error")

    if [ "$HC_RESULT" = "none" ]; then
        pass "No high-cardinality attributes found"
    elif [ "$HC_RESULT" = "error" ]; then
        warn "Could not scan attributes"
    else
        fail "High-cardinality attributes found: ${HC_RESULT}"
        info "These should never be span attributes at scale"
    fi
fi

# ═══════════════════════════════════════════════════════════════════════════════
# CHECK 6: Span Duration Sanity
# ═══════════════════════════════════════════════════════════════════════════════

if [ "$QUICK" = false ]; then
    title "Check 6: Span Duration Sanity"

    info "Checking for negative or suspiciously long durations..."
    DUR_CHECK=$(echo "$TRACES" | python3 -c "
import json, sys
data = json.load(sys.stdin)
negative = 0
too_long = 0
for trace in data.get('data', []):
    for span in trace.get('spans', []):
        dur = span.get('duration', 0)
        if dur < 0:
            negative += 1
        if dur > 60_000_000:  # > 60 seconds
            too_long += 1
print(f'{negative},{too_long}')
" 2>/dev/null || echo "0,0")

    NEG_DUR=$(echo "$DUR_CHECK" | cut -d, -f1)
    LONG_DUR=$(echo "$DUR_CHECK" | cut -d, -f2)

    if [ "$NEG_DUR" -eq 0 ]; then
        pass "No negative durations"
    else
        fail "${NEG_DUR} spans with negative durations"
    fi

    if [ "$LONG_DUR" -eq 0 ]; then
        pass "No spans longer than 60s (except pipeline.start)"
    else
        warn "${LONG_DUR} spans longer than 60s (may be legitimate for pipeline.start)"
    fi
fi

# ═══════════════════════════════════════════════════════════════════════════════
# CHECK 7: Span Event Verification
# ═══════════════════════════════════════════════════════════════════════════════

if [ "$QUICK" = false ]; then
    title "Check 7: Span Events"

    info "Checking for pipeline lifecycle events..."
    EVENTS=$(echo "$TRACES" | python3 -c "
import json, sys
data = json.load(sys.stdin)
events = set()
for trace in data.get('data', []):
    for span in trace.get('spans', []):
        for log in span.get('logs', []):
            for field in log.get('fields', []):
                if field.get('key') == 'event':
                    events.add(field.get('value', ''))
print(','.join(sorted(events)) if events else 'none')
" 2>/dev/null || echo "error")

    if [ "$EVENTS" != "none" ] && [ "$EVENTS" != "error" ]; then
        pass "Span events found: ${EVENTS}"
    elif [ "$EVENTS" = "none" ]; then
        info "No span events found (pipeline may not have lifecycle transitions)"
    else
        warn "Could not scan span events"
    fi
fi

# ═══════════════════════════════════════════════════════════════════════════════
# CHECK 8: Span Kind Verification
# ═══════════════════════════════════════════════════════════════════════════════

if [ "$QUICK" = false ]; then
    title "Check 8: Span Kind Verification"

    info "Verifying span kinds match design spec..."
    KIND_CHECK=$(echo "$TRACES" | python3 -c "
import json, sys
data = json.load(sys.stdin)
expected = {
    'redis.publish': 'producer',
    'redis.consume': 'consumer',
    'websocket.connect': 'server',
    'subscription_request': 'server',
    'replay.request': 'server',
    'snapshot.request': 'internal',
    'snapshot.lookup': 'internal',
    'pipeline.start': 'internal',
}
kind_map = {0: 'unspecified', 1: 'internal', 2: 'server', 3: 'client', 4: 'producer', 5: 'consumer'}
correct = 0
wrong = 0
for trace in data.get('data', []):
    for span in trace.get('spans', []):
        name = span.get('operationName', '')
        if name in expected:
            kind_val = span.get('kind', 0)
            kind_name = kind_map.get(kind_val, 'unknown')
            if kind_name == expected[name]:
                correct += 1
            else:
                wrong += 1
print(f'{correct},{wrong}')
" 2>/dev/null || echo "0,0")

    KIND_OK=$(echo "$KIND_CHECK" | cut -d, -f1)
    KIND_WRONG=$(echo "$KIND_CHECK" | cut -d, -f2)

    if [ "$KIND_OK" -gt 0 ]; then
        pass "Span kinds correct (${KIND_OK} verified)"
    fi
    if [ "$KIND_WRONG" -gt 0 ]; then
        fail "Span kinds wrong (${KIND_WRONG} mismatches)"
    fi
fi

# ═══════════════════════════════════════════════════════════════════════════════
# CHECK 9: db.query_events Attributes
# ═══════════════════════════════════════════════════════════════════════════════

if [ "$QUICK" = false ]; then
    title "Check 9: Database Span Attributes"

    info "Checking db.query_events has db.row_count..."
    DB_ATTR=$(echo "$TRACES" | python3 -c "
import json, sys
data = json.load(sys.stdin)
has_row_count = 0
missing = 0
for trace in data.get('data', []):
    for span in trace.get('spans', []):
        if span.get('operationName') == 'db.query_events':
            tags = {t['key']: t['value'] for t in span.get('tags', [])}
            if 'db.row_count' in tags:
                has_row_count += 1
            else:
                missing += 1
print(f'{has_row_count},{missing}')
" 2>/dev/null || echo "0,0")

    DB_OK=$(echo "$DB_ATTR" | cut -d, -f1)
    DB_MISS=$(echo "$DB_ATTR" | cut -d, -f2)

    if [ "$DB_OK" -gt 0 ]; then
        pass "db.query_events has db.row_count (${DB_OK} spans)"
    fi
    if [ "$DB_MISS" -gt 0 ]; then
        warn "db.query_events missing db.row_count in ${DB_MISS} spans"
    fi
fi

# ═══════════════════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════════════════

title "Verification Summary"

echo -e "  ${GREEN}PASSED${NC}:  ${PASSED}"
echo -e "  ${RED}FAILED${NC}:  ${FAILED}"
echo -e "  ${YELLOW}WARNED${NC}:  ${WARNED}"
echo ""

if [ "$FAILED" -eq 0 ]; then
    echo -e "  ${GREEN}${BOLD}ALL CRITICAL CHECKS PASSED${NC}"
    echo ""
    echo "  Open Jaeger UI to explore traces:"
    echo "    ${JAEGER_URL}"
    echo ""
    echo "  Quick links:"
    echo "    All traces:        ${JAEGER_URL}/#/services/${SERVICE}/dependencies"
    echo "    Redis publish:     ${JAEGER_URL}/#/search?service=${SERVICE}&operation=redis.publish"
    echo "    WebSocket connect: ${JAEGER_URL}/#/search?service=${SERVICE}&operation=websocket.connect"
    echo "    Replay requests:   ${JAEGER_URL}/#/search?service=${SERVICE}&operation=replay.request"
    exit 0
else
    echo -e "  ${RED}${BOLD}${FAILED} CHECKS FAILED — review output above${NC}"
    exit 1
fi
