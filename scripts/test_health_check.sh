#!/bin/bash
# =============================================================================
# Integration Test: Health Check Endpoints
# =============================================================================
# Tests:
#   1. All endpoints respond correctly when healthy
#   2. Kill Redis → readiness fails, liveness stays OK
#   3. Restart Redis → readiness recovers
#
# Prerequisites:
#   - Docker stack running (make docker-up)
#   - curl, jq installed
# =============================================================================

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

GATEWAY_URL="http://localhost:8080"
PASSED=0
FAILED=0

log() {
    echo -e "${YELLOW}[$(date '+%H:%M:%S')]${NC} $1"
}

pass() {
    echo -e "${GREEN}  ✓ PASS:${NC} $1"
    ((PASSED++))
}

fail() {
    echo -e "${RED}  ✗ FAIL:${NC} $1"
    ((FAILED++))
}

assert_status() {
    local endpoint=$1
    local expected=$2
    local actual=$3
    local body=$4

    if [ "$actual" = "$expected" ]; then
        pass "$endpoint returned $expected"
    else
        fail "$endpoint expected $expected, got $actual"
        echo "    Body: $body"
    fi
}

assert_json_field() {
    local endpoint=$1
    local field=$2
    local expected=$3
    local body=$4

    local actual=$(echo "$body" | jq -r ".$field" 2>/dev/null)
    if [ "$actual" = "$expected" ]; then
        pass "$endpoint $field = $expected"
    else
        fail "$endpoint $field expected '$expected', got '$actual'"
    fi
}

# =============================================================================
# Phase 1: All healthy
# =============================================================================
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Phase 1: All healthy"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Wait for gateway to be ready
log "Waiting for gateway to be ready..."
for i in $(seq 1 30); do
    if curl -sf "$GATEWAY_URL/health" > /dev/null 2>&1; then
        break
    fi
    sleep 1
done

# Test /health (liveness, legacy)
log "Testing /health (liveness, legacy)..."
RESPONSE=$(curl -sf -w "\n%{http_code}" "$GATEWAY_URL/health" 2>/dev/null)
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)
assert_status "/health" "200" "$HTTP_CODE" "$BODY"
assert_json_field "/health" "status" "ok" "$BODY"

# Test /liveness
log "Testing /liveness..."
RESPONSE=$(curl -sf -w "\n%{http_code}" "$GATEWAY_URL/liveness" 2>/dev/null)
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)
assert_status "/liveness" "200" "$HTTP_CODE" "$BODY"
assert_json_field "/liveness" "status" "ok" "$BODY"

# Test /readiness
log "Testing /readiness..."
RESPONSE=$(curl -sf -w "\n%{http_code}" "$GATEWAY_URL/readiness" 2>/dev/null)
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)
assert_status "/readiness" "200" "$HTTP_CODE" "$BODY"
assert_json_field "/readiness" "status" "ok" "$BODY"

# Verify Redis check is included
REDIS_STATUS=$(echo "$BODY" | jq -r '.checks[] | select(.name=="redis") | .ok' 2>/dev/null)
if [ "$REDIS_STATUS" = "true" ]; then
    pass "Redis check is healthy"
else
    fail "Redis check should be healthy, got: $REDIS_STATUS"
fi

# Test /ready (legacy)
log "Testing /ready (legacy)..."
RESPONSE=$(curl -sf -w "\n%{http_code}" "$GATEWAY_URL/ready" 2>/dev/null)
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)
assert_status "/ready" "200" "$HTTP_CODE" "$BODY"

# Test /health/detail
log "Testing /health/detail..."
RESPONSE=$(curl -sf -w "\n%{http_code}" "$GATEWAY_URL/health/detail" 2>/dev/null)
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)
assert_status "/health/detail" "200" "$HTTP_CODE" "$BODY"

echo ""

# =============================================================================
# Phase 2: Kill Redis
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Phase 2: Kill Redis"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

log "Stopping Redis container..."
docker stop rtmds-redis 2>/dev/null || true
sleep 2

# Test /liveness — should still be 200 (no external deps)
log "Testing /liveness (should still be OK)..."
RESPONSE=$(curl -sf -w "\n%{http_code}" "$GATEWAY_URL/liveness" 2>/dev/null)
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)
assert_status "/liveness" "200" "$HTTP_CODE" "$BODY"

# Test /health — should still be 200 (liveness)
log "Testing /health (should still be OK)..."
RESPONSE=$(curl -sf -w "\n%{http_code}" "$GATEWAY_URL/health" 2>/dev/null)
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)
assert_status "/health" "200" "$HTTP_CODE" "$BODY"

# Test /readiness — should be 503 (Redis down)
log "Testing /readiness (should be 503)..."
RESPONSE=$(curl -s -w "\n%{http_code}" "$GATEWAY_URL/readiness" 2>/dev/null)
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)
assert_status "/readiness" "503" "$HTTP_CODE" "$BODY"

# Verify Redis check failed
REDIS_OK=$(echo "$BODY" | jq -r '.checks[] | select(.name=="redis") | .ok' 2>/dev/null)
if [ "$REDIS_OK" = "false" ]; then
    pass "Redis check correctly reports failure"
else
    fail "Redis check should be false when Redis is down, got: $REDIS_OK"
fi

# Verify overall status is degraded
OVERALL_STATUS=$(echo "$BODY" | jq -r '.status' 2>/dev/null)
if [ "$OVERALL_STATUS" = "degraded" ]; then
    pass "Overall status is degraded"
else
    fail "Overall status should be degraded, got: $OVERALL_STATUS"
fi

echo ""

# =============================================================================
# Phase 3: Restart Redis
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Phase 3: Restart Redis"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

log "Starting Redis container..."
docker start rtmds-redis 2>/dev/null || true

log "Waiting for Redis to be ready..."
for i in $(seq 1 30); do
    if docker exec rtmds-redis redis-cli ping 2>/dev/null | grep -q PONG; then
        break
    fi
    sleep 1
done
sleep 2

# Test /readiness — should recover
log "Testing /readiness (should recover to 200)..."
RESPONSE=$(curl -sf -w "\n%{http_code}" "$GATEWAY_URL/readiness" 2>/dev/null)
HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)
assert_status "/readiness" "200" "$HTTP_CODE" "$BODY"

# Verify Redis check recovered
REDIS_OK=$(echo "$BODY" | jq -r '.checks[] | select(.name=="redis") | .ok' 2>/dev/null)
if [ "$REDIS_OK" = "true" ]; then
    pass "Redis check recovered"
else
    fail "Redis check should be true after restart, got: $REDIS_OK"
fi

echo ""

# =============================================================================
# Summary
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Summary"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo -e "${GREEN}Passed: $PASSED${NC}"
echo -e "${RED}Failed: $FAILED${NC}"
echo ""

if [ $FAILED -gt 0 ]; then
    echo -e "${RED}TESTS FAILED${NC}"
    exit 1
fi

echo -e "${GREEN}ALL TESTS PASSED${NC}"
exit 0
