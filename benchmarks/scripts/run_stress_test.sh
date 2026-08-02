#!/usr/bin/env bash
# run_stress_test.sh — Automates progressive load increase to find system saturation point.
#
# Steps load up progressively (e.g., 500, 1000, 2000, 5000, 10000 clients).
# Stops when throughput flatlines or latency degrades severely.

set -euo pipefail

COMPOSE_FILE="docker-compose.benchmark.yml"
RESULTS_DIR="docs/results/stress_test"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
STAGE_DURATION="${STAGE_DURATION:-30}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

log_info()  { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_ok()    { echo -e "${GREEN}[PASS]${NC}  $*"; }
log_fail()  { echo -e "${RED}[FAIL]${NC}  $*"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_title() { echo -e "\n${BOLD}${CYAN}═══ $* ═══${NC}\n"; }

# Progressive load stages
LOAD_STAGES=( 100 500 1000 2500 5000 )
SYMBOL_COUNT=10
LATENCY_THRESHOLD_MS=1000.0  # Saturation limit for P99

cleanup_stack() {
    log_info "Cleaning up stress test stack..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
}

start_stack() {
    log_info "Starting stack for stress test..."
    cleanup_stack
    docker compose -f "$COMPOSE_FILE" up --build -d gateway1 nginx redis
    
    sleep 5
    local all_healthy=false
    for i in $(seq 1 30); do
        if [ "$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 http://localhost:8080/health 2>/dev/null)" = "200" ]; then
            all_healthy=true
            break
        fi
        sleep 2
    done
    
    if [ "$all_healthy" = true ]; then
        log_ok "System ready"
        return 0
    else
        log_fail "System not ready after 60s"
        return 1
    fi
}

run_stage() {
    local clients="$1"
    local run_dir="${RESULTS_DIR}/stage_${clients}_${TIMESTAMP}"
    mkdir -p "$run_dir"
    
    log_info "Starting Phase: ${clients} clients (Duration: ${STAGE_DURATION}s)"
    
    if go run cmd/benchmark/main.go \
        -url ws://localhost:8080/ws \
        -clients "$clients" \
        -symbols "$SYMBOL_COUNT" \
        -duration "${STAGE_DURATION}s" \
        -churn_rate 5.0 \
        -output "${run_dir}/benchmark.json" 2>&1 | tee "${run_dir}/benchmark.log"; then
        
        local p99
        p99=$(python3 -c "import json; d=json.load(open('${run_dir}/benchmark.json')); print(f\"{d['latency']['p99_ms']:.2f}\")" 2>/dev/null || echo "0")
        local tput
        tput=$(python3 -c "import json; d=json.load(open('${run_dir}/benchmark.json')); print(f\"{d['messages_per_sec']:.0f}\")" 2>/dev/null || echo "0")
        
        log_ok "Stage completed. Throughput: ${tput} msg/s, P99 Latency: ${p99} ms"
        
        # Check saturation
        if (( $(echo "$p99 > $LATENCY_THRESHOLD_MS" | bc -l) )); then
            log_warn "SATURATION POINT DETECTED. P99 latency ($p99 ms) exceeded threshold ($LATENCY_THRESHOLD_MS ms)."
            return 1
        fi
        
        return 0
    else
        log_fail "Benchmark client failed during stage."
        return 1
    fi
}

main() {
    log_title "Production Stress Test Automation"
    mkdir -p "$RESULTS_DIR"
    
    if ! start_stack; then
        exit 1
    fi
    
    local saturated=false
    for clients in "${LOAD_STAGES[@]}"; do
        if ! run_stage "$clients"; then
            saturated=true
            break
        fi
        log_info "Cooling down before next stage..."
        sleep 5
    done
    
    if [ "$saturated" = true ]; then
        log_title "Stress Test Complete: Saturation Identified"
    else
        log_title "Stress Test Complete: System Scaled Without Failing"
    fi
    
    cleanup_stack
}

main
