#!/usr/bin/env bash
# run_spike_test.sh — Automates spike testing for RTMDS.
#
# Simulates a baseline load, then introduces a massive sudden burst of connections,
# observing system elasticity and recovery back to baseline.

set -euo pipefail

COMPOSE_FILE="docker-compose.benchmark.yml"
RESULTS_DIR="docs/results/spike_test"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

BASELINE_CLIENTS="${BASELINE_CLIENTS:-500}"
SPIKE_CLIENTS="${SPIKE_CLIENTS:-5000}"
BASELINE_DURATION="${BASELINE_DURATION:-60}"
SPIKE_START_DELAY="${SPIKE_START_DELAY:-20}"
SPIKE_DURATION="${SPIKE_DURATION:-15}"
SYMBOL_COUNT="${SYMBOL_COUNT:-10}"

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
log_title() { echo -e "\n${BOLD}${CYAN}═══ $* ═══${NC}\n"; }

cleanup_stack() {
    log_info "Cleaning up spike test stack..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
}

start_stack() {
    log_info "Starting stack for spike test..."
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

main() {
    log_title "Production Spike Test Automation"
    
    local run_dir="${RESULTS_DIR}/run_${TIMESTAMP}"
    mkdir -p "$run_dir"
    
    if ! start_stack; then
        exit 1
    fi
    
    log_info "Configuration:"
    log_info "  Baseline: ${BASELINE_CLIENTS} clients for ${BASELINE_DURATION}s"
    log_info "  Spike:    +${SPIKE_CLIENTS} clients for ${SPIKE_DURATION}s (starting at t=${SPIKE_START_DELAY}s)"
    
    # Start metrics collection in background
    bash scripts/collect_metrics.sh $((BASELINE_DURATION + 10)) "${run_dir}/metrics.csv" &
    local metrics_pid=$!
    
    # Launch spike job in background to trigger after SPIKE_START_DELAY
    (
        sleep "$SPIKE_START_DELAY"
        log_warn ">>> TRIGGERING SPIKE: Adding ${SPIKE_CLIENTS} connections! <<<"
        go run cmd/benchmark/main.go \
            -url ws://localhost:8080/ws \
            -clients "$SPIKE_CLIENTS" \
            -symbols "$SYMBOL_COUNT" \
            -duration "${SPIKE_DURATION}s" \
            -output "${run_dir}/benchmark_spike.json" > "${run_dir}/benchmark_spike.log" 2>&1
        log_ok "<<< SPIKE COMPLETE >>>"
    ) &
    local spike_pid=$!
    
    # Run baseline benchmark
    log_info "Starting baseline load..."
    if go run cmd/benchmark/main.go \
        -url ws://localhost:8080/ws \
        -clients "$BASELINE_CLIENTS" \
        -symbols "$SYMBOL_COUNT" \
        -duration "${BASELINE_DURATION}s" \
        -output "${run_dir}/benchmark_baseline.json" 2>&1 | tee "${run_dir}/benchmark_baseline.log"; then
        
        log_ok "Baseline completed successfully."
    else
        log_fail "Baseline benchmark client failed."
    fi
    
    # Wait for background jobs
    wait $spike_pid 2>/dev/null || true
    wait $metrics_pid 2>/dev/null || true
    
    cleanup_stack
    log_title "Spike Test Finished. Results saved to: $run_dir"
}

main
