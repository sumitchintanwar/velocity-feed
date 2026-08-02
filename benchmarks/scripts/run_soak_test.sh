#!/usr/bin/env bash
# run_soak_test.sh — Automates long-duration stability and memory leak testing.
#
# Runs a moderate, sustained load for an extended duration, periodically capturing
# memory and goroutine profiles.

set -euo pipefail

COMPOSE_FILE="docker-compose.benchmark.yml"
RESULTS_DIR="docs/results/soak_test"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

SOAK_DURATION="${SOAK_DURATION:-3600}" # Default: 1 hour
CLIENT_COUNT="${CLIENT_COUNT:-500}"
SYMBOL_COUNT="${SYMBOL_COUNT:-10}"
PPROF_INTERVAL="${PPROF_INTERVAL:-600}" # Capture pprof every 10 mins

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
    log_info "Cleaning up soak test stack..."
    docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
}

start_stack() {
    log_info "Starting stack for soak test..."
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

collect_profiles_loop() {
    local run_dir="$1"
    local elapsed=0
    
    while [ $elapsed -lt "$SOAK_DURATION" ]; do
        sleep "$PPROF_INTERVAL" || return 0
        elapsed=$((elapsed + PPROF_INTERVAL))
        
        log_info "Capturing pprof profiles at ${elapsed}s..."
        curl -s -o "${run_dir}/heap_${elapsed}s.pprof" http://localhost:8080/debug/pprof/heap || true
        curl -s -o "${run_dir}/goroutine_${elapsed}s.pprof" http://localhost:8080/debug/pprof/goroutine || true
    done
}

main() {
    log_title "Production Soak Test Automation"
    
    local run_dir="${RESULTS_DIR}/run_${TIMESTAMP}"
    mkdir -p "$run_dir"
    
    if ! start_stack; then
        exit 1
    fi
    
    log_info "Starting soak test: ${CLIENT_COUNT} clients, ${SOAK_DURATION}s duration"
    
    # Start pprof collection in background
    collect_profiles_loop "$run_dir" &
    local profiler_pid=$!
    
    # Start metrics collection in background
    bash scripts/collect_metrics.sh $((SOAK_DURATION + 30)) "${run_dir}/metrics.csv" &
    local metrics_pid=$!
    
    # Run the benchmark
    if go run cmd/benchmark/main.go \
        -url ws://localhost:8080/ws \
        -clients "$CLIENT_COUNT" \
        -symbols "$SYMBOL_COUNT" \
        -duration "${SOAK_DURATION}s" \
        -output "${run_dir}/benchmark.json" 2>&1 | tee "${run_dir}/benchmark.log"; then
        
        log_ok "Soak test completed successfully."
    else
        log_fail "Benchmark client failed."
    fi
    
    # Stop background tasks
    kill $profiler_pid 2>/dev/null || true
    wait $metrics_pid 2>/dev/null || true
    
    cleanup_stack
    log_title "Soak Test Finished. Results saved to: $run_dir"
}

main
