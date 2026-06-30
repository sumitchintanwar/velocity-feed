#!/usr/bin/env bash
# run_benchmark.sh — Orchestrates distributed benchmark runs for RTMDS.
#
# Runs benchmarks with 1, 3, and 5 gateways, collecting throughput,
# latency, CPU, and memory metrics.
#
# Usage:
#   ./scripts/run_benchmark.sh [scenario]
#   ./scripts/run_benchmark.sh           # Run all scenarios
#   ./scripts/run_benchmark.sh 1gw       # Run 1-gateway only
#   ./scripts/run_benchmark.sh 3gw       # Run 3-gateway only
#   ./scripts/run_benchmark.sh 5gw       # Run 5-gateway only

set -euo pipefail

COMPOSE_FILE="docker-compose.benchmark.yml"
RESULTS_DIR="docs/results/benchmark"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BENCH_DURATION="${BENCH_DURATION:-60}"
CLIENT_COUNT="${CLIENT_COUNT:-100}"
SYMBOL_COUNT="${SYMBOL_COUNT:-5}"
CHURN_RATE="${CHURN_RATE:-5.0}" # 5% connection drop/reconnect per minute for realism

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

# ─── Helpers ──────────────────────────────────────────────────────────

container_running() {
    docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null | grep -q "true"
}

wait_healthy() {
    local container="$1"
    local timeout="${2:-30}"
    local elapsed=0
    while [ $elapsed -lt $timeout ]; do
        if container_running "$container"; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    return 1
}

check_health() {
    curl -s -o /dev/null -w "%{http_code}" --max-time 5 http://localhost:8080/health 2>/dev/null || echo "000"
}

get_gateway_count() {
    curl -s --max-time 5 http://localhost:8080/gateways 2>/dev/null | \
        grep -o '"count":[0-9]*' | cut -d: -f2 || echo "0"
}

cleanup_stack() {
    log_info "Cleaning up benchmark stack..."
    docker compose -f "$COMPOSE_FILE" --profile 5gateways down -v --remove-orphans 2>/dev/null || true
    sleep 2
}

start_stack() {
    local num_gateways="$1"
    local profile_flag=""
    
    if [ "$num_gateways" -eq 5 ]; then
        profile_flag="--profile 5gateways"
    fi
    
    log_info "Starting stack with $num_gateways gateways..."
    cleanup_stack
    
    if [ "$num_gateways" -eq 1 ]; then
        docker compose -f "$COMPOSE_FILE" up --build -d gateway1 nginx redis
    else
        docker compose -f "$COMPOSE_FILE" $profile_flag up --build -d
    fi
    
    # Wait for services
    log_info "Waiting for services to start..."
    sleep 5
    
    # Wait for health checks
    local all_healthy=false
    for i in $(seq 1 30); do
        if [ "$(check_health)" = "200" ]; then
            all_healthy=true
            break
        fi
        sleep 2
    done
    
    if [ "$all_healthy" = true ]; then
        local gw_count
        gw_count=$(get_gateway_count)
        log_ok "System ready: $gw_count gateways active"
        return 0
    else
        log_fail "System not ready after 60s"
        return 1
    fi
}

# ─── Benchmark Scenarios ─────────────────────────────────────────────

run_benchmark() {
    local num_gateways="$1"
    local scenario_name="${num_gateways}gw"
    
    log_title "Benchmark: $num_gateways Gateway(s)"
    
    # Start stack
    if ! start_stack "$num_gateways"; then
        log_fail "Failed to start $num_gateways-gateway stack"
        return 1
    fi
    
    # Create results directory
    local scenario_dir="${RESULTS_DIR}/${scenario_name}_${TIMESTAMP}"
    mkdir -p "$scenario_dir"
    
    # Start metrics collection in background
    log_info "Starting metrics collection..."
    bash scripts/collect_metrics.sh $((BENCH_DURATION + 30)) "${scenario_dir}/metrics.csv" &
    local metrics_pid=$!
    
    # Run Go benchmark client
    log_info "Running benchmark client: $CLIENT_COUNT clients, $BENCH_DURATION duration"
    
    if go run cmd/benchmark/main.go \
        -url ws://localhost:8080/ws \
        -clients "$CLIENT_COUNT" \
        -symbols "$SYMBOL_COUNT" \
        -duration "${BENCH_DURATION}s" \
        -churn_rate "$CHURN_RATE" \
        -output "${scenario_dir}/benchmark.json" 2>&1 | tee "${scenario_dir}/benchmark.log"; then
        
        log_ok "Benchmark completed successfully"
    else
        log_warn "Benchmark client exited with non-zero status"
    fi
    
    # Wait for metrics collection to finish
    wait $metrics_pid 2>/dev/null || true
    
    # Collect final system snapshot
    log_info "Collecting final system snapshot..."
    docker stats --no-stream > "${scenario_dir}/docker_stats.txt" 2>/dev/null || true
    docker exec rtmds-bench-redis redis-cli INFO all > "${scenario_dir}/redis_info.txt" 2>/dev/null || true
    
    # Cleanup
    cleanup_stack
    
    # Print summary
    print_scenario_summary "$scenario_dir" "$num_gateways"
    
    echo "$scenario_dir"
}

print_scenario_summary() {
    local dir="$1"
    local num_gateways="$2"
    
    log_title "Summary: $num_gateways Gateway(s)"
    
    if [ -f "${dir}/benchmark.json" ]; then
        # Extract key metrics from JSON
        local throughput latency_p50 latency_p99 cpu mem
        
        throughput=$(python3 -c "import json; d=json.load(open('${dir}/benchmark.json')); print(f\"{d['messages_per_sec']:.0f}\")" 2>/dev/null || echo "N/A")
        latency_p50=$(python3 -c "import json; d=json.load(open('${dir}/benchmark.json')); print(f\"{d['latency']['p50_ms']:.2f}\")" 2>/dev/null || echo "N/A")
        latency_p99=$(python3 -c "import json; d=json.load(open('${dir}/benchmark.json')); print(f\"{d['latency']['p99_ms']:.2f}\")" 2>/dev/null || echo "N/A")
        
        echo "  Throughput:     $throughput msg/sec"
        echo "  Latency P50:    $latency_p50 ms"
        echo "  Latency P99:    $latency_p99 ms"
        echo ""
        echo "  Results saved to: $dir"
    fi
}

# ─── Final Report ─────────────────────────────────────────────────────

generate_final_report() {
    local report_file="${RESULTS_DIR}/BENCHMARK_REPORT_${TIMESTAMP}.md"
    
    log_title "Generating Final Report"
    
    cat > "$report_file" <<EOF
# RTMDS Distributed Benchmark Report

**Date:** $(date -u +"%Y-%m-%d %H:%M:%S UTC")
**Duration:** ${BENCH_DURATION}s per scenario (Use >900s for official soak tests)
**Clients:** ${CLIENT_COUNT}
**Symbols:** ${SYMBOL_COUNT} (Zipfian distribution)
**Client Churn Rate:** ${CHURN_RATE}%/min (Authenticates Handshake Overhead)
**Payload Size:** ~128 bytes JSON per quote

---

## Executive Summary

| Metric | 1 Gateway | 3 Gateways | 5 Gateways |
|--------|-----------|------------|------------|
EOF
    
    # Extract and compare results
    for scenario in 1gw 3gw 5gw; do
        local dir=$(find "${RESULTS_DIR}" -maxdepth 1 -name "${scenario}_*" -type d | sort -r | head -1)
        if [ -n "$dir" ] && [ -f "${dir}/benchmark.json" ]; then
            local throughput p50 p99
            throughput=$(python3 -c "import json; d=json.load(open('${dir}/benchmark.json')); print(f\"{d['messages_per_sec']:.0f}\")" 2>/dev/null || echo "N/A")
            p50=$(python3 -c "import json; d=json.load(open('${dir}/benchmark.json')); print(f\"{d['latency']['p50_ms']:.2f}\")" 2>/dev/null || echo "N/A")
            p99=$(python3 -c "import json; d=json.load(open('${dir}/benchmark.json')); print(f\"{d['latency']['p99_ms']:.2f}\")" 2>/dev/null || echo "N/A")
            
            eval "${scenario}_throughput=$throughput"
            eval "${scenario}_p50=$p50"
            eval "${scenario}_p99=$p99"
        fi
    done
    
    cat >> "$report_file" <<EOF
| Throughput (msg/sec) | ${1gw_throughput:-N/A} | ${3gw_throughput:-N/A} | ${5gw_throughput:-N/A} |
| Latency P50 (ms) | ${1gw_p50:-N/A} | ${3gw_p50:-N/A} | ${5gw_p50:-N/A} |
| Latency P99 (ms) | ${1gw_p99:-N/A} | ${3gw_p99:-N/A} | ${5gw_p99:-N/A} |

---

## Scaling Efficiency

EOF
    
    # Calculate scaling efficiency
    if [ -n "${1gw_throughput:-}" ] && [ "${1gw_throughput}" != "N/A" ]; then
        local base_throughput=$1gw_throughput
        
        for gw_count in 3 5; do
            local current_throughput="${${gw_count}gw_throughput:-N/A}"
            if [ "$current_throughput" != "N/A" ]; then
                local efficiency=$(python3 -c "print(f\"{$current_throughput / ($base_throughput * $gw_count) * 100:.1f}\")" 2>/dev/null || echo "N/A")
                echo "- ${gw_count} Gateways: ${efficiency}% efficiency" >> "$report_file"
            fi
        done
    fi
    
    cat >> "$report_file" <<EOF

---

## Detailed Results

### 1 Gateway
- Results: \`${RESULTS_DIR}/1gw_*/\`
- Benchmark JSON, metrics CSV, Docker stats, Redis info

### 3 Gateways
- Results: \`${RESULTS_DIR}/3gw_*/\`
- Benchmark JSON, metrics CSV, Docker stats, Redis info

### 5 Gateways
- Results: \`${RESULTS_DIR}/5gw_*/\`
- Benchmark JSON, metrics CSV, Docker stats, Redis info

---

## Configuration

- **Feed Symbols:** AAPL, MSFT, GOOG, AMZN, TSLA, META, NVDA, JPM, V, JNJ
- **Redis:** 512MB maxmemory, allkeys-lru
- **Gateway Resources:** 2 CPU, 512MB memory limit per instance
- **Nginx:** least_conn with max_fails=2, fail_timeout=10s

---

## Success Criteria

| Criterion | Target | Status |
|-----------|--------|--------|
| Linear Scaling | >80% efficiency | TBD |
| P50 Latency | <5ms | TBD |
| P99 Latency | <50ms | TBD |
| No Memory Leaks | Stable memory | TBD |
EOF
    
    log_ok "Report generated: $report_file"
}

# ─── Main ─────────────────────────────────────────────────────────────

main() {
    local scenario="${1:-all}"
    
    echo -e "${BOLD}${CYAN}"
    echo "╔═══════════════════════════════════════════════════════════╗"
    echo "║       RTMDS Distributed Benchmark Suite                ║"
    echo "║  Multi-Gateway Performance Testing                     ║"
    echo "╚═══════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    
    log_info "Configuration:"
    log_info "  Duration:   ${BENCH_DURATION}s"
    log_info "  Clients:    ${CLIENT_COUNT}"
    log_info "  Symbols:    ${SYMBOL_COUNT}"
    log_info "  Results:    ${RESULTS_DIR}"
    
    # Ensure results directory exists
    mkdir -p "$RESULTS_DIR"
    
    # Clean up any existing stack
    cleanup_stack
    
    case "$scenario" in
        1gw)
            run_benchmark 1
            ;;
        3gw)
            run_benchmark 3
            ;;
        5gw)
            run_benchmark 5
            ;;
        all)
            run_benchmark 1
            sleep 5
            run_benchmark 3
            sleep 5
            run_benchmark 5
            generate_final_report
            ;;
        *)
            echo "Unknown scenario: $scenario"
            echo "Available: 1gw, 3gw, 5gw, all"
            exit 1
            ;;
    esac
    
    log_ok "Benchmark suite complete!"
}

main "$@"
