#!/usr/bin/env bash
# collect_metrics.sh — Collects system metrics during benchmark runs.
#
# Usage:
#   ./scripts/collect_metrics.sh <duration> <output_file>
#
# Example:
#   ./scripts/collect_metrics.sh 60 docs/results/metrics_1gw.csv

set -euo pipefail

DURATION="${1:-60}"
OUTPUT_FILE="${2:-/tmp/metrics.csv}"
INTERVAL=2

# Container names
REDIS="rtmds-bench-redis"
GATEWAYS=("rtmds-bench-gw1" "rtmds-bench-gw2" "rtmds-bench-gw3")

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[METRICS]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }

# Check if container exists
container_exists() {
    docker inspect "$1" >/dev/null 2>&1
}

# Get container CPU usage percentage
get_cpu_percent() {
    local container="$1"
    if ! container_exists "$container"; then
        echo "0"
        return
    fi
    
    docker stats "$container" --no-stream --format "{{.CPUPerc}}" 2>/dev/null | tr -d '%' || echo "0"
}

# Get container memory usage in MB
get_memory_mb() {
    local container="$1"
    if ! container_exists "$container"; then
        echo "0"
        return
    fi
    
    docker stats "$container" --no-stream --format "{{.MemUsage}}" 2>/dev/null | awk '{print $1}' | sed 's/[^0-9.]//g' || echo "0"
}

# Get Redis metrics
get_redis_metrics() {
    if ! container_exists "$REDIS"; then
        echo "0,0,0,0,0"
        return
    fi
    
    local info
    info=$(docker exec "$REDIS" redis-cli INFO stats 2>/dev/null || echo "")
    
    local ops=$(echo "$info" | grep "instantaneous_ops_per_sec:" | cut -d: -f2 | tr -d '\r' || echo "0")
    local mem=$(docker exec "$REDIS" redis-cli INFO memory 2>/dev/null | grep "used_memory_human:" | cut -d: -f2 | tr -d '\r' || echo "0")
    local clients=$(docker exec "$REDIS" redis-cli INFO clients 2>/dev/null | grep "connected_clients:" | cut -d: -f2 | tr -d '\r' || echo "0")
    local channels=$(docker exec "$REDIS" redis-cli INFO pubsub 2>/dev/null | grep "pubsub_channels:" | cut -d: -f2 | tr -d '\r' || echo "0")
    local memory_bytes=$(docker exec "$REDIS" redis-cli INFO memory 2>/dev/null | grep "used_memory:" | cut -d: -f2 | tr -d '\r' || echo "0")
    
    echo "${ops},${memory_bytes},${clients},${channels},${mem}"
}

# Get gateway connection count from /gateways endpoint
get_gateway_count() {
    curl -s --max-time 2 http://localhost:8080/gateways 2>/dev/null | \
        grep -o '"count":[0-9]*' | cut -d: -f2 || echo "0"
}

# Main collection loop
main() {
    log_info "Starting metrics collection for ${DURATION}s"
    log_info "Output: ${OUTPUT_FILE}"
    
    # Create CSV header
    echo "timestamp,redis_ops_sec,redis_memory_bytes,redis_clients,redis_channels,redis_memory_human,$(
        for i in "${!GATEWAYS[@]}"; do
            echo -n "gw$((i+1))_cpu,gw$((i+1))_mem_mb,"
        done
    )gateway_count" > "$OUTPUT_FILE"
    
    local start_time
    start_time=$(date +%s)
    local elapsed=0
    
    while [ $elapsed -lt "$DURATION" ]; do
        local timestamp
        timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
        
        # Redis metrics
        local redis_metrics
        redis_metrics=$(get_redis_metrics)
        
        # Gateway metrics
        local gw_metrics=""
        for gw in "${GATEWAYS[@]}"; do
            if container_exists "$gw"; then
                local cpu mem
                cpu=$(get_cpu_percent "$gw")
                mem=$(get_memory_mb "$gw")
                gw_metrics+="${cpu},${mem},"
            else
                gw_metrics+="0,0,"
            fi
        done
        
        # Gateway count
        local gw_count
        gw_count=$(get_gateway_count)
        
        # Write to CSV
        echo "${timestamp},${redis_metrics},${gw_metrics}${gw_count}" >> "$OUTPUT_FILE"
        
        sleep "$INTERVAL"
        elapsed=$(($(date +%s) - start_time))
        
        # Progress indicator
        printf "\r  Collecting... %ds / %ds" "$elapsed" "$DURATION"
    done
    
    echo ""
    log_info "Collection complete. Data saved to: ${OUTPUT_FILE}"
    
    # Print summary
    print_summary "$OUTPUT_FILE"
}

print_summary() {
    local file="$1"
    
    echo ""
    echo "╔═══════════════════════════════════════════════════════════╗"
    echo "║                  METRICS SUMMARY                        ║"
    echo "╚═══════════════════════════════════════════════════════════╝"
    
    # Skip header, calculate averages
    if [ -f "$file" ]; then
        local total_lines
        total_lines=$(tail -n +2 "$file" | wc -l)
        
        if [ "$total_lines" -gt 0 ]; then
            echo "Data points: $total_lines"
            echo ""
            
            # Redis average ops/sec
            local avg_ops
            avg_ops=$(tail -n +2 "$file" | cut -d, -f2 | awk '{sum+=$1; n++} END {if(n>0) print sum/n; else print 0}')
            echo "Redis avg ops/sec: $avg_ops"
            
            # Gateway CPU averages
            local gw_index=1
            while [ $gw_index -le 3 ]; do
                local cpu_col=$((gw_index * 2 + 1))
                local avg_cpu
                avg_cpu=$(tail -n +2 "$file" | cut -d, -f$cpu_col | awk '{sum+=$1; n++} END {if(n>0) print sum/n; else print 0}')
                echo "Gateway $gw_index avg CPU: ${avg_cpu}%"
                gw_index=$((gw_index + 1))
            done
        fi
    fi
}

# Run main
main
