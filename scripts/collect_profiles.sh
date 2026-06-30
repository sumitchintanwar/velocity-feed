#!/usr/bin/env bash
# collect_profiles.sh
# Automates the collection of Go pprof profiles and execution traces from the RTMDS Admin API.
# Usage: ./collect_profiles.sh <host:port> [duration_seconds]
# Environment Variables:
#   RTMDS_ADMIN_TOKEN  (required) Token for authentication

set -euo pipefail

HOST="${1:-localhost:9091}"
DURATION="${2:-30}"
TOKEN="${RTMDS_ADMIN_TOKEN:-}"

if [ -z "$TOKEN" ]; then
    echo "Usage: RTMDS_ADMIN_TOKEN=<token> $0 <host:port> [duration_seconds]"
    echo "Error: RTMDS_ADMIN_TOKEN environment variable is required to prevent credential leakage in shell history."
    exit 1
fi

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUT_DIR="profiles_${TIMESTAMP}"
mkdir -p "$OUT_DIR"

echo "Collecting profiles from $HOST for $DURATION seconds. Outputs will be saved to $OUT_DIR/"

# Function to download a profile
download_profile() {
    local profile_name=$1
    local query_params=$2
    local output_file="${OUT_DIR}/${profile_name}.pprof"
    
    echo "-> Starting collection: $profile_name"
    curl.exe -s -H "Authorization: Bearer ${TOKEN}" \
         "http://${HOST}/admin/diagnostics/debug/pprof/${profile_name}${query_params}" \
         -o "$output_file"
    echo "<- Finished collection: $profile_name (saved to $output_file)"
}

# Download immediate profiles in the background
download_profile "heap" "" &
download_profile "allocs" "" &
download_profile "goroutine" "" &
download_profile "mutex" "" &
download_profile "block" "" &
download_profile "threadcreate" "" &

# Wait for immediate profiles to finish before launching heavy time-based ones
wait

echo "Immediate profiles collected. Starting time-based profiles sequentially to avoid observer skew..."

# Download time-based profiles sequentially
# Running CPU profiling and execution tracing concurrently deeply skews the results of both.
download_profile "profile" "?seconds=${DURATION}"
download_profile "trace" "?seconds=${DURATION}"

echo "All profiles collected successfully in $OUT_DIR/"
echo "To analyze, use: go tool pprof $OUT_DIR/profile.pprof"
