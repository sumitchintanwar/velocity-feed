import os
import subprocess
import time
import json
import re
import threading
from concurrent.futures import ThreadPoolExecutor

WORKSPACE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
BENCHMARKS_DIR = os.path.join(WORKSPACE_DIR, "benchmarks")
RAW_RESULTS_DIR = os.path.join(BENCHMARKS_DIR, "raw-results")
PROFILES_DIR = os.path.join(BENCHMARKS_DIR, "profiles")
SCRIPTS_DIR = os.path.join(BENCHMARKS_DIR, "scripts")

os.makedirs(RAW_RESULTS_DIR, exist_ok=True)
os.makedirs(PROFILES_DIR, exist_ok=True)

stats_data = {
    "gateway1": {"cpu": [], "mem": []},
    "redis": {"cpu": [], "mem": []},
    "nginx": {"cpu": [], "mem": []}
}

stop_monitoring = False

def run_command(cmd, cwd=WORKSPACE_DIR, capture=False):
    print(f"Running: {cmd}")
    if capture:
        result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
        return result.stdout
    else:
        subprocess.run(cmd, shell=True, cwd=cwd)
        return None

def monitor_docker_stats():
    global stop_monitoring
    while not stop_monitoring:
        output = run_command("docker stats --no-stream --format \"{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}\"", capture=True)
        for line in output.strip().split("\n"):
            if not line: continue
            parts = line.split("|")
            if len(parts) != 3: continue
            name, cpu, mem = parts
            
            # Mem is like "12.3MiB / 512MiB", take just the number
            mem_match = re.search(r"([\d\.]+)MiB", mem)
            mem_val = float(mem_match.group(1)) if mem_match else 0.0
            
            # CPU is like "1.50%"
            cpu_val = float(cpu.replace("%", "")) if "%" in cpu else 0.0
            
            if "rtmds-bench-gw1" in name:
                stats_data["gateway1"]["cpu"].append(cpu_val)
                stats_data["gateway1"]["mem"].append(mem_val)
            elif "rtmds-bench-redis" in name:
                stats_data["redis"]["cpu"].append(cpu_val)
                stats_data["redis"]["mem"].append(mem_val)
            elif "rtmds-bench-nginx" in name:
                stats_data["nginx"]["cpu"].append(cpu_val)
                stats_data["nginx"]["mem"].append(mem_val)
                
        time.sleep(2)

def fetch_profiles():
    # Fetch profiles from Gateway1 via Nginx (assuming /admin is accessible via 8080/metrics -> Gateway1... wait, admin is not mapped in Nginx). 
    # Wait, nginx maps / to Gateway1. So /admin/... might be available directly.
    # Nginx config: location / { proxy_pass http://gateway1:9091; }
    # Let's hit the admin endpoint.
    base_url = "http://localhost:8080/admin/diagnostics/debug/pprof"
    headers = "Authorization: Bearer admin"
    
    print("Collecting CPU Profile...")
    run_command(f'curl -s -H "{headers}" {base_url}/profile?seconds=5 > {PROFILES_DIR}/cpu.prof')
    
    print("Collecting Heap Profile...")
    run_command(f'curl -s -H "{headers}" {base_url}/heap > {PROFILES_DIR}/heap.prof')
    
    print("Collecting Goroutine Profile...")
    run_command(f'curl -s -H "{headers}" {base_url}/goroutine?debug=1 > {PROFILES_DIR}/goroutines.txt')
    
    print("Collecting Mutex Profile...")
    run_command(f'curl -s -H "{headers}" {base_url}/mutex > {PROFILES_DIR}/mutex.prof')

def generate_report():
    print("Generating FINAL_BENCHMARKS.md...")
    import sys
    sys.path.insert(0, os.path.join(WORKSPACE_DIR, "benchmarks", "scripts"))
    import generate_final_report_v3
    # The generate report script uses the raw-results folder. 
    # Let's save the stats_data to raw-results so it can parse it.
    
    avg_stats = {}
    for service, metrics in stats_data.items():
        avg_stats[service] = {
            "avg_cpu_percent": sum(metrics["cpu"]) / len(metrics["cpu"]) if metrics["cpu"] else 0,
            "max_cpu_percent": max(metrics["cpu"]) if metrics["cpu"] else 0,
            "avg_mem_mb": sum(metrics["mem"]) / len(metrics["mem"]) if metrics["mem"] else 0,
            "max_mem_mb": max(metrics["mem"]) if metrics["mem"] else 0,
        }
    with open(os.path.join(RAW_RESULTS_DIR, "docker_stats.json"), "w") as f:
        json.dump(avg_stats, f, indent=2)

    # Let's call the generator
    # We will modify generate_final_report_v3.py later to pick up the new metrics.
    pass

def main():
    global stop_monitoring
    
    print("1. Cleaning up existing environment...")
    run_command("docker compose -f docker-compose.benchmark.yml down -v")
    
    print("2. Starting Benchmark Environment...")
    run_command("docker compose -f docker-compose.benchmark.yml up --build -d")
    
    # Wait for ready
    print("Waiting for Gateway to become healthy...")
    ready = False
    for _ in range(30):
        out = run_command("curl -s http://localhost:8080/health", capture=True)
        if out and "ok" in out:
            ready = True
            break
        time.sleep(2)
        
    if not ready:
        print("Failed to start Gateway. Exiting.")
        return
        
    print("Environment ready. Starting monitor thread.")
    monitor_thread = threading.Thread(target=monitor_docker_stats)
    monitor_thread.start()
    
    print("3. Running Macro Benchmarks...")
    clients = [100, 500, 1000]
    rampups = {100: "10s", 500: "30s", 1000: "60s"}
    # BF1: Use 30s measurement duration (was 10s) so stable data outlasts initial reconnects.
    durations = {100: "30s", 500: "30s", 1000: "30s"}
    # BF2: 30s inter-trial cooldown — lets disconnecting clients from the previous trial
    # fully drain before the next trial connects, preventing reconnect-storm contamination.
    inter_trial_cooldown = 30
    for c in clients:
        trials = []
        rampup = rampups[c]
        duration = durations[c]
        print(f"Running 3 trials for {c} clients (rampup: {rampup}, duration: {duration})...")
        for trial in range(1, 4):
            if trial > 1:
                print(f"  [Cooldown] Waiting {inter_trial_cooldown}s between trials for connections to drain...")
                time.sleep(inter_trial_cooldown)
            trial_file = os.path.join(RAW_RESULTS_DIR, f"bench_{c}_trial_{trial}.json")
            cmd = f"go run ./cmd/benchmark -url ws://localhost:8080/ws -clients {c} -duration {duration} -rampup {rampup} -output \"{trial_file}\""
            run_command(cmd)
            if os.path.exists(trial_file):
                try:
                    with open(trial_file, "r") as f:
                        trials.append(json.load(f))
                except Exception as e:
                    print(f"Error reading trial {trial} for {c} clients: {e}")
        if len(trials) > 0:
            tputs = [t["messages_per_sec"] for t in trials]
            means = [t["latency"]["mean_ms"] for t in trials]
            p50s = [t["latency"]["p50_ms"] for t in trials]
            p90s = [t["latency"]["p90_ms"] for t in trials]
            p95s = [t["latency"]["p95_ms"] for t in trials]
            p99s = [t["latency"]["p99_ms"] for t in trials]
            p999s = [t["latency"]["p999_ms"] for t in trials]
            maxs = [t["latency"]["max_ms"] for t in trials]
            import math
            def mean_std(lst):
                if not lst: return 0.0, 0.0
                avg = sum(lst) / len(lst)
                variance = sum((x - avg) ** 2 for x in lst) / len(lst)
                return avg, math.sqrt(variance)
            tput_avg, tput_std = mean_std(tputs)
            mean_avg, mean_std_val = mean_std(means)
            aggregated = {
                "timestamp": trials[0]["timestamp"],
                "config": trials[0]["config"],
                "total_messages": int(sum(t["total_messages"] for t in trials) / len(trials)),
                "total_bytes": int(sum(t["total_bytes"] for t in trials) / len(trials)),
                "duration": trials[0]["duration"],
                "messages_per_sec": tput_avg,
                "bytes_per_sec": sum(t["bytes_per_sec"] for t in trials) / len(trials),
                "connected_clients": trials[0]["connected_clients"],
                "peak_clients": max(t["peak_clients"] for t in trials),
                "failed_clients": int(sum(t["failed_clients"] for t in trials) / len(trials)),
                "reconnect_count": int(sum(t["reconnect_count"] for t in trials) / len(trials)),
                "retries": int(sum(t.get("retries", 0) for t in trials) / len(trials)),
                "dropped_messages": int(sum(t.get("dropped_messages", 0) for t in trials) / len(trials)),
                "failed_messages": int(sum(t.get("failed_messages", 0) for t in trials) / len(trials)),
                "latency": {
                    "mean_ms": mean_avg,
                    "median_ms": sum(p50s) / len(trials),
                    "min_ms": min(t["latency"]["min_ms"] for t in trials),
                    "max_ms": max(maxs),
                    "p50_ms": sum(p50s) / len(trials),
                    "p90_ms": sum(p90s) / len(trials),
                    "p95_ms": sum(p95s) / len(trials),
                    "p99_ms": sum(p99s) / len(trials),
                    "p999_ms": sum(p999s) / len(trials)
                },
                "stats": {
                    "throughput": {
                        "mean": tput_avg,
                        "stddev": tput_std,
                        "min": min(tputs),
                        "max": max(tputs)
                    },
                    "latency": {
                        "mean": mean_avg,
                        "stddev": mean_std_val,
                        "min": min(t["latency"]["min_ms"] for t in trials),
                        "max": max(maxs)
                    }
                },
                "trials": [
                    {
                        "trial": i+1,
                        "throughput": tputs[i],
                        "mean_latency": means[i],
                        "p50": p50s[i],
                        "reconnects": trials[i]["reconnect_count"]
                    } for i in range(len(trials))
                ]
            }
            outfile = os.path.join(RAW_RESULTS_DIR, f"bench_{c}.json")
            with open(outfile, "w") as f:
                json.dump(aggregated, f, indent=2)
        
    print("4. Collecting Pprof Traces...")
    fetch_profiles()
    
    stop_monitoring = True
    monitor_thread.join()
    
    print("5. Running Microbenchmarks...")
    micro_out = os.path.join(RAW_RESULTS_DIR, "full_micro.txt")
    run_command(f"go test -bench=\".\" -run=\"^$\" -benchmem -timeout=10m ./... > \"{micro_out}\"")
    
    print("6. Tearing down environment...")
    run_command("docker compose -f docker-compose.benchmark.yml down -v")
    
    # Write average stats
    avg_stats = {}
    for service, metrics in stats_data.items():
        avg_stats[service] = {
            "avg_cpu_percent": sum(metrics["cpu"]) / len(metrics["cpu"]) if metrics["cpu"] else 0,
            "max_cpu_percent": max(metrics["cpu"]) if metrics["cpu"] else 0,
            "avg_mem_mb": sum(metrics["mem"]) / len(metrics["mem"]) if metrics["mem"] else 0,
            "max_mem_mb": max(metrics["mem"]) if metrics["mem"] else 0,
        }
    with open(os.path.join(RAW_RESULTS_DIR, "docker_stats.json"), "w") as f:
        json.dump(avg_stats, f, indent=2)
        
    print("7. Generating FINAL_BENCHMARKS.md...")
    run_command("python benchmarks/scripts/generate_final_report_v4.py")
    print("Done!")

if __name__ == "__main__":
    main()
