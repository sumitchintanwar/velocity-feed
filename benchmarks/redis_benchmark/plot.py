import json
import os
import matplotlib.pyplot as plt
import numpy as np
import sys

def plot_results(filename):
    if not os.path.exists(filename):
        print(f"File {filename} not found.")
        return

    with open(filename, 'r') as f:
        data = json.load(f)

    target_rate = data["target_rate"]
    
    # Latency Plot
    plt.figure(figsize=(10, 6))
    
    pub_lat = data.get("pub_latency_ms", [])
    sub_lat = data.get("sub_latency_ms", [])
    
    plt.hist(pub_lat, bins=50, alpha=0.5, label='Publish Latency', color='blue', edgecolor='black')
    plt.hist(sub_lat, bins=50, alpha=0.5, label='Subscribe Latency', color='red', edgecolor='black')
    
    plt.title(f'Redis Pub/Sub Latency at {target_rate} msg/sec')
    plt.xlabel('Latency (ms)')
    plt.ylabel('Frequency')
    plt.legend(loc='upper right')
    
    plt.savefig(f'redis_latency_{target_rate}.png')
    plt.close()
    print(f"Chart generated for {target_rate} msg/sec.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        plot_results(sys.argv[1])
    else:
        # Aggregated scaling plot
        rates = []
        achieved = []
        
        for file in sorted(os.listdir(".")):
            if file.startswith("redis_results_") and file.endswith(".json"):
                plot_results(file)
                with open(file, 'r') as f:
                    d = json.load(f)
                    rates.append(d["target_rate"])
                    achieved.append(d["throughput_msg_sec"])
        
        if rates:
            plt.figure(figsize=(10, 6))
            plt.plot(rates, achieved, marker='o', linestyle='-', color='g', label='Achieved Throughput')
            plt.plot(rates, rates, linestyle='--', color='gray', label='Target Throughput (Ideal)')
            plt.title('Redis Pub/Sub Scaling Behavior')
            plt.xlabel('Target Rate (msg/sec)')
            plt.ylabel('Achieved Throughput (msg/sec)')
            plt.xscale('log')
            plt.yscale('log')
            plt.legend()
            plt.grid(True, which="both", ls="--")
            plt.savefig('redis_scaling_throughput.png')
            plt.close()
            print("Scaling summary chart generated.")
