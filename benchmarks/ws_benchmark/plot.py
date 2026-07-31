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

    clients = data["clients"]
    
    # 1. Latency Histogram
    plt.figure(figsize=(10, 6))
    plt.hist(data["receive_latency_ms"], bins=50, color='skyblue', edgecolor='black')
    plt.title(f'Receive Latency Distribution ({clients} clients)')
    plt.xlabel('Latency (ms)')
    plt.ylabel('Count')
    plt.savefig(f'receive_latency_{clients}.png')
    plt.close()

    # 2. Connect vs Reconnect Boxplot
    plt.figure(figsize=(8, 6))
    connects = data.get("connect_times_ms", [])
    reconnects = data.get("reconnect_times_ms", [])
    
    plot_data = []
    labels = []
    if connects:
        plot_data.append(connects)
        labels.append('Initial Connects')
    if reconnects:
        plot_data.append(reconnects)
        labels.append('Reconnects')
        
    if plot_data:
        plt.boxplot(plot_data, labels=labels)
        plt.title(f'Connection Times ({clients} clients)')
        plt.ylabel('Latency (ms)')
        plt.savefig(f'connect_times_{clients}.png')
    plt.close()
    
    print(f"Charts generated for {clients} clients.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        plot_results(sys.argv[1])
    else:
        for file in os.listdir("."):
            if file.startswith("ws_results_") and file.endswith(".json"):
                plot_results(file)
