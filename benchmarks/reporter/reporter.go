package reporter

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"
)

// Result holds the aggregated metrics from a benchmark run.
type Result struct {
	Name           string        `json:"name"`
	Duration       time.Duration `json:"duration"`
	Clients        int           `json:"clients"`
	MessagesSent   uint64        `json:"messages_sent"`
	MessagesRecv   uint64        `json:"messages_received"`
	Dropped        uint64        `json:"dropped"`
	Throughput     float64       `json:"throughput_msg_per_sec"`
	LatencyAvg     time.Duration `json:"latency_avg"`
	LatencyMedian  time.Duration `json:"latency_median"`
	LatencyP95     time.Duration `json:"latency_p95"`
	LatencyP99     time.Duration `json:"latency_p99"`
	LatencyMax     time.Duration `json:"latency_max"`
	CPUCores       float64       `json:"cpu_cores"`
	MemoryMB       float64       `json:"memory_mb"`
	NetworkBand_MB float64       `json:"network_bandwidth_mb_per_sec"`
	GoRoutines     int           `json:"go_routines"`
	GCPauses       float64       `json:"gc_pauses"`
}

// CalculatePercentiles takes a slice of latencies (modified in-place) and populates the Result.
func (r *Result) CalculatePercentiles(latencies []time.Duration) {
	if len(latencies) == 0 {
		return
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	
	var sum time.Duration
	for _, l := range latencies {
		sum += l
	}
	
	r.LatencyAvg = sum / time.Duration(len(latencies))
	r.LatencyMedian = latencies[len(latencies)/2]
	r.LatencyP95 = latencies[(len(latencies)*95)/100]
	r.LatencyP99 = latencies[(len(latencies)*99)/100]
	r.LatencyMax = latencies[len(latencies)-1]
}

// WriteJSON writes the result to a JSON file.
func (r *Result) WriteJSON(filename string) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}

// WriteCSV writes the result to a CSV file.
func (r *Result) WriteCSV(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	headers := []string{
		"Name", "Duration(s)", "Clients", "Msgs_Sent", "Msgs_Recv", "Dropped", "Throughput(msg/s)",
		"Lat_Avg(ms)", "Lat_Median(ms)", "Lat_P95(ms)", "Lat_P99(ms)", "Lat_Max(ms)",
		"CPU", "Mem(MB)", "Net(MB/s)", "Goroutines", "GCPauses(s)",
	}
	if err := writer.Write(headers); err != nil {
		return err
	}

	record := []string{
		r.Name,
		fmt.Sprintf("%.2f", r.Duration.Seconds()),
		fmt.Sprintf("%d", r.Clients),
		fmt.Sprintf("%d", r.MessagesSent),
		fmt.Sprintf("%d", r.MessagesRecv),
		fmt.Sprintf("%d", r.Dropped),
		fmt.Sprintf("%.2f", r.Throughput),
		fmt.Sprintf("%.2f", float64(r.LatencyAvg.Microseconds())/1000.0),
		fmt.Sprintf("%.2f", float64(r.LatencyMedian.Microseconds())/1000.0),
		fmt.Sprintf("%.2f", float64(r.LatencyP95.Microseconds())/1000.0),
		fmt.Sprintf("%.2f", float64(r.LatencyP99.Microseconds())/1000.0),
		fmt.Sprintf("%.2f", float64(r.LatencyMax.Microseconds())/1000.0),
		fmt.Sprintf("%.2f", r.CPUCores),
		fmt.Sprintf("%.2f", r.MemoryMB),
		fmt.Sprintf("%.2f", r.NetworkBand_MB),
		fmt.Sprintf("%d", r.GoRoutines),
		fmt.Sprintf("%.4f", r.GCPauses),
	}
	return writer.Write(record)
}

// WriteMarkdown writes the result to a Markdown table.
func (r *Result) WriteMarkdown(filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	md := fmt.Sprintf(`# Benchmark Report: %s

| Metric | Value |
|--------|-------|
| Duration | %.2fs |
| Clients | %d |
| Throughput | %.2f msg/s |
| Messages Sent | %d |
| Messages Received | %d |
| Dropped Messages | %d |
| Avg Latency | %.2f ms |
| Median Latency | %.2f ms |
| P95 Latency | %.2f ms |
| P99 Latency | %.2f ms |
| Max Latency | %.2f ms |
| CPU Cores | %.2f |
| Memory Usage | %.2f MB |
| Network Bandwidth | %.2f MB/s |
| Goroutines | %d |
| GC Pauses | %.4f s |
`, r.Name, r.Duration.Seconds(), r.Clients, r.Throughput, r.MessagesSent, r.MessagesRecv, r.Dropped,
		float64(r.LatencyAvg.Microseconds())/1000.0,
		float64(r.LatencyMedian.Microseconds())/1000.0,
		float64(r.LatencyP95.Microseconds())/1000.0,
		float64(r.LatencyP99.Microseconds())/1000.0,
		float64(r.LatencyMax.Microseconds())/1000.0,
		r.CPUCores, r.MemoryMB, r.NetworkBand_MB, r.GoRoutines, r.GCPauses)

	_, err = file.WriteString(md)
	return err
}
