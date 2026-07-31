package reporter

import (
	"bufio"
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// MetricSnapshot holds the scraped data for a moment in time.
type MetricSnapshot struct {
	CPUUsage   float64
	MemoryMB   float64
	Goroutines int
	GCPauses   float64
}

// ScrapeMetrics hits the target Prometheus /metrics endpoint and extracts relevant runtime gauges.
func ScrapeMetrics(ctx context.Context, targetURL string) (MetricSnapshot, error) {
	var snap MetricSnapshot
	req, err := http.NewRequestWithContext(ctx, "GET", targetURL, nil)
	if err != nil {
		return snap, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return snap, err
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		
		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			continue
		}
		
		key, valStr := parts[0], parts[1]
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}
		
		switch {
		case strings.HasPrefix(key, "go_goroutines"):
			snap.Goroutines = int(val)
		case strings.HasPrefix(key, "go_memstats_alloc_bytes"):
			snap.MemoryMB = val / 1024.0 / 1024.0
		case strings.HasPrefix(key, "process_cpu_seconds_total"):
			snap.CPUUsage = val // this requires diffing to get utilization
		case strings.HasPrefix(key, "go_gc_duration_seconds_sum"):
			snap.GCPauses = val // also requires diffing
		}
	}
	return snap, scanner.Err()
}

// PollMetrics runs in the background and aggregates metrics over a test run.
func PollMetrics(ctx context.Context, targetURL string, interval time.Duration) *Result {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var first, last MetricSnapshot
	var maxMem float64
	var maxG int
	firstScraped := false

	for {
		select {
		case <-ctx.Done():
			res := &Result{}
			if firstScraped {
				res.MemoryMB = maxMem
				res.GoRoutines = maxG
				res.CPUCores = (last.CPUUsage - first.CPUUsage) / (time.Since(time.Now()).Seconds() * -1.0)
				res.GCPauses = last.GCPauses - first.GCPauses
			}
			return res
		case <-ticker.C:
			snap, err := ScrapeMetrics(ctx, targetURL)
			if err == nil {
				if !firstScraped {
					first = snap
					firstScraped = true
				}
				last = snap
				if snap.MemoryMB > maxMem {
					maxMem = snap.MemoryMB
				}
				if snap.Goroutines > maxG {
					maxG = snap.Goroutines
				}
			}
		}
	}
}
