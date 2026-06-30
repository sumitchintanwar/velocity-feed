package validation

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/sumit/rtmds/internal/platform/chaos"
)

// LogValidator checks if a specific log signature was emitted during the experiment.
type LogValidator struct {
	ExpectedMessage string
	ExpectedLevel   string
	MockFound       bool // Keeping this mocked until Loki/Elasticsearch API is explicitly integrated
}

func (v *LogValidator) Name() string {
	return "LogValidator: " + v.ExpectedMessage
}

func (v *LogValidator) Assert(ctx context.Context) (chaos.ValidationResult, error) {
	if !v.MockFound {
		return chaos.ValidationResult{
			Success: false,
			Reason:  "Expected log signature was not found in the aggregator",
		}, nil
	}

	return chaos.ValidationResult{
		Success: true,
		Reason:  "Log signature successfully matched",
	}, nil
}

// MetricValidator queries Prometheus to assert that a system metric behaved correctly under chaos.
type MetricValidator struct {
	Query         string
	Condition     func(value float64) bool
	PrometheusURL string // Defaults to "http://localhost:9090"
	HTTPClient    *http.Client
}

func (v *MetricValidator) Name() string {
	return "MetricValidator: " + v.Query
}

func (v *MetricValidator) Assert(ctx context.Context) (chaos.ValidationResult, error) {
	promURL := v.PrometheusURL
	if promURL == "" {
		promURL = "http://localhost:9090"
	}

	client := v.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}

	reqURL := fmt.Sprintf("%s/api/v1/query?query=%s", promURL, url.QueryEscape(v.Query))
	
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return chaos.ValidationResult{Success: false}, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		// Might just mean prometheus is unavailable, let polling loop try again
		return chaos.ValidationResult{Success: false, Reason: "failed to connect to prometheus"}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return chaos.ValidationResult{Success: false, Reason: fmt.Sprintf("prometheus returned status: %d", resp.StatusCode)}, nil
	}

	var payload struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Value []interface{} `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return chaos.ValidationResult{Success: false, Reason: "failed to decode prometheus response"}, err
	}

	if len(payload.Data.Result) == 0 {
		return chaos.ValidationResult{Success: false, Reason: "no data returned from prometheus query"}, nil
	}

	// Extract the scalar value (Prometheus returns [timestamp, "value"])
	if len(payload.Data.Result[0].Value) < 2 {
		return chaos.ValidationResult{Success: false, Reason: "invalid value format from prometheus"}, nil
	}

	valStr, ok := payload.Data.Result[0].Value[1].(string)
	if !ok {
		return chaos.ValidationResult{Success: false, Reason: "value is not a string"}, nil
	}

	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return chaos.ValidationResult{Success: false, Reason: "failed to parse metric value"}, err
	}

	if !v.Condition(val) {
		return chaos.ValidationResult{
			Success: false,
			Reason:  fmt.Sprintf("Metric value %f did not satisfy the boundary condition", val),
		}, nil
	}

	return chaos.ValidationResult{
		Success: true,
		Reason:  "Metric boundary satisfied",
	}, nil
}
