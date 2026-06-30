import re

with open('cmd/benchmark/main.go', 'r', encoding='utf-8') as f:
    content = f.read()

pattern = r"""\t\t// Extract timestamp from message for latency measurement
\t\tvar msg struct \{
\t\t\tType    string `json:"type"`
\t\t\tPayload struct \{
\t\t\t\tTimestamp string `json:"timestamp"`
\t\t\t\} `json:"payload"`
\t\t\}
\t\tif err := json\.Unmarshal\(message, &msg\); err == nil && msg\.Payload\.Timestamp != "" \{
\t\t\tif ts, err := time\.Parse\(time\.RFC3339Nano, msg\.Payload\.Timestamp\); err == nil \{
\t\t\t\tlatencyMs := float64\(time\.Since\(ts\)\.Microseconds\(\)\) / 1000\.0
\t\t\t\tif latencyMs > 0 && latencyMs < 10000 \{ // filter outliers
\t\t\t\t\tlatencyHist\.Record\(latencyMs\)
\t\t\t\t\}
\t\t\t\}
\t\t\}"""

replacement = """\t\t// Extract timestamp (zero-allocation fast path)
\t\ttsStr := extractTimestampFast(message)
\t\tif tsStr != "" {
\t\t\tif ts, err := time.Parse(time.RFC3339Nano, tsStr); err == nil {
\t\t\t\tlatencyMs := float64(time.Since(ts).Microseconds()) / 1000.0
\t\t\t\tif latencyMs > 0 && latencyMs < 10000 { // filter outliers
\t\t\t\t\tlatencyHist.Record(latencyMs)
\t\t\t\t}
\t\t\t}
\t\t}"""

new_content = re.sub(pattern, replacement, content)
with open('cmd/benchmark/main.go', 'w', encoding='utf-8') as f:
    f.write(new_content)

print("Replaced:", content != new_content)
