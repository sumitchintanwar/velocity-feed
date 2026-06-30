package transport

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/sumit/rtmds/internal/eventlog"
)

// maxExportEvents is the hard limit on total events for a bulk export.
// Prevents abuse and excessive database load.
const maxExportEvents = 100000

// handleReplayExport returns an HTTP handler for bulk exporting historical
// market events. Streams results in pages to avoid loading entire datasets
// into memory. Supports CSV and JSON (newline-delimited) formats.
//
// Query parameters:
//
//	symbol  - filter by symbol (optional)
//	from    - start time, RFC3339 format (optional)
//	to      - end time, RFC3339 format (optional)
//	format  - "csv" (default) or "json" (newline-delimited JSON)
func handleReplayExport(repo eventlog.Repository, limiter *concurrencyLimiter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientIP := getClientIP(r)

		if !limiter.acquire(clientIP) {
			writeError(w, http.StatusTooManyRequests,
				"concurrent query limit exceeded (max "+strconv.Itoa(maxConcurrentQueriesPerClient)+" per client)")
			return
		}
		defer limiter.release(clientIP)

		// Parse query parameters.
		q := eventlog.ReplayQuery{
			Limit: 1000, // page size for streaming
		}
		q.Symbol = r.URL.Query().Get("symbol")

		if fromStr := r.URL.Query().Get("from"); fromStr != "" {
			t, err := time.Parse(time.RFC3339Nano, fromStr)
			if err != nil {
				writeError(w, http.StatusBadRequest, "invalid 'from' parameter: "+err.Error())
				return
			}
			q.From = t
		}

		if toStr := r.URL.Query().Get("to"); toStr != "" {
			t, err := time.Parse(time.RFC3339Nano, toStr)
			if err != nil {
				writeError(w, http.StatusBadRequest, "invalid 'to' parameter: "+err.Error())
				return
			}
			q.To = t
		}

		format := r.URL.Query().Get("format")
		if format == "" {
			format = "csv"
		}
		if format != "csv" && format != "json" {
			writeError(w, http.StatusBadRequest, "invalid 'format' parameter: must be 'csv' or 'json'")
			return
		}

		ctx := r.Context()
		totalExported := 0

		if format == "csv" {
			w.Header().Set("Content-Type", "text/csv; charset=utf-8")
			w.Header().Set("Content-Disposition", "attachment; filename=replay_export.csv")
			flusher, _ := w.(http.Flusher)

			writer := csv.NewWriter(w)
			// Write header.
			_ = writer.Write([]string{
				"event_id", "timestamp", "symbol", "event_type",
				"price", "bid", "ask", "volume", "exchange", "provider",
			})
			if flusher != nil {
				flusher.Flush()
			}

			for {
				if totalExported >= maxExportEvents {
					break
				}

				result, err := repo.QueryEvents(ctx, q)
				if err != nil {
					writeError(w, http.StatusInternalServerError, "query failed: "+err.Error())
					return
				}

				for _, e := range result.Events {
					_ = writer.Write([]string{
						strconv.FormatInt(e.EventID, 10),
						e.Timestamp.Format(time.RFC3339Nano),
						e.Symbol,
						e.EventType,
						formatFloat(e.Price),
						formatFloat(e.Bid),
						formatFloat(e.Ask),
						strconv.FormatInt(e.Volume, 10),
						e.Exchange,
						e.Provider,
					})
				}
				writer.Flush()
				if err := writer.Error(); err != nil {
					return
				}
				if flusher != nil {
					flusher.Flush()
				}

				totalExported += len(result.Events)

				if !result.HasMore || result.NextCursor == nil {
					break
				}
				q.Cursor = *result.NextCursor
			}
		} else {
			// Newline-delimited JSON.
			w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
			w.Header().Set("Content-Disposition", "attachment; filename=replay_export.json")
			encoder := json.NewEncoder(w)

			for {
				if totalExported >= maxExportEvents {
					break
				}

				result, err := repo.QueryEvents(ctx, q)
				if err != nil {
					writeError(w, http.StatusInternalServerError, "query failed: "+err.Error())
					return
				}

				for _, e := range result.Events {
					_ = encoder.Encode(e)
				}

				totalExported += len(result.Events)

				if !result.HasMore || result.NextCursor == nil {
					break
				}
				q.Cursor = *result.NextCursor
			}
		}
	}
}

// formatFloat formats a float64 as a string, returning empty string for zero.
func formatFloat(f float64) string {
	if f == 0 {
		return ""
	}
	return fmt.Sprintf("%g", f)
}
