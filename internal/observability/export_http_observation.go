package observability

import (
	"crypto/tls"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"
)

const (
	exportHTTPMetrics = iota
	exportHTTPLogs
	exportHTTPRequestFacts
	exportHTTPRequestSpans
	exportHTTPAppEvents
	exportHTTPRouteDecisions
	exportHTTPMissingLinks
	exportHTTPOther
	exportHTTPStageCount
)

var exportHTTPStageNames = [...]string{"metrics", "logs", "analytics_request_facts", "analytics_request_spans", "analytics_app_events", "analytics_edge_route_decisions", "analytics_edge_route_decision_missing_links", "analytics_other"}
var exportHTTPMilestoneNames = [...]string{"get_connection", "dns_start", "dns_done", "connect_start", "connect_done", "tls_start", "tls_done", "connection_ready", "request_written", "first_response_byte"}

func exportHTTPTableIndex(table string) int {
	switch table {
	case "request_facts":
		return exportHTTPRequestFacts
	case "request_spans":
		return exportHTTPRequestSpans
	case "app_events":
		return exportHTTPAppEvents
	case "edge_route_decisions":
		return exportHTTPRouteDecisions
	case "edge_route_decision_missing_links":
		return exportHTTPMissingLinks
	default:
		return exportHTTPOther
	}
}

// Fixed arrays and two receipts per fixed exporter purpose. No request, URL,
// hostname, address, headers, body, table argument or raw error is retained.
type exportHTTPStageCounters struct {
	started, completed, success, failed atomic.Uint64
	inflight                            atomic.Int64
	totalNanos, maxNanos                atomic.Uint64
	mu                                  sync.Mutex
	latest, latestFailed                *exportHTTPReceipt
}

type exportHTTPMilestone struct {
	Name         string `json:"name"`
	AfterStartNS int64  `json:"after_start_ns"`
}

type exportHTTPReceipt struct {
	StartedAt          time.Time             `json:"started_at"`
	FinishedAt         time.Time             `json:"finished_at"`
	DurationNS         int64                 `json:"duration_ns"`
	Outcome            string                `json:"outcome"`
	StatusCode         int                   `json:"status_code,omitempty"`
	ConnectionAttempts uint32                `json:"connection_attempts"`
	Connections        uint32                `json:"connections"`
	ReusedConnections  uint32                `json:"reused_connections"`
	WriteError         bool                  `json:"write_error"`
	Milestones         []exportHTTPMilestone `json:"milestones"`
}

type exportHTTPStageSnapshot struct {
	Purpose      string             `json:"purpose"`
	Started      uint64             `json:"started"`
	Completed    uint64             `json:"completed"`
	Inflight     int64              `json:"inflight"`
	Success      uint64             `json:"success"`
	Failed       uint64             `json:"failed"`
	TotalNanos   uint64             `json:"total_ns"`
	MaxNanos     uint64             `json:"max_ns"`
	Latest       *exportHTTPReceipt `json:"latest,omitempty"`
	LatestFailed *exportHTTPReceipt `json:"latest_failed,omitempty"`
}

func cloneExportHTTPReceipt(receipt *exportHTTPReceipt) *exportHTTPReceipt {
	if receipt == nil {
		return nil
	}
	copy := *receipt
	copy.Milestones = append([]exportHTTPMilestone(nil), receipt.Milestones...)
	return &copy
}

func (o *exportObservations) httpSnapshot() []exportHTTPStageSnapshot {
	rows := make([]exportHTTPStageSnapshot, exportHTTPStageCount)
	for i, purpose := range exportHTTPStageNames {
		c := &o.http[i]
		row := exportHTTPStageSnapshot{Purpose: purpose, Started: c.started.Load(), Completed: c.completed.Load(),
			Inflight: c.inflight.Load(), Success: c.success.Load(), Failed: c.failed.Load(), TotalNanos: c.totalNanos.Load(), MaxNanos: c.maxNanos.Load()}
		c.mu.Lock()
		row.Latest, row.LatestFailed = cloneExportHTTPReceipt(c.latest), cloneExportHTTPReceipt(c.latestFailed)
		c.mu.Unlock()
		rows[i] = row
	}
	return rows
}

// Instrument only the three exporters' existing Do calls, not ClickHouse
// queries. Do not read/replace/close the body, alter retry/timeout behavior or
// retain callback arguments. Existing trace hooks are composed by httptrace.
func observedExportHTTP(client *http.Client, req *http.Request, o *exportObservations, index int) (*http.Response, error) {
	if o == nil {
		return client.Do(req)
	}
	if index < 0 || index >= exportHTTPStageCount {
		index = exportHTTPOther
	}
	c := &o.http[index]
	c.started.Add(1)
	c.inflight.Add(1)
	started := time.Now()
	receipt := exportHTTPReceipt{StartedAt: started.UTC()}
	var mu sync.Mutex
	var seen [len(exportHTTPMilestoneNames)]bool
	var offsets [len(exportHTTPMilestoneNames)]int64
	finalized := false
	mark := func(index int, change func()) {
		mu.Lock()
		defer mu.Unlock()
		// net/http permits callbacks on other goroutines, even after Do has
		// returned. A completed receipt covers only callbacks observed by then.
		if finalized {
			return
		}
		if !seen[index] {
			seen[index], offsets[index] = true, time.Since(started).Nanoseconds()
		}
		if change != nil {
			change()
		}
	}
	trace := &httptrace.ClientTrace{
		GetConn: func(string) {
			mark(0, func() { receipt.ConnectionAttempts = min(receipt.ConnectionAttempts+1, uint32(65535)) })
		},
		DNSStart:          func(httptrace.DNSStartInfo) { mark(1, nil) },
		DNSDone:           func(httptrace.DNSDoneInfo) { mark(2, nil) },
		ConnectStart:      func(string, string) { mark(3, nil) },
		ConnectDone:       func(string, string, error) { mark(4, nil) },
		TLSHandshakeStart: func() { mark(5, nil) },
		TLSHandshakeDone:  func(tls.ConnectionState, error) { mark(6, nil) },
		GotConn: func(info httptrace.GotConnInfo) {
			mark(7, func() {
				receipt.Connections = min(receipt.Connections+1, uint32(65535))
				if info.Reused {
					receipt.ReusedConnections = min(receipt.ReusedConnections+1, uint32(65535))
				}
			})
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			mark(8, func() { receipt.WriteError = receipt.WriteError || info.Err != nil })
		},
		GotFirstResponseByte: func() { mark(9, nil) },
	}
	response, err := client.Do(req.WithContext(httptrace.WithClientTrace(req.Context(), trace)))
	mu.Lock()
	finalized = true
	receipt.FinishedAt = time.Now().UTC()
	receipt.DurationNS = time.Since(started).Nanoseconds()
	receipt.Outcome = "transport_error"
	if response != nil {
		receipt.StatusCode = response.StatusCode
	}
	if err == nil {
		receipt.Outcome = "http_error"
		if response != nil && response.StatusCode >= 200 && response.StatusCode < 300 {
			receipt.Outcome = "http_success"
		}
	}
	for i, name := range exportHTTPMilestoneNames {
		if seen[i] {
			receipt.Milestones = append(receipt.Milestones, exportHTTPMilestone{Name: name, AfterStartNS: offsets[i]})
		}
	}
	mu.Unlock()
	if receipt.Outcome == "http_success" {
		c.success.Add(1)
	} else {
		c.failed.Add(1)
	}
	nanos := uint64(receipt.DurationNS)
	c.totalNanos.Add(nanos)
	for previous := c.maxNanos.Load(); nanos > previous; previous = c.maxNanos.Load() {
		if c.maxNanos.CompareAndSwap(previous, nanos) {
			break
		}
	}
	c.mu.Lock()
	c.latest = &receipt
	if receipt.Outcome != "http_success" {
		c.latestFailed = &receipt
	}
	c.mu.Unlock()
	c.completed.Add(1)
	c.inflight.Add(-1)
	return response, err
}
