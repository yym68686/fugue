package edge

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// This value is captured under the manager lock, before returning a reservation
// decision. Reading the manager again later would describe a different request.
type edgeBodyBudgetSnapshot struct {
	Mode           string    `json:"mode"`
	Budget         int64     `json:"budget_bytes"`
	Used           int64     `json:"used_bytes"`
	Active         int64     `json:"active_requests"`
	Requested      int64     `json:"requested_bytes"`
	Reserve        int64     `json:"reserve_bytes"`
	DiskRatio      float64   `json:"disk_ratio"`
	Available      *int64    `json:"available_bytes,omitempty"`
	SampledAt      time.Time `json:"sampled_at"`
	ParentFallback bool      `json:"parent_fallback"`
	PathErrno      string    `json:"path_errno,omitempty"`
	ParentErrno    string    `json:"parent_errno,omitempty"`
	Reason         string    `json:"reason"`
}

type edgeBodyBudgetError struct{ snapshot edgeBodyBudgetSnapshot }

func (e *edgeBodyBudgetError) Error() string {
	return fmt.Sprintf("request body buffer unavailable: reason=%s requested=%d used=%d budget=%d active=%d", e.snapshot.Reason, e.snapshot.Requested, e.snapshot.Used, e.snapshot.Budget, e.snapshot.Active)
}

func edgeBodyFilesystemErrno(err error) string {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return fmt.Sprint(int(errno))
	}
	if err != nil {
		return "unknown"
	}
	return ""
}

func sampleEdgeBodyBudget(path string, reserve int64, ratio float64) edgeBodyBudgetSnapshot {
	if reserve < 0 {
		reserve = 0
	}
	if ratio <= 0 || ratio > 1 {
		ratio = 0.25
	}
	s := edgeBodyBudgetSnapshot{Mode: "dynamic", Reserve: reserve, DiskRatio: ratio, SampledAt: time.Now().UTC(), Reason: "available"}
	available, err := filesystemAvailableBytes(path)
	if err != nil {
		s.PathErrno = edgeBodyFilesystemErrno(err)
		s.ParentFallback = true
		parent := filepath.Dir(strings.TrimSpace(path))
		if parent == "" || parent == "." {
			parent = "/"
		}
		available, err = filesystemAvailableBytes(parent)
		if err != nil {
			s.ParentErrno = edgeBodyFilesystemErrno(err)
			s.Reason = "filesystem_stat_failed"
			return s
		}
	}
	s.Available = &available
	if available <= reserve {
		s.Reason = "disk_reserve_threshold"
		return s
	}
	s.Budget = int64(float64(available-reserve) * ratio)
	if s.Budget <= 0 {
		s.Reason = "disk_reserve_threshold"
	}
	return s
}

func (m *edgeRequestBodyBufferManager) decisionLocked(requested int64) edgeBodyBudgetSnapshot {
	s := m.sample
	s.Budget = m.budget
	s.Used, s.Active, s.Requested = m.used, m.active, requested
	return s
}

func (m *edgeRequestBodyBufferManager) unavailableLocked(requested int64, reason string) error {
	s := m.decisionLocked(requested)
	s.Reason = reason
	return &edgeBodyBudgetError{snapshot: s}
}

type edgeBodyBufferWriteError struct{ err error }

func (e *edgeBodyBufferWriteError) Error() string { return e.err.Error() }
func (e *edgeBodyBufferWriteError) Unwrap() error { return e.err }

func (o *edgeProxyObservation) captureBodyBufferError(err error, reason string) {
	o.RequestBodyBufferError = err.Error()
	o.RequestBodyBufferReason = reason
	var budgetErr *edgeBodyBudgetError
	if errors.As(err, &budgetErr) {
		s := budgetErr.snapshot
		o.RequestBodyBudgetSnapshot = &s
		o.RequestBodyBufferReason = s.Reason
		o.RequestBodyBufferBudget, o.RequestBodyBufferUsed, o.RequestBodyBufferActive = s.Budget, s.Used, s.Active
	}
}

// Called only before reading any part of the request. A consumed POST cannot
// be replayed. Preserve the selected size boundary, even without a route policy.
func streamUnconsumedRequestBody(r *http.Request, o *edgeProxyObservation, maxBytes int64) {
	o.RequestBodyStreamFallback = true
	r.Body = &edgePolicyLimitedReadCloser{reader: r.Body, ctx: r.Context(), maxBytes: maxBytes}
}

func (m *edgeRequestBodyBufferManager) recordStreamFallback(reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fallbacks == nil {
		m.fallbacks = make(map[string]uint64)
	}
	m.fallbacks[reason]++
}

func (m *edgeRequestBodyBufferManager) writeMetrics(w io.Writer) {
	m.mu.Lock()
	m.refreshBudgetLocked()
	budget, used, active, sample := m.budget, m.used, m.active, m.sample
	fallbacks := make(map[string]uint64, len(m.fallbacks))
	for k, v := range m.fallbacks {
		fallbacks[k] = v
	}
	m.mu.Unlock()
	for _, metric := range []struct {
		name, help string
		value      int64
	}{
		{"budget_bytes", "Current request body disk buffer budget.", budget},
		{"used_bytes", "Bytes reserved by this worker for request body spooling.", used},
		{"active_requests", "Active disk buffer reservations in this worker.", active},
		{"reserve_bytes", "Disk bytes excluded from dynamic buffering.", sample.Reserve},
		{"statfs_available", "Whether dynamic disk availability was observed.", int64(boolGauge(sample.Available != nil))},
	} {
		fmt.Fprintf(w, "# HELP fugue_edge_body_buffer_%s %s\n# TYPE fugue_edge_body_buffer_%s gauge\nfugue_edge_body_buffer_%s %d\n", metric.name, metric.help, metric.name, metric.name, metric.value)
	}
	if sample.Available != nil {
		fmt.Fprintf(w, "# TYPE fugue_edge_body_buffer_available_bytes gauge\nfugue_edge_body_buffer_available_bytes %d\n", *sample.Available)
	}
	fmt.Fprintln(w, "# TYPE fugue_edge_body_buffer_stream_fallback_total counter")
	// Fixed vocabulary: no request, app, path or free-form error labels.
	for _, reason := range []string{"disk_reserve_threshold", "filesystem_stat_failed", "reservation_exceeds_budget", "budget_in_use", "directory_create_failed", "file_create_failed"} {
		fmt.Fprintf(w, "fugue_edge_body_buffer_stream_fallback_total{reason=%q} %d\n", reason, fallbacks[reason])
	}
}
