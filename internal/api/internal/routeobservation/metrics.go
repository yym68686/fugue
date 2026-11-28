package routeobservation

import (
	"io"
	"strings"
	"sync"
	"sync/atomic"

	"fugue/internal/observability"
)

const (
	FallbackFencingNotReady = "fencing_not_ready"
	FallbackActiveEmpty     = "active_inventory_empty"
)

// Metrics owns the process counters. The fixed labels are part of the metrics
// contract; untrusted input can neither add series nor grow per-process state.
type Metrics struct {
	mu                  sync.Mutex
	heartbeats          [4]uint64
	inventoryFallbacks  [2]uint64
	publicationTimeouts atomic.Uint64
}

func (m *Metrics) RecordHeartbeat(raw string) {
	var index int
	switch strings.TrimSpace(raw) {
	case "edge-control-group-authority/v1":
		index = 0
	case "core-api/v1", "core-api", "core_api":
		index = 1
	case "":
		index = 2
	default:
		index = 3
	}
	m.mu.Lock()
	m.heartbeats[index]++
	m.mu.Unlock()
}

func (m *Metrics) RecordInventoryFallback(reason string) {
	var index int
	switch strings.TrimSpace(reason) {
	case FallbackFencingNotReady:
		index = 0
	case FallbackActiveEmpty:
		index = 1
	default:
		return
	}
	m.mu.Lock()
	m.inventoryFallbacks[index]++
	m.mu.Unlock()
}

func (m *Metrics) RecordPublicationTimeout() {
	m.publicationTimeouts.Add(1)
}

func (m *Metrics) WritePrometheus(w io.Writer) {
	var heartbeats [4]uint64
	var fallbacks [2]uint64
	if m != nil {
		observability.WriteMetricHeader(w, "fugue_traffic_publication_timeouts_total", "Traffic publications rolled back after their convergence deadline.", "counter")
		observability.WriteMetricSample(w, "fugue_traffic_publication_timeouts_total", nil, float64(m.publicationTimeouts.Load()))
		m.mu.Lock()
		heartbeats = m.heartbeats
		fallbacks = m.inventoryFallbacks
		m.mu.Unlock()
	}
	observability.WriteMetricHeader(w, "fugue_edge_route_source_heartbeats_total", "Authenticated edge heartbeats by route bundle source declaration.", "counter")
	for i, label := range [...]string{"edge_control", "core_api", "unknown", "other"} {
		observability.WriteMetricSample(w, "fugue_edge_route_source_heartbeats_total", map[string]string{"source": label}, float64(heartbeats[i]))
	}
	observability.WriteMetricHeader(w, "fugue_edge_route_inventory_legacy_fallback_total", "Route inventory selections that used the legacy-authoritative compatibility path.", "counter")
	for i, label := range [...]string{FallbackFencingNotReady, FallbackActiveEmpty} {
		observability.WriteMetricSample(w, "fugue_edge_route_inventory_legacy_fallback_total", map[string]string{"reason": label}, float64(fallbacks[i]))
	}
}
