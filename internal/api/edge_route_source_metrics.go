package api

import (
	"io"
	"strings"

	"fugue/internal/observability"
)

const edgeControlRouteSourceV1 = "edge-control-group-authority/v1"

func normalizeEdgeRouteSourceMetric(raw string) string {
	switch strings.TrimSpace(raw) {
	case edgeControlRouteSourceV1:
		return "edge_control"
	case "core-api/v1", "core-api", "core_api":
		return "core_api"
	case "":
		return "unknown"
	default:
		return "other"
	}
}

func (s *Server) recordEdgeRouteSourceHeartbeat(raw string) {
	if s == nil {
		return
	}
	source := normalizeEdgeRouteSourceMetric(raw)
	s.edgeRouteSourceMu.Lock()
	if s.edgeRouteSourceHeartbeats == nil {
		s.edgeRouteSourceHeartbeats = map[string]uint64{"edge_control": 0, "core_api": 0, "unknown": 0, "other": 0}
	}
	s.edgeRouteSourceHeartbeats[source]++
	s.edgeRouteSourceMu.Unlock()
}

func (s *Server) writeEdgeRouteSourceMetrics(w io.Writer) {
	counts := map[string]uint64{}
	if s != nil {
		s.edgeRouteSourceMu.Lock()
		for source, count := range s.edgeRouteSourceHeartbeats {
			counts[source] = count
		}
		s.edgeRouteSourceMu.Unlock()
	}
	observability.WriteMetricHeader(w, "fugue_edge_route_source_heartbeats_total", "Authenticated edge heartbeats by route bundle source declaration.", "counter")
	for _, source := range []string{"edge_control", "core_api", "unknown", "other"} {
		observability.WriteMetricSample(w, "fugue_edge_route_source_heartbeats_total", map[string]string{"source": source}, float64(counts[source]))
	}
}
