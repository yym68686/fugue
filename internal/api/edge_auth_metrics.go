package api

import (
	"io"

	"fugue/internal/observability"
)

// recordEdgeAuthMethod records only the bounded authentication outcome. It
// deliberately omits token values, edge IDs, and request paths so the metric
// remains safe to expose from the process metrics endpoint.
func (s *Server) recordEdgeAuthMethod(method string) {
	if s == nil {
		return
	}
	s.edgeAuthMu.Lock()
	if s.edgeAuthMethods == nil {
		s.edgeAuthMethods = map[string]uint64{"scoped": 0, "legacy": 0, "denied": 0}
	}
	if _, ok := s.edgeAuthMethods[method]; ok {
		s.edgeAuthMethods[method]++
	}
	s.edgeAuthMu.Unlock()
}

func (s *Server) writeEdgeAuthMetrics(w io.Writer) {
	counts := map[string]uint64{}
	if s != nil {
		s.edgeAuthMu.Lock()
		for method, count := range s.edgeAuthMethods {
			counts[method] = count
		}
		s.edgeAuthMu.Unlock()
	}
	observability.WriteMetricHeader(w, "fugue_edge_authentications_total", "Edge authentication attempts by outcome method.", "counter")
	for _, method := range []string{"scoped", "legacy", "denied"} {
		observability.WriteMetricSample(w, "fugue_edge_authentications_total", map[string]string{"method": method}, float64(counts[method]))
	}
}
