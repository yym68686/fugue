package api

import (
	"io"

	"fugue/internal/observability"
)

const (
	nodeUpdaterEdgeIdentityMatched = "matched"
	nodeUpdaterEdgeIdentityMissing = "missing"
	nodeUpdaterEdgeIdentityError   = "error"
)

func (s *Server) recordNodeUpdaterEdgeIdentityLookup(outcome string) {
	if s == nil {
		return
	}
	s.nodeUpdaterEdgeIdentityMu.Lock()
	if s.nodeUpdaterEdgeIdentityLookups == nil {
		s.nodeUpdaterEdgeIdentityLookups = map[string]uint64{
			nodeUpdaterEdgeIdentityMatched: 0,
			nodeUpdaterEdgeIdentityMissing: 0,
			nodeUpdaterEdgeIdentityError:   0,
		}
	}
	if _, ok := s.nodeUpdaterEdgeIdentityLookups[outcome]; ok {
		s.nodeUpdaterEdgeIdentityLookups[outcome]++
	}
	s.nodeUpdaterEdgeIdentityMu.Unlock()
}

func (s *Server) writeNodeUpdaterEdgeIdentityMetrics(w io.Writer) {
	counts := map[string]uint64{}
	if s != nil {
		s.nodeUpdaterEdgeIdentityMu.Lock()
		for outcome, count := range s.nodeUpdaterEdgeIdentityLookups {
			counts[outcome] = count
		}
		s.nodeUpdaterEdgeIdentityMu.Unlock()
	}
	observability.WriteMetricHeader(w, "fugue_node_updater_edge_identity_lookup_total", "Node updater edge credential lookups by exact edge identity result.", "counter")
	for _, outcome := range []string{
		nodeUpdaterEdgeIdentityMatched,
		nodeUpdaterEdgeIdentityMissing,
		nodeUpdaterEdgeIdentityError,
	} {
		observability.WriteMetricSample(w, "fugue_node_updater_edge_identity_lookup_total", map[string]string{"outcome": outcome}, float64(counts[outcome]))
	}
}
