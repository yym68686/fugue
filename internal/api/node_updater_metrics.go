package api

import (
	"io"

	"fugue/internal/observability"
)

const (
	nodeUpdaterEdgeIdentityMatched  = "matched"
	nodeUpdaterEdgeIdentityMissing  = "missing"
	nodeUpdaterEdgeIdentityError    = "error"
	nodeUpdaterEdgeInventoryMatched = "matched"
	nodeUpdaterEdgeInventoryNoMatch = "no_match"
	nodeUpdaterEdgeInventoryError   = "error"
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

// recordNodeUpdaterEdgeInventoryFallback records use of the legacy complete
// edge inventory when a node updater request does not carry an explicit group.
// The bounded outcome labels make the migration decision observable without
// exposing node identities or request data.
func (s *Server) recordNodeUpdaterEdgeInventoryFallback(outcome string) {
	if s == nil {
		return
	}
	s.nodeUpdaterEdgeInventoryMu.Lock()
	if s.nodeUpdaterEdgeInventoryFallbacks == nil {
		s.nodeUpdaterEdgeInventoryFallbacks = map[string]uint64{
			nodeUpdaterEdgeInventoryMatched: 0,
			nodeUpdaterEdgeInventoryNoMatch: 0,
			nodeUpdaterEdgeInventoryError:   0,
		}
	}
	if _, ok := s.nodeUpdaterEdgeInventoryFallbacks[outcome]; ok {
		s.nodeUpdaterEdgeInventoryFallbacks[outcome]++
	}
	s.nodeUpdaterEdgeInventoryMu.Unlock()
}

func (s *Server) writeNodeUpdaterEdgeInventoryMetrics(w io.Writer) {
	counts := map[string]uint64{}
	if s != nil {
		s.nodeUpdaterEdgeInventoryMu.Lock()
		for outcome, count := range s.nodeUpdaterEdgeInventoryFallbacks {
			counts[outcome] = count
		}
		s.nodeUpdaterEdgeInventoryMu.Unlock()
	}
	observability.WriteMetricHeader(w, "fugue_node_updater_edge_inventory_fallback_total", "Node updater edge credential lookups that used the complete legacy edge inventory, by outcome.", "counter")
	for _, outcome := range []string{
		nodeUpdaterEdgeInventoryMatched,
		nodeUpdaterEdgeInventoryNoMatch,
		nodeUpdaterEdgeInventoryError,
	} {
		observability.WriteMetricSample(w, "fugue_node_updater_edge_inventory_fallback_total", map[string]string{"outcome": outcome}, float64(counts[outcome]))
	}
}
