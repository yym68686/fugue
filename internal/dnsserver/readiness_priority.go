package dnsserver

import (
	"fugue/internal/dnsroutesource"
	"fugue/internal/platformconfig"
)

// Preferences only order existing requirements. They cannot select answers,
// change quorum, reuse proof authority or add an endpoint to the signed plan.
func dnsProbePriorities(payload dnsServingPayload, nodeID string) (preferred, candidates map[string]bool) {
	preferred, candidates = map[string]bool{}, map[string]bool{}
	if payload.Plan == nil {
		return preferred, candidates
	}
	type hostEdge struct{ host, edge string }
	primaries, pool := map[hostEdge]bool{}, map[hostEdge]bool{}
	for _, view := range payload.Queries {
		if view.NodeID != nodeID {
			continue
		}
		for _, record := range view.Records {
			primary := ""
			order := record.AnswerPolicy.PhysicalOrder
			if selection := record.AnswerPolicy.PhysicalSelection; selection != nil {
				primary = selection.PrimaryEdgeID
			} else if order != nil && len(order.OrderedEdgeIDs) > 0 {
				primary = order.OrderedEdgeIDs[0]
			}
			if primary != "" {
				primaries[hostEdge{record.Name, primary}] = true
			}
			var ordered []string
			if policy := record.AnswerPolicy.Availability; policy != nil {
				ordered = policy.OrderedEdgeIDs
			} else if order != nil {
				ordered = order.OrderedEdgeIDs
			}
			for _, edgeID := range ordered {
				pool[hostEdge{record.Name, edgeID}] = true
			}
		}
	}
	probes := make(map[string]platformconfig.DNSReadinessProbe, len(payload.Plan.Probes))
	for _, probe := range payload.Plan.Probes {
		probes[probe.ID] = probe
	}
	for _, record := range payload.Plan.Records {
		for _, target := range record.Targets {
			binding := hostEdge{record.Hostname, target.EdgeID}
			if !primaries[binding] && !pool[binding] {
				continue
			}
			for _, id := range target.ProbeIDs {
				if probe, exists := probes[id]; exists && probe.EdgeID == target.EdgeID && probe.EdgeGroupID == target.EdgeGroupID && probe.Address == target.Address {
					key := dnsroutesource.ProbeKey(probe)
					if primaries[binding] {
						preferred[key] = true
					}
					if pool[binding] {
						candidates[key] = true
					}
				}
			}
		}
	}
	return preferred, candidates
}
