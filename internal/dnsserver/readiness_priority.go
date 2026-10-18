package dnsserver

import (
	"fugue/internal/dnsroutesource"
	"fugue/internal/platformconfig"
)

// Preferences only order declared probes. They do not select answers, change
// quorum, reuse proof authority or add an endpoint to the signed plan.
func dnsPreferredProbeKeys(payload dnsServingPayload, nodeID string) map[string]bool {
	keys := map[string]bool{}
	if payload.Plan == nil {
		return keys
	}
	primaries := map[string]map[string]bool{}
	for _, view := range payload.Queries {
		if view.NodeID != nodeID {
			continue
		}
		for _, record := range view.Records {
			primary := ""
			if selection := record.AnswerPolicy.PhysicalSelection; selection != nil {
				primary = selection.PrimaryEdgeID
			} else if order := record.AnswerPolicy.PhysicalOrder; order != nil && len(order.OrderedEdgeIDs) > 0 {
				primary = order.OrderedEdgeIDs[0]
			}
			if primary != "" {
				if primaries[record.Name] == nil {
					primaries[record.Name] = map[string]bool{}
				}
				primaries[record.Name][primary] = true
			}
		}
	}
	probes := make(map[string]platformconfig.DNSReadinessProbe, len(payload.Plan.Probes))
	for _, probe := range payload.Plan.Probes {
		probes[probe.ID] = probe
	}
	for _, record := range payload.Plan.Records {
		for _, target := range record.Targets {
			if !primaries[record.Hostname][target.EdgeID] {
				continue
			}
			for _, id := range target.ProbeIDs {
				if probe, exists := probes[id]; exists && probe.EdgeID == target.EdgeID && probe.Address == target.Address {
					keys[dnsroutesource.ProbeKey(probe)] = true
				}
			}
		}
	}
	return keys
}
