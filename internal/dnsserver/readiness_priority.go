package dnsserver

import (
	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Preferences only order existing requirements. They cannot select answers,
// change quorum, reuse proof authority or add an endpoint to the signed plan.
func dnsProbePriorities(payload dnsServingPayload, nodeID string, runtimePlan *platformconfig.DNSReadinessPlan) (preferred, candidates map[string]bool) {
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
			selection := record.AnswerPolicy.PhysicalSelection
			if selection != nil {
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
			} else if record.AnswerPolicy.PolicyKind == model.DNSAnswerPolicyKindPhysicalQuality &&
				selection != nil && selection.Version == model.DNSPhysicalSelectionVersion {
				// The verified historical quality order is used by the same
				// serving selector as the fixed order. Keep its already signed
				// dependencies in the same bounded renewal queue. The business
				// selector has a separate admission model; leave it unchanged.
				ordered = selection.OrderedEdgeIDs
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
	type targetKey struct{ host, edge, group, address string }
	type targetPriority struct{ primary, candidate bool }
	targets := map[targetKey]targetPriority{}
	note := func(probe platformconfig.DNSReadinessProbe, priority targetPriority) {
		key := dnsroutesource.ProbeKey(probe)
		if priority.primary {
			preferred[key] = true
		}
		if priority.candidate {
			candidates[key] = true
		}
	}
	// A verified runtime publication can use a different path than the
	// embedded baseline plan. Prioritize that actual probe only if its
	// physical identity remains part of the signed record's eligible set.
	for _, record := range payload.Plan.Records {
		for _, target := range record.Targets {
			binding := hostEdge{record.Hostname, target.EdgeID}
			priority := targetPriority{primaries[binding], pool[binding]}
			if !priority.primary && !priority.candidate {
				continue
			}
			for _, id := range target.ProbeIDs {
				if probe, exists := probes[id]; exists && probe.EdgeID == target.EdgeID && probe.EdgeGroupID == target.EdgeGroupID && probe.Address == target.Address {
					if runtimePlan == nil {
						note(probe, priority)
						continue
					}
					key := targetKey{probe.Hostname, probe.EdgeID, probe.EdgeGroupID, probe.Address}
					// Multiple records can share this physical dependency.
					previous := targets[key]
					targets[key] = targetPriority{previous.primary || priority.primary, previous.candidate || priority.candidate}
				}
			}
		}
	}
	if runtimePlan != nil {
		for _, probe := range runtimePlan.Probes {
			if priority, ok := targets[targetKey{probe.Hostname, probe.EdgeID, probe.EdgeGroupID, probe.Address}]; ok {
				note(probe, priority)
			}
		}
	}
	return preferred, candidates
}
