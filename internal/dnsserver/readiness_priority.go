package dnsserver

import (
	"sort"
	"strings"

	"fugue/internal/dnsroutesource"
	"fugue/internal/platformconfig"
)

// Preferences only order existing requirements. They cannot select answers,
// change quorum, reuse proof authority or add an endpoint to the signed plan.
type dnsProbePriority struct {
	preferred, candidates, availability map[string]bool
	sole, alternateCover                map[string]bool
}

type dnsFamilyEdge struct{ host, family, edge string }

// Plan one complementary refresh per simple RRset. Record order can otherwise
// put the same hosts at the tail of *every* physical queue. This is a scheduling
// hint only, never an answer selector, a lease, or permission to omit a probe.
// Account for pinned records first, then distribute flexible work over their
// actual signed alternatives, with the signed order as a deterministic tie.
func dnsAlternateRefreshCover(payload dnsServingPayload, node string) map[dnsFamilyEdge]bool {
	cost := map[dnsFamilyEdge]int{}
	for _, r := range payload.Plan.Records {
		if r.MinimumHealthyEdges != 1 || r.RequireDualStack || r.MinDistinctCells > 1 || len(r.MinDistinctDomains) != 0 {
			continue
		}
		for _, target := range r.Targets {
			cost[dnsFamilyEdge{r.Hostname, target.Family, target.EdgeID}] += len(target.ProbeIDs)
		}
	}
	type choice struct {
		host, family, signature string
		edges                   []string
	}
	var choices []choice
	for _, view := range payload.Queries {
		if view.NodeID != node {
			continue
		}
		for _, r := range view.Records {
			if (r.Type != "A" && r.Type != "AAAA") || r.AnswerPolicy.Availability != nil {
				continue
			}
			var order []string
			if p := r.AnswerPolicy.PhysicalSelection; p != nil {
				order = p.OrderedEdgeIDs
			} else if p := r.AnswerPolicy.PhysicalOrder; p != nil {
				order = p.OrderedEdgeIDs
			}
			var edges []string
			for _, edge := range order {
				if cost[dnsFamilyEdge{r.Name, r.Type, edge}] > 0 {
					edges = append(edges, edge)
				}
			}
			if len(edges) > 0 {
				choices = append(choices, choice{r.Name, r.Type, strings.Join(edges, "\x00"), edges})
			}
		}
	}
	sort.Slice(choices, func(i, j int) bool {
		a, b := choices[i], choices[j]
		if len(a.edges) != len(b.edges) {
			return len(a.edges) < len(b.edges)
		}
		if a.host != b.host {
			return a.host < b.host
		}
		if a.family != b.family {
			return a.family < b.family
		}
		return a.signature < b.signature
	})
	load := map[string]int{}
	chosen, alternate := map[dnsFamilyEdge]bool{}, map[dnsFamilyEdge]bool{}
	for _, c := range choices {
		best := dnsFamilyEdge{c.host, c.family, c.edges[0]}
		for _, edge := range c.edges {
			key := dnsFamilyEdge{c.host, c.family, edge}
			if chosen[key] {
				best = key
				break
			}
			if load[edge]+cost[key] < load[best.edge]+cost[best] {
				best = key
			}
		}
		if chosen[best] {
			continue
		}
		chosen[best] = true
		load[best.edge] += cost[best]
		if len(c.edges) > 1 {
			alternate[best] = true
		}
	}
	return alternate
}

func dnsProbePriorities(payload dnsServingPayload, nodeID string, runtimePlan *platformconfig.DNSReadinessPlan) dnsProbePriority {
	out := dnsProbePriority{preferred: map[string]bool{}, candidates: map[string]bool{}, availability: map[string]bool{}, sole: map[string]bool{}, alternateCover: map[string]bool{}}
	if payload.Plan == nil {
		return out
	}
	type hostEdge struct{ host, edge string }
	sole := map[dnsFamilyEdge]bool{}
	cover := dnsAlternateRefreshCover(payload, nodeID)
	primaries, pool, availabilityPool := map[hostEdge]bool{}, map[hostEdge]bool{}, map[hostEdge]bool{}
	for _, view := range payload.Queries {
		if view.NodeID != nodeID {
			continue
		}
		for _, record := range view.Records {
			primary := ""
			var ordered []string
			policy := record.AnswerPolicy
			if selection := policy.PhysicalSelection; selection != nil {
				primary = selection.PrimaryEdgeID
				ordered = selection.OrderedEdgeIDs
			} else if order := policy.PhysicalOrder; order != nil && len(order.OrderedEdgeIDs) > 0 {
				primary = order.OrderedEdgeIDs[0]
				ordered = order.OrderedEdgeIDs
			}
			if primary != "" {
				primaries[hostEdge{record.Name, primary}] = true
			}
			if availability := policy.Availability; availability != nil {
				// With explicit availability, materialization only keeps
				// targets in its independently authorized recovery pool.
				ordered = availability.OrderedEdgeIDs
			} else if (record.Type == "A" || record.Type == "AAAA") && len(ordered) == 1 && ordered[0] != "" {
				// A fresh proof of another physical edge cannot materialize
				// this RRset, even when the generic route quorum is met.
				sole[dnsFamilyEdge{record.Name, record.Type, ordered[0]}] = true
			}
			for _, edgeID := range ordered {
				key := hostEdge{record.Name, edgeID}
				pool[key] = true
				if policy.Availability != nil {
					availabilityPool[key] = true
				}
			}
		}
	}
	probes := make(map[string]platformconfig.DNSReadinessProbe, len(payload.Plan.Probes))
	for _, probe := range payload.Plan.Probes {
		probes[probe.ID] = probe
	}
	type targetKey struct{ host, edge, group, address string }
	type targetPriority struct{ primary, candidate, availability, sole, alternate bool }
	targets := map[targetKey]targetPriority{}
	note := func(probe platformconfig.DNSReadinessProbe, priority targetPriority) {
		key := dnsroutesource.ProbeKey(probe)
		if priority.primary {
			out.preferred[key] = true
		}
		if priority.candidate {
			out.candidates[key] = true
		}
		if priority.availability {
			out.availability[key] = true
		}
		if priority.sole {
			out.sole[key] = true
		}
		if priority.alternate {
			out.alternateCover[key] = true
		}
	}
	// A verified runtime publication can use a different path than the
	// embedded baseline plan. Prioritize that actual probe only if its
	// physical identity remains part of the signed record's eligible set.
	for _, record := range payload.Plan.Records {
		for _, target := range record.Targets {
			binding := hostEdge{record.Hostname, target.EdgeID}
			priority := targetPriority{primaries[binding], pool[binding], availabilityPool[binding],
				sole[dnsFamilyEdge{record.Hostname, target.Family, target.EdgeID}], cover[dnsFamilyEdge{record.Hostname, target.Family, target.EdgeID}]}
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
					targets[key] = targetPriority{
						previous.primary || priority.primary,
						previous.candidate || priority.candidate,
						previous.availability || priority.availability,
						previous.sole || priority.sole,
						previous.alternate || priority.alternate,
					}
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
	return out
}
