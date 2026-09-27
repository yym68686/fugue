package dnsserver

import (
	"reflect"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Only a verified successor with the same per-record route proofs and hard policy can
// supply fresh facts to the retained checkpoint. Per-record authorization and
// selection rules must also agree. Scores are observations: retaining the old
// score cannot introduce a new endpoint or prolong either artifact's authority.
func compatibleDNSReleaseProbes(old *dnsServingState, bridge *dnsReleaseBridge) map[string]bool {
	if bridge == nil || old == nil || old.payload.Plan == nil || bridge.payload.Plan == nil ||
		bridge.candidate.Release.ID == old.record.Candidate.Release.ID ||
		!bridge.candidate.Release.ReleasedAt.After(old.record.Candidate.Release.ReleasedAt) {
		return nil
	}
	previous, next := old.payload, bridge.payload
	oldRules, newRules := dnsBridgeRules(previous.Policy.DNSAnswerRules), dnsBridgeRules(next.Policy.DNSAnswerRules)
	oldQueries, newQueries := dnsBridgeQueries(previous.Queries), dnsBridgeQueries(next.Queries)
	oldPlan, newPlan := previous.Plan, next.Plan
	previous.Plan, next.Plan = nil, nil
	previous.Generation, next.Generation = "", ""
	previous.Lineage, next.Lineage = platformconfig.Lineage{}, platformconfig.Lineage{}
	previous.Policy.Generation, next.Policy.Generation = "", ""
	previous.Policy.DNSAnswerRules, next.Policy.DNSAnswerRules = nil, nil
	previous.Queries, next.Queries = nil, nil
	if !reflect.DeepEqual(previous, next) {
		return nil
	}
	nextProbes := make(map[string]platformconfig.DNSReadinessProbe, len(newPlan.Probes))
	for _, probe := range newPlan.Probes {
		nextProbes[probe.ID] = probe
	}
	nextRecords := make(map[string]platformconfig.DNSReadinessRecord, len(newPlan.Records))
	for _, record := range newPlan.Records {
		nextRecords[record.Hostname] = record
	}
	allowed := make(map[string]bool, len(oldPlan.Probes))
	for _, probe := range oldPlan.Probes {
		if next, exists := nextProbes[probe.ID]; exists && probe == next {
			allowed[probe.ID] = true
		}
	}
	for _, record := range oldPlan.Records {
		if next, exists := nextRecords[record.Hostname]; exists && reflect.DeepEqual(record, next) &&
			len(oldQueries[record.Hostname]) > 0 &&
			reflect.DeepEqual(oldQueries[record.Hostname], newQueries[record.Hostname]) &&
			reflect.DeepEqual(oldRules[record.Hostname], newRules[record.Hostname]) {
			continue
		}
		// A probe shared by records cannot silently carry a changed record
		// along with an unchanged one. Conservatively exclude that probe.
		for _, target := range record.Targets {
			for _, id := range target.ProbeIDs {
				delete(allowed, id)
			}
		}
	}
	return allowed
}

func dnsBridgeRules(rules []platformconfig.DNSAnswerRule) map[string][]platformconfig.DNSAnswerRule {
	byHost := make(map[string][]platformconfig.DNSAnswerRule)
	for _, rule := range rules {
		byHost[rule.Hostname] = append(byHost[rule.Hostname], rule)
	}
	return byHost
}

func dnsBridgeQueries(views []platformconfig.DNSQueryView) map[string][]platformconfig.DNSQueryView {
	byHost := make(map[string][]platformconfig.DNSQueryView)
	for _, view := range views {
		for _, record := range view.Records {
			row := record
			row.Candidates = dnsBridgeCandidates(record.Candidates)
			row.ScopedCandidates = append([]model.EdgeDNSScopedAnswerCandidates(nil), record.ScopedCandidates...)
			for i := range row.ScopedCandidates {
				row.ScopedCandidates[i].Candidates = dnsBridgeCandidates(record.ScopedCandidates[i].Candidates)
			}
			byHost[row.Name] = append(byHost[row.Name], platformconfig.DNSQueryView{NodeID: view.NodeID, EdgeGroupID: view.EdgeGroupID, Zone: view.Zone, Records: []model.EdgeDNSRecord{row}})
		}
	}
	return byHost
}

func dnsBridgeCandidates(in []model.EdgeDNSAnswerCandidate) []model.EdgeDNSAnswerCandidate {
	out := append([]model.EdgeDNSAnswerCandidate(nil), in...)
	for i := range out {
		out[i].Score, out[i].ScoreBreakdown, out[i].Reason = 0, nil, ""
	}
	return out
}
