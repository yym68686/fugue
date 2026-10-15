package dnsserver

import (
	"cmp"
	"reflect"
	"slices"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Only a verified successor with the same per-record route proofs and hard policy can
// supply fresh facts to the retained checkpoint. Per-record authorization and
// selection rules must also agree. Scores are observations: retaining the old
// score cannot introduce a new endpoint or prolong either artifact's authority.
func compatibleDNSReleaseProbes(old *dnsServingState, bridge *dnsReleaseBridge) map[string]bool {
	probes, records := dnsReleaseCompatibilityForProof(old, bridge, true)
	if old == nil || old.payload.Plan == nil {
		return probes
	}
	for _, record := range old.payload.Plan.Records {
		if records[record.Hostname] {
			continue
		}
		for _, target := range record.Targets {
			for _, identity := range target.ProbeIDs {
				delete(probes, identity)
			}
		}
	}
	return probes
}

func dnsReleaseCompatibility(old *dnsServingState, bridge *dnsReleaseBridge) (map[string]bool, map[string]bool) {
	return dnsReleaseCompatibilityForProof(old, bridge, false)
}

func dnsReleaseCompatibilityForProof(old *dnsServingState, bridge *dnsReleaseBridge, retainSelection bool) (map[string]bool, map[string]bool) {
	if bridge == nil || old == nil || old.payload.Plan == nil || bridge.payload.Plan == nil ||
		bridge.candidate.Release.ID == old.record.Candidate.Release.ID ||
		!bridge.candidate.Release.ReleasedAt.After(old.record.Candidate.Release.ReleasedAt) {
		return nil, nil
	}
	previous, next := old.payload, bridge.payload
	oldRules, newRules := dnsBridgeRules(previous.Policy.DNSAnswerRules), dnsBridgeRules(next.Policy.DNSAnswerRules)
	oldQueries, newQueries := dnsBridgeQueries(previous.Queries), dnsBridgeQueries(next.Queries)
	retainEquivalentDNSPhysicalOrder(oldQueries, newQueries, oldRules, newRules)
	if retainSelection {
		retainDNSSelectionComparison(oldQueries)
		retainDNSSelectionComparison(newQueries)
	}
	oldTraffic, newTraffic := dnsBridgeTrafficConstraints(previous), dnsBridgeTrafficConstraints(next)
	oldPlan, newPlan := previous.Plan, next.Plan
	previous.Plan, next.Plan = nil, nil
	previous.Generation, next.Generation = "", ""
	previous.Lineage, next.Lineage = platformconfig.Lineage{}, platformconfig.Lineage{}
	previous.Policy.Generation, next.Policy.Generation = "", ""
	previous.Policy.DNSAnswerRules, next.Policy.DNSAnswerRules = nil, nil
	previous.Policy.TrafficConstraints, next.Policy.TrafficConstraints = nil, nil
	previous.Queries, next.Queries = nil, nil
	if !reflect.DeepEqual(previous, next) {
		return nil, nil
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
		if next, exists := nextProbes[probe.ID]; exists && reflect.DeepEqual(probe, next) {
			allowed[probe.ID] = true
		}
	}
	records := make(map[string]bool, len(oldPlan.Records))
	for _, record := range oldPlan.Records {
		if next, exists := nextRecords[record.Hostname]; exists && reflect.DeepEqual(record, next) &&
			len(oldQueries[record.Hostname]) > 0 &&
			reflect.DeepEqual(oldQueries[record.Hostname], newQueries[record.Hostname]) &&
			reflect.DeepEqual(oldRules[record.Hostname], newRules[record.Hostname]) &&
			reflect.DeepEqual(oldTraffic[record.Hostname], newTraffic[record.Hostname]) {
			records[record.Hostname] = true
		}
	}
	return allowed, records
}

// The compiler can represent an unchanged verified order either as an explicit
// order or as a measured physical selection. Compare those equivalent forms
// without changing either serving payload. Business allocation, scopes, actual
// reordering, primary epochs and all other authorization fields stay exact.
func retainEquivalentDNSPhysicalOrder(oldQueries, newQueries map[string][]platformconfig.DNSQueryView, oldRules, newRules map[string][]platformconfig.DNSAnswerRule) {
	for host, before := range oldQueries {
		after := newQueries[host]
		if len(before) != len(after) {
			continue
		}
		orders := map[string]*model.DNSPhysicalOrder{}
		for index := range before {
			a, b := &before[index], &after[index]
			if a.NodeID != b.NodeID || a.Zone != b.Zone || a.EdgeGroupID != b.EdgeGroupID || len(a.Records) != 1 || len(b.Records) != 1 || a.Records[0].Type != b.Records[0].Type {
				continue
			}
			fixed, measured := &a.Records[0].AnswerPolicy, &b.Records[0].AnswerPolicy
			if measured.PolicyKind == model.DNSAnswerPolicyKindPhysicalOrder {
				fixed, measured = measured, fixed
			}
			if fixed.PolicyKind != model.DNSAnswerPolicyKindPhysicalOrder || fixed.PhysicalSelection != nil || model.ValidateDNSPhysicalOrder(fixed.PhysicalOrder) != nil ||
				measured.PolicyKind != model.DNSAnswerPolicyKindPhysicalQuality || measured.PhysicalOrder != nil || measured.PhysicalSelection == nil {
				continue
			}
			selection := measured.PhysicalSelection
			if selection.Version != model.DNSPhysicalSelectionVersion || selection.BusinessAllocation != nil || selection.Scope != "global" ||
				measured.RankingVersion != model.DNSPhysicalSelectionVersion || measured.RankingScope != "global" ||
				len(selection.OrderedEdgeIDs) == 0 || selection.PrimaryEdgeID != selection.OrderedEdgeIDs[0] ||
				!slices.Equal(fixed.PhysicalOrder.OrderedEdgeIDs, selection.OrderedEdgeIDs) || !reflect.DeepEqual(fixed.PhysicalOrder.PrimarySince, selection.PrimarySince) {
				continue
			}
			order := model.CloneDNSPhysicalOrder(fixed.PhysicalOrder)
			measured.PolicyKind, measured.PhysicalOrder, measured.PhysicalSelection = model.DNSAnswerPolicyKindPhysicalOrder, order, nil
			measured.RankingVersion, measured.RankingScope = fixed.RankingVersion, fixed.RankingScope
			// Reason describes how that identical order was observed, not who may
			// answer. Every endpoint and policy guard is still compared below.
			fixed.Reason, measured.Reason = "", ""
			orders[a.NodeID+"\x00"+a.Records[0].Type] = order
		}
		for _, rules := range []map[string][]platformconfig.DNSAnswerRule{oldRules, newRules} {
			for index := range rules[host] {
				rule := &rules[host][index]
				if order := orders[rule.NodeID+"\x00"+rule.Type]; order != nil && rule.SelectionMode == model.DNSAnswerPolicyKindPhysicalQuality && rule.PhysicalOrder == nil {
					rule.SelectionMode, rule.PhysicalOrder = model.DNSAnswerPolicyKindPhysicalOrder, model.CloneDNSPhysicalOrder(order)
				}
			}
		}
	}
}

func retainDNSSelectionComparison(queries map[string][]platformconfig.DNSQueryView) {
	for _, views := range queries {
		for _, view := range views {
			for index := range view.Records {
				selection := view.Records[index].AnswerPolicy.PhysicalSelection
				if selection == nil {
					continue
				}
				allocation := selection.BusinessAllocation
				if allocation != nil && (allocation.Policy.Version != model.BusinessEdgeUnifiedVersion || allocation.State.Unified == nil) {
					continue
				}
				selection.PrimaryEdgeID, selection.OrderedEdgeIDs, selection.PrimarySince = "", nil, nil
				if allocation == nil {
					continue
				}
				allocation.State.BasisPoints = nil
				allocation.State.LastAdjustedAt = time.Time{}
				allocation.State.LastEvidenceBucket = time.Time{}
				allocation.State.Unified = &model.BusinessUnifiedState{}
			}
		}
	}
}

// Application rollout constraints authorize only that application's records.
// A changed stable/candidate release must not invalidate an unrelated hostname's
// otherwise identical proof plan. Preserve exact tenant and application scope;
// shared policy fields and each record's route digests remain mandatory checks.
func dnsBridgeTrafficConstraints(payload dnsServingPayload) map[string][]platformconfig.TrafficPolicyConstraint {
	// App IDs are globally unique, including for legacy constraints without a
	// tenant field. Compare the entire constraint so tenant drift also rejects.
	byApp := map[string][]platformconfig.TrafficPolicyConstraint{}
	for _, constraint := range payload.Policy.TrafficConstraints {
		byApp[constraint.AppID] = append(byApp[constraint.AppID], constraint)
	}
	byHost := map[string][]platformconfig.TrafficPolicyConstraint{}
	seen := map[string]map[string]bool{}
	add := func(host, app string) {
		if app == "" {
			return
		}
		if seen[host] == nil {
			seen[host] = map[string]bool{}
		}
		if !seen[host][app] {
			seen[host][app] = true
			byHost[host] = append(byHost[host], byApp[app]...)
		}
	}
	for _, record := range payload.Records {
		add(record.Hostname, record.AppID)
	}
	for _, view := range payload.Queries {
		for _, record := range view.Records {
			add(record.Name, record.AppID)
		}
	}
	return byHost
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
			row.AnswerPolicy.PhysicalSelection = dnsBridgePhysicalSelection(record.AnswerPolicy.PhysicalSelection)
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

func dnsBridgePhysicalSelection(selection *model.DNSPhysicalSelection) *model.DNSPhysicalSelection {
	result := model.CloneDNSPhysicalSelection(selection)
	if result == nil {
		return nil
	}
	result.EvidenceDigest, result.DNSReceiptID, result.LoadedDigest, result.PolicyDigest = "", "", "", ""
	result.CapturedAt = time.Time{}
	if allocation := result.BusinessAllocation; allocation != nil {
		allocation.ReceiptDigest = ""
		allocation.ValidUntil = time.Time{}
		allocation.State.LastEvaluatedAt = time.Time{}
		allocation.State.EvidenceValidUntil = time.Time{}
		if allocation.State.Unified != nil {
			allocation.State.Unified.Sequence = 0
		}
	}
	return result
}

func dnsBridgeCandidates(in []model.EdgeDNSAnswerCandidate) []model.EdgeDNSAnswerCandidate {
	out := append([]model.EdgeDNSAnswerCandidate(nil), in...)
	for i := range out {
		out[i].Score, out[i].ScoreBreakdown, out[i].Reason = 0, nil, ""
	}
	// Ranking can permute the same authorized endpoints between snapshots.
	// Canonicalize only this comparison copy; the retained artifact keeps its
	// exact ordering, weights and query-selection behavior.
	slices.SortFunc(out, func(a, b model.EdgeDNSAnswerCandidate) int {
		if c := cmp.Compare(a.EdgeID, b.EdgeID); c != 0 {
			return c
		}
		if c := cmp.Compare(a.EdgeGroupID, b.EdgeGroupID); c != 0 {
			return c
		}
		return cmp.Compare(a.IP, b.IP)
	})
	return out
}
