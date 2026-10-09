package dnsserver

import (
	"sort"

	"fugue/internal/model"
)

func declaredPhysicalEdgeOrder(record model.EdgeDNSRecord, hint dnsGeoHint, liveTLSProbeEnabled bool) ([]model.EdgeDNSAnswerCandidate, edgeDNSCandidateOrderDecision) {
	policy := record.AnswerPolicy
	decision := edgeDNSCandidateOrderDecision{Policy: policy, ClientScopeResolution: edgeDNSClientScopeResolution(hint, false), MatchedScopeKey: "global"}
	if model.ValidateDNSPhysicalOrder(policy.PhysicalOrder) != nil || policy.PhysicalSelection != nil || policy.ECSEnabled ||
		len(record.ScopedCandidates) != 0 || policy.ExplorationPercent != 0 || policy.SelectedEdgeGroupID != "" ||
		policy.ShadowSelectedEdgeGroupID != "" || len(policy.PreferredEdgeGroups) != 0 || len(policy.FallbackEdgeGroups) != 0 || policy.Weight != 0 || policy.Priority != 0 {
		return nil, decision
	}
	order := policy.PhysicalOrder.OrderedEdgeIDs
	positions := make(map[string]int, len(order))
	for index, edgeID := range order {
		positions[edgeID] = index
	}
	candidates := []model.EdgeDNSAnswerCandidate{}
	for _, candidate := range record.Candidates {
		if _, included := positions[candidate.EdgeID]; !included {
			continue
		}
		if candidate.EdgeID == order[0] {
			decision.SelectedCandidateKey = edgeDNSCandidateDecisionKey(candidate)
			decision.SelectedEdgeGroupID = candidate.EdgeGroupID
		}
		if edgeDNSCandidateEligible(candidate, policy, liveTLSProbeEnabled) {
			candidates = append(candidates, candidate)
			if candidate.EdgeID == order[0] {
				decision.SelectedCandidateEligible = true
			}
		}
	}
	sort.Slice(candidates, func(left, right int) bool {
		if positions[candidates[left].EdgeID] != positions[candidates[right].EdgeID] {
			return positions[candidates[left].EdgeID] < positions[candidates[right].EdgeID]
		}
		return candidates[left].IP < candidates[right].IP
	})
	if hint.decisionEntropy != nil {
		for _, candidate := range candidates {
			decision.Ranking = append(decision.Ranking, DNSDecisionRank{Candidate: candidate, SortScore: positions[candidate.EdgeID]})
		}
	}
	return candidates, decision
}

func physicalEdgeOrderedCandidates(record model.EdgeDNSRecord, hint dnsGeoHint, liveTLSProbeEnabled bool) ([]model.EdgeDNSAnswerCandidate, edgeDNSCandidateOrderDecision) {
	policy := record.AnswerPolicy
	decision := edgeDNSCandidateOrderDecision{Policy: policy, ClientScopeResolution: edgeDNSClientScopeResolution(hint, false), MatchedScopeKey: "global"}
	selection := policy.PhysicalSelection
	if model.ValidateDNSPhysicalSelection(selection) != nil || selection.Scope != "global" || len(record.ScopedCandidates) != 0 || policy.ExplorationPercent != 0 {
		return nil, decision
	}
	positions := make(map[string]int, len(selection.OrderedEdgeIDs))
	for index, edgeID := range selection.OrderedEdgeIDs {
		positions[edgeID] = index
	}
	candidates := make([]model.EdgeDNSAnswerCandidate, 0, len(record.Candidates))
	for _, candidate := range record.Candidates {
		if _, included := positions[candidate.EdgeID]; !included {
			continue
		}
		if candidate.EdgeID == selection.PrimaryEdgeID {
			decision.SelectedCandidateKey = edgeDNSCandidateDecisionKey(candidate)
			decision.SelectedEdgeGroupID = candidate.EdgeGroupID
		}
		if edgeDNSCandidateEligible(candidate, policy, liveTLSProbeEnabled) {
			candidates = append(candidates, candidate)
			if candidate.EdgeID == selection.PrimaryEdgeID {
				decision.SelectedCandidateEligible = true
			}
		}
	}
	sort.Slice(candidates, func(left, right int) bool {
		if positions[candidates[left].EdgeID] != positions[candidates[right].EdgeID] {
			return positions[candidates[left].EdgeID] < positions[candidates[right].EdgeID]
		}
		return candidates[left].IP < candidates[right].IP
	})
	if hint.decisionEntropy != nil {
		decision.Ranking = make([]DNSDecisionRank, 0, len(candidates))
		for _, candidate := range candidates {
			decision.Ranking = append(decision.Ranking, DNSDecisionRank{Candidate: candidate, SortScore: positions[candidate.EdgeID]})
		}
	}
	return candidates, decision
}
