package dnsserver

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"fugue/internal/model"
)

func businessAnswerTTL(record model.EdgeDNSRecord, now time.Time, ttl uint32) uint32 {
	selection := record.AnswerPolicy.PhysicalSelection
	if selection == nil || selection.Version != model.DNSPhysicalBusinessSelectionVersion || selection.BusinessAllocation == nil {
		return ttl
	}
	allocation := selection.BusinessAllocation
	if allocation.Policy.Mode == "paused" || now.Before(allocation.Policy.StartsAt) || !now.Before(allocation.ValidUntil) || !now.Before(allocation.Policy.ExpiresAt) {
		return ttl
	}
	deadline := allocation.ValidUntil
	if allocation.Policy.ExpiresAt.Before(deadline) {
		deadline = allocation.Policy.ExpiresAt
	}
	return min(ttl, uint32(deadline.Sub(now)/time.Second))
}

func businessEdgeOrderedCandidates(record model.EdgeDNSRecord, hint dnsGeoHint, now time.Time, liveTLSProbeEnabled bool) ([]model.EdgeDNSAnswerCandidate, edgeDNSCandidateOrderDecision) {
	selection := record.AnswerPolicy.PhysicalSelection
	decision := edgeDNSCandidateOrderDecision{Policy: record.AnswerPolicy, ClientScopeResolution: edgeDNSClientScopeResolution(hint, false), MatchedScopeKey: "global"}
	if model.ValidateDNSPhysicalSelection(selection) != nil || selection.Version != model.DNSPhysicalBusinessSelectionVersion ||
		selection.CapturedAt.After(now) || record.AnswerPolicy.PhysicalOrder != nil || record.AnswerPolicy.ECSEnabled ||
		len(record.ScopedCandidates) != 0 || record.AnswerPolicy.ExplorationPercent != 0 || record.AnswerPolicy.SelectedEdgeGroupID != "" ||
		record.AnswerPolicy.ShadowSelectedEdgeGroupID != "" || len(record.AnswerPolicy.PreferredEdgeGroups) != 0 || len(record.AnswerPolicy.FallbackEdgeGroups) != 0 || record.AnswerPolicy.Weight != 0 || record.AnswerPolicy.Priority != 0 {
		return nil, decision
	}
	allocation := selection.BusinessAllocation
	positions := map[string]int{}
	for index, edgeID := range selection.OrderedEdgeIDs {
		positions[edgeID] = index
	}
	eligible := []model.EdgeDNSAnswerCandidate{}
	for _, candidate := range record.Candidates {
		if _, included := positions[candidate.EdgeID]; !included || allocation.State.Stopped[candidate.EdgeID] != "" {
			continue
		}
		if candidate.EdgeID == selection.PrimaryEdgeID {
			decision.SelectedCandidateKey, decision.SelectedEdgeGroupID = edgeDNSCandidateDecisionKey(candidate), candidate.EdgeGroupID
		}
		if edgeDNSCandidateEligible(candidate, record.AnswerPolicy, liveTLSProbeEnabled) {
			eligible = append(eligible, candidate)
			if candidate.EdgeID == selection.PrimaryEdgeID {
				decision.SelectedCandidateEligible = true
			}
		}
	}
	sort.Slice(eligible, func(left, right int) bool {
		if positions[eligible[left].EdgeID] != positions[eligible[right].EdgeID] {
			return positions[eligible[left].EdgeID] < positions[eligible[right].EdgeID]
		}
		return eligible[left].IP < eligible[right].IP
	})
	if allocation.Policy.Mode != "paused" && !now.Before(allocation.Policy.StartsAt) && now.Before(allocation.Policy.ExpiresAt) && now.Before(allocation.ValidUntil) {
		seed := strings.Join([]string{selection.EvidenceDigest, allocation.Policy.IntentID, record.Name, record.Type, hint.IP, strconv.FormatInt(now.UnixNano(), 10)}, "\x00")
		bucket := dnsDecisionBucketValue(hint, seed, 10000, "business_allocation")
		weights := allocation.State.BasisPoints
		allocationKind := "business_allocation"
		if unified := allocation.State.Unified; unified != nil {
			if migration := unified.MigrationBasisPoints[allocation.ExecutionNodeID]; migration != nil {
				weights, allocationKind = migration, "business_migration"
			} else {
				explorationTotal := 0
				for _, weight := range unified.ExplorationBasisPoints {
					explorationTotal += weight
				}
				if bucket < explorationTotal {
					weights, allocationKind = unified.ExplorationBasisPoints, "business_exploration"
				} else {
					bucket -= explorationTotal
					weights, allocationKind = unified.QualityBasisPoints, "business_quality"
				}
			}
		}
		edges := append([]string(nil), allocation.Policy.CandidateEdgeIDs...)
		sort.Strings(edges)
		selected := ""
		for _, edgeID := range edges {
			weight := weights[edgeID]
			if bucket < weight {
				selected = edgeID
				break
			}
			bucket -= weight
		}
		if allocation.State.Unified != nil {
			for _, candidate := range eligible {
				if candidate.EdgeID == selected {
					decision.ExplorationKind = allocationKind
					decision.ExplorationCandidateKey = edgeDNSCandidateDecisionKey(candidate)
				}
			}
		}
		if selected != "" && selected != selection.PrimaryEdgeID {
			for index, candidate := range eligible {
				if candidate.EdgeID == selected {
					copy(eligible[1:index+1], eligible[:index])
					eligible[0] = candidate
					decision.ExplorationKind = allocationKind
					decision.ExplorationCandidateKey = edgeDNSCandidateDecisionKey(candidate)
					break
				}
			}
		}
	}
	if hint.decisionEntropy != nil {
		for index, candidate := range eligible {
			decision.Ranking = append(decision.Ranking, DNSDecisionRank{Candidate: candidate, SortScore: index})
		}
	}
	return eligible, decision
}
