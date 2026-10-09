package dnsserver

import (
	"fugue/internal/model"
	"sort"
	"strconv"
	"strings"
	"time"
)

func replayLegacyDNSCandidateOrder(record model.EdgeDNSRecord, hint dnsGeoHint, now time.Time, liveTLSProbeEnabled bool) ([]model.EdgeDNSAnswerCandidate, edgeDNSCandidateOrderDecision) {
	policy := record.AnswerPolicy
	sourceCandidates := record.Candidates
	decision := edgeDNSCandidateOrderDecision{
		Policy:                policy,
		ClientScopeResolution: edgeDNSClientScopeResolution(hint, false),
		MatchedScopeKey:       "global",
		SelectedEdgeGroupID:   strings.TrimSpace(policy.SelectedEdgeGroupID),
	}
	if scoped, ok := edgeDNSScopedCandidatesForHint(record.ScopedCandidates, hint); ok {
		sourceCandidates = scoped.Candidates
		if strings.TrimSpace(scoped.PolicyKind) != "" {
			policy.PolicyKind = scoped.PolicyKind
		}
		if strings.TrimSpace(scoped.Reason) != "" {
			policy.Reason = scoped.Reason
		}
		policy.SelectedEdgeGroupID = strings.TrimSpace(scoped.SelectedEdgeGroupID)
		decision.ClientScopeResolution = edgeDNSClientScopeResolution(hint, true)
		decision.MatchedScopeKey = firstNonEmpty(strings.TrimSpace(scoped.ScopeKey), "global")
		decision.ScopedProfileMatched = true
		decision.SelectedEdgeGroupID = strings.TrimSpace(scoped.SelectedEdgeGroupID)
	}
	decision.Policy = policy
	candidates := make([]model.EdgeDNSAnswerCandidate, 0, len(sourceCandidates))
	for _, candidate := range sourceCandidates {
		if !edgeDNSCandidateEligible(candidate, policy, liveTLSProbeEnabled) {
			continue
		}
		candidates = append(candidates, candidate)
	}
	hasLatencyScore := strings.TrimSpace(policy.PolicyKind) == model.DNSAnswerPolicyKindLatencyAware && edgeDNSAnyCandidateScore(candidates)
	sort.SliceStable(candidates, func(i, j int) bool {
		ai := edgeDNSCandidateSortScore(candidates[i], policy, hint, hasLatencyScore)
		aj := edgeDNSCandidateSortScore(candidates[j], policy, hint, hasLatencyScore)
		if ai != aj {
			return ai < aj
		}
		if candidates[i].Priority != candidates[j].Priority {
			return candidates[i].Priority < candidates[j].Priority
		}
		if candidates[i].Weight != candidates[j].Weight {
			return candidates[i].Weight > candidates[j].Weight
		}
		if candidates[i].EdgeGroupID != candidates[j].EdgeGroupID {
			return candidates[i].EdgeGroupID < candidates[j].EdgeGroupID
		}
		return candidates[i].IP < candidates[j].IP
	})
	if hint.decisionEntropy != nil {
		decision.Ranking = make([]DNSDecisionRank, 0, len(candidates))
		for _, candidate := range candidates {
			decision.Ranking = append(decision.Ranking, DNSDecisionRank{Candidate: candidate, SortScore: edgeDNSCandidateSortScore(candidate, policy, hint, hasLatencyScore)})
		}
	}
	if index := edgeDNSSelectedCandidateIndex(candidates, decision.SelectedEdgeGroupID); index >= 0 {
		decision.SelectedCandidateEligible = true
		if index > 0 {
			selected := candidates[index]
			copy(candidates[1:index+1], candidates[0:index])
			candidates[0] = selected
		}
		decision.SelectedCandidateKey = edgeDNSCandidateDecisionKey(candidates[0])
	}
	if promoted, ok := edgeDNSMaybePromoteNodeExploration(record, policy, hint, candidates, now); ok {
		decision.ExplorationKind = "same_group"
		if len(promoted) > 0 {
			decision.ExplorationCandidateKey = edgeDNSCandidateDecisionKey(promoted[0])
		}
		return promoted, decision
	}
	primaryEdgeGroupID := ""
	if len(candidates) > 0 {
		primaryEdgeGroupID = strings.TrimSpace(candidates[0].EdgeGroupID)
	}
	promoted := edgeDNSMaybePromoteExploration(record, policy, hint, candidates, now)
	if len(promoted) > 0 && !strings.EqualFold(strings.TrimSpace(promoted[0].EdgeGroupID), primaryEdgeGroupID) {
		decision.ExplorationKind = "cross_group"
		decision.ExplorationCandidateKey = edgeDNSCandidateDecisionKey(promoted[0])
	}
	return promoted, decision
}

func edgeDNSSelectedCandidateIndex(candidates []model.EdgeDNSAnswerCandidate, selectedEdgeGroupID string) int {
	selectedEdgeGroupID = strings.TrimSpace(selectedEdgeGroupID)
	if selectedEdgeGroupID == "" {
		return -1
	}
	for index, candidate := range candidates {
		if strings.EqualFold(strings.TrimSpace(candidate.EdgeGroupID), selectedEdgeGroupID) {
			return index
		}
	}
	return -1
}

func edgeDNSAnyCandidateScore(candidates []model.EdgeDNSAnswerCandidate) bool {
	for _, candidate := range candidates {
		if candidate.Score > 0 {
			return true
		}
	}
	return false
}

func edgeDNSScopedCandidatesForHint(scoped []model.EdgeDNSScopedAnswerCandidates, hint dnsGeoHint) (model.EdgeDNSScopedAnswerCandidates, bool) {
	bestScore := 0
	var best model.EdgeDNSScopedAnswerCandidates
	for _, candidate := range scoped {
		score := edgeDNSScopedCandidateMatchScore(candidate, hint)
		if score == 0 {
			continue
		}
		if score > bestScore || (score == bestScore && candidate.ScopeKey < best.ScopeKey) {
			bestScore = score
			best = candidate
		}
	}
	return best, bestScore > 0
}

func edgeDNSScopedCandidateMatchScore(candidate model.EdgeDNSScopedAnswerCandidates, hint dnsGeoHint) int {
	score := 0
	if hint.ASN != "" && candidate.ASN != "" && strings.EqualFold(candidate.ASN, hint.ASN) {
		score += 8000
	}
	if hint.Country != "" && candidate.Country != "" && strings.EqualFold(candidate.Country, hint.Country) {
		score += 4000
	}
	if hint.Region != "" && candidate.Region != "" && strings.EqualFold(candidate.Region, hint.Region) {
		score += 2000
	}
	if score == 0 {
		return 0
	}
	return score
}

func edgeDNSMaybePromoteExploration(record model.EdgeDNSRecord, policy model.DNSAnswerPolicy, hint dnsGeoHint, candidates []model.EdgeDNSAnswerCandidate, now time.Time) []model.EdgeDNSAnswerCandidate {
	if len(candidates) <= 1 {
		return candidates
	}
	seed, ok := edgeDNSExplorationSeed(record, policy, hint, now)
	if !ok {
		return candidates
	}
	percent := edgeDNSExplorationPercent(policy)
	if dnsDecisionBucketValue(hint, seed, 100, "exploration_percent") >= percent {
		return candidates
	}
	rest := len(candidates) - 1
	if rest <= 0 {
		return candidates
	}
	index := 1 + dnsDecisionBucketValue(hint, seed+"|cross-group", rest, "cross_group_index")
	if index <= 0 || index >= len(candidates) {
		return candidates
	}
	out := append([]model.EdgeDNSAnswerCandidate(nil), candidates...)
	explorer := out[index]
	copy(out[1:index+1], out[0:index])
	out[0] = explorer
	return out
}

func edgeDNSMaybePromoteNodeExploration(record model.EdgeDNSRecord, policy model.DNSAnswerPolicy, hint dnsGeoHint, candidates []model.EdgeDNSAnswerCandidate, now time.Time) ([]model.EdgeDNSAnswerCandidate, bool) {
	if len(candidates) <= 1 {
		return candidates, false
	}
	primaryGroupID := strings.TrimSpace(candidates[0].EdgeGroupID)
	if primaryGroupID == "" {
		return candidates, false
	}
	siblingIndexes := make([]int, 0, len(candidates)-1)
	for index := 1; index < len(candidates); index++ {
		if strings.EqualFold(strings.TrimSpace(candidates[index].EdgeGroupID), primaryGroupID) {
			siblingIndexes = append(siblingIndexes, index)
		}
	}
	if len(siblingIndexes) == 0 {
		return candidates, false
	}
	seed, ok := edgeDNSExplorationSeed(record, policy, hint, now)
	if !ok {
		return candidates, false
	}
	percent := edgeDNSExplorationPercent(policy)
	if dnsDecisionBucketValue(hint, seed, 100, "exploration_percent") >= percent {
		return candidates, false
	}
	index := siblingIndexes[dnsDecisionBucketValue(hint, seed+"|same-group", len(siblingIndexes), "same_group_index")]
	out := append([]model.EdgeDNSAnswerCandidate(nil), candidates...)
	explorer := out[index]
	copy(out[1:index+1], out[0:index])
	out[0] = explorer
	return out, true
}

func edgeDNSExplorationSeed(record model.EdgeDNSRecord, policy model.DNSAnswerPolicy, hint dnsGeoHint, now time.Time) (string, bool) {
	switch strings.TrimSpace(policy.PolicyKind) {
	case model.DNSAnswerPolicyKindDisabled, model.DNSAnswerPolicyKindPinned:
		return "", false
	}
	if edgeDNSExplorationPercent(policy) <= 0 {
		return "", false
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	bucket := now.Unix() / int64((10 * time.Minute).Seconds())
	scope := firstNonEmpty(hint.ASN, hint.Region, hint.Country, hint.EdgeGroupID, hint.IP, hint.Source, "global")
	seed := strings.Join([]string{
		normalizeName(record.Name),
		strings.ToUpper(strings.TrimSpace(record.Type)),
		scope,
		strconv.FormatInt(bucket, 10),
	}, "|")
	return seed, true
}

func edgeDNSExplorationPercent(policy model.DNSAnswerPolicy) int {
	percent := policy.ExplorationPercent
	if percent <= 0 {
		return 0
	}
	if percent > 50 {
		return 50
	}
	return percent
}

func edgeDNSCandidateSortScore(candidate model.EdgeDNSAnswerCandidate, policy model.DNSAnswerPolicy, hint dnsGeoHint, latencyScoreMode bool) int {
	score := candidate.Priority * 100
	switch policy.PolicyKind {
	case model.DNSAnswerPolicyKindLatencyAware:
		if latencyScoreMode {
			// Composite quality scores already include latency, error, body-read and cache health.
			// Keep route priority as a preference and weight as a tie-break, not the primary signal.
			score = 500000 + candidate.Priority*10 - candidate.Weight
			if candidate.Score > 0 {
				score = int(candidate.Score) + candidate.Priority*10 - candidate.Weight
			}
		} else {
			score = candidate.Priority*10 - candidate.Weight*20
		}
	case model.DNSAnswerPolicyKindWeighted:
		score -= candidate.Weight * 20
	}
	if policy.PolicyKind != model.DNSAnswerPolicyKindLatencyAware {
		if hint.EdgeGroupID != "" && strings.EqualFold(candidate.EdgeGroupID, hint.EdgeGroupID) {
			score -= 10000
		}
		if hint.Country != "" && strings.EqualFold(candidate.Country, hint.Country) {
			score -= 5000
		}
		if hint.Region != "" && strings.EqualFold(candidate.Region, hint.Region) {
			score -= 2500
		}
		if hint.ASN != "" && strings.Contains(strings.ToLower(candidate.Reason), "asn_"+strings.ToLower(hint.ASN)) {
			score -= 1250
		}
		if strings.EqualFold(candidate.Reason, "same_region") {
			score -= 250
		}
	}
	if candidate.Score > 0 && !(policy.PolicyKind == model.DNSAnswerPolicyKindLatencyAware && latencyScoreMode) {
		score += int(candidate.Score)
	}
	return score
}
