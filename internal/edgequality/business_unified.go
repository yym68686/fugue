package edgequality

import (
	"errors"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
)

func evaluateUnifiedBusiness(input BusinessExplorationInput) (BusinessExplorationDecision, error) {
	policy := input.Policy
	digest, err := businessDigest(policy)
	if err != nil {
		return BusinessExplorationDecision{}, err
	}
	decision := BusinessExplorationDecision{Action: "hold", Reasons: []string{}, Windows: []BusinessExplorationWindow{}, Coverage: []BusinessExplorationCoverage{}, Comparisons: []BusinessExplorationComparison{}, Rejected: map[string]int{}, UsedSampleIDs: []string{}, ValidUntil: minBusinessTime(policy.ExpiresAt, input.CapturedAt.Add(time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second))}
	decision.State = BusinessExplorationState{IntentID: policy.IntentID, PolicyDigest: digest, BasisPoints: map[string]int{}, Stopped: map[string]string{}, LastAdjustedAt: input.CapturedAt, LastEvaluatedAt: input.CapturedAt, EvidenceValidUntil: decision.ValidUntil,
		Unified: &model.BusinessUnifiedState{PrimaryEdgeID: policy.BaselineEdgeID, TargetEdgeID: policy.BaselineEdgeID, PrimarySince: input.CapturedAt, Sequence: 1, QualityBasisPoints: map[string]int{}, ExplorationBasisPoints: map[string]int{}}}
	if previous := input.Previous; previous != nil {
		if len(input.InitialDNSBasisPoints) != 0 {
			return decision, errors.New("migration initialization cannot reset existing unified state")
		}
		allocation := &model.DNSBusinessAllocation{Policy: policy, State: *previous, ValidUntil: minBusinessTime(previous.EvidenceValidUntil, previous.LastEvaluatedAt.Add(time.Duration(policy.EvidenceMaxAgeSeconds)*time.Second)), ReceiptDigest: "sha256:" + strings.Repeat("0", 64)}
		for nodeID := range policy.InitialDNSPublications {
			allocation.ExecutionNodeID = nodeID
			break
		}
		if previous.LastEvaluatedAt.After(input.CapturedAt) || model.ValidateDNSBusinessAllocation(allocation, previous.LastEvaluatedAt) != nil {
			return decision, errors.New("unified business predecessor differs from policy, time or allocation invariants")
		}
		decision.State = model.CloneDNSBusinessAllocation(allocation).State
		decision.State.LastEvaluatedAt = input.CapturedAt
		decision.State.Unified.Sequence++
	}
	candidates := map[string]Candidate{}
	for _, candidate := range input.Candidates {
		if !slices.Contains(policy.CandidateEdgeIDs, candidate.EdgeID) || candidates[candidate.EdgeID].EdgeID != "" {
			return decision, errors.New("unified business candidate set differs from explicit intent")
		}
		candidates[candidate.EdgeID] = candidate
	}
	data, err := collectUnifiedBusinessWindows(input, candidates, &decision)
	if err != nil {
		return decision, err
	}
	state, unified := &decision.State, decision.State.Unified
	primary := unified.PrimaryEdgeID
	for _, edgeID := range policy.CandidateEdgeIDs {
		if !businessCandidateReady(candidates[edgeID], input.CapturedAt, policy.EvidenceMaxAgeSeconds) {
			state.Stopped[edgeID] = "candidate_not_route_ready"
		}
	}
	primaryDegraded := false
	for _, window := range data.windows {
		if window.MetricConnections["rtt"] >= policy.MinimumConnectionsPerBucket && window.RTTP90MS > policy.StopRTTMS ||
			window.MetricConnections["retransmission"] >= policy.MinimumConnectionsPerBucket && window.RetransmitP90 > policy.StopRetransmissionRate {
			if window.EdgeID == primary {
				primaryDegraded = true
			} else {
				state.Stopped[window.EdgeID] = "observed_transport_stop_threshold"
			}
		}
	}
	if state.Stopped[primary] != "" {
		state.BasisPoints = map[string]int{}
		decision.Action, decision.Reasons = "withhold", []string{"primary_unavailable_use_independent_verified_failover"}
		return decision, nil
	}
	active := policy.Mode != "paused" && !input.CapturedAt.Before(policy.StartsAt) && input.CapturedAt.Before(policy.ExpiresAt)
	unified.ExplorationBasisPoints = unifiedExplorationWeights(policy, state.Stopped, active)
	qualityBudget := 10000
	for _, weight := range unified.ExplorationBasisPoints {
		qualityBudget -= weight
	}
	if input.Previous == nil {
		unified.QualityBasisPoints = map[string]int{primary: qualityBudget}
		decision.Action = "initialize"
		if len(input.InitialDNSBasisPoints) != 0 {
			if len(input.InitialDNSBasisPoints) != len(policy.InitialDNSPublications) {
				return decision, errors.New("incomplete unified migration initialization")
			}
			unified.MigrationBasisPoints = map[string]map[string]int{}
			for nodeID, weights := range input.InitialDNSBasisPoints {
				if _, exists := policy.InitialDNSPublications[nodeID]; !exists {
					return decision, errors.New("undeclared unified migration executor")
				}
				if err := model.ValidateBusinessWeights(policy, weights); err != nil {
					return decision, err
				}
				unified.MigrationBasisPoints[nodeID] = maps.Clone(weights)
			}
		}
	}
	remaining := qualityBudget
	for edgeID, weight := range unified.QualityBasisPoints {
		if edgeID == primary || state.Stopped[edgeID] != "" {
			continue
		}
		remaining -= weight
	}
	for edgeID := range state.Stopped {
		delete(unified.QualityBasisPoints, edgeID)
	}
	unified.QualityBasisPoints[primary] = remaining
	if state.Stopped[unified.TargetEdgeID] != "" {
		unified.TargetEdgeID = primary
	}
	for _, weights := range unified.MigrationBasisPoints {
		for edgeID := range state.Stopped {
			weights[primary] += weights[edgeID]
			delete(weights, edgeID)
		}
	}
	ids := append([]string(nil), policy.CandidateEdgeIDs...)
	sort.Strings(ids)
	byEdge := map[string]BusinessExplorationComparison{}
	for _, edgeID := range ids {
		if edgeID == primary || state.Stopped[edgeID] != "" {
			continue
		}
		comparison := compareUnifiedBusiness(input, data, primary, edgeID)
		byEdge[edgeID] = comparison
		decision.Comparisons = append(decision.Comparisons, comparison)
	}
	if !active || policy.Mode == "observe" {
		unified.QualityBasisPoints, unified.TargetEdgeID = map[string]int{primary: qualityBudget}, primary
		if !active {
			decision.Action = "baseline_only"
			decision.Reasons = append(decision.Reasons, "intent_paused_or_outside_lease_preserve_current_primary")
		} else {
			decision.Reasons = append(decision.Reasons, "fixed_exploration_only")
		}
		finishUnifiedAllocation(input, &decision)
		return decision, nil
	}
	if unified.MigrationBasisPoints != nil {
		finishUnifiedAllocation(input, &decision)
		winner := unifiedBusinessWinner(input, data, decision.Comparisons)
		outperformed := false
		for _, comparison := range decision.Comparisons {
			outperformed = outperformed || comparison.Verdict == "better"
		}
		if outperformed {
			if winner != nil && winner.PromotionQualified && input.Previous != nil && input.CapturedAt.Sub(state.LastAdjustedAt) >= time.Duration(policy.CooldownSeconds)*time.Second {
				unified.PrimaryEdgeID, unified.TargetEdgeID, unified.PrimarySince = winner.EdgeID, winner.EdgeID, input.CapturedAt
				unified.QualityBasisPoints = map[string]int{winner.EdgeID: qualityBudget}
				state.LastAdjustedAt = input.CapturedAt
				state.EvidenceValidUntil = unifiedComparisonExpiry(input, *winner, data, primary)
				decision.Action = "promote"
				decision.Reasons = append(decision.Reasons, "qualified_migration_winner_promoted_preserve_executing_weights")
				finishUnifiedAllocation(input, &decision)
			} else {
				decision.Reasons = append(decision.Reasons, "migration_target_outperformed_wait_for_qualified_promotion")
			}
			return decision, nil
		}
		if primaryDegraded {
			decision.Reasons = append(decision.Reasons, "migration_target_transport_degraded")
		}
		if !primaryDegraded && input.Previous != nil && input.CapturedAt.Sub(state.LastAdjustedAt) >= time.Duration(policy.CooldownSeconds)*time.Second {
			migrateUnifiedAllocations(policy, decision.State.BasisPoints, unified.MigrationBasisPoints)
			state.LastAdjustedAt = input.CapturedAt
			decision.Action = "migrate"
		}
		converged := true
		for _, weights := range unified.MigrationBasisPoints {
			if !equalBusinessWeights(policy, weights, state.BasisPoints) {
				converged = false
			}
		}
		if converged {
			unified.MigrationBasisPoints = nil
		}
		decision.Reasons = append(decision.Reasons, "explicit_bounded_migration_to_shared_target")
		return decision, nil
	}
	adaptive := false
	fresh := true
	expiry := policy.ExpiresAt
	for edgeID, weight := range unified.QualityBasisPoints {
		if edgeID == primary || weight == 0 {
			continue
		}
		adaptive = true
		comparison := byEdge[edgeID]
		if comparison.Verdict == "insufficient" || comparison.Verdict == "" {
			fresh = false
		} else {
			expiry = minBusinessTime(expiry, unifiedComparisonExpiry(input, comparison, data, primary))
		}
	}
	if adaptive {
		if fresh {
			state.EvidenceValidUntil = expiry
		}
		if !input.CapturedAt.Before(state.EvidenceValidUntil) {
			unified.QualityBasisPoints, unified.TargetEdgeID = map[string]int{primary: qualityBudget}, primary
			decision.Action, decision.Reasons = "evidence_expired", []string{"adaptive_evidence_expired_preserve_promoted_primary"}
			state.EvidenceValidUntil = decision.ValidUntil
			finishUnifiedAllocation(input, &decision)
			return decision, nil
		}
		decision.ValidUntil = minBusinessTime(decision.ValidUntil, state.EvidenceValidUntil)
	}
	if input.Previous == nil || input.CapturedAt.Sub(state.LastAdjustedAt) < time.Duration(policy.CooldownSeconds)*time.Second {
		decision.Reasons = append(decision.Reasons, "cooldown")
		finishUnifiedAllocation(input, &decision)
		return decision, nil
	}
	latestBucket := time.Unix(input.CapturedAt.Unix()/int64(policy.BucketSeconds)*int64(policy.BucketSeconds)-int64(policy.BucketSeconds), 0).UTC()
	if !latestBucket.After(state.LastEvidenceBucket) {
		decision.Reasons = append(decision.Reasons, "evidence_bucket_already_consumed")
		finishUnifiedAllocation(input, &decision)
		return decision, nil
	}
	for _, comparison := range decision.Comparisons {
		if comparison.Verdict != "worse" || unified.QualityBasisPoints[comparison.EdgeID] == 0 {
			continue
		}
		amount := min(policy.MaximumStepBasisPoints, unified.QualityBasisPoints[comparison.EdgeID])
		unified.QualityBasisPoints[comparison.EdgeID] -= amount
		unified.QualityBasisPoints[primary] += amount
		unified.TargetEdgeID = primary
		decision.Action = "decrease"
		state.LastEvidenceBucket = latestBucket
		state.EvidenceValidUntil = unifiedComparisonExpiry(input, comparison, data, primary)
		decision.ValidUntil = minBusinessTime(decision.ValidUntil, state.EvidenceValidUntil)
		finishUnifiedAllocation(input, &decision)
		return decision, nil
	}
	winner := unifiedBusinessWinner(input, data, decision.Comparisons)
	if winner == nil {
		decision.Reasons = append(decision.Reasons, "no_unique_qualified_winner")
		finishUnifiedAllocation(input, &decision)
		return decision, nil
	}
	target := winner.EdgeID
	amount := min(policy.MaximumStepBasisPoints, qualityBudget-unified.QualityBasisPoints[target])
	if !winner.PromotionQualified {
		amount = min(amount, max(0, (unified.QualityBasisPoints[primary]-unified.QualityBasisPoints[target]-1)/2))
	}
	if amount == 0 {
		decision.Reasons = append(decision.Reasons, "promotion_evidence_required_or_target_reached")
		finishUnifiedAllocation(input, &decision)
		return decision, nil
	}
	unified.TargetEdgeID = target
	remainingStep := amount
	for _, donor := range ids {
		if donor == target || donor == primary {
			continue
		}
		transfer := min(remainingStep, unified.QualityBasisPoints[donor])
		unified.QualityBasisPoints[donor] -= transfer
		remainingStep -= transfer
	}
	unified.QualityBasisPoints[primary] -= remainingStep
	unified.QualityBasisPoints[target] += amount
	decision.Action = "increase"
	if winner.PromotionQualified && unified.QualityBasisPoints[target] > unified.QualityBasisPoints[primary] {
		unified.PrimaryEdgeID, unified.PrimarySince = target, input.CapturedAt
		decision.Action = "promote"
		decision.Reasons = append(decision.Reasons, "sustained_shared_cohort_winner_promoted")
	}
	state.LastEvidenceBucket = latestBucket
	state.EvidenceValidUntil = unifiedComparisonExpiry(input, *winner, data, primary)
	decision.ValidUntil = minBusinessTime(decision.ValidUntil, state.EvidenceValidUntil)
	finishUnifiedAllocation(input, &decision)
	return decision, nil
}

func unifiedBusinessWinner(input BusinessExplorationInput, data unifiedWindows, comparisons []BusinessExplorationComparison) *BusinessExplorationComparison {
	winners := []BusinessExplorationComparison{}
	for _, comparison := range comparisons {
		if comparison.Verdict == "better" {
			winners = append(winners, comparison)
		}
	}
	for index := range winners {
		candidate := &winners[index]
		best := true
		for _, other := range winners {
			if other.EdgeID != candidate.EdgeID && compareUnifiedBusiness(input, data, other.EdgeID, candidate.EdgeID).Verdict != "better" {
				best = false
			}
		}
		if best {
			return candidate
		}
	}
	return nil
}

func unifiedExplorationWeights(policy model.BusinessEdgeExplorationPolicy, stopped map[string]string, active bool) map[string]int {
	weights := map[string]int{}
	if !active || policy.ExplorationBasisPoints == 0 {
		return weights
	}
	eligible := []string{}
	for _, edgeID := range policy.CandidateEdgeIDs {
		if stopped[edgeID] == "" {
			eligible = append(eligible, edgeID)
		}
	}
	sort.Strings(eligible)
	for index, edgeID := range eligible {
		weights[edgeID] = policy.ExplorationBasisPoints / len(eligible)
		if index < policy.ExplorationBasisPoints%len(eligible) {
			weights[edgeID]++
		}
	}
	return weights
}

func finishUnifiedAllocation(input BusinessExplorationInput, decision *BusinessExplorationDecision) {
	state := &decision.State
	state.BasisPoints = maps.Clone(state.Unified.QualityBasisPoints)
	for edgeID, weight := range state.Unified.ExplorationBasisPoints {
		state.BasisPoints[edgeID] += weight
	}
	if input.Previous != nil && !reflect.DeepEqual(input.Previous.BasisPoints, state.BasisPoints) {
		state.LastAdjustedAt = input.CapturedAt
		if decision.Action == "hold" {
			decision.Action = "stop_candidates"
		}
	}
}

func equalBusinessWeights(policy model.BusinessEdgeExplorationPolicy, left, right map[string]int) bool {
	for _, edgeID := range policy.CandidateEdgeIDs {
		if left[edgeID] != right[edgeID] {
			return false
		}
	}
	return true
}

func migrateUnifiedAllocations(policy model.BusinessEdgeExplorationPolicy, target map[string]int, migrations map[string]map[string]int) {
	ids := append([]string(nil), policy.CandidateEdgeIDs...)
	sort.Strings(ids)
	for _, weights := range migrations {
		budget := policy.MaximumStepBasisPoints
		for _, recipient := range ids {
			need := max(0, target[recipient]-weights[recipient])
			for _, donor := range ids {
				amount := min(budget, need, max(0, weights[donor]-target[donor]))
				weights[donor] -= amount
				weights[recipient] += amount
				need -= amount
				budget -= amount
			}
		}
	}
}

func businessPercentile(values []float64, fraction float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sort.Float64s(values)
	return networkQuantile(values, fraction)
}
