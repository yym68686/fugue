package edgequality

import (
	"sort"
	"time"

	"fugue/internal/model"
)

func sortedNetworkValues(values []float64) []float64 {
	copy := append([]float64(nil), values...)
	sort.Float64s(copy)
	return copy
}

func comparisonCohorts(policy Policy, current, challenger []Observation) []string {
	if !IsContinuityNetworkPolicy(policy.Version) {
		return networkSharedCohorts(current, challenger)
	}
	rounds := map[string]string{}
	for _, observation := range current {
		if observation.ClientSource == "authenticated_client_probe" && observation.ClientProbeRoundID != "" && validNetworkCohort(observation.ClientCohort) {
			rounds[observation.ClientProbeRoundID] = observation.ClientCohort
		}
	}
	shared := map[string]bool{}
	for _, observation := range challenger {
		if observation.ClientSource == "authenticated_client_probe" && observation.ClientProbeRoundID != "" && validNetworkCohort(observation.ClientCohort) && rounds[observation.ClientProbeRoundID] == observation.ClientCohort {
			shared[observation.ClientCohort] = true
		}
	}
	cohorts := []string{}
	for cohort := range shared {
		cohorts = append(cohorts, cohort)
	}
	sort.Strings(cohorts)
	return cohorts
}

// V7 compares the same authenticated round from the same attested peer
// network. Passive connections and one-sided successful rounds cannot give an
// incumbent a sampling advantage. Service and capacity observations stay intact.
func pairedComparisonObservations(policy Policy, observations map[string][]Observation, current, challenger, cohort string) map[string][]Observation {
	if !IsContinuityNetworkPolicy(policy.Version) {
		return observations
	}
	rounds := map[string]bool{}
	for _, observation := range observations[current] {
		if observation.ClientSource == "authenticated_client_probe" && observation.ClientCohort == cohort && observation.ClientProbeRoundID != "" {
			rounds[observation.ClientProbeRoundID] = true
		}
	}
	common := map[string]bool{}
	for _, observation := range observations[challenger] {
		if observation.ClientSource == "authenticated_client_probe" && observation.ClientCohort == cohort && rounds[observation.ClientProbeRoundID] {
			common[observation.ClientProbeRoundID] = true
		}
	}
	result := map[string][]Observation{}
	for _, edgeID := range []string{current, challenger} {
		for _, observation := range observations[edgeID] {
			if observation.ClientSource != "" && (observation.ClientSource != "authenticated_client_probe" || observation.ClientCohort != cohort || !common[observation.ClientProbeRoundID]) {
				continue
			}
			result[edgeID] = append(result[edgeID], observation)
		}
	}
	return result
}

func originalQualityTime(selection *model.DNSPhysicalSelection) *time.Time {
	if selection == nil || model.ValidateDNSPhysicalSelection(selection) != nil || selection.BusinessAllocation != nil || selection.QualityState != "measured" && selection.QualityState != "retained" {
		return nil
	}
	if selection.QualityObservedAt != nil {
		at := *selection.QualityObservedAt
		return &at
	}
	// Historical measured receipts retain their actual original capture time.
	at := selection.CapturedAt
	return &at
}

func classifyContinuityQuality(snapshot Snapshot, result *Result) {
	result.QualityState = "learning"
	for _, comparison := range result.Comparisons {
		if comparison.Ready && comparison.Cohort != "" {
			result.QualityState = "measured"
			// Preserve the measurement time across captures. Merely reading the
			// same still-fresh observations must not renew the retention clock.
			latest := map[string]time.Time{}
			byEdge := map[string][]Observation{}
			for _, observation := range snapshot.Observations {
				byEdge[observation.EdgeID] = append(byEdge[observation.EdgeID], observation)
			}
			paired := pairedComparisonObservations(snapshot.Policy, byEdge, comparison.IncumbentEdgeID, comparison.EdgeID, comparison.Cohort)
			for edge, observations := range paired {
				for _, observation := range observations {
					if observation.ClientSource == "authenticated_client_probe" && observation.ObservedAt.After(latest[edge]) {
						latest[edge] = observation.ObservedAt
					}
				}
			}
			at := latest[comparison.EdgeID]
			if latest[comparison.IncumbentEdgeID].Before(at) {
				at = latest[comparison.IncumbentEdgeID]
			}
			if !at.IsZero() && (result.QualityObservedAt == nil || at.After(*result.QualityObservedAt)) {
				result.QualityObservedAt = &at
			}
		}
	}
	if result.QualityState == "measured" {
		return
	}
	if result.Hypothesis == "hold" && snapshot.PreviousSelection != nil && snapshot.PreviousSelection.PrimaryEdgeID == snapshot.CurrentEdgeID && snapshot.PreviousSelection.Scope == snapshot.Scope {
		at := originalQualityTime(snapshot.PreviousSelection)
		if at != nil && !at.After(snapshot.CapturedAt) && (snapshot.Policy.Version == LastValidPolicyVersion || snapshot.CapturedAt.Sub(*at) <= time.Duration(snapshot.Policy.RetainOrderSeconds)*time.Second) {
			result.QualityState, result.QualityObservedAt = "retained", at
			result.Blockers = append(result.Blockers, "comparable_client_evidence_stale_previous_order_retained")
			return
		}
	}
	result.Blockers = append(result.Blockers, "comparable_paired_client_evidence_required")
}
