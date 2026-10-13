package edgequality

import (
	"errors"
	"reflect"
	"sort"
	"time"

	"fugue/internal/model"
)

func MergeUnifiedBusinessInputs(inputs []BusinessExplorationInput) (BusinessExplorationInput, error) {
	if len(inputs) == 0 || inputs[0].Policy.Version != model.BusinessEdgeUnifiedVersion || len(inputs) != len(inputs[0].Policy.InitialDNSPublications) {
		return BusinessExplorationInput{}, errors.New("unified business decision requires every declared DNS predecessor")
	}
	merged := inputs[0]
	merged.Candidates, merged.NetworkSamples = []Candidate{}, []model.EdgeNetworkSample{}
	merged.InitialDNSBasisPoints = nil
	if merged.Previous == nil {
		merged.InitialDNSBasisPoints = map[string]map[string]int{}
	}
	candidates, samples := map[string]Candidate{}, map[string]model.EdgeNetworkSample{}
	for _, input := range inputs {
		if !reflect.DeepEqual(input.Policy, merged.Policy) || input.Hostname != merged.Hostname || input.PathPrefix != merged.PathPrefix || input.TrafficClass != merged.TrafficClass || !reflect.DeepEqual(input.Previous, merged.Previous) {
			return merged, errors.New("unified business inputs disagree on service, policy or serving predecessor; wait for convergence")
		}
		if input.CapturedAt.After(merged.CapturedAt) {
			merged.CapturedAt = input.CapturedAt
		}
		for nodeID, weights := range input.InitialDNSBasisPoints {
			if merged.Previous != nil {
				return merged, errors.New("initialized unified predecessor cannot carry migration initialization")
			}
			if _, present := merged.InitialDNSBasisPoints[nodeID]; present {
				return merged, errors.New("duplicate DNS migration predecessor")
			}
			merged.InitialDNSBasisPoints[nodeID] = weights
		}
		for _, candidate := range input.Candidates {
			previous, present := candidates[candidate.EdgeID]
			if present {
				if previous.EdgeGroupID != candidate.EdgeGroupID || previous.RouteGeneration != candidate.RouteGeneration || previous.RouteProofVerified != candidate.RouteProofVerified {
					return merged, errors.New("unified business candidate route identity changed across source captures")
				}
				if previous.ProofObservedAt == nil || candidate.ProofObservedAt == nil {
					candidate.ProofObservedAt = nil
				} else if previous.ProofObservedAt.Before(*candidate.ProofObservedAt) {
					candidate.ProofObservedAt = previous.ProofObservedAt
				}
				candidate.HardGates = append(append([]string(nil), candidate.HardGates...), previous.HardGates...)
				sort.Strings(candidate.HardGates)
				candidate.HardGates = uniqueBusinessStrings(candidate.HardGates)
			}
			candidates[candidate.EdgeID] = candidate
		}
		for _, sample := range input.NetworkSamples {
			if previous, exists := samples[sample.ID]; exists && !reflect.DeepEqual(previous, sample) {
				return merged, errors.New("conflicting raw business sample identity")
			}
			samples[sample.ID] = sample
		}
	}
	for _, input := range inputs {
		if merged.CapturedAt.Sub(input.CapturedAt) > time.Duration(merged.Policy.EvidenceMaxAgeSeconds)*time.Second {
			return merged, errors.New("unified business capture group exceeded freshness budget")
		}
		if len(input.Candidates) != len(merged.Policy.CandidateEdgeIDs) {
			return merged, errors.New("unified candidate set is incomplete")
		}
		seen := map[string]bool{}
		for _, candidate := range input.Candidates {
			if seen[candidate.EdgeID] {
				return merged, errors.New("duplicate unified candidate")
			}
			seen[candidate.EdgeID] = true
		}
		for _, edgeID := range merged.Policy.CandidateEdgeIDs {
			if !seen[edgeID] {
				return merged, errors.New("missing unified candidate")
			}
		}
	}
	for _, candidate := range candidates {
		merged.Candidates = append(merged.Candidates, candidate)
	}
	if len(merged.InitialDNSBasisPoints) == 0 {
		merged.InitialDNSBasisPoints = nil
	}
	for _, sample := range samples {
		merged.NetworkSamples = append(merged.NetworkSamples, sample)
	}
	sort.Slice(merged.Candidates, func(left, right int) bool { return merged.Candidates[left].EdgeID < merged.Candidates[right].EdgeID })
	sort.Slice(merged.NetworkSamples, func(left, right int) bool { return merged.NetworkSamples[left].ID < merged.NetworkSamples[right].ID })
	return merged, nil
}

func uniqueBusinessStrings(values []string) []string {
	result := []string{}
	for _, value := range values {
		if len(result) == 0 || result[len(result)-1] != value {
			result = append(result, value)
		}
	}
	return result
}
