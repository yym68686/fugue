package model

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"math"
	"slices"
	"time"
)

type BusinessUnifiedPolicy struct {
	MinimumConnectionsPerCohort         int     `json:"minimum_connections_per_cohort"`
	RTTAdvantageMS                      float64 `json:"rtt_advantage_ms"`
	PromotionMinimumConnections         int     `json:"promotion_minimum_connections"`
	PromotionMinimumCoverageBasisPoints int     `json:"promotion_minimum_coverage_basis_points"`
}

func (policy *BusinessUnifiedPolicy) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 4 {
		return errors.New("unified business policy requires all explicit thresholds")
	}
	for _, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return errors.New("unified thresholds cannot be null")
		}
	}
	type plain BusinessUnifiedPolicy
	var decoded plain
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		return err
	}
	*policy = BusinessUnifiedPolicy(decoded)
	return nil
}

type BusinessUnifiedState struct {
	MigrationBasisPoints   map[string]map[string]int `json:"migration_basis_points,omitempty"`
	PrimaryEdgeID          string                    `json:"primary_edge_id"`
	TargetEdgeID           string                    `json:"target_edge_id"`
	PrimarySince           time.Time                 `json:"primary_since"`
	Sequence               int64                     `json:"sequence"`
	QualityBasisPoints     map[string]int            `json:"quality_basis_points"`
	ExplorationBasisPoints map[string]int            `json:"exploration_basis_points"`
}

func CloneBusinessUnifiedState(state *BusinessUnifiedState) *BusinessUnifiedState {
	if state == nil {
		return nil
	}
	cloned := *state
	cloned.QualityBasisPoints = maps.Clone(state.QualityBasisPoints)
	cloned.ExplorationBasisPoints = maps.Clone(state.ExplorationBasisPoints)
	if state.MigrationBasisPoints != nil {
		cloned.MigrationBasisPoints = map[string]map[string]int{}
		for nodeID, weights := range state.MigrationBasisPoints {
			cloned.MigrationBasisPoints[nodeID] = maps.Clone(weights)
		}
	}
	return &cloned
}

func ValidateBusinessUnifiedPolicy(policy BusinessEdgeExplorationPolicy) error {
	value := policy.Unified
	if value == nil || policy.MaximumCandidateBasisPoints != 10000 ||
		value.MinimumConnectionsPerCohort < 3 || value.MinimumConnectionsPerCohort > 10000 ||
		value.PromotionMinimumConnections < value.MinimumConnectionsPerCohort || value.PromotionMinimumConnections > 10000 ||
		value.PromotionMinimumCoverageBasisPoints < policy.MinimumCohortCoverageBasisPoints || value.PromotionMinimumCoverageBasisPoints > 10000 ||
		value.RTTAdvantageMS <= 0 || value.RTTAdvantageMS > 60000 || math.IsNaN(value.RTTAdvantageMS) || math.IsInf(value.RTTAdvantageMS, 0) ||
		policy.ExplorationBasisPoints > 0 && policy.ExplorationBasisPoints < len(policy.CandidateEdgeIDs) {
		return errors.New("invalid unified business promotion, exploration or evidence policy")
	}
	return nil
}

func ValidateBusinessUnifiedState(policy BusinessEdgeExplorationPolicy, state BusinessEdgeExplorationState, capturedAt, validUntil time.Time) error {
	unified := state.Unified
	if unified == nil || unified.Sequence < 1 || unified.Sequence == math.MaxInt64 ||
		!slices.Contains(policy.CandidateEdgeIDs, unified.PrimaryEdgeID) || !slices.Contains(policy.CandidateEdgeIDs, unified.TargetEdgeID) ||
		unified.PrimarySince.IsZero() || unified.PrimarySince.After(capturedAt) || state.Stopped[unified.PrimaryEdgeID] != "" || state.Stopped[unified.TargetEdgeID] != "" {
		return errors.New("invalid unified business primary, target or decision sequence")
	}
	totalQuality, totalExploration := 0, 0
	for _, weights := range []map[string]int{unified.QualityBasisPoints, unified.ExplorationBasisPoints, state.BasisPoints} {
		for edgeID, weight := range weights {
			if !slices.Contains(policy.CandidateEdgeIDs, edgeID) || weight < 0 || weight > 10000 || state.Stopped[edgeID] != "" && weight != 0 {
				return errors.New("unified allocation contains invalid or stopped candidate weight")
			}
		}
	}
	for _, edgeID := range policy.CandidateEdgeIDs {
		quality, exploration := unified.QualityBasisPoints[edgeID], unified.ExplorationBasisPoints[edgeID]
		if state.BasisPoints[edgeID] != quality+exploration {
			return errors.New("business allocation does not separate quality and exploration budgets")
		}
		totalQuality += quality
		totalExploration += exploration
	}
	if totalQuality+totalExploration != 10000 || totalExploration > policy.ExplorationBasisPoints ||
		policy.Mode == "observe" && unified.QualityBasisPoints[unified.PrimaryEdgeID] != totalQuality ||
		policy.Mode == "paused" && (totalExploration != 0 || unified.QualityBasisPoints[unified.PrimaryEdgeID] != 10000) ||
		unified.QualityBasisPoints[unified.PrimaryEdgeID] != totalQuality && validUntil.After(state.EvidenceValidUntil) {
		return errors.New("unified allocation exceeds exploration budget or evidence lifetime")
	}
	if unified.MigrationBasisPoints != nil {
		if len(unified.MigrationBasisPoints) != len(policy.InitialDNSPublications) {
			return errors.New("unified migration must cover every declared DNS executor")
		}
		for nodeID, weights := range unified.MigrationBasisPoints {
			if _, present := policy.InitialDNSPublications[nodeID]; !present {
				return errors.New("undeclared migration executor")
			}
			if err := ValidateBusinessWeights(policy, weights); err != nil {
				return err
			}
			for edgeID, weight := range weights {
				if weight > 0 && state.Stopped[edgeID] != "" {
					return errors.New("migration cannot allocate stopped candidate")
				}
			}
		}
	}
	return nil
}

func ValidateBusinessWeights(policy BusinessEdgeExplorationPolicy, weights map[string]int) error {
	total := 0
	for edgeID, weight := range weights {
		if !slices.Contains(policy.CandidateEdgeIDs, edgeID) || weight < 0 || weight > 10000 {
			return errors.New("invalid business migration allocation")
		}
		total += weight
	}
	if total != 10000 {
		return errors.New("business migration allocation must sum to 10000")
	}
	return nil
}

func BusinessServingPrimary(allocation *DNSBusinessAllocation) string {
	if allocation.State.Unified == nil {
		return allocation.Policy.BaselineEdgeID
	}
	primary := allocation.State.Unified.PrimaryEdgeID
	weights := allocation.State.Unified.MigrationBasisPoints[allocation.ExecutionNodeID]
	if weights == nil {
		return primary
	}
	for _, edgeID := range allocation.Policy.CandidateEdgeIDs {
		if weights[edgeID] > weights[primary] || weights[edgeID] == weights[primary] && edgeID < primary {
			primary = edgeID
		}
	}
	return primary
}

func BusinessExecutionWeights(allocation *DNSBusinessAllocation, at time.Time) map[string]int {
	if allocation.Policy.Mode == "paused" || at.Before(allocation.Policy.StartsAt) || !at.Before(allocation.Policy.ExpiresAt) || !at.Before(allocation.ValidUntil) {
		return map[string]int{BusinessServingPrimary(allocation): 10000}
	}
	if allocation.State.Unified != nil {
		if weights := allocation.State.Unified.MigrationBasisPoints[allocation.ExecutionNodeID]; weights != nil {
			return maps.Clone(weights)
		}
	}
	return maps.Clone(allocation.State.BasisPoints)
}
