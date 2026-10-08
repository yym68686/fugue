package model

import (
	"errors"
	"math"
)

const PhysicalNetworkPolicyVersion = "physical-network-cohort-v2"

type PhysicalEdgeQualityPolicy struct {
	MaximumNodeUtilization float64 `json:"maximum_node_utilization,omitempty"`
	Version                string  `json:"version"`
	WindowSeconds          int     `json:"window_seconds"`
	BucketSeconds          int     `json:"bucket_seconds"`
	RequiredBuckets        int     `json:"required_buckets"`
	MinimumRecords         int     `json:"minimum_records"`
	CooldownSeconds        int     `json:"cooldown_seconds"`
	EvidenceMaxAgeSeconds  int     `json:"evidence_max_age_seconds"`
	AdvantageMS            float64 `json:"advantage_ms"`
	AdvantageRatio         float64 `json:"advantage_ratio"`
	UnknownCostMS          float64 `json:"unknown_cost_ms"`
	UncertaintyMS          float64 `json:"uncertainty_ms"`
	FailureCostMS          float64 `json:"failure_cost_ms"`
	CapacityCostMS         float64 `json:"capacity_cost_ms"`
	ThroughputCostMS       float64 `json:"throughput_cost_ms"`
	ThroughputTargetBPS    float64 `json:"throughput_target_bps"`
	ProbeIntervalSeconds   int     `json:"probe_interval_seconds"`
	ProbeBudgetPerInterval int     `json:"probe_budget_per_interval"`
}

func ValidatePhysicalEdgeQualityPolicy(policy PhysicalEdgeQualityPolicy) error {
	if policy.Version != PhysicalNetworkPolicyVersion || math.IsNaN(policy.MaximumNodeUtilization) || math.IsInf(policy.MaximumNodeUtilization, 0) || policy.MaximumNodeUtilization <= 0 || policy.MaximumNodeUtilization > 1 {
		return errors.New("unsupported physical network policy or node utilization limit")
	}
	if policy.WindowSeconds < 60 || policy.WindowSeconds > 86400 || policy.BucketSeconds < 60 || policy.BucketSeconds > policy.WindowSeconds || policy.RequiredBuckets < 2 || policy.RequiredBuckets > 12 || policy.RequiredBuckets*policy.BucketSeconds > policy.WindowSeconds || policy.MinimumRecords < 2 || policy.MinimumRecords > 10000 || policy.CooldownSeconds < 0 || policy.CooldownSeconds > 86400 || policy.ProbeIntervalSeconds < 60 || policy.ProbeIntervalSeconds > 86400 || policy.ProbeBudgetPerInterval < 0 || policy.ProbeBudgetPerInterval > 2 || policy.EvidenceMaxAgeSeconds < 60 || policy.EvidenceMaxAgeSeconds > policy.WindowSeconds {
		return errors.New("physical network policy outside bounded measurement windows")
	}
	for _, value := range []float64{policy.AdvantageMS, policy.AdvantageRatio, policy.UnknownCostMS, policy.UncertaintyMS, policy.FailureCostMS, policy.CapacityCostMS, policy.ThroughputCostMS, policy.ThroughputTargetBPS} {
		if math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 || value > 1e12 {
			return errors.New("physical network policy requires explicit bounded costs")
		}
	}
	if policy.AdvantageRatio >= 1 {
		return errors.New("physical network advantage ratio outside bounds")
	}
	return nil
}
