package model

import "fmt"

type BlastRadiusPolicy struct {
	PreserveMinHealthyEdgeGroups    int `json:"preserve_min_healthy_edge_groups,omitempty"`
	PreserveMinEligibleEdgesPerHost int `json:"preserve_min_eligible_edges_per_host,omitempty"`
	MaxRemovedEdgeGroups            int `json:"max_removed_edge_groups,omitempty"`
	MaxRemovedEdgesPerHost          int `json:"max_removed_edges_per_host,omitempty"`
}

type BlastRadiusEvaluation struct {
	Pass       bool              `json:"pass"`
	Scope      string            `json:"scope"`
	Reason     string            `json:"reason,omitempty"`
	Before     map[string]int    `json:"before,omitempty"`
	After      map[string]int    `json:"after,omitempty"`
	Violations map[string]string `json:"violations,omitempty"`
}

func EvaluateBlastRadius(before, after map[string]int, scope string, policy BlastRadiusPolicy) BlastRadiusEvaluation {
	eval := BlastRadiusEvaluation{
		Pass:       true,
		Scope:      scope,
		Before:     copyIntMap(before),
		After:      copyIntMap(after),
		Violations: map[string]string{},
	}
	minimum := policy.PreserveMinEligibleEdgesPerHost
	if scope == "edge-group" || scope == "edge-groups" {
		minimum = policy.PreserveMinHealthyEdgeGroups
	}
	if minimum <= 0 {
		minimum = 1
	}
	for key, beforeCount := range before {
		afterCount := after[key]
		if afterCount < 0 {
			afterCount = 0
		}
		if beforeCount > 0 && afterCount < minimum {
			eval.Pass = false
			eval.Violations[key] = "blast radius would reduce eligible count below minimum"
		}
		removed := beforeCount - afterCount
		if removed > 0 && policy.MaxRemovedEdgesPerHost > 0 && removed > policy.MaxRemovedEdgesPerHost {
			eval.Pass = false
			eval.Violations[key] = fmt.Sprintf(
				"blast radius would remove %d eligible edges, exceeding maximum %d",
				removed,
				policy.MaxRemovedEdgesPerHost,
			)
		}
	}
	if policy.MaxRemovedEdgeGroups > 0 {
		removedGroups := 0
		for key, beforeCount := range before {
			if beforeCount > 0 && after[key] <= 0 {
				removedGroups++
			}
		}
		if removedGroups > policy.MaxRemovedEdgeGroups {
			eval.Pass = false
			eval.Violations["_aggregate"] = fmt.Sprintf(
				"blast radius would remove %d edge groups, exceeding maximum %d",
				removedGroups,
				policy.MaxRemovedEdgeGroups,
			)
		}
	}
	if !eval.Pass {
		eval.Reason = "blast radius cap exceeded"
	}
	if len(eval.Violations) == 0 {
		eval.Violations = nil
	}
	return eval
}

func copyIntMap(in map[string]int) map[string]int {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
