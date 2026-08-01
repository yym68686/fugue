package api

import (
	"io"
	"strings"

	"fugue/internal/model"
	"fugue/internal/observability"
)

func (s *Server) writeEdgeExclusionMetrics(w io.Writer) {
	policies, err := s.store.ListEdgeRoutePolicies()
	if err != nil {
		observability.WriteGaugeMetric(w, "fugue_edge_exclusion_metrics_error", "Whether edge exclusion metrics collection failed.", map[string]string{"stage": "policies"}, 1)
		return
	}
	now, err := s.store.EdgeRoutePolicyTime()
	if err != nil {
		observability.WriteGaugeMetric(w, "fugue_edge_exclusion_metrics_error", "Whether edge exclusion metrics collection failed.", map[string]string{"stage": "clock"}, 1)
		return
	}
	counts := make(map[string]int)
	for _, policy := range policies {
		if !model.EdgeRoutePolicyHasExclusions(policy) {
			continue
		}
		lifecycle := model.EdgeRoutePolicyExclusionLifecycleAt(policy, now)
		scope := model.EdgeRoutePolicyExclusionScope(policy)
		counts[lifecycle+"\x00"+scope]++
	}
	lifecycles := []string{
		model.EdgeExclusionLifecycleActive,
		model.EdgeExclusionLifecycleExpiring24H,
		model.EdgeExclusionLifecycleExpiring1H,
		model.EdgeExclusionLifecycleExpiredHold,
		model.EdgeExclusionLifecycleLegacyHold,
	}
	scopes := []string{model.EdgeExclusionScopeEdge, model.EdgeExclusionScopeEdgeGroup, model.EdgeExclusionScopeMixed}
	for _, lifecycle := range lifecycles {
		for _, scope := range scopes {
			observability.WriteGaugeMetric(w, "fugue_edge_exclusion_policies", "Edge exclusion policies by fail-closed lifecycle and scope.", map[string]string{"lifecycle": lifecycle, "scope": scope}, float64(counts[lifecycle+"\x00"+scope]))
		}
	}

	nodes, _, err := s.store.ListActiveEdgeNodes("")
	if err != nil {
		observability.WriteGaugeMetric(w, "fugue_edge_exclusion_metrics_error", "Whether edge exclusion metrics collection failed.", map[string]string{"stage": "active_edges"}, 1)
		return
	}
	healthyByGroup := make(map[string][]string)
	for _, node := range nodes {
		if node.Healthy && !node.Draining {
			groupID := strings.TrimSpace(strings.ToLower(node.EdgeGroupID))
			healthyByGroup[groupID] = append(healthyByGroup[groupID], strings.TrimSpace(strings.ToLower(node.ID)))
		}
	}
	atRisk, noRoute, singleGroup := 0, 0, 0
	for _, policy := range policies {
		if !model.EdgeRoutePolicyHasExclusions(policy) {
			continue
		}
		exclusions := edgeRoutePolicyActiveExclusions(policy, now)
		healthy := edgeRouteHealthyNodeCountAfterExclusions(healthyByGroup, exclusions)
		if groupID := strings.TrimSpace(strings.ToLower(policy.EdgeGroupID)); groupID != "" {
			healthy = edgeRouteHealthyNodeCountForGroupsAfterExclusions(healthyByGroup, []string{groupID}, exclusions)
		}
		minimum := policy.MinHealthyEdgeNodes
		if minimum <= 0 {
			minimum = 1
		}
		if healthy < minimum {
			atRisk++
		}
		if healthy == 0 {
			noRoute++
		}
		healthyGroups := 0
		for groupID, nodeIDs := range healthyByGroup {
			if pinned := strings.TrimSpace(strings.ToLower(policy.EdgeGroupID)); pinned != "" && pinned != groupID {
				continue
			}
			if edgeRouteHealthyNodeCountForGroupsAfterExclusions(map[string][]string{groupID: nodeIDs}, []string{groupID}, exclusions) > 0 {
				healthyGroups++
			}
		}
		if healthyGroups == 1 {
			singleGroup++
		}
	}
	observability.WriteGaugeMetric(w, "fugue_edge_exclusion_redundancy_at_risk", "Number of exclusion policies below their minimum healthy edge redundancy.", nil, float64(atRisk))
	observability.WriteGaugeMetric(w, "fugue_edge_exclusion_single_group", "Number of exclusion policies retaining only one healthy edge group.", nil, float64(singleGroup))
	observability.WriteGaugeMetric(w, "fugue_edge_exclusion_no_safe_route", "Number of exclusion policies with no safe non-excluded active edge route.", nil, float64(noRoute))
	observability.WriteGaugeMetric(w, "fugue_edge_exclusion_metrics_error", "Whether edge exclusion metrics collection failed.", map[string]string{"stage": "all"}, 0)
}
