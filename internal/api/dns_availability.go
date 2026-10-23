package api

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Quality refresh may fail independently of availability publication. Only an
// explicit signed host policy can authorize the deterministic fallback. No
// failed answer is turned into successful quality or allocation evidence.
func (s *Server) capturePhysicalDNSQueries(ctx context.Context, projection *platformIntentProjectionResponse, policy platformconfig.DNSQueryPolicy) error {
	if len(policy.AvailabilityRoutes) == 0 {
		return s.capturePhysicalDNSQueriesStrict(ctx, projection, policy)
	}
	var coverage []map[string]any
	for _, route := range policy.PhysicalRoutes {
		one := policy
		one.PhysicalRoutes = []platformconfig.PhysicalQualityRoute{route}
		if err := s.capturePhysicalDNSQueriesStrict(ctx, projection, one); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			availability := platformconfig.DNSAvailabilityForHost(&policy, route.Hostname)
			if availability == nil {
				return err
			}
			if fallbackErr := applyDNSAvailabilityBaseline(projection, route.Hostname, availability); fallbackErr != nil {
				return fallbackErr
			}
			if s.log != nil {
				s.log.Printf("DNS quality refresh unavailable; independently authorized availability retained hostname=%s error=%v", route.Hostname, err)
			}
		} else if route.BusinessExploration != nil {
			if facts, ok := projection.RuntimeSnapshot.Facts["business_exploration_coverage"].([]map[string]any); ok {
				coverage = append(coverage, facts...)
			}
		}
	}
	if len(coverage) > 0 {
		projection.RuntimeSnapshot.Facts["business_exploration_coverage"] = coverage
	}
	return nil
}

func applyDNSAvailabilityBaseline(projection *platformIntentProjectionResponse, hostname string, policy *model.DNSAvailabilityPolicy) error {
	if model.ValidateDNSAvailabilityPolicy(policy) != nil || !reflect.DeepEqual(policy, platformconfig.DNSAvailabilityForHost(projection.Policy.DNSQueryPolicy, hostname)) {
		return fmt.Errorf("invalid independent availability authority")
	}
	// Work on detached projection slices. Any invalid family/ownership leaves the
	// original projection untouched; no partial fallback publication is possible.
	rules := append([]platformconfig.DNSAnswerRule(nil), projection.Policy.DNSAnswerRules...)
	facts := append([]platformconfig.DNSSelectionObservation(nil), projection.RuntimeSnapshot.DNSSelections...)
	matched := 0
	for i, fact := range facts {
		if fact.Hostname != hostname {
			continue
		}
		if fact.Type != "A" && fact.Type != "AAAA" {
			return fmt.Errorf("availability cannot rewrite a non-address record")
		}
		available := map[string]bool{}
		for _, candidate := range fact.Candidates {
			available[candidate.EdgeID] = true
		}
		order := &model.DNSPhysicalOrder{Version: "physical-order-v1"}
		for _, id := range policy.OrderedEdgeIDs {
			if available[id] {
				order.OrderedEdgeIDs = append(order.OrderedEdgeIDs, id)
			}
		}
		if model.ValidateDNSPhysicalOrder(order) != nil {
			return fmt.Errorf("availability pool has no authorized endpoint")
		}
		found := false
		for j, rule := range rules {
			if rule.NodeID == fact.NodeID && rule.Hostname == fact.Hostname && rule.Type == fact.Type {
				if rule.ECSEnabled || rule.ExplorationPercent != 0 || rule.ScopedSelectionMode != "" || len(rule.PreferredEdgeGroups) != 0 || len(rule.FallbackEdgeGroups) != 0 || rule.SelectionMode != model.DNSAnswerPolicyKindPhysicalQuality && rule.SelectionMode != model.DNSAnswerPolicyKindPhysicalOrder {
					return fmt.Errorf("availability cannot bypass a non-physical constraint")
				}
				nextRule := rule
				nextRule.SelectionMode, nextRule.PhysicalOrder = model.DNSAnswerPolicyKindPhysicalOrder, order
				rules[j] = nextRule
				found = true
			}
		}
		if !found {
			return fmt.Errorf("availability lacks an exact owned DNS rule")
		}
		fact.PhysicalSelection, fact.PhysicalEvidence = nil, nil
		fact.RankingVersion, fact.RankingScope = "", ""
		fact.Reason = "availability_only_quality_unavailable"
		fact.SourceDigest, fact.SourceGeneration = "", ""
		digest, err := platformconfig.Digest(fact)
		if err != nil {
			return err
		}
		fact.SourceDigest, fact.SourceGeneration = digest, "dns-observation_"+strings.TrimPrefix(digest, "sha256:")
		facts[i] = fact
		matched++
	}
	if matched == 0 {
		return fmt.Errorf("availability hostname has no owned dynamic query")
	}
	next := platformconfig.NormalizePolicySnapshot(projection.Policy)
	next.DNSAnswerRules = rules
	generation, err := platformconfig.PolicySnapshotGeneration(next)
	if err != nil {
		return err
	}
	next.Generation = generation
	projection.Policy = next
	projection.RuntimeSnapshot.PolicyGeneration = generation
	projection.RuntimeSnapshot.DNSSelections = facts
	return nil
}
