package dnsserver

import (
	"slices"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

// Forward recovery executes the successor's original selector with its own
// complete, exact proof quorum. A newly signed selection within the same physical
// candidate pool need not wait for unrelated records to converge. This projection
// is never used to attach successor proofs to the retained selector or checkpoint.
func normalizeDNSForwardPhysicalOrders(before, after map[string][]platformconfig.DNSQueryView, oldRules, newRules map[string][]platformconfig.DNSAnswerRule) {
	for host, previous := range before {
		next := after[host]
		if len(previous) != len(next) {
			continue
		}
		oldIndex, newIndex := indexDNSPhysicalRules(oldRules[host]), indexDNSPhysicalRules(newRules[host])
		for i := range previous {
			a, b := &previous[i], &next[i]
			if a.NodeID != b.NodeID || a.Zone != b.Zone || a.EdgeGroupID != b.EdgeGroupID || len(a.Records) != 1 || len(b.Records) != 1 || a.Records[0].Type != b.Records[0].Type {
				continue
			}
			left, right := dnsForwardPhysicalOrder(*a, oldIndex), dnsForwardPhysicalOrder(*b, newIndex)
			if left == nil || right == nil || !slices.Equal(left.OrderedEdgeIDs, right.OrderedEdgeIDs) {
				continue
			}
			for _, pair := range []struct {
				view  *platformconfig.DNSQueryView
				rules []platformconfig.DNSAnswerRule
			}{{a, oldRules[host]}, {b, newRules[host]}} {
				policy := &pair.view.Records[0].AnswerPolicy
				policy.PolicyKind, policy.PhysicalOrder, policy.PhysicalSelection = model.DNSAnswerPolicyKindPhysicalOrder, model.CloneDNSPhysicalOrder(left), nil
				policy.RankingVersion, policy.RankingScope, policy.Reason = "", "", ""
				for j := range pair.rules {
					rule := &pair.rules[j]
					if rule.Type == pair.view.Records[0].Type && (rule.NodeID == "" || rule.NodeID == pair.view.NodeID) {
						rule.SelectionMode, rule.PhysicalOrder = model.DNSAnswerPolicyKindPhysicalOrder, model.CloneDNSPhysicalOrder(left)
					}
				}
			}
		}
	}
}

func dnsForwardPhysicalOrder(view platformconfig.DNSQueryView, rules map[string]*platformconfig.DNSAnswerRule) *model.DNSPhysicalOrder {
	if !dnsPhysicalRuleMatchesQuery(rules, view) {
		return nil
	}
	policy := view.Records[0].AnswerPolicy
	var order *model.DNSPhysicalOrder
	switch policy.PolicyKind {
	case model.DNSAnswerPolicyKindPhysicalOrder:
		if policy.PhysicalSelection != nil || model.ValidateDNSPhysicalOrder(policy.PhysicalOrder) != nil {
			return nil
		}
		order = model.CloneDNSPhysicalOrder(policy.PhysicalOrder)
	case model.DNSAnswerPolicyKindPhysicalQuality:
		selection := policy.PhysicalSelection
		if policy.PhysicalOrder != nil || selection == nil || selection.Version != model.DNSPhysicalSelectionVersion ||
			selection.Scope != "global" || selection.BusinessAllocation != nil || policy.RankingVersion != model.DNSPhysicalSelectionVersion ||
			policy.RankingScope != "global" || len(selection.OrderedEdgeIDs) == 0 || selection.PrimaryEdgeID != selection.OrderedEdgeIDs[0] {
			return nil
		}
		order = &model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: slices.Clone(selection.OrderedEdgeIDs)}
		if model.ValidateDNSPhysicalOrder(order) != nil {
			return nil
		}
	default:
		return nil
	}
	order.PrimarySince = nil
	// The signed candidate inventory is the authorization boundary. The old
	// selector may have temporarily excluded an otherwise authorized candidate;
	// only the successor's own proofs and selector can readmit it. Never add a
	// missing candidate, change its address, or reuse the retained selector.
	pool := make(map[string]bool)
	for _, candidate := range view.Records[0].Candidates {
		if candidate.EdgeID == "" {
			return nil
		}
		pool[candidate.EdgeID] = true
	}
	for _, id := range order.OrderedEdgeIDs {
		if !pool[id] {
			return nil
		}
	}
	order.OrderedEdgeIDs = nil
	for id := range pool {
		order.OrderedEdgeIDs = append(order.OrderedEdgeIDs, id)
	}
	slices.Sort(order.OrderedEdgeIDs)
	return order
}
