package dnslegacy

import (
	"fmt"
	"sort"
	"strings"

	"fugue/internal/model"
)

func GlobalOrder(record model.EdgeDNSRecord) (model.DNSPhysicalOrder, error) {
	policy := record.AnswerPolicy
	if len(record.ScopedCandidates) != 0 || len(record.Candidates) == 0 || policy.PolicyKind == model.DNSAnswerPolicyKindPhysicalQuality || policy.PolicyKind == model.DNSAnswerPolicyKindPhysicalOrder {
		return model.DNSPhysicalOrder{}, fmt.Errorf("unscoped legacy configuration required")
	}
	switch policy.PolicyKind {
	case model.DNSAnswerPolicyKindGeo, model.DNSAnswerPolicyKindLatencyAware, model.DNSAnswerPolicyKindWeighted, model.DNSAnswerPolicyKindGlobal, model.DNSAnswerPolicyKindPinned:
	default:
		return model.DNSPhysicalOrder{}, fmt.Errorf("unsupported legacy configuration")
	}
	candidates := append([]model.EdgeDNSAnswerCandidate(nil), record.Candidates...)
	hasScore := false
	for _, candidate := range candidates {
		hasScore = hasScore || candidate.Score > 0
	}
	score := func(candidate model.EdgeDNSAnswerCandidate) int {
		value := candidate.Priority * 100
		switch policy.PolicyKind {
		case model.DNSAnswerPolicyKindLatencyAware:
			if hasScore {
				value = 500000 + candidate.Priority*10 - candidate.Weight
				if candidate.Score > 0 {
					value = int(candidate.Score) + candidate.Priority*10 - candidate.Weight
				}
			} else {
				value = candidate.Priority*10 - candidate.Weight*20
			}
		case model.DNSAnswerPolicyKindWeighted:
			value -= candidate.Weight * 20
		}
		if policy.PolicyKind != model.DNSAnswerPolicyKindLatencyAware && strings.EqualFold(candidate.Reason, "same_region") {
			value -= 250
		}
		if candidate.Score > 0 && policy.PolicyKind != model.DNSAnswerPolicyKindLatencyAware {
			value += int(candidate.Score)
		}
		return value
	}
	sort.SliceStable(candidates, func(left, right int) bool {
		first, second := candidates[left], candidates[right]
		if score(first) != score(second) {
			return score(first) < score(second)
		}
		if first.Priority != second.Priority {
			return first.Priority < second.Priority
		}
		if first.Weight != second.Weight {
			return first.Weight > second.Weight
		}
		if first.EdgeGroupID != second.EdgeGroupID {
			return first.EdgeGroupID < second.EdgeGroupID
		}
		return first.IP < second.IP
	})
	for index, candidate := range candidates {
		if strings.TrimSpace(policy.SelectedEdgeGroupID) != "" && strings.EqualFold(strings.TrimSpace(candidate.EdgeGroupID), strings.TrimSpace(policy.SelectedEdgeGroupID)) {
			copy(candidates[1:index+1], candidates[:index])
			candidates[0] = candidate
			break
		}
	}
	order := model.DNSPhysicalOrder{Version: "physical-order-v1"}
	seen := map[string]bool{}
	for _, candidate := range candidates {
		if candidate.EdgeID == "" {
			return order, fmt.Errorf("legacy candidate lacks physical identity")
		}
		if !seen[candidate.EdgeID] {
			seen[candidate.EdgeID] = true
			order.OrderedEdgeIDs = append(order.OrderedEdgeIDs, candidate.EdgeID)
		}
	}
	return order, model.ValidateDNSPhysicalOrder(&order)
}
