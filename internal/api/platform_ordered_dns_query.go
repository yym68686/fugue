package api

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func projectOrderedDNSQueries(result *platformIntentProjectionResponse, strategy platformconfig.DNSQueryPolicy, nodes []model.EdgeNode, observed time.Time) error {
	if platformconfig.ValidateDNSQueryPolicy(&strategy) != nil || strategy.OrderedProjection == nil || observed.IsZero() {
		return fmt.Errorf("explicit ordered DNS projection required")
	}
	endpoints := map[string]platformconfig.DNSEdgeEndpoint{}
	for _, endpoint := range result.RuntimeSnapshot.DNSEdgeEndpoints {
		endpoints[endpoint.EdgeID] = endpoint
	}
	candidates := map[string]model.EdgeDNSAnswerCandidate{}
	for _, node := range nodes {
		endpoint, present := endpoints[node.ID]
		if !present {
			continue
		}
		if endpoint.EdgeGroupID != node.EdgeGroupID {
			return fmt.Errorf("ordered DNS endpoint owner differs from frozen topology")
		}
		for _, pair := range []struct {
			address  string
			declared []string
		}{{node.PublicIPv4, endpoint.A}, {node.PublicIPv6, endpoint.AAAA}} {
			if pair.address == "" {
				continue
			}
			if !stringSliceContains(pair.declared, pair.address) || candidates[pair.address].EdgeID != "" && candidates[pair.address].EdgeID != node.ID {
				return fmt.Errorf("ordered DNS address lacks unique frozen ownership")
			}
			candidates[pair.address] = model.EdgeDNSAnswerCandidate{IP: pair.address, EdgeID: node.ID, EdgeGroupID: node.EdgeGroupID}
		}
	}
	physical := map[string]bool{}
	for _, route := range strategy.PhysicalRoutes {
		physical[route.Hostname] = true
	}
	overrides := map[string]model.DNSPhysicalOrder{}
	for _, entry := range strategy.OrderedProjection.Overrides {
		overrides[entry.NodeID+"\x00"+entry.Hostname+"\x00"+entry.Type] = entry.Order
	}
	rules := []platformconfig.DNSAnswerRule{}
	facts := []platformconfig.DNSSelectionObservation{}
	for _, record := range result.Intent.DNS {
		options := platformconfig.DNSPlacementOptions(record)
		if options == nil {
			continue
		}
		owners, _, err := placementRecordRoutes(*result, record)
		if err != nil {
			return err
		}
		enabled := record.Status == "" || record.Status == model.EdgeRouteStatusActive
		for _, owner := range owners {
			enabled = enabled && platformconfig.DNSRouteStateAllowed(record, owner, result.Policy)
		}
		if !enabled {
			continue
		}
		for _, consumer := range result.Intent.DNSConsumers {
			within := false
			for _, zone := range consumer.Zones {
				within = within || edgeDNSTargetWithinZone(record.Hostname, zone)
			}
			if !within {
				continue
			}
			for _, family := range []string{"A", "AAAA"} {
				if family == "A" && (options.IPv4Policy == "ipv6_only" || options.IPv6Policy == "ipv6_only") || family == "AAAA" && (options.IPv4Policy == "ipv4_only" || options.IPv6Policy == "ipv4_only") {
					continue
				}
				available := []platformconfig.DNSSelectionCandidate{}
				for address, candidate := range candidates {
					if (family == "AAAA") != strings.Contains(address, ":") || !platformconfig.DNSPlacementAllowsEdge(record, owners, candidate.EdgeID, candidate.EdgeGroupID) {
						continue
					}
					available = append(available, platformconfig.DNSSelectionCandidate{IP: address, EdgeID: candidate.EdgeID, EdgeGroupID: candidate.EdgeGroupID})
				}
				if len(available) == 0 {
					continue
				}
				sort.Slice(available, func(left, right int) bool { return available[left].IP < available[right].IP })
				order := strategy.OrderedProjection.DefaultOrder
				if override, found := overrides[consumer.NodeID+"\x00"+record.Hostname+"\x00"+family]; found {
					order = override
				}
				allowed := map[string]bool{}
				for _, candidate := range available {
					allowed[candidate.EdgeID] = true
				}
				filtered := &model.DNSPhysicalOrder{Version: order.Version}
				for _, edgeID := range order.OrderedEdgeIDs {
					if allowed[edgeID] {
						filtered.OrderedEdgeIDs = append(filtered.OrderedEdgeIDs, edgeID)
					}
				}
				mode, reason := model.DNSAnswerPolicyKindPhysicalOrder, "explicit_physical_order_quality_unknown"
				if physical[record.Hostname] {
					mode, reason, filtered = model.DNSAnswerPolicyKindPhysicalQuality, "awaiting_bound_physical_evidence", nil
				} else if model.ValidateDNSPhysicalOrder(filtered) != nil {
					return fmt.Errorf("configured physical order has no authorized endpoint")
				}
				ttl := max(strategy.MinimumTTLSeconds, min(strategy.MaximumTTLSeconds, record.TTL))
				rules = append(rules, platformconfig.DNSAnswerRule{NodeID: consumer.NodeID, Hostname: record.Hostname, Type: family, SelectionMode: mode, PhysicalOrder: filtered, TTLSeconds: ttl})
				fact := platformconfig.DNSSelectionObservation{NodeID: consumer.NodeID, Hostname: record.Hostname, Type: family, ObservedAt: observed, Reason: reason, Candidates: available}
				digest, err := platformconfig.Digest(fact)
				if err != nil {
					return err
				}
				fact.SourceDigest, fact.SourceGeneration = digest, "dns-observation_"+strings.TrimPrefix(digest, "sha256:")
				facts = append(facts, fact)
			}
		}
	}
	if err := platformconfig.ValidateDNSAnswerRules(rules); err != nil {
		return err
	}
	policy := platformconfig.NormalizePolicySnapshot(result.Policy)
	policy.DNSQueryPolicy = &strategy
	policy.DNSAnswerRules = rules
	policy = platformconfig.NormalizePolicySnapshot(policy)
	generation, err := platformconfig.PolicySnapshotGeneration(policy)
	if err != nil {
		return err
	}
	policy.Generation = generation
	result.Policy = policy
	result.RuntimeSnapshot.PolicyGeneration = generation
	result.RuntimeSnapshot.DNSSelections = facts
	result.CapturedAt = observed
	result.RuntimeSnapshot.CapturedAt = &result.CapturedAt
	return nil
}
