package api

import (
	"fmt"
	"net/netip"
	"sort"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

// This migration adapter captures endpoint identity, never heartbeat readiness.
// Probe defaults become explicit versioned policy inputs before compilation.
func projectDNSReadiness(result *platformIntentProjectionResponse, nodes []model.EdgeNode, captured time.Time) error {
	return projectDNSReadinessWithPolicy(result, nodes, captured, nil)
}

func projectDNSReadinessWithPolicy(result *platformIntentProjectionResponse, nodes []model.EdgeNode, captured time.Time, input *platformproducer.ProjectionPolicyInput) error {
	endpoints := []platformconfig.DNSEdgeEndpoint{}
	if len(nodes) == 0 || len(nodes) > 4096 {
		return fmt.Errorf("DNS readiness requires bounded edge topology")
	}
	for _, node := range nodes {
		endpoint := platformconfig.DNSEdgeEndpoint{EdgeID: node.ID, EdgeGroupID: node.EdgeGroupID, ObservedAt: captured}
		for _, address := range []string{node.PublicIPv4, node.PublicIPv6} {
			if address == "" {
				continue
			}
			ip, err := netip.ParseAddr(address)
			if err != nil || !platformconfig.PublicDNSFlattenIP(ip) || ip.String() != address {
				return fmt.Errorf("DNS readiness endpoint is not canonical public address")
			}
			if ip.Is4() {
				endpoint.A = append(endpoint.A, address)
			} else {
				endpoint.AAAA = append(endpoint.AAAA, address)
			}
		}
		if len(endpoint.A)+len(endpoint.AAAA) == 0 {
			return fmt.Errorf("required edge has no observable public DNS endpoint")
		}
		endpoints = append(endpoints, endpoint)
	}
	sort.Slice(endpoints, func(i, j int) bool { return endpoints[i].EdgeID < endpoints[j].EdgeID })
	policy := result.Policy
	if input == nil {
		groups := map[string]bool{}
		for _, node := range nodes {
			groups[node.EdgeGroupID] = true
		}
		policy.TrafficRolloutCohorts = nil
		allGroups := make([]string, 0, len(groups))
		for group := range groups {
			policy.TrafficRolloutCohorts = append(policy.TrafficRolloutCohorts, platformconfig.TrafficRolloutCohort{ID: group, EdgeGroupIDs: []string{group}})
			allGroups = append(allGroups, group)
		}
		policy.TrafficRolloutCohorts = append(policy.TrafficRolloutCohorts, platformconfig.TrafficRolloutCohort{ID: "complete", EdgeGroupIDs: allGroups})
		policy.TrafficRolloutCohorts = platformconfig.NormalizeTrafficRolloutCohorts(policy.TrafficRolloutCohorts)
		policy.DNSReadiness = &platformconfig.DNSReadinessPolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxConcurrency: 8, MaxProbes: 4096}
		policy.TLSReadiness = &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxConcurrency: 8, MaxProbes: 4096}
	} else {
		policy.DNSReadiness = input.DNSReadiness
		policy.TLSReadiness = input.TLSReadiness
		policy.TrafficRolloutCohorts = input.Cohorts
	}
	policy = platformconfig.NormalizePolicySnapshot(policy)
	generation, err := platformconfig.PolicySnapshotGeneration(policy)
	if err != nil {
		return err
	}
	policy.Generation = generation
	result.Policy = policy
	result.RuntimeSnapshot.PolicyGeneration = generation
	result.RuntimeSnapshot.DNSEdgeEndpoints = endpoints
	result.CapturedAt = captured
	result.RuntimeSnapshot.CapturedAt = &result.CapturedAt
	return nil
}
