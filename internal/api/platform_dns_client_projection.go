package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"fugue/internal/platformconfig"
	appsv1 "k8s.io/api/apps/v1"
)

func dnsClientRulesFromWorkloads(workloads []appsv1.DaemonSet) (map[string][]platformconfig.DNSClientRule, error) {
	out := map[string][]platformconfig.DNSClientRule{}
	if len(workloads) > 4096 {
		return nil, fmt.Errorf("too many DNS workloads")
	}
	for _, w := range workloads {
		if w.DeletionTimestamp != nil {
			continue
		}
		for _, c := range w.Spec.Template.Spec.Containers {
			isDNS := false
			for _, e := range c.Env {
				if e.Name == "FUGUE_DNS_ZONE" {
					isDNS = true
				}
			}
			if !isDNS {
				continue
			}
			if len(c.EnvFrom) > 0 {
				return nil, fmt.Errorf("DNS client capture requires resolved environment declarations")
			}
			group, raw := "", ""
			seen := map[string]bool{}
			for _, e := range c.Env {
				if e.Name != "FUGUE_EDGE_GROUP_ID" && e.Name != "FUGUE_DNS_GEOIP_OVERRIDES_JSON" {
					continue
				}
				if e.ValueFrom != nil || seen[e.Name] {
					return nil, fmt.Errorf("DNS client capture requires unambiguous explicit values")
				}
				seen[e.Name] = true
				if e.Name == "FUGUE_EDGE_GROUP_ID" {
					group = strings.TrimSpace(e.Value)
				} else {
					raw = strings.TrimSpace(e.Value)
				}
			}
			if group == "" {
				return nil, fmt.Errorf("DNS client policy group missing")
			}
			if _, ok := out[group]; ok {
				return nil, fmt.Errorf("ambiguous DNS client policy group")
			}
			rules := []platformconfig.DNSClientRule{}
			if raw != "" {
				if len(raw) > 131072 {
					return nil, fmt.Errorf("DNS client policy declaration exceeds limit")
				}
				dec := json.NewDecoder(bytes.NewBufferString(raw))
				dec.DisallowUnknownFields()
				if dec.Decode(&rules) != nil || rules == nil || dec.Decode(&struct{}{}) != io.EOF {
					return nil, fmt.Errorf("invalid DNS client policy declaration")
				}
			}
			normalized := platformconfig.NormalizePolicySnapshot(platformconfig.PolicySnapshot{DNSClientPolicies: []platformconfig.DNSClientPolicy{{NodeID: group, Rules: rules}}}).DNSClientPolicies
			if err := platformconfig.ValidateDNSClientPolicies(normalized); err != nil {
				return nil, err
			}
			out[group] = normalized[0].Rules
		}
	}
	return out, nil
}

func projectDNSClientPolicies(result *platformIntentProjectionResponse, rules map[string][]platformconfig.DNSClientRule) error {
	policy := result.Policy
	policy.DNSClientPolicies = nil
	for _, c := range result.Intent.DNSConsumers {
		declared, exists := rules[c.EdgeGroupID]
		if !exists {
			return fmt.Errorf("DNS client policy missing for required consumer")
		}
		policy.DNSClientPolicies = append(policy.DNSClientPolicies, platformconfig.DNSClientPolicy{NodeID: c.NodeID, Rules: append([]platformconfig.DNSClientRule{}, declared...)})
	}
	if err := platformconfig.ValidateDNSClientPolicyOwnership(policy.DNSClientPolicies, result.Intent.DNSConsumers); err != nil {
		return err
	}
	policy = platformconfig.NormalizePolicySnapshot(policy)
	generation, err := platformconfig.PolicySnapshotGeneration(policy)
	if err != nil {
		return err
	}
	policy.Generation = generation
	result.Policy = policy
	result.RuntimeSnapshot.PolicyGeneration = generation
	return nil
}
