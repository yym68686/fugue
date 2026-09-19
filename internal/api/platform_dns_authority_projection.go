package api

import (
	"fmt"
	"fugue/internal/platformconfig"
	appsv1 "k8s.io/api/apps/v1"
	"strconv"
	"strings"
)

func dnsAuthorityFromWorkloads(workloads []appsv1.DaemonSet) (map[string]platformconfig.DNSAuthorityPolicy, error) {
	out := map[string]platformconfig.DNSAuthorityPolicy{}
	for _, w := range workloads {
		if w.DeletionTimestamp != nil {
			continue
		}
		for _, c := range w.Spec.Template.Spec.Containers {
			isDNS := false
			for _, e := range c.Env {
				isDNS = isDNS || e.Name == "FUGUE_DNS_ZONE"
			}
			if !isDNS {
				continue
			}
			if len(c.EnvFrom) > 0 {
				return nil, fmt.Errorf("DNS authority capture requires explicit environment")
			}
			env := map[string]string{}
			for _, e := range c.Env {
				switch e.Name {
				case "FUGUE_EDGE_GROUP_ID", "FUGUE_DNS_NAMESERVERS", "FUGUE_DNS_TTL":
					if _, ok := env[e.Name]; ok || e.ValueFrom != nil {
						return nil, fmt.Errorf("ambiguous DNS authority declaration")
					}
					env[e.Name] = e.Value
				}
			}
			g := strings.TrimSpace(env["FUGUE_EDGE_GROUP_ID"])
			if g == "" {
				return nil, fmt.Errorf("DNS authority group missing")
			}
			if _, ok := out[g]; ok {
				return nil, fmt.Errorf("duplicate DNS authority group")
			}
			p := platformconfig.DNSAuthorityPolicy{TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600}
			if raw := strings.TrimSpace(env["FUGUE_DNS_TTL"]); raw != "" {
				var err error
				p.TTLSeconds, err = strconv.Atoi(raw)
				if err != nil {
					return nil, err
				}
			}
			for _, ns := range strings.Split(env["FUGUE_DNS_NAMESERVERS"], ",") {
				if ns = strings.TrimSpace(ns); ns != "" {
					p.Nameservers = append(p.Nameservers, normalizeExternalAppDomain(ns))
				}
			}
			out[g] = p
		}
	}
	return out, nil
}

func projectDNSAuthorityPolicies(result *platformIntentProjectionResponse, groups map[string]platformconfig.DNSAuthorityPolicy) error {
	policy := result.Policy
	policy.DNSAuthorities = nil
	for _, c := range result.Intent.DNSConsumers {
		declared, ok := groups[c.EdgeGroupID]
		if !ok {
			return fmt.Errorf("DNS authority workload absent")
		}
		for _, zone := range c.Zones {
			p := declared
			p.NodeID, p.Zone = c.NodeID, zone
			p.Nameservers = append([]string(nil), declared.Nameservers...)
			if len(p.Nameservers) == 0 {
				p.Nameservers = []string{"ns1." + zone}
			}
			policy.DNSAuthorities = append(policy.DNSAuthorities, p)
		}
	}
	if err := platformconfig.ValidateDNSAuthorityOwnership(policy.DNSAuthorities, result.Intent.DNSConsumers); err != nil {
		return err
	}
	policy = platformconfig.NormalizePolicySnapshot(policy)
	gen, err := platformconfig.PolicySnapshotGeneration(policy)
	if err != nil {
		return err
	}
	policy.Generation = gen
	result.Policy = policy
	result.RuntimeSnapshot.PolicyGeneration = gen
	return nil
}
