package platformconfig

import (
	"encoding/json"
	"testing"

	"fugue/internal/model"
)

func physicalPolicyFixture() DNSQueryPolicy {
	return DNSQueryPolicy{RankingMode: "active", PreferenceMode: "runtime_locality", MinimumTTLSeconds: 60, MaximumTTLSeconds: 120,
		PhysicalRoutes: []PhysicalQualityRoute{{Hostname: "app.example.test", TrafficClass: "streaming", Policy: model.PhysicalEdgeQualityPolicy{
			Version: model.PhysicalNetworkPolicyVersion, MaximumNodeUtilization: 0.85, WindowSeconds: 1800, BucketSeconds: 300, RequiredBuckets: 3, MinimumRecords: 3,
			CooldownSeconds: 900, EvidenceMaxAgeSeconds: 600, AdvantageMS: 20, AdvantageRatio: 0.15, UnknownCostMS: 30, UncertaintyMS: 5, FailureCostMS: 1000,
			CapacityCostMS: 200, ThroughputCostMS: 100, ThroughputTargetBPS: 1048576, ProbeIntervalSeconds: 300, ProbeBudgetPerInterval: 1}}}}
}

func TestPhysicalDNSQueryPolicyRequiresCompleteExplicitOptIn(t *testing.T) {
	policy := physicalPolicyFixture()
	raw, err := json.Marshal(policy)
	if err != nil {
		t.Fatal(err)
	}
	var decoded DNSQueryPolicy
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"null_routes", "partial_route", "unknown_route_field", "unknown_policy_field", "omitted_cooldown", "omitted_budget", "null_budget"} {
		t.Run(name, func(t *testing.T) {
			var fields map[string]any
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			route := fields["physical_routes"].([]any)[0].(map[string]any)
			values := route["policy"].(map[string]any)
			switch name {
			case "null_routes":
				fields["physical_routes"] = nil
			case "partial_route":
				delete(route, "traffic_class")
			case "unknown_route_field":
				route["preferred_country"] = "aa"
			case "unknown_policy_field":
				values["force"] = true
			case "omitted_cooldown":
				delete(values, "cooldown_seconds")
			case "omitted_budget":
				delete(values, "probe_budget_per_interval")
			case "null_budget":
				values["probe_budget_per_interval"] = nil
			}
			changed, _ := json.Marshal(fields)
			if json.Unmarshal(changed, &decoded) == nil {
				t.Fatal("accepted partial physical publication intent")
			}
		})
	}
	for _, edit := range []func(*DNSQueryPolicy){
		func(value *DNSQueryPolicy) {
			value.PhysicalRoutes = append(value.PhysicalRoutes, value.PhysicalRoutes[0])
		},
		func(value *DNSQueryPolicy) { value.RankingMode = "shadow" },
		func(value *DNSQueryPolicy) { value.PhysicalRoutes[0].Hostname = "*.example.test" },
		func(value *DNSQueryPolicy) { value.PhysicalRoutes[0].Policy.Version = "network-only-experiment-v1" },
		func(value *DNSQueryPolicy) { value.PhysicalRoutes[0].Policy.MaximumNodeUtilization = 0 },
		func(value *DNSQueryPolicy) { value.PhysicalRoutes[0].Policy.RequiredBuckets = 1 },
	} {
		changed := physicalPolicyFixture()
		edit(&changed)
		if ValidateDNSQueryPolicy(&changed) == nil {
			t.Fatal("accepted invalid physical publication strategy", changed)
		}
	}
}

func TestPhysicalDNSQueryPolicyNormalizationDoesNotAliasIntent(t *testing.T) {
	policy := physicalPolicyFixture()
	copy := NormalizePolicySnapshot(PolicySnapshot{DNSQueryPolicy: &policy})
	copy.DNSQueryPolicy.PhysicalRoutes[0].Hostname = "other.example.test"
	if policy.PhysicalRoutes[0].Hostname != "app.example.test" {
		t.Fatal("normalized policy mutated original intent")
	}
}
