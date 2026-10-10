package platformconfig

import (
	"encoding/json"
	"testing"

	"fugue/internal/model"
)

func TestDynamicQualityPolicyRequiresExplicitBudgetsAndCompleteCosts(t *testing.T) {
	policy := orderedPolicyFixture()
	policy.DynamicQuality = &DynamicQualityPolicy{Mode: "all_dynamic", RefreshQueriesPerCycle: 32, RefreshConcurrency: 4, Policy: model.PhysicalEdgeQualityPolicy{Version: model.PhysicalDeliveryNetworkPolicyVersion, MaximumNodeUtilization: 0.85, WindowSeconds: 1800, BucketSeconds: 300, RequiredBuckets: 3, MinimumRecords: 3, CooldownSeconds: 900, EvidenceMaxAgeSeconds: 600, AdvantageMS: 20, AdvantageRatio: 0.15, UnknownCostMS: 30, UncertaintyMS: 5, FailureCostMS: 1000, CapacityCostMS: 200, ThroughputCostMS: 100, ThroughputTargetBPS: 1048576, ProbeIntervalSeconds: 300, ProbeBudgetPerInterval: 1}}
	raw, err := json.Marshal(policy)
	var decoded DNSQueryPolicy
	if err != nil || json.Unmarshal(raw, &decoded) != nil || decoded.DynamicQuality == nil {
		t.Fatal(string(raw), err)
	}
	for _, field := range []string{"policy", "mode", "refresh_queries_per_cycle", "refresh_concurrency"} {
		var fields map[string]any
		json.Unmarshal(raw, &fields)
		delete(fields["dynamic_quality"].(map[string]any), field)
		invalid, _ := json.Marshal(fields)
		if json.Unmarshal(invalid, &decoded) == nil {
			t.Fatal("accepted incomplete dynamic quality policy", field)
		}
	}
	cloned := NormalizePolicySnapshot(PolicySnapshot{DNSQueryPolicy: &policy})
	cloned.DNSQueryPolicy.DynamicQuality.RefreshConcurrency = 1
	if policy.DynamicQuality.RefreshConcurrency != 4 {
		t.Fatal("dynamic policy aliases input")
	}
}
