package platformconfig

import (
	"encoding/json"
	"testing"

	"fugue/internal/model"
)

func orderedPolicyFixture() DNSQueryPolicy {
	return DNSQueryPolicy{RankingMode: "active", PreferenceMode: "runtime_locality", MinimumTTLSeconds: 60, MaximumTTLSeconds: 120,
		OrderedProjection: &DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a", "edge-b"}}, Overrides: []DNSOrderOverride{}}}
}

func TestOrderedProjectionPolicyRejectsIncompleteAuthority(t *testing.T) {
	for _, scenario := range []string{"valid", "missing overrides", "null overrides", "unknown field", "null projection", "duplicate", "geo", "exploration", "wrong version"} {
		t.Run(scenario, func(t *testing.T) {
			policy := orderedPolicyFixture()
			if scenario == "duplicate" {
				override := DNSOrderOverride{NodeID: "dns-a", Hostname: "app.example.test", Type: "A", Order: policy.OrderedProjection.DefaultOrder}
				policy.OrderedProjection.Overrides = []DNSOrderOverride{override, override}
			}
			if scenario == "geo" {
				policy.ECSEnabled = true
			}
			if scenario == "exploration" {
				policy.ExplorationPercent = 5
			}
			if scenario == "wrong version" {
				policy.OrderedProjection.DefaultOrder.Version = "unknown"
			}
			raw, _ := json.Marshal(policy)
			var object map[string]any
			json.Unmarshal(raw, &object)
			ordered := object["ordered_projection"].(map[string]any)
			switch scenario {
			case "missing overrides":
				delete(ordered, "overrides")
			case "null overrides":
				ordered["overrides"] = nil
			case "unknown field":
				ordered["skip_health"] = true
			case "null projection":
				object["ordered_projection"] = nil
			}
			raw, _ = json.Marshal(object)
			var decoded DNSQueryPolicy
			if err := json.Unmarshal(raw, &decoded); (err == nil) != (scenario == "valid") {
				t.Fatal("policy validation differs", scenario, err)
			}
		})
	}
}

func TestOrderedProjectionPolicyNormalizationDoesNotAliasConfiguration(t *testing.T) {
	policy := orderedPolicyFixture()
	policy.OrderedProjection.Overrides = []DNSOrderOverride{{NodeID: "dns-a", Hostname: "app.example.test", Type: "A", Order: policy.OrderedProjection.DefaultOrder}}
	normalized := NormalizePolicySnapshot(PolicySnapshot{DNSQueryPolicy: &policy})
	normalized.DNSQueryPolicy.OrderedProjection.DefaultOrder.OrderedEdgeIDs[0] = "other"
	normalized.DNSQueryPolicy.OrderedProjection.Overrides[0].Order.OrderedEdgeIDs[0] = "other"
	if policy.OrderedProjection.DefaultOrder.OrderedEdgeIDs[0] != "edge-a" || policy.OrderedProjection.Overrides[0].Order.OrderedEdgeIDs[0] != "edge-a" {
		t.Fatal("normalized order aliases caller")
	}
}
