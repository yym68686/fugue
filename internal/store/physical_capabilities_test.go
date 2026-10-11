package store

import (
	"testing"

	"fugue/internal/model"
)

func TestCombinedPhysicalCapabilitiesPreserveIndependentResults(t *testing.T) {
	for _, version := range []string{"", model.PhysicalFailureAwareNetworkPolicyVersion, model.PhysicalComparableDeliveryPolicyVersion} {
		for _, dynamic := range []bool{false, true} {
			policy := map[string]any{"dns_query_policy": map[string]any{"physical_routes": []any{map[string]any{"policy": map[string]any{"version": version}}}}, "dns_answer_rules": []any{map[string]any{"selection_mode": model.DNSAnswerPolicyKindPhysicalOrder}}}
			if dynamic {
				policy["dns_query_policy"].(map[string]any)["dynamic_quality"] = map[string]any{"policy": map[string]any{"version": version}}
			}
			a := model.PlatformArtifact{Content: map[string]any{"policy": policy, "other": []any{"unrelated"}}}
			got, err := physicalCapabilitiesRequired(a)
			if err != nil {
				if _, oldErr := physicalNetworkCapabilityRequired(a); oldErr == nil {
					t.Fatal("new decoder rejected previously valid policy", err)
				}
				continue
			}
			for _, pair := range []struct {
				actual bool
				old    func(model.PlatformArtifact) (bool, error)
			}{{got.network, physicalNetworkCapabilityRequired}, {got.delivery, physicalDeliveryCapabilityRequired}, {got.failureAware, physicalFailureAwareCapabilityRequired}, {got.comparableDelivery, physicalComparableDeliveryCapabilityRequired}, {got.order, physicalOrderCapabilityRequired}, {got.projection, physicalOrderProjectionCapabilityRequired}} {
				want, err := pair.old(a)
				if err != nil || pair.actual != want {
					t.Fatal("capability behavior changed", version, dynamic, err)
				}
			}
		}
	}
	if _, err := physicalCapabilitiesRequired(model.PlatformArtifact{Content: map[string]any{"invalid": make(chan int)}}); err == nil {
		t.Fatal("malformed content accepted")
	}
}
