package store

import (
	"reflect"
	"testing"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

func TestDynamicQualityTransactionIsFencedAndPreservesServingLKG(t *testing.T) {
	for _, scenario := range []string{"success", "adopt_equivalent", "reject_distinct", "stale_full", "constraint_changed", "static_changed", "frozen"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := physicalDNSFixtureWithQuery(t, "", func(query map[string]any) {
				query["ecs_enabled"], query["exploration_percent"] = false, 0
				query["ordered_projection"] = platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a"}}, Overrides: []platformconfig.DNSOrderOverride{}}
				if scenario == "adopt_equivalent" || scenario == "reject_distinct" {
					policy := edgequality.DefaultNetworkPolicy()
					if scenario == "reject_distinct" {
						policy.AdvantageMS += 10
					}
					query["physical_routes"] = []platformconfig.PhysicalQualityRoute{{Hostname: "app.example.test", TrafficClass: "streaming", Policy: policy}}
				}
			})
			previous, err := platformproducer.Decode(fixture.old)
			if err != nil {
				t.Fatal(err)
			}
			source, err := fixture.s.GetPlatformArtifact(previous.DNSPolicyArtifactID)
			if err != nil {
				t.Fatal(err)
			}
			source.Content["generation"] = "dns-dynamic"
			policy := edgequality.DefaultNetworkPolicy()
			policy.Version = edgequality.DeliveryNetworkPolicyVersion
			source.Content["dns_query_policy"].(map[string]any)["dynamic_quality"] = &platformconfig.DynamicQualityPolicy{Mode: "all_dynamic", Policy: policy, RefreshQueriesPerCycle: 16, RefreshConcurrency: 4}
			if scenario == "adopt_equivalent" || scenario == "reject_distinct" {
				delete(source.Content["dns_query_policy"].(map[string]any), "physical_routes")
			}
			if scenario == "constraint_changed" {
				source.Content["minimum_healthy_edges"] = 2
			}
			source = savePhysicalDNSTestArtifact(t, fixture.s, source.ArtifactKind, source.ScopeKey, "dns-dynamic", source.Content)
			next := previous
			next.Generation = "producer-dynamic"
			next.DNSPolicyArtifactID, next.DNSPolicyDigest = source.ID, source.ContentHash
			if scenario == "static_changed" {
				next.StaticIntentArtifactID = "foreign"
			}
			fixture.next = savePhysicalDNSTestArtifact(t, fixture.s, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, next.Generation, next)
			fixture.request.ProducerReconfiguration.Operation = "dynamic_quality"
			if scenario == "stale_full" {
				fixture.request.ProducerReconfiguration.ServingFull.FencingToken++
			}
			if scenario == "frozen" {
				fixture.freeze(t, "global")
			}
			fixture.bindKey(t)
			_, _, _, _, err = fixture.s.ReleasePlatformArtifact(fixture.next.ID, fixture.request, testPlatformPrincipal())
			if (err == nil) != (scenario == "success" || scenario == "adopt_equivalent") {
				t.Fatal("unexpected configuration outcome", scenario, err)
			}
			lkg, err := fixture.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
			if err != nil || !reflect.DeepEqual(lkg, &fixture.lkg) {
				t.Fatal("configuration changed positive LKG", err)
			}
			_, full, found, err := fixture.s.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "full")
			if err != nil || !found || full.ID != fixture.request.ProducerReconfiguration.ServingFull.ReleaseID {
				t.Fatal("configuration changed serving full", err)
			}
		})
	}
}
