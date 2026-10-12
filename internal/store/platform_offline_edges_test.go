package store

import (
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformproducer"
)

func TestOfflineConfigurationIsFencedAndPreservesServingLKG(t *testing.T) {
	for _, scenario := range []string{"success", "stale_full", "constraint_changed", "static_changed", "frozen", "two_holds", "invalid_date"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := physicalDNSFixtureWithQuery(t, "", func(query map[string]any) {})
			previous, err := platformproducer.Decode(fixture.old)
			if err != nil {
				t.Fatal(err)
			}
			source, err := fixture.s.GetPlatformArtifact(previous.DNSPolicyArtifactID)
			if err != nil {
				t.Fatal(err)
			}
			holds := []platformproducer.OfflineEdge{{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.8.8", Reason: "quota_exhausted", ExpectedRecoveryDate: "2020-01-01"}}
			if scenario == "two_holds" {
				holds = append(holds, platformproducer.OfflineEdge{EdgeID: "edge-b", EdgeGroupID: "edge-group-a", PublicIPv4: "9.9.9.9", Reason: "maintenance"})
			}
			if scenario == "invalid_date" {
				holds[0].ExpectedRecoveryDate = "yesterday"
			}
			source.Content["generation"], source.Content["offline_edges"] = "dns-offline", holds
			if scenario == "constraint_changed" {
				source.Content["minimum_healthy_edges"] = 2
			}
			source = savePhysicalDNSTestArtifact(t, fixture.s, source.ArtifactKind, source.ScopeKey, "dns-offline", source.Content)
			next := previous
			next.Generation = "producer-offline"
			next.DNSPolicyArtifactID, next.DNSPolicyDigest = source.ID, source.ContentHash
			if scenario == "static_changed" {
				next.StaticIntentArtifactID = "foreign"
			}
			fixture.next = savePhysicalDNSTestArtifact(t, fixture.s, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, next.Generation, next)
			fixture.request.ProducerReconfiguration.Operation = "edge_offline"
			if scenario == "stale_full" {
				fixture.request.ProducerReconfiguration.ServingFull.FencingToken++
			}
			if scenario == "frozen" {
				fixture.freeze(t, "global")
			}
			fixture.bindKey(t)
			_, _, _, _, err = fixture.s.ReleasePlatformArtifact(fixture.next.ID, fixture.request, testPlatformPrincipal())
			if (err == nil) != (scenario == "success") {
				t.Fatal("unexpected offline transition", scenario, err)
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
