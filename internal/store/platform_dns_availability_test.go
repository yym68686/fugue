package store

import (
	"reflect"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
)

func TestDNSAvailabilityTransactionPreservesServingAndRejectsStaleAuthority(t *testing.T) {
	for _, scenario := range []string{"success", "stale_policy", "stale_full", "unverified", "other_constraint", "two_hosts", "wrong_operation"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := physicalDNSReconfigurationFixture(t, "")
			previous, err := platformproducer.Decode(fixture.old)
			if err != nil {
				t.Fatal(err)
			}
			source, err := fixture.s.GetPlatformArtifact(previous.DNSPolicyArtifactID)
			if err != nil {
				t.Fatal(err)
			}
			source.Content["generation"] = "availability-source"
			query := source.Content["dns_query_policy"].(map[string]any)
			routes := []platformconfig.DNSAvailabilityRoute{{Hostname: "app.example.test", Policy: model.DNSAvailabilityPolicy{Version: model.DNSAvailabilityVersion, OrderedEdgeIDs: []string{"edge-a", "edge-b"}, RecoverySuccesses: 3, RecoveryMinSeconds: 60}}}
			if scenario == "two_hosts" {
				other := routes[0]
				other.Hostname = "other.example.test"
				routes = append(routes, other)
			}
			query["availability_routes"] = routes
			if scenario == "other_constraint" {
				query["maximum_ttl_seconds"] = float64(180)
			}
			source = savePhysicalDNSTestArtifact(t, fixture.s, source.ArtifactKind, source.ScopeKey, "availability-source", source.Content)
			next := previous
			next.Generation = "availability-producer"
			next.DNSPolicyArtifactID, next.DNSPolicyDigest = source.ID, source.ContentHash
			fixture.next = savePhysicalDNSTestArtifact(t, fixture.s, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, next.Generation, next)
			fixture.request.ProducerReconfiguration.Operation = "dns_availability"
			switch scenario {
			case "stale_policy":
				fixture.request.ProducerReconfiguration.PreviousPolicy.FencingToken++
			case "stale_full":
				fixture.request.ProducerReconfiguration.ServingFull.FencingToken++
			case "unverified":
				fixture.setBaselineVerification(t, false)
			case "wrong_operation":
				fixture.request.ProducerReconfiguration.Operation = "dynamic_quality"
			}
			fixture.bindKey(t)
			release, err := fixture.release()
			if (scenario == "success") != (err == nil) {
				t.Fatalf("%s: %v", scenario, err)
			}
			if scenario == "success" {
				again, err := fixture.release()
				if err != nil || again.ID != release.ID {
					t.Fatal("retry not idempotent", err)
				}
			}
			artifact, _, found, err := fixture.s.GetActivePlatformArtifact(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, "shadow")
			want := fixture.old.ID
			if scenario == "success" {
				want = fixture.next.ID
			}
			if err != nil || !found || artifact.ID != want {
				t.Fatal("failed transaction changed authority", err)
			}
			lkg, err := fixture.s.GetPlatformLKG(model.PlatformArtifactKindReleaseSet, "global")
			if err != nil || !reflect.DeepEqual(lkg, &fixture.lkg) {
				t.Fatal("configuration overwrote positive LKG", err)
			}
			_, full, found, err := fixture.s.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "full")
			if err != nil || !found || full.ID != fixture.request.ProducerReconfiguration.ServingFull.ReleaseID {
				t.Fatal("configuration changed serving publication", err)
			}
		})
	}
}
