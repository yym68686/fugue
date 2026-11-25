package store

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/platformsafety"
)

func TestHostedFleetHandoffRequiresExactDynamicQualityActivation(t *testing.T) {
	for _, scenario := range []string{"fleet only", "also changes refresh", "different evidence", "same producer", "moved full"} {
		t.Run(scenario, func(t *testing.T) {
			policy := edgequality.DefaultNetworkPolicy()
			policy.Version = edgequality.DeliveryNetworkPolicyVersion
			dynamic := platformconfig.DynamicQualityPolicy{Mode: "all_dynamic", Policy: policy, RefreshQueriesPerCycle: 16, RefreshConcurrency: 4}
			f := physicalDNSFixtureWithQuery(t, "", func(query map[string]any) {
				query["ordered_projection"] = platformconfig.DNSOrderedProjection{DefaultOrder: model.DNSPhysicalOrder{Version: "physical-order-v1", OrderedEdgeIDs: []string{"edge-a"}}, Overrides: []platformconfig.DNSOrderOverride{}}
				query["dynamic_quality"] = dynamic
			})
			r := f.request.ProducerReconfiguration
			mockDigest := "sha256:" + strings.Repeat("b", 64)
			reference, err := json.Marshal(platformconfig.PreviousTrafficPublicationReference{ReleaseSetID: "older-parent", ReleaseSetDigest: mockDigest, ReleaseID: "older-full", FencingToken: 1, RouteArtifactID: "older-route", RouteArtifactDigest: mockDigest, TLSArtifactID: "older-tls", TLSArtifactDigest: mockDigest, DNSArtifactID: "older-dns", DNSArtifactDigest: mockDigest})
			if err != nil {
				t.Fatal(err)
			}
			keys := f.s.platformArtifactSigningKeyring()
			if err := f.s.withLockedState(true, func(state *model.State) error {
				i := platformArtifactIndex(state.PlatformArtifacts, r.ServingFull.ArtifactID)
				base := state.PlatformArtifacts[i]
				base.Metadata[platformconfig.HostedAddressSourceMetadata] = string(reference)
				base.Metadata[platformconfig.HostedAddressReservationsDigestMetadata] = mockDigest
				base, err = platformsafety.SignPlatformArtifact(base, keys)
				if err != nil {
					return err
				}
				state.PlatformArtifacts[i] = base
				r.ServingFull.ContentHash = base.ContentHash
				f.lkg.ArtifactProvenance = base.Provenance
				f.lkg.ContentHash = base.ContentHash
				f.lkg, err = platformsafety.SignPlatformLKGSnapshot(f.lkg, keys)
				state.PlatformLKGSnapshots[0] = f.lkg
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if scenario == "same producer" {
				if err := f.s.ValidateDNSProducerSelection(t.Context(), r.PreviousPolicy, r.ServingFull); err != nil {
					t.Fatal("ordinary same-policy read must still work", err)
				}
				if err := f.s.ValidateHostedFleetHandoffSelection(t.Context(), r.PreviousPolicy, r.ServingFull); !errors.Is(err, ErrConflict) {
					t.Fatal("ordinary policy was allowed to use handoff authority", err)
				}
				return
			}
			previous, err := platformproducer.Decode(f.old)
			if err != nil {
				t.Fatal(err)
			}
			source, err := f.s.GetPlatformArtifact(previous.DNSPolicyArtifactID)
			if err != nil {
				t.Fatal(err)
			}
			dynamic.FleetDefault, dynamic.FleetDefaultHosts = "active", []string{"app.example.test"}
			if scenario == "also changes refresh" {
				dynamic.RefreshConcurrency++
			}
			source.Content["generation"] = "dns-fleet-default"
			source.Content["dns_query_policy"].(map[string]any)["dynamic_quality"] = dynamic
			source = savePhysicalDNSTestArtifact(t, f.s, source.ArtifactKind, source.ScopeKey, "dns-fleet-default", source.Content)
			next := previous
			next.Generation = "producer-fleet-default"
			next.DNSPolicyArtifactID, next.DNSPolicyDigest = source.ID, source.ContentHash
			f.next = savePhysicalDNSTestArtifact(t, f.s, model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, next.Generation, next)
			r.Operation = "dynamic_quality"
			f.bindKey(t)
			active, err := f.release()
			if err != nil {
				t.Fatal("exact activation", err)
			}
			pin := model.PlatformPublicationPrecondition{ArtifactID: f.next.ID, ContentHash: f.next.ContentHash, ReleaseID: active.ID, FencingToken: active.FencingToken}
			if err := f.s.ValidateDNSProducerSelection(t.Context(), pin, r.ServingFull); !errors.Is(err, ErrConflict) {
				t.Fatal("ordinary authority crossed producer policies", err)
			}
			if scenario == "different evidence" {
				if err := f.s.withLockedState(true, func(state *model.State) error {
					f.lkg.VerificationEvidenceHash = mockDigest
					lkg, err := platformsafety.SignPlatformLKGSnapshot(f.lkg, keys)
					state.PlatformLKGSnapshots[0] = lkg
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "moved full" {
				r.ServingFull.FencingToken++
			}
			err = f.s.ValidateHostedFleetHandoffSelection(t.Context(), pin, r.ServingFull)
			if (scenario == "fleet only" && err != nil) || (scenario != "fleet only" && !errors.Is(err, ErrConflict)) {
				t.Fatal("unexpected handoff result", err)
			}
		})
	}
}

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
