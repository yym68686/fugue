package api

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformproducer"
)

func TestGlobalOfflineMembershipUsesPinnedInputAndPreservesRequiredSurvivors(t *testing.T) {
	_, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	create := func(kind, scope, generation string, value any) model.PlatformArtifact {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var content map[string]any
		if err := json.Unmarshal(raw, &content); err != nil {
			t.Fatal(err)
		}
		artifact, err := server.store.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: kind, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: scope}, Generation: generation, Content: content})
		if err != nil {
			t.Fatal(err)
		}
		artifact, err = server.store.ValidatePlatformArtifact(artifact.ID, []model.PlatformArtifactValidationResult{{Name: "isolated-fixture", Pass: true}})
		if err != nil {
			t.Fatal(err)
		}
		return artifact
	}
	policy, consumers, dns := pinnedDNSFixture()
	_, _, strategy, _ := directQueryFixture()
	policy.DNSQueryPolicy, policy.DNSPlacementMode = &strategy, platformconfig.DNSPlacementConsumerReadiness
	policy.OfflineEdges = []platformproducer.OfflineEdge{{EdgeID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "1.1.1.1", Reason: "maintenance"}}
	intent := create(model.PlatformArtifactKindPlatformIntent, "global", "static", platformconfig.PlatformIntent{SchemaVersion: platformconfig.SchemaVersion, Scope: "global", Generation: "static", DNSConsumers: consumers})
	input := create(model.PlatformArtifactKindPolicySnapshot, "global", policy.Generation, policy)
	producer := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "producer", Mode: "serving", InputSource: "business-static-intent", StaticIntentArtifactID: intent.ID, StaticIntentDigest: intent.ContentHash, DNSPolicyArtifactID: input.ID, DNSPolicyDigest: input.ContentHash, RequireDNSQueryPolicy: true, TargetScope: "global", IntervalSeconds: 60, RefreshSeconds: 600, Serving: &platformproducer.ServingPolicy{CanaryRuleRef: "cohort=complete", GrayMinSeconds: 120, FullMinSeconds: 120, RolloutTimeoutSeconds: 600}}
	producer.RequireApplicationDomains, producer.RequireRouteDefaults = true, true
	authority := create(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, producer.Generation, producer)
	_, release, _, _, err := server.store.ReleasePlatformArtifact(authority.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, platformProducerPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	plan := platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{{EdgeID: "edge-b", Address: "9.9.9.9:443"}}, Records: []platformconfig.DNSReadinessRecord{{Hostname: "app.example.test", Targets: []platformconfig.DNSReadinessTarget{{EdgeID: "edge-b", Address: "9.9.9.9"}}}}}
	child := create(model.PlatformArtifactKindDNSAnswerBundle, "global", "candidate-dns", map[string]any{"readiness_plan": plan})
	parent := model.PlatformArtifact{ID: "parent", ArtifactKind: model.PlatformArtifactKindReleaseSet, ScopeKey: "global", CreatedByType: model.ActorTypeBootstrap, CreatedByID: platformproducer.Actor,
		Content: map[string]any{"artifact_ids": []any{child.ID}, "artifact_kinds": []any{model.PlatformArtifactKindDNSAnswerBundle}}, Metadata: map[string]string{platformproducer.DNSPolicyIDMetadata: input.ID, platformproducer.DNSPolicyDigestMetadata: input.ContentHash, platformproducer.StaticIntentIDMetadata: intent.ID, platformproducer.StaticIntentDigestMetadata: intent.ContentHash, platformproducer.PolicyReleaseMetadata: release.ID}}
	topology := platformcontrol.ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{ID: "edge-a", EdgeGroupID: "edge-group-a", PublicIPv4: "1.1.1.1", Healthy: true}, {ID: "edge-b", EdgeGroupID: "edge-group-a", PublicIPv4: "9.9.9.9", Healthy: false}}, DNSNodes: dns}
	for _, scenario := range []string{"valid", "source_digest", "policy_release", "static_digest", "creator", "candidate", "identity"} {
		t.Run(scenario, func(t *testing.T) {
			candidate := parent
			candidate.Metadata = map[string]string{}
			for key, value := range parent.Metadata {
				candidate.Metadata[key] = value
			}
			inventory := topology
			inventory.EdgeNodes = append([]model.EdgeNode(nil), topology.EdgeNodes...)
			switch scenario {
			case "source_digest":
				candidate.Metadata[platformproducer.DNSPolicyDigestMetadata] = "foreign"
			case "policy_release":
				candidate.Metadata[platformproducer.PolicyReleaseMetadata] = "missing"
			case "static_digest":
				candidate.Metadata[platformproducer.StaticIntentDigestMetadata] = "foreign"
			case "creator":
				candidate.CreatedByID = "foreign"
			case "candidate":
				bad := plan
				bad.Probes = []platformconfig.DNSReadinessProbe{{EdgeID: "edge-a", Address: "1.1.1.1:443"}}
				other := create(model.PlatformArtifactKindDNSAnswerBundle, "global", "unsafe-dns", map[string]any{"readiness_plan": bad})
				candidate.Content = map[string]any{"artifact_ids": []any{other.ID}, "artifact_kinds": []any{model.PlatformArtifactKindDNSAnswerBundle}}
			case "identity":
				inventory.EdgeNodes[0].PublicIPv4 = "8.8.4.4"
			}
			selected, err := server.applyGlobalOfflineConsumerIntent(candidate, inventory)
			if (err == nil) != (scenario == "valid") {
				t.Fatal("unexpected offline consumer outcome", scenario, err)
			}
			if err != nil {
				return
			}
			if !reflect.DeepEqual(selected.EdgeNodes, topology.EdgeNodes[1:]) || !reflect.DeepEqual(selected.DNSNodes, topology.DNSNodes) {
				t.Fatal("unrelated membership changed")
			}
			set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: "parent", ArtifactReleaseID: "candidate", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ScopeKey: "global", Generation: "generation", Revision: 1, PreparedAt: time.Now(), Topology: selected})
			if err != nil || set.RequiredCardinality != 1 || set.OptionalCardinality != 0 || len(set.Consumers) != 1 || !set.Consumers[0].Required || set.Consumers[0].NodeID != "edge-b" {
				t.Fatal("survivor ceased to require exact convergence", set, err)
			}
		})
	}
	without := policy
	without.Generation, without.OfflineEdges = "before-hold", nil
	previous := create(model.PlatformArtifactKindPolicySnapshot, "global", without.Generation, without)
	parent.Metadata[platformproducer.DNSPolicyIDMetadata], parent.Metadata[platformproducer.DNSPolicyDigestMetadata] = previous.ID, previous.ContentHash
	selected, err := server.applyGlobalOfflineConsumerIntent(parent, topology)
	if err != nil || !reflect.DeepEqual(selected, topology) {
		t.Fatal("current hold changed a parent pinned to the previous input", err)
	}
}
