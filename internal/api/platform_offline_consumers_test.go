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
	observedIntent, observedHolds := server.observeGlobalDNSAdmission(time.Now().UTC())
	if observedIntent.State != "allowed" || observedIntent.ProducerPolicyID != authority.ID || observedIntent.ProducerReleaseID != release.ID || observedIntent.InputArtifactID != input.ID || observedIntent.ServingEvidence != "not_evaluated" || !reflect.DeepEqual(observedHolds, policy.OfflineEdges) {
		t.Fatal("observed offline intent lost its exact signed configuration source", observedIntent, observedHolds)
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
	t.Run("retained held Worker never enters DNS or ordinary quorum", func(t *testing.T) {
		at := time.Now().UTC()
		hold := policy.OfflineEdges[0]
		hold.Reason, hold.KeepHostedRouteTLS = "quota_exhausted", true
		hold.Recovery = &platformproducer.OfflineEdgeRecoveryPolicy{Hostname: "app.example.test", Path: "/", ProbeIntervalSeconds: 300, ProbeTimeoutSeconds: 5, SuccessThreshold: 3, FailureThreshold: 1, FreshnessSeconds: 1800}
		next, nextProducer := policy, producer
		next.Generation, next.OfflineEdges = "retained-input", []platformproducer.OfflineEdge{hold}
		source := create(model.PlatformArtifactKindPolicySnapshot, "global", next.Generation, next)
		nextProducer.Generation, nextProducer.DNSPolicyArtifactID, nextProducer.DNSPolicyDigest = "retained-producer", source.ID, source.ContentHash
		authority := create(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, nextProducer.Generation, nextProducer)
		_, release, _, _, err := server.store.ReleasePlatformArtifact(authority.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, platformProducerPrincipal())
		if err != nil {
			t.Fatal(err)
		}
		candidate := parent
		candidate.Provenance.SignedAt = at
		candidate.Metadata = map[string]string{platformproducer.DNSPolicyIDMetadata: source.ID, platformproducer.DNSPolicyDigestMetadata: source.ContentHash,
			platformproducer.StaticIntentIDMetadata: intent.ID, platformproducer.StaticIntentDigestMetadata: intent.ContentHash, platformproducer.PolicyReleaseMetadata: release.ID}
		inventory := topology
		inventory.EdgeNodes = append(append([]model.EdgeNode(nil), topology.EdgeNodes...), model.EdgeNode{ID: "edge-c", EdgeGroupID: "edge-group-a", PublicIPv4: "8.8.4.4"})
		unbound, err := server.applyGlobalOfflineConsumerIntent(candidate, inventory)
		if err != nil || len(unbound.HostedRouteTLSOnly) != 0 || !reflect.DeepEqual(unbound.EdgeNodes, inventory.EdgeNodes[1:]) {
			t.Fatalf("retained a credential-only Worker or added it to ordinary membership: %+v, %v", unbound, err)
		}
		prior := server.workerFrontBindings
		t.Cleanup(func() { server.workerFrontBindings = prior })
		server.workerFrontBindings, err = ParseWorkerFrontBindings(`[{"node":"edge-a","service_uid":"11111111-1111-1111-1111-111111111111"}]`)
		if err != nil {
			t.Fatal(err)
		}
		selected, err := server.applyGlobalOfflineConsumerIntent(candidate, inventory)
		if err != nil || !reflect.DeepEqual(selected.EdgeNodes, inventory.EdgeNodes[1:]) || !reflect.DeepEqual(selected.HostedRouteTLSOnly, inventory.EdgeNodes[:1]) || !reflect.DeepEqual(selected.DNSNodes, inventory.DNSNodes) {
			t.Fatalf("hosted-only registration changed DNS or ordinary Worker membership: %+v, %v", selected, err)
		}
		for _, kind := range []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig, model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindEdgeRankingPolicy} {
			set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: "parent", ArtifactReleaseID: "full", ArtifactKind: kind, ScopeKey: "global", Generation: "generation", Revision: 1, PreparedAt: at, Topology: selected})
			if err != nil {
				t.Fatal(err)
			}
			var marked int
			for _, member := range set.Consumers {
				if member.HostedRouteTLSOnly {
					marked++
					if !platformcontrol.AllowsHostedRouteTLSOnly(member) || member.NodeID != hold.EdgeID {
						t.Fatal(member)
					}
				}
			}
			if kind != model.PlatformArtifactKindEdgeRouteBundle && kind != model.PlatformArtifactKindCaddyRouteConfig {
				if marked != 0 {
					t.Fatalf("%s gained a hosted-only Worker", kind)
				}
				continue
			}
			if marked != 1 || set.RequiredCardinality != 2 || set.OptionalCardinality != 1 {
				t.Fatalf("%s changed the required Worker quorum: %+v", kind, set)
			}
			if current := platformcontrol.ProjectExpectedConsumerSetToTopology(set, selected); !reflect.DeepEqual(current.Consumers, set.Consumers) {
				t.Fatalf("%s projection lost the signed hosted-only role", kind)
			}
			wrong := selected
			wrong.HostedRouteTLSOnly = append([]model.EdgeNode(nil), selected.HostedRouteTLSOnly...)
			wrong.HostedRouteTLSOnly[0].EdgeGroupID = "edge-group-foreign"
			if current := platformcontrol.ProjectExpectedConsumerSetToTopology(set, wrong); current.RequiredCardinality != 2 || current.OptionalCardinality != 0 {
				t.Fatalf("%s projection let the hosted-only role move to a different group", kind)
			}
		}
		bad := plan
		bad.Probes = append(append([]platformconfig.DNSReadinessProbe(nil), plan.Probes...), platformconfig.DNSReadinessProbe{EdgeID: hold.EdgeID, Address: hold.PublicIPv4 + ":443"})
		unsafeDNS := create(model.PlatformArtifactKindDNSAnswerBundle, "global", "retained-unsafe-dns", map[string]any{"readiness_plan": bad})
		candidate.Content = map[string]any{"artifact_ids": []any{unsafeDNS.ID}, "artifact_kinds": []any{model.PlatformArtifactKindDNSAnswerBundle}}
		if _, err := server.applyGlobalOfflineConsumerIntent(candidate, inventory); err == nil {
			t.Fatal("route/TLS flag re-enabled a held DNS probe")
		}
	})
	t.Run("replacement expectation comes from pinned recovered source", func(t *testing.T) {
		at := time.Now().UTC()
		hold := policy.OfflineEdges[0]
		hold.Recovery = &platformproducer.OfflineEdgeRecoveryPolicy{Hostname: "app.example.test", Path: "/", ProbeIntervalSeconds: 300, ProbeTimeoutSeconds: 5, SuccessThreshold: 3, FailureThreshold: 1, FreshnessSeconds: 1800}
		hold.WorkerEnrollment = testWorkerEnrollment(at.Add(-time.Hour))
		next := policy
		next.Generation, next.OfflineEdges = "replacement-input", []platformproducer.OfflineEdge{hold}
		source := create(model.PlatformArtifactKindPolicySnapshot, "global", next.Generation, next)
		producer.Generation, producer.DNSPolicyArtifactID, producer.DNSPolicyDigest = "replacement-producer", source.ID, source.ContentHash
		newPolicy := create(model.PlatformArtifactKindPolicySnapshot, platformproducer.Scope, producer.Generation, producer)
		_, newRelease, _, _, err := server.store.ReleasePlatformArtifact(newPolicy.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "shadow"}, platformProducerPrincipal())
		if err != nil {
			t.Fatal(err)
		}
		withRecoveredEdge := plan
		withRecoveredEdge.Probes = append(append([]platformconfig.DNSReadinessProbe(nil), plan.Probes...), platformconfig.DNSReadinessProbe{EdgeID: hold.EdgeID, Address: "1.1.1.1:443"})
		child := create(model.PlatformArtifactKindDNSAnswerBundle, "global", "replacement-dns", map[string]any{"readiness_plan": withRecoveredEdge})
		newParent := parent
		newParent.Provenance.SignedAt = at
		newParent.Content = map[string]any{"artifact_ids": []any{child.ID}, "artifact_kinds": []any{model.PlatformArtifactKindDNSAnswerBundle}}
		newParent.Metadata = map[string]string{platformproducer.DNSPolicyIDMetadata: source.ID, platformproducer.DNSPolicyDigestMetadata: source.ContentHash,
			platformproducer.StaticIntentIDMetadata: intent.ID, platformproducer.StaticIntentDigestMetadata: intent.ContentHash, platformproducer.PolicyReleaseMetadata: newRelease.ID}
		if _, err := server.applyGlobalOfflineConsumerIntent(newParent, topology); err == nil {
			t.Fatal("DNS included the replacement before signed recovery")
		}
		proof := platformproducer.OfflineRecoveryObservation{EdgeID: hold.EdgeID, DeclarationDigest: platformproducer.OfflineRecoveryDigest(hold), LastAttempt: at, LastSuccess: at, ProofValidUntil: at.Add(time.Minute), Successes: 3, Admitted: true}
		raw, _ := json.Marshal(map[string]any{"captured_at": at, "observations": []platformproducer.OfflineRecoveryObservation{proof}})
		newParent.Metadata[platformproducer.OfflineRecoveryMetadata] = string(raw)
		selected, err := server.applyGlobalOfflineConsumerIntent(newParent, topology)
		if err != nil || len(selected.EdgeNodes) != 2 || len(selected.WorkerEnrollments) != 1 || selected.WorkerEnrollments[hold.EdgeID] != hold.WorkerEnrollment.ID {
			t.Fatal("published topology lost exact replacement identity", selected, err)
		}
		for _, kind := range []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig} {
			set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: "parent", ArtifactReleaseID: "full", ArtifactKind: kind, ScopeKey: "global", Generation: "generation", PreparedAt: at, Topology: selected})
			if err != nil {
				t.Fatal(err)
			}
			legacy := platformcontrol.ProjectExpectedConsumerSetToTopology(set, topology)
			current := platformcontrol.ProjectExpectedConsumerSetToTopology(set, selected)
			if legacy.RequiredCardinality != 1 || current.RequiredCardinality != 2 || !reflect.DeepEqual(current.Consumers, platformcontrol.ProjectExpectedConsumerOwners(set).Consumers) {
				t.Fatal("convergence projection must retain the pinned Worker enrollment", kind, legacy.Consumers, current.Consumers)
			}
		}
	})
}
