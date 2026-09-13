package api

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestLegacyConsumerFactsProjectObservedStateWithoutTrustingIt(t *testing.T) {
	state, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	set := model.PlatformExpectedConsumerSet{
		ID: "expected-legacy-facts", ReleaseSetID: "release-shadow", ArtifactReleaseID: "release-id",
		ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, Scope: model.PlatformArtifactScope{ScopeType: "global", Key: "global"}, ScopeKey: "global",
		ExpectedGeneration: "route-shadow", TopologyRevision: "topology-1", Revision: 1, RequiresConsumers: true, RequiredCardinality: 1,
		HeartbeatDeadline: time.Now().UTC().Add(time.Minute), ConvergenceDeadline: time.Now().UTC().Add(2 * time.Minute),
		Consumers: []model.PlatformExpectedConsumer{{ConsumerID: "edge-worker:node-a", Component: model.PlatformConsumerComponentEdgeWorker, NodeID: "node-a", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ScopeKey: "global", FailureDomain: "edge-group:group-a", Cohort: "group-a", Required: true, ExpectedProtocolVersion: "v1", AcceptedProtocolVersions: []string{"v1"}, ExpectedSchemaVersion: "v1", AcceptedSchemaVersions: []string{"v1"}, ExpectedGeneration: "route-shadow", HeartbeatFreshnessSeconds: 90, HeartbeatDeadline: time.Now().UTC().Add(time.Minute), ConvergenceDeadline: time.Now().UTC().Add(2 * time.Minute)}},
	}
	if _, err := state.CreatePlatformExpectedConsumerSet(set); err != nil {
		t.Fatal(err)
	}
	server.recordLegacyConsumerFacts("node-a", "group-a", "route-old", "", "", "lkg-old", true, "")
	consumers, err := state.ListPlatformConsumers(model.PlatformArtifactKindEdgeRouteBundle, "global")
	if err != nil || len(consumers) != 1 {
		t.Fatalf("expected one projected consumer fact: %d %v", len(consumers), err)
	}
	if consumers[0].IdentityVerified || consumers[0].ActualGeneration != "route-old" || consumers[0].ApplyStatus != model.PlatformConsumerApplyStatusApplied || consumers[0].ProbeStatus != model.PlatformConsumerProbeStatusPassed {
		t.Fatalf("legacy fact was incorrectly trusted or lost: %+v", consumers[0])
	}
	status := server.validateReleaseSetConvergence(model.PlatformArtifact{ID: "release-shadow"})
	if status.Pass || !strings.Contains(status.Message, "have not converged") {
		t.Fatalf("legacy old generation must not pass convergence: %+v", status)
	}
}
