package platformcontrol

import (
	"testing"
	"time"

	"fugue/internal/model"
)

func TestBuildExpectedConsumerSetIsDeterministicAndScoped(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)
	edgeUS := model.EdgeNode{ID: "edge-us-1", EdgeGroupID: "edge-group-us", Country: "us", Region: "us-west", Status: model.EdgeHealthHealthy}
	edgeDE := model.EdgeNode{ID: "edge-de-1", EdgeGroupID: "edge-group-de", Country: "de", Region: "eu-central", Status: model.EdgeHealthHealthy}
	req := ExpectedConsumerSetBuildRequest{
		ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle,
		Scope:        model.PlatformArtifactScope{Country: "US"},
		ScopeKey:     "country:us",
		Generation:   "route-gen-7",
		PreparedAt:   now,
		Topology: ExpectedConsumerTopology{
			EdgeNodes: []model.EdgeNode{edgeDE, edgeUS},
		},
	}
	first, err := BuildExpectedConsumerSet(req)
	if err != nil {
		t.Fatalf("build expected consumer set: %v", err)
	}
	req.Topology.EdgeNodes = []model.EdgeNode{edgeUS, edgeDE}
	second, err := BuildExpectedConsumerSet(req)
	if err != nil {
		t.Fatalf("build reordered expected consumer set: %v", err)
	}
	if first.TopologyRevision != second.TopologyRevision || first.ID != second.ID {
		t.Fatalf("topology revision must be order-independent: first=%+v second=%+v", first, second)
	}
	if !first.RequiresConsumers || first.RequiredCardinality != 2 || first.OptionalCardinality != 0 {
		t.Fatalf("expected edge worker and caddy front for one scoped edge, got %+v", first)
	}
	for _, consumer := range first.Consumers {
		if consumer.NodeID != edgeUS.ID || consumer.FailureDomain == "" || consumer.Cohort != edgeUS.EdgeGroupID ||
			consumer.ExpectedProtocolVersion != model.PlatformConsumerProtocolVersionV1 || consumer.ExpectedSchemaVersion != model.PlatformConsumerSchemaVersionV1 ||
			consumer.HeartbeatDeadline.IsZero() || consumer.ConvergenceDeadline.IsZero() {
			t.Fatalf("unexpected scoped consumer: %+v", consumer)
		}
	}
}

func TestEvaluateConsumerConvergenceRequiresAllExpectedConsumers(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 1, 0, 0, 0, time.UTC)
	set := mustBuildExpectedConsumerSet(t, ExpectedConsumerSetBuildRequest{
		ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle,
		ScopeKey:     "global",
		Generation:   "route-gen-8",
		PreparedAt:   now,
		Topology: ExpectedConsumerTopology{EdgeNodes: []model.EdgeNode{{
			ID: "edge-1", EdgeGroupID: "edge-group-1", Status: model.EdgeHealthHealthy,
		}}},
	})

	observed := []model.PlatformConsumerInstance{
		passingConsumer(set.Consumers[0], now.Add(20*time.Second)),
		passingConsumer(set.Consumers[1], now.Add(20*time.Second)),
	}
	status := EvaluateConsumerConvergence(set, observed, now.Add(30*time.Second))
	if !status.Pass || status.State != model.InvariantEvidenceStatePass || status.RequiredPassing != 2 || status.RequiredObserved != 2 {
		t.Fatalf("expected complete convergence, got %+v", status)
	}

	status = EvaluateConsumerConvergence(set, observed[:1], now.Add(30*time.Second))
	if status.Pass || status.State != model.InvariantEvidenceStateUnknown || status.RequiredObserved != 1 {
		t.Fatalf("missing required consumer must be unknown and blocking, got %+v", status)
	}
}

func TestEvaluateConsumerConvergenceRejectsZeroConsumerStaleAndEmptyEvidence(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 2, 0, 0, 0, time.UTC)
	emptySet := mustBuildExpectedConsumerSet(t, ExpectedConsumerSetBuildRequest{
		ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle,
		ScopeKey:     "global",
		Generation:   "dns-gen-2",
		PreparedAt:   now,
	})
	emptyStatus := EvaluateConsumerConvergence(emptySet, nil, now)
	if emptyStatus.Pass || emptyStatus.State != model.InvariantEvidenceStateUnknown {
		t.Fatalf("zero expected consumers for a consumer-backed artifact must not pass: %+v", emptyStatus)
	}

	set := mustBuildExpectedConsumerSet(t, ExpectedConsumerSetBuildRequest{
		ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle,
		ScopeKey:     "global",
		Generation:   "dns-gen-2",
		PreparedAt:   now,
		Topology: ExpectedConsumerTopology{DNSNodes: []model.DNSNode{{
			ID: "dns-1", EdgeGroupID: "edge-group-1", Zone: "fugue.pro", Status: model.EdgeHealthHealthy,
		}}},
	})
	stale := passingConsumer(set.Consumers[0], now.Add(-5*time.Minute))
	status := EvaluateConsumerConvergence(set, []model.PlatformConsumerInstance{stale}, now)
	if status.Pass || status.State != model.InvariantEvidenceStateStale {
		t.Fatalf("stale heartbeat must not pass: %+v", status)
	}

	empty := passingConsumer(set.Consumers[0], now)
	empty.DesiredGeneration = ""
	empty.ApplyStatus = ""
	empty.ProbeStatus = "unknown"
	status = EvaluateConsumerConvergence(set, []model.PlatformConsumerInstance{empty}, now)
	if status.Pass || status.State != model.InvariantEvidenceStateUnknown {
		t.Fatalf("empty or unknown consumer evidence must not pass: %+v", status)
	}
}

func TestEvaluateConsumerConvergenceRejectsDriftIdentityAndExpiredLKG(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 3, 0, 0, 0, time.UTC)
	set := mustBuildExpectedConsumerSet(t, ExpectedConsumerSetBuildRequest{
		ArtifactKind: model.PlatformArtifactKindNodeDesiredState,
		ScopeKey:     "global",
		Generation:   "node-gen-4",
		PreparedAt:   now,
		Topology: ExpectedConsumerTopology{NodeUpdaters: []model.NodeUpdater{{
			ID: "updater-1", ClusterNodeName: "node-1", Status: model.NodeUpdaterStatusActive,
		}}},
	})
	bad := passingConsumer(set.Consumers[0], now)
	bad.Component = model.PlatformConsumerComponentRuntimeAgent
	bad.ActualGeneration = "node-gen-3"
	bad.LKGExpired = true
	status := EvaluateConsumerConvergence(set, []model.PlatformConsumerInstance{bad}, now)
	if status.Pass || status.State != model.InvariantEvidenceStateFail {
		t.Fatalf("identity mismatch, generation drift, and expired LKG must fail: %+v", status)
	}
}

func TestEvaluateConsumerConvergenceSupportsExplicitNNMinusOneCompatibility(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 4, 0, 0, 0, time.UTC)
	set := mustBuildExpectedConsumerSet(t, ExpectedConsumerSetBuildRequest{
		ArtifactKind: model.PlatformArtifactKindRuntimePlacementPlan,
		ScopeKey:     "global",
		Generation:   "placement-gen-5",
		PreparedAt:   now,
		Topology: ExpectedConsumerTopology{Runtimes: []model.Runtime{{
			ID: "runtime-1", ClusterNodeName: "node-1", Status: model.RuntimeStatusActive,
		}}},
	})
	set.Consumers[0].ExpectedProtocolVersion = "v2"
	set.Consumers[0].AcceptedProtocolVersions = []string{"v2", "v1"}
	set.Consumers[0].ExpectedSchemaVersion = "v2"
	set.Consumers[0].AcceptedSchemaVersions = []string{"v2", "v1"}
	set.Consumers[0].CompatibilityCapabilities = []string{"mixed-version-read"}
	consumer := passingConsumer(set.Consumers[0], now)
	consumer.ProtocolVersion = "v1"
	consumer.SchemaVersion = "v1"
	consumer.CompatibilityCapabilities = []string{"mixed-version-read"}
	status := EvaluateConsumerConvergence(set, []model.PlatformConsumerInstance{consumer}, now)
	if !status.Pass {
		t.Fatalf("explicit N/N-1 compatibility must pass: %+v", status)
	}
}

func TestOptionalOfflineConsumerDoesNotBlockRequiredConsumer(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 7, 10, 5, 0, 0, 0, time.UTC)
	set := mustBuildExpectedConsumerSet(t, ExpectedConsumerSetBuildRequest{
		ArtifactKind: model.PlatformArtifactKindRuntimeContinuityPlan,
		ScopeKey:     "global",
		Generation:   "continuity-gen-3",
		PreparedAt:   now,
		Topology: ExpectedConsumerTopology{Runtimes: []model.Runtime{
			{ID: "runtime-active", Status: model.RuntimeStatusActive},
			{ID: "runtime-offline", Status: model.RuntimeStatusOffline},
		}},
	})
	if set.RequiredCardinality != 1 || set.OptionalCardinality != 1 {
		t.Fatalf("expected one required and one optional runtime, got %+v", set)
	}
	var required model.PlatformExpectedConsumer
	for _, consumer := range set.Consumers {
		if consumer.Required {
			required = consumer
		}
	}
	status := EvaluateConsumerConvergence(set, []model.PlatformConsumerInstance{passingConsumer(required, now)}, now)
	if !status.Pass || status.State != model.InvariantEvidenceStatePass {
		t.Fatalf("missing optional consumer must not block required convergence: %+v", status)
	}
}

func mustBuildExpectedConsumerSet(t *testing.T, req ExpectedConsumerSetBuildRequest) model.PlatformExpectedConsumerSet {
	t.Helper()
	set, err := BuildExpectedConsumerSet(req)
	if err != nil {
		t.Fatalf("build expected consumer set: %v", err)
	}
	return set
}

func passingConsumer(expected model.PlatformExpectedConsumer, observedAt time.Time) model.PlatformConsumerInstance {
	return model.PlatformConsumerInstance{
		ConsumerID:                expected.ConsumerID,
		Component:                 expected.Component,
		NodeID:                    expected.NodeID,
		ArtifactKind:              expected.ArtifactKind,
		ScopeKey:                  expected.ScopeKey,
		ProtocolVersion:           expected.ExpectedProtocolVersion,
		SchemaVersion:             expected.ExpectedSchemaVersion,
		CompatibilityCapabilities: append([]string(nil), expected.CompatibilityCapabilities...),
		DesiredGeneration:         expected.ExpectedGeneration,
		ActualGeneration:          expected.ExpectedGeneration,
		LKGGeneration:             "verified-lkg",
		ApplyStatus:               model.PlatformConsumerApplyStatusApplied,
		ProbeStatus:               model.PlatformConsumerProbeStatusPassed,
		LastHeartbeatAt:           observedAt,
		UpdatedAt:                 observedAt,
	}
}
