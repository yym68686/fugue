package store

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

const (
	edgeActivationTestPlan     = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	edgeActivationTestEvidence = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	edgeActivationTestRecord   = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	edgeActivationTestLegacy   = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
)

func TestEdgeActivationPhasedCutoverAndCrashReplay(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "store.json")
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	createEdgeInstanceControl(t, s, "edge-de-1", "edge-group-country-de")
	legacy := model.EdgeNode{ID: "edge-de-1", EdgeGroupID: "edge-group-country-de", Healthy: true, Status: model.EdgeHealthHealthy, TLSStatus: model.EdgeTLSStatusReady, RouteBundleVersion: "legacy", CaddyRouteCount: 1}
	if _, _, err := s.UpdateEdgeHeartbeat(legacy); err != nil {
		t.Fatal(err)
	}
	state, err := s.GetEdgeActivationState()
	if err != nil || state.Phase != model.EdgeActivationPhaseLegacyAuthoritative || state.RouteAuthority != model.EdgeRouteAuthorityLegacy {
		t.Fatalf("default activation drifted: state=%+v err=%v", state, err)
	}

	state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseShadow, nil, nil, "")
	candidate := healthyEdgeTestInstance("edge-de-1", "edge-group-country-de", model.EdgeSlotB, "pod-b", "release-b")
	heartbeatEdgeInstanceTwice(t, s, candidate)
	expected := []model.EdgeExpectedInstance{{EdgeID: candidate.EdgeID, EdgeGroupID: candidate.EdgeGroupID, Slot: candidate.Slot, InstanceUID: candidate.InstanceUID, ReleaseEpoch: candidate.ReleaseEpoch}}
	epochs := []model.EdgeActiveEpoch{{EdgeGroupID: candidate.EdgeGroupID, Slot: candidate.Slot, ReleaseEpoch: candidate.ReleaseEpoch, FenceSequence: 2, MinHealthyInstances: 1}}
	state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseFenced, expected, epochs, "")
	nodes, _, err := s.ListActiveEdgeNodes(candidate.EdgeGroupID)
	if err != nil || len(nodes) != 1 || nodes[0].RouteBundleVersion != "legacy" {
		t.Fatalf("phase2 must leave legacy routing authoritative: nodes=%+v err=%v", nodes, err)
	}

	state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseActive, expected, nil, "api-generation-7")
	nodes, _, err = s.ListActiveEdgeNodes(candidate.EdgeGroupID)
	if err != nil || len(nodes) != 1 || !nodes[0].Healthy || nodes[0].RouteBundleVersion != "routegen-current" {
		t.Fatalf("phase3 did not atomically select candidate: nodes=%+v err=%v", nodes, err)
	}
	if len(state.Receipts) != 3 || state.RouteAuthority != model.EdgeRouteAuthorityActiveEpoch || state.SoakStartedAt == nil {
		t.Fatalf("cutover receipt state drifted: %+v", state)
	}

	reopened := New(path)
	if err := reopened.Init(); err != nil {
		t.Fatalf("reopen after cutover: %v", err)
	}
	replayed, err := reopened.GetEdgeActivationState()
	if err != nil || replayed.Generation != state.Generation || replayed.Phase != state.Phase || len(replayed.Receipts) != len(state.Receipts) {
		t.Fatalf("durable activation replay drifted: state=%+v err=%v", replayed, err)
	}
	state = advanceEdgeActivationTest(t, reopened, replayed, model.EdgeActivationPhaseEnforced, expected, nil, "api-generation-7")
	if _, _, err := reopened.UpdateEdgeHeartbeat(legacy); !errors.Is(err, ErrConflict) {
		t.Fatalf("phase4 must fence legacy route-health writes, got %v", err)
	}
	if state.Phase != model.EdgeActivationPhaseEnforced || len(state.Receipts) != 4 {
		t.Fatalf("enforced state drifted: %+v", state)
	}
}

func TestEdgeActivationRejectsSkippedOrUnhealthyFence(t *testing.T) {
	t.Parallel()
	s := newEdgeInstanceTestStore(t)
	createEdgeInstanceControl(t, s, "edge-us-1", "edge-group-country-us")
	state, _ := s.GetEdgeActivationState()
	advance := edgeActivationTestAdvance(state, model.EdgeActivationPhaseActive, nil, nil, "api-generation-1")
	if _, err := s.AdvanceEdgeActivation(advance); !errors.Is(err, ErrConflict) {
		t.Fatalf("phase skip must conflict, got %v", err)
	}
	state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseShadow, nil, nil, "")
	instance := healthyEdgeTestInstance("edge-us-1", "edge-group-country-us", model.EdgeSlotB, "pod-b", "release-b")
	if _, err := s.UpdateEdgeInstanceHeartbeat(instance); err != nil {
		t.Fatal(err)
	}
	expected := []model.EdgeExpectedInstance{{EdgeID: instance.EdgeID, EdgeGroupID: instance.EdgeGroupID, Slot: instance.Slot, InstanceUID: instance.InstanceUID, ReleaseEpoch: instance.ReleaseEpoch}}
	epochs := []model.EdgeActiveEpoch{{EdgeGroupID: instance.EdgeGroupID, Slot: instance.Slot, ReleaseEpoch: instance.ReleaseEpoch, FenceSequence: 1, MinHealthyInstances: 1}}
	advance = edgeActivationTestAdvance(state, model.EdgeActivationPhaseFenced, expected, epochs, "")
	if _, err := s.AdvanceEdgeActivation(advance); !errors.Is(err, ErrEdgeInstanceFencingNotReady) {
		t.Fatalf("one healthy observation must not pass phase2, got %v", err)
	}
}

func advanceEdgeActivationTest(t *testing.T, s *Store, current model.EdgeActivationState, phase string, expected []model.EdgeExpectedInstance, epochs []model.EdgeActiveEpoch, apiGeneration string) model.EdgeActivationState {
	t.Helper()
	next, err := s.AdvanceEdgeActivation(edgeActivationTestAdvance(current, phase, expected, epochs, apiGeneration))
	if err != nil {
		t.Fatalf("advance %s -> %s: %v", current.Phase, phase, err)
	}
	return next
}

func edgeActivationTestAdvance(current model.EdgeActivationState, phase string, expected []model.EdgeExpectedInstance, epochs []model.EdgeActiveEpoch, apiGeneration string) model.EdgeActivationAdvance {
	return model.EdgeActivationAdvance{
		ExpectedGeneration: current.Generation, ToPhase: phase,
		PlanDigest: edgeActivationTestPlan, EvidenceDigest: edgeActivationTestEvidence,
		ReleaseID: "release-test", ReleaseRecordUID: "record-uid", ReleaseRecordVersion: "10", ReleaseRecordDigest: edgeActivationTestRecord,
		ExpectedInstances: expected, ActiveEpochs: epochs, LegacySnapshotDigest: edgeActivationTestLegacy,
		APIReplicaGeneration: apiGeneration, Actor: "bootstrap/test",
		ReleaseFence: "github:test/repo:1:1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		PhaseNonce:   fmt.Sprintf("sha256:%064x", current.Generation), AuthorizationDigest: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		AuthorizationKeyID: "test-key", AuthorizationKeyGeneration: "generation-test", AuthorizationRunnerObservedSecretUID: "secret-uid", AuthorizationRunnerObservedSecretVersion: "1",
	}
}
