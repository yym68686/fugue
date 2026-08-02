package store

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

func TestEdgeRemediationIsDurableCASAndCannotExpandOnActiveFailure(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "store.json")
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	active := healthyEdgeTestInstance("edge-us", "edge-group-country-us", model.EdgeSlotB, "active-pod", "release-active")
	if _, _, err := s.UpdateEdgeHeartbeat(active.Node); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		if _, err := s.UpdateEdgeInstanceHeartbeat(active); err != nil {
			t.Fatal(err)
		}
	}
	state, _ := s.GetEdgeActivationState()
	state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseShadow, nil, nil, "")
	expected := []model.EdgeExpectedInstance{{EdgeID: active.EdgeID, EdgeGroupID: active.EdgeGroupID, Slot: active.Slot, InstanceUID: active.InstanceUID, ReleaseEpoch: active.ReleaseEpoch}}
	epochs := []model.EdgeActiveEpoch{{EdgeGroupID: active.EdgeGroupID, Slot: active.Slot, ReleaseEpoch: active.ReleaseEpoch, FenceSequence: 7, MinHealthyInstances: 1}}
	state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseFenced, expected, epochs, "")
	state = advanceEdgeActivationTest(t, s, state, model.EdgeActivationPhaseActive, expected, nil, "api-generation")

	inactive := healthyEdgeTestInstance("edge-us", active.EdgeGroupID, model.EdgeSlotA, "inactive-pod", "release-old")
	inactive.Node.Healthy = false
	inactive.Node.Status = model.EdgeHealthUnhealthy
	inactive.FailureClass = model.EdgeInstanceFailureSignatureInvalid
	if _, err := s.UpdateEdgeInstanceHeartbeat(inactive); err != nil {
		t.Fatal(err)
	}
	target := model.EdgeRemediationTarget{EdgeID: inactive.EdgeID, EdgeGroupID: inactive.EdgeGroupID, Slot: inactive.Slot, InstanceUID: inactive.InstanceUID, ReleaseEpoch: inactive.ReleaseEpoch, DaemonSetName: "fugue-edge-a", DaemonSetUID: "daemonset-a", DaemonSetVersion: "42", FailureClass: inactive.FailureClass}
	prepare := edgeRemediationTestAdvance(state, 0, model.EdgeRemediationPhasePrepared, target)
	state, err := s.AdvanceEdgeRemediation(prepare)
	if err != nil {
		t.Fatal(err)
	}
	if state.Remediation == nil || state.Remediation.Phase != model.EdgeRemediationPhasePrepared || state.Remediation.Sequence != 1 {
		t.Fatalf("remediation prepare was not durable: %+v", state.Remediation)
	}
	if _, err := s.AdvanceEdgeActivation(edgeActivationTestAdvance(state, model.EdgeActivationPhaseEnforced, expected, nil, "api-generation")); !errors.Is(err, ErrConflict) {
		t.Fatalf("release must not race a prepared remediation: %v", err)
	}
	commit := edgeRemediationTestAdvance(state, 1, model.EdgeRemediationPhaseCommitted, target)
	commit.ReleaseFence = state.Remediation.ReleaseFence
	commit.Nonce = state.Remediation.Nonce
	commit.AuthorizationDigest = state.Remediation.AuthorizationDigest
	state, err = s.AdvanceEdgeRemediation(commit)
	if err != nil {
		t.Fatal(err)
	}
	if state.Remediation == nil || state.Remediation.Phase != model.EdgeRemediationPhaseCommitted {
		t.Fatalf("remediation commit missing: %+v", state.Remediation)
	}
	if _, err := s.AdvanceEdgeActivation(edgeActivationTestAdvance(state, model.EdgeActivationPhaseEnforced, expected, nil, "api-generation")); !errors.Is(err, ErrConflict) {
		t.Fatalf("committed remediation must remain fenced until verification: %v", err)
	}
	if _, err := s.AdvanceEdgeRemediation(commit); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate commit must fail CAS: %v", err)
	}
	verified := edgeRemediationTestAdvance(state, state.Remediation.Sequence, model.EdgeRemediationPhaseVerified, target)
	verified.ReleaseFence = state.Remediation.ReleaseFence
	verified.Nonce = state.Remediation.Nonce
	state, err = s.AdvanceEdgeRemediation(verified)
	if err != nil || state.Remediation.Phase != model.EdgeRemediationPhaseVerified {
		t.Fatalf("verified remediation did not release the action fence: %+v err=%v", state.Remediation, err)
	}

	active.Node.Healthy = false
	active.Node.Status = model.EdgeHealthUnhealthy
	if _, err := s.UpdateEdgeInstanceHeartbeat(active); err != nil {
		t.Fatal(err)
	}
	state, _ = s.GetEdgeActivationState()
	next := edgeRemediationTestAdvance(state, state.Remediation.Sequence, model.EdgeRemediationPhasePrepared, target)
	if _, err := s.AdvanceEdgeRemediation(next); !errors.Is(err, ErrEdgeInstanceFencingNotReady) {
		t.Fatalf("dual-slot failure must not expand remediation: %v", err)
	}
}

func edgeRemediationTestAdvance(state model.EdgeActivationState, actionSequence uint64, phase string, target model.EdgeRemediationTarget) model.EdgeRemediationAdvance {
	nonce := fmt.Sprintf("sha256:%064x", state.Generation+actionSequence+100)
	return model.EdgeRemediationAdvance{
		ExpectedActivationGeneration: state.Generation, ExpectedActionSequence: actionSequence, ToPhase: phase,
		ActiveEvidenceDigest: edgeActivationTestEvidence, PlatformEvidenceDigest: edgeActivationTestLegacy, KubernetesDigest: edgeActivationTestRecord,
		Target: target, Actor: "bootstrap/remediator", ReleaseFence: "github:test/repo:9:1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Nonce: nonce, AuthorizationDigest: edgeActivationTestPlan,
		AuthorizationKeyID: "test-key", AuthorizationKeyGeneration: "generation-test", AuthorizationRunnerObservedSecretUID: "secret-uid", AuthorizationRunnerObservedSecretVersion: "1",
	}
}
