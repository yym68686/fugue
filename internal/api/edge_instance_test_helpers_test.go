package api

import (
	"fmt"
	"testing"

	"fugue/internal/model"
	"fugue/internal/store"
)

type edgeRouteHeartbeatStore interface {
	UpdateEdgeHeartbeat(model.EdgeNode) (model.EdgeNode, model.EdgeGroup, error)
	UpdateEdgeInstanceHeartbeat(model.EdgeNodeInstance) (model.EdgeNodeInstance, error)
}

func activateExactEpochForAPITest(t *testing.T, storeState *store.Store, instances ...model.EdgeNodeInstance) {
	t.Helper()
	state, err := storeState.GetEdgeActivationState()
	if err != nil {
		t.Fatal(err)
	}
	advance := func(phase string, expected []model.EdgeExpectedInstance, epochs []model.EdgeActiveEpoch, apiGeneration string) {
		state, err = storeState.AdvanceEdgeActivation(model.EdgeActivationAdvance{
			ExpectedGeneration: state.Generation, ToPhase: phase,
			PlanDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", EvidenceDigest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			ReleaseID: "test-release", ReleaseRecordUID: "test-record", ReleaseRecordVersion: "1", ReleaseRecordDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			ExpectedInstances: expected, ActiveEpochs: epochs, LegacySnapshotDigest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			APIReplicaGeneration: apiGeneration, Actor: "bootstrap/test",
			ReleaseFence: "github:test/repo:1:1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			PhaseNonce:   fmt.Sprintf("sha256:%064x", state.Generation), AuthorizationDigest: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			AuthorizationKeyID: "test-key", AuthorizationKeyGeneration: "generation-test", AuthorizationRunnerObservedSecretUID: "secret-uid", AuthorizationRunnerObservedSecretVersion: "1",
		})
		if err != nil {
			t.Fatalf("advance edge activation to %s: %v", phase, err)
		}
	}
	advance(model.EdgeActivationPhaseShadow, nil, nil, "")
	expected := make([]model.EdgeExpectedInstance, 0, len(instances))
	epochByGroup := map[string]model.EdgeActiveEpoch{}
	for _, instance := range instances {
		expected = append(expected, model.EdgeExpectedInstance{EdgeID: instance.EdgeID, EdgeGroupID: instance.EdgeGroupID, Slot: instance.Slot, InstanceUID: instance.InstanceUID, ReleaseEpoch: instance.ReleaseEpoch})
		epochByGroup[instance.EdgeGroupID] = model.EdgeActiveEpoch{EdgeGroupID: instance.EdgeGroupID, Slot: instance.Slot, ReleaseEpoch: instance.ReleaseEpoch, FenceSequence: 1, MinHealthyInstances: 1}
	}
	epochs := make([]model.EdgeActiveEpoch, 0, len(epochByGroup))
	for _, epoch := range epochByGroup {
		epochs = append(epochs, epoch)
	}
	advance(model.EdgeActivationPhaseFenced, expected, epochs, "")
	advance(model.EdgeActivationPhaseActive, expected, nil, "api-test-generation")
}

func recordActiveEdgeHeartbeatForAPITest(t *testing.T, storeState edgeRouteHeartbeatStore, node model.EdgeNode) error {
	t.Helper()
	if _, _, err := storeState.UpdateEdgeHeartbeat(node); err != nil {
		return err
	}
	epoch := "test-" + node.EdgeGroupID
	if node.TLSStatus == "" {
		node.TLSStatus = model.EdgeTLSStatusReady
	}
	instance := model.EdgeNodeInstance{
		EdgeID: node.ID, EdgeGroupID: node.EdgeGroupID, Slot: model.EdgeSlotDirect,
		InstanceUID: "test-" + node.ID, ReleaseEpoch: epoch, Node: node,
	}
	for observation := 0; observation < 2; observation++ {
		if _, err := storeState.UpdateEdgeInstanceHeartbeat(instance); err != nil {
			return err
		}
	}
	return nil
}
