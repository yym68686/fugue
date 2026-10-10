package edgequality

import (
	"testing"

	"fugue/internal/model"
)

func TestDeliveryLearningPreservesOnlyProvenCurrentEndpoint(t *testing.T) {
	receipt, binding := selectionFixture(t)
	snapshot := receipt.Snapshot
	snapshot.Policy = DefaultNetworkPolicy()
	snapshot.Policy.Version = DeliveryNetworkPolicyVersion
	snapshot.Observations = nil
	receipt, err := Capture(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	selection, err := CompileSelection(receipt, binding, snapshot.CapturedAt)
	if err != nil || selection.QualityState != "learning" || selection.PrimaryEdgeID != snapshot.CurrentEdgeID || selection.PrimarySince == nil {
		t.Fatal(selection, err)
	}
	snapshot.Policy.Version = model.PhysicalBoundedNetworkPolicyVersion
	receipt, err = Capture(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CompileSelection(receipt, binding, snapshot.CapturedAt); err == nil {
		t.Fatal("learning changed historical policy authority")
	}
	snapshot.Policy.Version = DeliveryNetworkPolicyVersion
	for index := range snapshot.Candidates {
		if snapshot.Candidates[index].EdgeID == snapshot.CurrentEdgeID {
			snapshot.Candidates[index].RouteProofVerified = false
		}
	}
	receipt, err = Capture(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CompileSelection(receipt, binding, snapshot.CapturedAt); err == nil {
		t.Fatal("learning bypassed current route proof")
	}
}
