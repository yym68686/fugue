package declarativerelease

import (
	"strings"
	"testing"
)

func TestOwnershipAdoptionReceiptBindsHealthyIndependentOwnership(t *testing.T) {
	component := edgeGroupFixture("alpha", "edge-group-metro-alpha").Control
	receipt := adoptionReceiptFixture(t, component, "edge-group-metro-alpha")
	if err := receipt.Validate(component, receipt.GroupID); err != nil {
		t.Fatalf("valid ownership adoption receipt: %v", err)
	}

	tampered := receipt
	tampered.Final.FieldManagers = []string{"helm"}
	if err := tampered.Validate(component, tampered.GroupID); err == nil || !strings.Contains(err.Error(), "independently healthy") {
		t.Fatalf("legacy manager receipt was accepted: %v", err)
	}
	tampered = receipt
	tampered.Final.ImageID = "sha256:" + strings.Repeat("6", 64)
	if err := tampered.Validate(component, tampered.GroupID); err == nil {
		t.Fatal("image identity drift was accepted")
	}
}

func adoptionReceiptFixture(t *testing.T, component Component, groupID string) OwnershipAdoptionReceipt {
	t.Helper()
	manager := component.Workload.FieldManager
	resource := ResourceObservation{
		Identity: ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control-alpha"},
		Present:  true, UID: "uid-1", ResourceVersion: "22", Generation: 3,
		FieldManagers: []string{manager, "k3s"}, ObjectDigest: "sha256:" + strings.Repeat("2", 64),
	}
	receipt := OwnershipAdoptionReceipt{
		APIVersion: OwnershipAdoptionReceiptAPIVersion, Kind: OwnershipAdoptionReceiptKind,
		Component: component.ID, GroupID: groupID, RunID: 123, RunAttempt: 1,
		TerminalReceiptDigest: "sha256:" + strings.Repeat("3", 64),
		Final: Observation{
			Present: true, Primary: resource.Identity, UID: resource.UID, ResourceVersion: resource.ResourceVersion,
			Generation: 3, ObservedGeneration: 3, Desired: 1, Updated: 1, Ready: 1, Available: 1,
			ConfigSHA: strings.Repeat("a", 40), ManifestSHA: strings.Repeat("a", 40), OCIRevision: strings.Repeat("a", 40),
			ImageRef: component.Artifact.Repository + "@sha256:" + strings.Repeat("4", 64), ImageID: "sha256:" + strings.Repeat("4", 64),
			HealthDigest: "sha256:" + strings.Repeat("5", 64), FieldManagers: []string{manager, "k3s"}, Resources: []ResourceObservation{resource},
		},
	}
	var err error
	receipt.ReceiptDigest, err = receipt.digest()
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}
