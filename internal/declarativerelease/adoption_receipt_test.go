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

func TestAPIOwnershipReceiptUsesComponentScopeAndRequiresExclusivePointers(t *testing.T) {
	component := Component{
		ID: "api", Family: "control-plane", Artifact: Artifact{Repository: "ghcr.io/yym68686/fugue-api"},
		Workload: Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", FieldManager: "fugue-api-declarative", Replicas: 2},
	}
	receipt := adoptionReceiptFixture(t, component, "api")
	receipt.Final.Desired = 2
	receipt.Final.Updated = 2
	receipt.Final.Ready = 2
	receipt.Final.Available = 2
	receipt.Final.Primary.Name = "fugue-fugue-api"
	receipt.Final.Resources[0].Identity = receipt.Final.Primary
	receipt.Final.Resources[0].ReviewedOwnershipApplied = true
	receipt.Final.Resources[0].ReviewedOwnershipExclusive = true
	var err error
	receipt.ReceiptDigest, err = receipt.digest()
	if err != nil || receipt.Validate(component, "api") != nil {
		t.Fatalf("valid API ownership receipt: digest=%v validate=%v", err, receipt.Validate(component, "api"))
	}
	tampered := receipt
	tampered.Final.Resources = append([]ResourceObservation(nil), receipt.Final.Resources...)
	tampered.Final.Resources[0].ReviewedOwnershipExclusive = false
	tampered.ReceiptDigest, _ = tampered.digest()
	if err := tampered.Validate(component, "api"); err == nil || !strings.Contains(err.Error(), "pointer-exclusive") {
		t.Fatalf("non-exclusive API receipt was accepted: %v", err)
	}
	if err := receipt.Validate(component, "edge-group-control-plane-api"); err == nil {
		t.Fatal("API receipt borrowed an Edge group scope")
	}
}

func TestOwnershipTerminalHandoffAuthorizationIsDigestBound(t *testing.T) {
	component := edgeGroupFixture("alpha", "edge-group-metro-alpha").Control
	receipt := adoptionReceiptFixture(t, component, "edge-group-metro-alpha")
	receipt.TerminalHandoff = &OwnershipTerminalHandoff{
		RunID: 456, RunAttempt: 1, FailedConfigSHA: strings.Repeat("b", 40),
		ForwardImageRef:       component.Artifact.Repository + "@sha256:" + strings.Repeat("c", 64),
		ArtifactReceiptDigest: "sha256:" + strings.Repeat("d", 64),
		Conflicts:             []OwnershipTerminalHandoffConflict{{Pointer: "/metadata/labels/app.kubernetes.io~1managed-by", LegacyManager: "helm"}},
	}
	var err error
	receipt.ReceiptDigest, err = receipt.digest()
	if err != nil || receipt.Validate(component, receipt.GroupID) != nil {
		t.Fatalf("valid terminal handoff authorization: digest=%v validate=%v", err, receipt.Validate(component, receipt.GroupID))
	}
	tampered := receipt
	copyHandoff := *receipt.TerminalHandoff
	copyHandoff.Conflicts = append([]OwnershipTerminalHandoffConflict(nil), receipt.TerminalHandoff.Conflicts...)
	copyHandoff.Conflicts[0].Pointer = "/spec/template"
	tampered.TerminalHandoff = &copyHandoff
	if err := tampered.Validate(component, tampered.GroupID); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("undigested handoff mutation was accepted: %v", err)
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
