package releasedomain

import (
	"bytes"
	"testing"
)

func TestMaterializeObservedLiveImageManifestChangesOnlyDeclaredImages(t *testing.T) {
	input := md1ActivationFixture(
		t,
		md1Deployment("fugue-dns", "dns", "registry.test/edge:helm-old"),
		md1Deployment("fugue-dns", "dns", "registry.test/edge@"+md0Digest("b")),
		[]md1OwnershipRule{{name: "fugue-dns", domain: DomainAuthoritativeDNS}},
		nil,
	)
	observedRaw := []byte(md1Deployment(
		"fugue-dns", "dns", "registry.test/edge@"+md0Digest("b"),
	))
	observed, err := MaterializeObservedLiveImageManifest(
		input.BaseManifest, observedRaw, input.Ownership, "fugue-system",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(observed, []byte("registry.test/edge@"+md0Digest("b"))) ||
		bytes.Contains(observed, []byte("registry.test/edge:helm-old")) {
		t.Fatalf("observed image was not projected:\n%s", observed)
	}
	if err := VerifyObservedLiveImageManifest(
		input.BaseManifest, observed, input.Ownership, "fugue-system",
	); err != nil {
		t.Fatalf("verify observed live image manifest: %v", err)
	}

	tampered := bytes.Replace(observed, []byte("app: fugue-dns"), []byte("app: different"), 1)
	if err := VerifyObservedLiveImageManifest(
		input.BaseManifest, tampered, input.Ownership, "fugue-system",
	); err == nil {
		t.Fatal("non-image observed-live mutation was accepted")
	}
}

func TestObservedLiveImageManifestSuppressesAlreadyActiveArtifactGap(t *testing.T) {
	targetDigest := md0Digest("b")
	input := md1ActivationFixture(
		t,
		md1DaemonSet("fugue-dns", "dns", "registry.test/edge:helm-old"),
		md1DaemonSet("fugue-dns", "dns", "registry.test/edge@"+targetDigest),
		[]md1OwnershipRule{{name: "fugue-dns", domain: DomainAuthoritativeDNS, kind: "DaemonSet"}},
		nil,
	)
	withoutObservedPlan, withoutObservedEvidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(withoutObservedPlan.Activations) != 0 || withoutObservedEvidence.Complete ||
		len(withoutObservedEvidence.Unresolved) != 1 ||
		withoutObservedEvidence.Unresolved[0].Reason != ImageActivationGapArtifactNotBuilt {
		t.Fatalf("Helm-only baseline did not expose the expected artifact gap: plan=%#v evidence=%#v", withoutObservedPlan, withoutObservedEvidence)
	}

	observed, err := MaterializeObservedLiveImageManifest(
		input.BaseManifest,
		[]byte(md1DaemonSet("fugue-dns", "dns", "registry.test/edge@"+targetDigest)),
		input.Ownership,
		"fugue-system",
	)
	if err != nil {
		t.Fatal(err)
	}
	input.ObservedLiveManifest = observed
	input.ImmutableTargetManifest, err = MaterializeObservedLiveRelativeTargetPublishedImageRefs(
		input.BaseManifest,
		observed,
		input.TargetManifest,
		input.Ownership,
		"fugue-system",
		input.BuildPlan.TargetCommit,
		input.BuildPlan,
		input.ReleasePlan,
	)
	if err != nil {
		t.Fatal(err)
	}
	plan, evidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Activations) != 0 || !evidence.Complete || len(evidence.Unresolved) != 0 {
		t.Fatalf("already-active image was not resolved: plan=%#v evidence=%#v", plan, evidence)
	}
	if plan.LiveStateDigest != digestBytesSHA256(observed) ||
		plan.LiveStateDigest == input.ReleasePlan.Digests.BaseManifest {
		t.Fatalf("activation live-state digest did not bind observed state: %#v", plan)
	}
}

func TestMaterializeObservedLiveImageManifestKeepsMissingWorkloadFailClosed(t *testing.T) {
	input := md1ActivationFixture(
		t,
		md1Deployment("fugue-api", "api", "registry.test/api:old"),
		md1Deployment("fugue-api", "api", "registry.test/api:new"),
		[]md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}},
		nil,
	)
	observed, err := MaterializeObservedLiveImageManifest(
		input.BaseManifest,
		[]byte("apiVersion: v1\nkind: List\nmetadata: {}\nitems: []\n"),
		input.Ownership,
		"fugue-system",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(observed, input.BaseManifest) {
		t.Fatalf("missing live workload did not retain the exact base image:\n%s", observed)
	}
	input.ObservedLiveManifest = observed
	input.ImmutableTargetManifest, err = MaterializeObservedLiveRelativeTargetPublishedImageRefs(
		input.BaseManifest, observed, input.TargetManifest, input.Ownership,
		"fugue-system", input.BuildPlan.TargetCommit, input.BuildPlan, input.ReleasePlan,
	)
	if err != nil {
		t.Fatal(err)
	}
	plan, evidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Activations) != 0 || evidence.Complete || len(evidence.Unresolved) != 1 ||
		evidence.Unresolved[0].LiveImageRef != "registry.test/api:old" {
		t.Fatalf("missing observation incorrectly resolved target activation: plan=%#v evidence=%#v", plan, evidence)
	}
}
