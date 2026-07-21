package releasedomain

import (
	"reflect"
	"testing"
)

func TestBuildCompositeDecompositionEvidenceProducesCanonicalSerialReport(t *testing.T) {
	plan, err := NewImageActivationPlan(md0BaseCommit, md0TargetCommit, md0Digest("1"), md0Digest("2"), []ImageActivation{
		md0Activation("controller-b", "controller", DomainControlPlane, md0Digest("3")),
		md0Activation("dns", "edge", DomainAuthoritativeDNS, md0Digest("4")),
		md0Activation("controller-a", "api", DomainControlPlane, md0Digest("5")),
	})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := NewImageActivationEvidence(ImageActivationEvidence{
		BaseCommit: plan.BaseCommit, TargetCommit: plan.TargetCommit,
		BuildArtifactPlanDigest:           plan.BuildArtifactPlanDigest,
		ResolvedImageActivationPlanDigest: plan.Digest,
		BuiltOnlyArtifacts:                []string{}, Unresolved: []ImageActivationGap{},
	})
	if err != nil {
		t.Fatal(err)
	}

	decomposition, err := BuildCompositeDecompositionEvidence(plan, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if !decomposition.Complete || len(decomposition.Steps) != 2 ||
		decomposition.Steps[0].Domain != DomainAuthoritativeDNS ||
		decomposition.Steps[1].Domain != DomainControlPlane ||
		!reflect.DeepEqual(decomposition.Steps[1].DependsOn, []string{string(DomainAuthoritativeDNS)}) ||
		!reflect.DeepEqual(decomposition.Steps[1].ActivationIDs, []string{"controller-a", "controller-b"}) {
		t.Fatalf("composite decomposition = %#v", decomposition)
	}
	repeated, err := BuildCompositeDecompositionEvidence(plan, evidence)
	if err != nil || !reflect.DeepEqual(repeated, decomposition) {
		t.Fatalf("decomposition is not deterministic: repeated=%#v err=%v", repeated, err)
	}
}

func TestBuildCompositeDecompositionEvidencePreservesGapsAndRejectsBindingDrift(t *testing.T) {
	plan, err := NewImageActivationPlan(md0BaseCommit, md0TargetCommit, md0Digest("1"), md0Digest("2"), []ImageActivation{
		md0Activation("controller", "controller", DomainControlPlane, md0Digest("3")),
	})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := NewImageActivationEvidence(ImageActivationEvidence{
		BaseCommit: plan.BaseCommit, TargetCommit: plan.TargetCommit,
		BuildArtifactPlanDigest:           plan.BuildArtifactPlanDigest,
		ResolvedImageActivationPlanDigest: plan.Digest,
		BuiltOnlyArtifacts:                []string{}, Unresolved: []ImageActivationGap{md0bOwnershipGap()},
	})
	if err != nil {
		t.Fatal(err)
	}
	decomposition, err := BuildCompositeDecompositionEvidence(plan, evidence)
	if err != nil {
		t.Fatal(err)
	}
	if decomposition.Complete || !reflect.DeepEqual(decomposition.UnresolvedActivationIDs, []string{"telemetry-agent-ownership"}) ||
		!reflect.DeepEqual(decomposition.Issues, []string{
			CompositeDecompositionIssueActivationIncomplete,
			CompositeDecompositionIssueTooFewDomains,
		}) {
		t.Fatalf("partial decomposition = %#v", decomposition)
	}

	drifted := evidence
	drifted.ResolvedImageActivationPlanDigest = md0Digest("9")
	drifted.Digest = imageActivationEvidenceDigest(drifted)
	if err := VerifyImageActivationEvidence(drifted); err != nil {
		t.Fatal(err)
	}
	if _, err := BuildCompositeDecompositionEvidence(plan, drifted); err == nil {
		t.Fatal("plan/evidence binding drift was accepted")
	}
}
