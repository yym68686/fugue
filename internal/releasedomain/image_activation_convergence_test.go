package releasedomain

import (
	"reflect"
	"testing"
)

func TestImageActivationConvergenceRequiresBuiltOnlyImageCacheArtifact(t *testing.T) {
	build, err := NewBuildArtifactPlan(md0BaseCommit, md0TargetCommit, md0Digest("1"), []BuildArtifact{
		{Name: "app_ssh", SourceBaseCommit: md0BaseCommit, ArtifactDigest: md0Digest("2"), ProvenanceDigest: md0Digest("3")},
		{Name: "edge", SourceBaseCommit: md0BaseCommit, ArtifactDigest: md0Digest("8"), ProvenanceDigest: md0Digest("9")},
		{Name: "image_cache", SourceBaseCommit: md0BaseCommit, ArtifactDigest: md0Digest("4"), ProvenanceDigest: md0Digest("5")},
		{Name: "telemetry_agent", SourceBaseCommit: md0BaseCommit, ArtifactDigest: md0Digest("6"), ProvenanceDigest: md0Digest("7")},
	})
	if err != nil {
		t.Fatal(err)
	}
	activation, err := NewImageActivationPlan(md0BaseCommit, md0TargetCommit, build.Digest, md0Digest("8"), nil)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := NewImageActivationEvidence(ImageActivationEvidence{
		BaseCommit: md0BaseCommit, TargetCommit: md0TargetCommit,
		BuildArtifactPlanDigest: build.Digest, ResolvedImageActivationPlanDigest: activation.Digest,
		BuiltOnlyArtifacts: []string{"app_ssh", "edge", "image_cache", "telemetry_agent"},
	})
	if err != nil {
		t.Fatal(err)
	}

	convergence, err := EvaluateImageActivationConvergence(build, activation, evidence)
	if err != nil {
		t.Fatalf("evaluate convergence: %v", err)
	}
	if convergence.Complete || !reflect.DeepEqual(convergence.PendingArtifacts, []string{"image_cache"}) {
		t.Fatalf("mandatory image-cache artifact was not retained: %#v", convergence)
	}
}

func TestImageActivationConvergenceAcceptsActivatedAndPolicyAuditOnlyArtifacts(t *testing.T) {
	build, err := NewBuildArtifactPlan(md0BaseCommit, md0TargetCommit, md0Digest("1"), []BuildArtifact{
		{Name: "app_ssh", SourceBaseCommit: md0BaseCommit, ArtifactDigest: md0Digest("2"), ProvenanceDigest: md0Digest("3")},
		{Name: "edge", SourceBaseCommit: md0BaseCommit, ArtifactDigest: md0Digest("7"), ProvenanceDigest: md0Digest("8")},
		{Name: "image_cache", SourceBaseCommit: md0BaseCommit, ArtifactDigest: md0Digest("4"), ProvenanceDigest: md0Digest("5")},
	})
	if err != nil {
		t.Fatal(err)
	}
	activation, err := NewImageActivationPlan(md0BaseCommit, md0TargetCommit, build.Digest, md0Digest("6"), []ImageActivation{
		md0Activation("image-cache", "image_cache", DomainImageCache, md0Digest("4")),
	})
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := NewImageActivationEvidence(ImageActivationEvidence{
		BaseCommit: md0BaseCommit, TargetCommit: md0TargetCommit,
		BuildArtifactPlanDigest: build.Digest, ResolvedImageActivationPlanDigest: activation.Digest,
		BuiltOnlyArtifacts: []string{"app_ssh", "edge"},
	})
	if err != nil {
		t.Fatal(err)
	}

	convergence, err := EvaluateImageActivationConvergence(build, activation, evidence)
	if err != nil {
		t.Fatalf("evaluate convergence: %v", err)
	}
	if !convergence.Complete || len(convergence.PendingArtifacts) != 0 {
		t.Fatalf("converged partition was rejected: %#v", convergence)
	}
}

func TestImageActivationConvergenceFailsClosedOnUnknownBuiltOnlyArtifact(t *testing.T) {
	build, err := NewBuildArtifactPlan(md0BaseCommit, md0TargetCommit, md0Digest("1"), []BuildArtifact{
		{Name: "future_component", SourceBaseCommit: md0BaseCommit, ArtifactDigest: md0Digest("2"), ProvenanceDigest: md0Digest("3")},
	})
	if err != nil {
		t.Fatal(err)
	}
	activation, err := NewImageActivationPlan(md0BaseCommit, md0TargetCommit, build.Digest, md0Digest("4"), nil)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := NewImageActivationEvidence(ImageActivationEvidence{
		BaseCommit: md0BaseCommit, TargetCommit: md0TargetCommit,
		BuildArtifactPlanDigest: build.Digest, ResolvedImageActivationPlanDigest: activation.Digest,
		BuiltOnlyArtifacts: []string{"future_component"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := EvaluateImageActivationConvergence(build, activation, evidence); err == nil {
		t.Fatal("unknown built-only artifact was accepted as converged")
	}
}
