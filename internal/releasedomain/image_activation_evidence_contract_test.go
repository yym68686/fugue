package releasedomain

import (
	"bytes"
	"reflect"
	"testing"
)

func TestImageActivationEvidenceCompleteAndIncompleteRoundTrip(t *testing.T) {
	complete, err := NewImageActivationEvidence(ImageActivationEvidence{
		BaseCommit: md0BaseCommit, TargetCommit: md0TargetCommit,
		BuildArtifactPlanDigest: md0Digest("a"), ResolvedImageActivationPlanDigest: md0Digest("b"),
		BuiltOnlyArtifacts: []string{"edge"},
	})
	if err != nil {
		t.Fatalf("new complete evidence: %v", err)
	}
	if !complete.Complete || complete.Unresolved == nil || len(complete.Unresolved) != 0 {
		t.Fatalf("complete evidence was not canonical: %#v", complete)
	}
	encoded, err := MarshalImageActivationEvidence(complete)
	if err != nil {
		t.Fatalf("marshal complete evidence: %v", err)
	}
	decoded, err := DecodeAndVerifyImageActivationEvidence(bytes.NewReader(encoded), complete.Digest)
	if err != nil || !reflect.DeepEqual(decoded, complete) {
		t.Fatalf("complete evidence round trip: decoded=%#v err=%v", decoded, err)
	}

	incomplete, err := NewImageActivationEvidence(ImageActivationEvidence{
		BaseCommit: md0BaseCommit, TargetCommit: md0TargetCommit,
		BuildArtifactPlanDigest: md0Digest("a"), ResolvedImageActivationPlanDigest: md0Digest("b"),
		BuiltOnlyArtifacts: []string{"edge"},
		Unresolved:         []ImageActivationGap{md0bOwnershipGap()},
	})
	if err != nil {
		t.Fatalf("new incomplete evidence: %v", err)
	}
	if incomplete.Complete || len(incomplete.Unresolved) != 1 || incomplete.Unresolved[0].Reason != ImageActivationGapOwnershipMissing {
		t.Fatalf("incomplete evidence was hidden: %#v", incomplete)
	}
}

func TestImageActivationEvidenceCanonicalizesButRejectsDuplicateInputs(t *testing.T) {
	first := md0bOwnershipGap()
	first.ID = "z-gap"
	second := md0bOwnershipGap()
	second.ID = "a-gap"
	evidence, err := NewImageActivationEvidence(ImageActivationEvidence{
		BaseCommit: md0BaseCommit, TargetCommit: md0TargetCommit,
		BuildArtifactPlanDigest: md0Digest("a"), ResolvedImageActivationPlanDigest: md0Digest("b"),
		BuiltOnlyArtifacts: []string{"telemetry_agent", "edge"},
		Unresolved:         []ImageActivationGap{first, second},
	})
	if err != nil {
		t.Fatalf("new canonical evidence: %v", err)
	}
	if !reflect.DeepEqual(evidence.BuiltOnlyArtifacts, []string{"edge", "telemetry_agent"}) ||
		evidence.Unresolved[0].ID != "a-gap" || evidence.Unresolved[1].ID != "z-gap" {
		t.Fatalf("evidence was not canonicalized: %#v", evidence)
	}

	if _, err := NewImageActivationEvidence(ImageActivationEvidence{
		BaseCommit: md0BaseCommit, TargetCommit: md0TargetCommit,
		BuildArtifactPlanDigest: md0Digest("a"), ResolvedImageActivationPlanDigest: md0Digest("b"),
		BuiltOnlyArtifacts: []string{"edge", "edge"},
	}); err == nil {
		t.Fatal("duplicate built-only artifact was accepted")
	}
	duplicate := md0bOwnershipGap()
	if _, err := NewImageActivationEvidence(ImageActivationEvidence{
		BaseCommit: md0BaseCommit, TargetCommit: md0TargetCommit,
		BuildArtifactPlanDigest: md0Digest("a"), ResolvedImageActivationPlanDigest: md0Digest("b"),
		Unresolved: []ImageActivationGap{duplicate, duplicate},
	}); err == nil {
		t.Fatal("duplicate gap identity was accepted")
	}
}

func TestImageActivationEvidenceRejectsInconsistentGapShapes(t *testing.T) {
	valid, err := NewImageActivationEvidence(ImageActivationEvidence{
		BaseCommit: md0BaseCommit, TargetCommit: md0TargetCommit,
		BuildArtifactPlanDigest: md0Digest("a"), ResolvedImageActivationPlanDigest: md0Digest("b"),
		Unresolved: []ImageActivationGap{md0bOwnershipGap()},
	})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*ImageActivationEvidence)
	}{
		{name: "false complete", mutate: func(value *ImageActivationEvidence) { value.Complete = true }},
		{name: "unknown reason", mutate: func(value *ImageActivationEvidence) { value.Unresolved[0].Reason = "theoretical" }},
		{name: "missing forward digest", mutate: func(value *ImageActivationEvidence) { value.Unresolved[0].ForwardRenderedDigest = "" }},
		{name: "ownership invented", mutate: func(value *ImageActivationEvidence) {
			value.Unresolved[0].OwnershipDomains = []Domain{DomainControlPlane}
		}},
		{name: "build match missing", mutate: func(value *ImageActivationEvidence) { value.Unresolved[0].MatchingBuildArtifacts = []string{} }},
		{name: "target digest mismatch", mutate: func(value *ImageActivationEvidence) {
			value.Unresolved[0].TargetImageRef = "registry.test/telemetry@" + md0Digest("a")
		}},
		{name: "not an image change", mutate: func(value *ImageActivationEvidence) {
			value.Unresolved[0].LiveImageRef = value.Unresolved[0].TargetImageRef
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := valid
			mutated.BuiltOnlyArtifacts = append([]string(nil), valid.BuiltOnlyArtifacts...)
			mutated.Unresolved = append([]ImageActivationGap(nil), valid.Unresolved...)
			mutated.Unresolved[0].MatchingBuildArtifacts = append([]string(nil), valid.Unresolved[0].MatchingBuildArtifacts...)
			mutated.Unresolved[0].OwnershipDomains = append([]Domain(nil), valid.Unresolved[0].OwnershipDomains...)
			test.mutate(&mutated)
			mutated.Digest = imageActivationEvidenceDigest(mutated)
			if err := VerifyImageActivationEvidence(mutated); err == nil {
				t.Fatal("inconsistent evidence was accepted")
			}
		})
	}
}

func TestImageActivationEvidenceStrictDecodeRejectsSchemaDrift(t *testing.T) {
	evidence, err := NewImageActivationEvidence(ImageActivationEvidence{
		BaseCommit: md0BaseCommit, TargetCommit: md0TargetCommit,
		BuildArtifactPlanDigest: md0Digest("a"), ResolvedImageActivationPlanDigest: md0Digest("b"),
		BuiltOnlyArtifacts: []string{"edge"},
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalImageActivationEvidence(evidence)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := bytes.Replace(encoded, []byte(`"kind":`), []byte(`"kind":"duplicate","kind":`), 1)
	if _, err := DecodeAndVerifyImageActivationEvidence(bytes.NewReader(duplicate), evidence.Digest); err == nil {
		t.Fatal("duplicate field was accepted")
	}
	unknown := bytes.Replace(encoded, []byte(`"kind":`), []byte(`"unknown":true,"kind":`), 1)
	if _, err := DecodeAndVerifyImageActivationEvidence(bytes.NewReader(unknown), evidence.Digest); err == nil {
		t.Fatal("unknown field was accepted")
	}
}

func md0bOwnershipGap() ImageActivationGap {
	return ImageActivationGap{
		ID: "telemetry-agent-ownership",
		Workload: ActivationWorkload{
			APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system",
			Name: "fugue-telemetry-agent", Container: "telemetry-agent",
		},
		LiveImageRef:   "registry.test/telemetry@" + md0Digest("c"),
		TargetImageRef: "registry.test/telemetry@" + md0Digest("d"),
		ArtifactDigest: md0Digest("d"), MatchingBuildArtifacts: []string{"telemetry_agent"},
		OwnershipDomains: []Domain{}, Reason: ImageActivationGapOwnershipMissing,
		ForwardRenderedDigest: md0Digest("e"), ReverseRenderedDigest: md0Digest("f"),
	}
}
