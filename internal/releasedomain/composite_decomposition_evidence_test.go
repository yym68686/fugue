package releasedomain

import (
	"bytes"
	"reflect"
	"testing"
)

func TestCompositeDecompositionEvidenceCompleteAndPartialRoundTrip(t *testing.T) {
	complete, err := NewCompositeDecompositionEvidence(md2aCompositeDecompositionFixture())
	if err != nil {
		t.Fatalf("new complete decomposition evidence: %v", err)
	}
	if !complete.Complete || len(complete.Issues) != 0 || len(complete.Steps) != 2 {
		t.Fatalf("complete evidence shape drifted: %#v", complete)
	}
	if !reflect.DeepEqual(complete.Steps[1].DependsOn, []string{"authoritative-dns"}) {
		t.Fatalf("serial dependency drifted: %v", complete.Steps[1].DependsOn)
	}
	encoded, err := MarshalCompositeDecompositionEvidence(complete)
	if err != nil {
		t.Fatalf("marshal complete decomposition evidence: %v", err)
	}
	decoded, err := DecodeAndVerifyCompositeDecompositionEvidence(bytes.NewReader(encoded), complete.Digest)
	if err != nil || !reflect.DeepEqual(decoded, complete) {
		t.Fatalf("complete decomposition round trip failed: decoded=%#v err=%v", decoded, err)
	}

	fixture := md2aCompositeDecompositionFixture()
	fixture.Steps = fixture.Steps[:1]
	fixture.UnresolvedActivationIDs = []string{"activation-gap-1"}
	partial, err := NewCompositeDecompositionEvidence(fixture)
	if err != nil {
		t.Fatalf("new partial decomposition evidence: %v", err)
	}
	if partial.Complete || !reflect.DeepEqual(partial.Issues, []string{
		CompositeDecompositionIssueActivationIncomplete,
		CompositeDecompositionIssueTooFewDomains,
	}) {
		t.Fatalf("partial evidence did not preserve closed issues: %#v", partial)
	}
}

func TestCompositeDecompositionEvidenceRejectsUnsafeStructure(t *testing.T) {
	valid, err := NewCompositeDecompositionEvidence(md2aCompositeDecompositionFixture())
	if err != nil {
		t.Fatalf("new decomposition evidence: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*CompositeDecompositionEvidence)
	}{
		{name: "manual adapter", mutate: func(value *CompositeDecompositionEvidence) { value.Steps[0].Adapter = "manual" }},
		{name: "domain alias id", mutate: func(value *CompositeDecompositionEvidence) { value.Steps[0].ID = "dns" }},
		{name: "non-serial dependency", mutate: func(value *CompositeDecompositionEvidence) { value.Steps[1].DependsOn = []string{} }},
		{name: "duplicate activation", mutate: func(value *CompositeDecompositionEvidence) { value.Steps[1].ActivationIDs = []string{"activate-dns"} }},
		{name: "missing reverse", mutate: func(value *CompositeDecompositionEvidence) { value.Steps[0].ReverseRenderedDigest = "" }},
		{name: "false complete", mutate: func(value *CompositeDecompositionEvidence) { value.Complete = false }},
		{name: "invented issue", mutate: func(value *CompositeDecompositionEvidence) {
			value.Issues = []string{CompositeDecompositionIssueTooFewDomains}
		}},
		{name: "resolved and unresolved", mutate: func(value *CompositeDecompositionEvidence) {
			value.UnresolvedActivationIDs = []string{"activate-dns"}
			value.Issues = []string{CompositeDecompositionIssueActivationIncomplete}
			value.Complete = false
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := valid
			mutated.Steps = canonicalCompositeDecompositionSteps(valid.Steps)
			mutated.UnresolvedActivationIDs = append([]string(nil), valid.UnresolvedActivationIDs...)
			mutated.Issues = append([]string(nil), valid.Issues...)
			test.mutate(&mutated)
			mutated.Digest = compositeDecompositionEvidenceDigest(mutated)
			if err := VerifyCompositeDecompositionEvidence(mutated); err == nil {
				t.Fatal("unsafe decomposition evidence must fail closed")
			}
		})
	}
}

func TestCompositeDecompositionEvidenceStrictDecodeRejectsSchemaDrift(t *testing.T) {
	valid, err := NewCompositeDecompositionEvidence(md2aCompositeDecompositionFixture())
	if err != nil {
		t.Fatalf("new decomposition evidence: %v", err)
	}
	encoded, err := MarshalCompositeDecompositionEvidence(valid)
	if err != nil {
		t.Fatalf("marshal decomposition evidence: %v", err)
	}
	duplicate := bytes.Replace(encoded, []byte(`"kind":`), []byte(`"kind":"duplicate","kind":`), 1)
	if _, err := DecodeAndVerifyCompositeDecompositionEvidence(bytes.NewReader(duplicate), valid.Digest); err == nil {
		t.Fatal("duplicate JSON field was accepted")
	}
	unknown := bytes.Replace(encoded, []byte(`"kind":`), []byte(`"unknown":true,"kind":`), 1)
	if _, err := DecodeAndVerifyCompositeDecompositionEvidence(bytes.NewReader(unknown), valid.Digest); err == nil {
		t.Fatal("unknown JSON field was accepted")
	}
}

func md2aCompositeDecompositionFixture() CompositeDecompositionEvidence {
	dnsAdapter, _ := fixedAdapterForDomain(DomainAuthoritativeDNS)
	controlAdapter, _ := fixedAdapterForDomain(DomainControlPlane)
	return CompositeDecompositionEvidence{
		BaseCommit: md0BaseCommit, TargetCommit: md0TargetCommit,
		ImageActivationPlanDigest: md0Digest("1"), ImageActivationEvidenceDigest: md0Digest("2"),
		Steps: []CompositeDecompositionStep{
			{
				ID: string(DomainAuthoritativeDNS), Domain: DomainAuthoritativeDNS, Adapter: dnsAdapter,
				DependsOn: []string{}, ActivationIDs: []string{"activate-dns"},
				ForwardRenderedDigest: md0Digest("3"), ReverseRenderedDigest: md0Digest("4"),
			},
			{
				ID: string(DomainControlPlane), Domain: DomainControlPlane, Adapter: controlAdapter,
				DependsOn: []string{string(DomainAuthoritativeDNS)}, ActivationIDs: []string{"activate-api"},
				ForwardRenderedDigest: md0Digest("5"), ReverseRenderedDigest: md0Digest("6"),
			},
		},
		UnresolvedActivationIDs: []string{},
	}
}
