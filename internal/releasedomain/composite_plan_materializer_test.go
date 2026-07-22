package releasedomain

import (
	"reflect"
	"strings"
	"testing"
)

func TestMaterializeCompositeReleasePlanDerivesStrictPlanFromCompleteEvidence(t *testing.T) {
	decomposition, err := NewCompositeDecompositionEvidence(md2aCompositeDecompositionFixture())
	if err != nil {
		t.Fatal(err)
	}
	observations := compositeMaterializerObservations()
	plan, err := MaterializeCompositeReleasePlan(decomposition, "7", "11", observations)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyCompositeReleasePlan(plan); err != nil {
		t.Fatalf("strict plan did not verify: %v", err)
	}
	if plan.BaseCommit != decomposition.BaseCommit || plan.TargetCommit != decomposition.TargetCommit ||
		plan.ImageActivationPlanDigest != decomposition.ImageActivationPlanDigest || plan.Generation != "7" || plan.FencingEpoch != "11" {
		t.Fatalf("plan binding = %#v", plan)
	}
	for index, step := range plan.Steps {
		decomposed := decomposition.Steps[index]
		if step.ID != decomposed.ID || step.Domain != decomposed.Domain || step.Adapter != decomposed.Adapter ||
			!reflect.DeepEqual(step.DependsOn, decomposed.DependsOn) ||
			!reflect.DeepEqual(step.ActivationIDs, decomposed.ActivationIDs) ||
			step.BaseVersion != decomposed.ReverseRenderedDigest || step.TargetVersion != decomposed.ForwardRenderedDigest ||
			step.ForwardRenderedDigest != decomposed.ForwardRenderedDigest || step.ReverseRenderedDigest != decomposed.ReverseRenderedDigest {
			t.Fatalf("step %d was not evidence-derived: %#v", index, step)
		}
	}

	reversed := []CompositeDomainObservationRequirement{observations[1], observations[0]}
	repeated, err := MaterializeCompositeReleasePlan(decomposition, "7", "11", reversed)
	if err != nil || !reflect.DeepEqual(repeated, plan) {
		t.Fatalf("observation ordering changed materialization: %#v err=%v", repeated, err)
	}

	decomposition.Steps[0].ActivationIDs[0] = "mutated"
	observations[0].MinimumSamples = "99"
	if err := VerifyCompositeReleasePlan(plan); err != nil || plan.Steps[0].ActivationIDs[0] != "activate-dns" || plan.Steps[0].Observation.MinimumSamples != "5" {
		t.Fatalf("plan retained caller-owned input: %#v err=%v", plan, err)
	}
}

func TestMaterializeCompositeReleasePlanFailsClosed(t *testing.T) {
	complete, err := NewCompositeDecompositionEvidence(md2aCompositeDecompositionFixture())
	if err != nil {
		t.Fatal(err)
	}
	validObservations := compositeMaterializerObservations()

	incompleteFixture := md2aCompositeDecompositionFixture()
	incompleteFixture.UnresolvedActivationIDs = []string{"activate-unresolved"}
	incomplete, err := NewCompositeDecompositionEvidence(incompleteFixture)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name          string
		decomposition CompositeDecompositionEvidence
		generation    string
		fence         string
		observations  []CompositeDomainObservationRequirement
	}{
		{name: "incomplete", decomposition: incomplete, generation: "7", fence: "11", observations: validObservations},
		{name: "missing observation", decomposition: complete, generation: "7", fence: "11", observations: validObservations[:1]},
		{name: "duplicate observation", decomposition: complete, generation: "7", fence: "11", observations: []CompositeDomainObservationRequirement{validObservations[0], validObservations[0]}},
		{name: "extra observation", decomposition: complete, generation: "7", fence: "11", observations: append(append([]CompositeDomainObservationRequirement(nil), validObservations...), CompositeDomainObservationRequirement{Domain: DomainBackup, HealthEvidenceDigest: md0Digest("9"), MinimumSamples: "5", WindowSeconds: "120", RollbackBudgetSeconds: "300"})},
		{name: "zero generation", decomposition: complete, generation: "0", fence: "11", observations: validObservations},
		{name: "noncanonical fence", decomposition: complete, generation: "7", fence: "011", observations: validObservations},
		{name: "invalid health digest", decomposition: complete, generation: "7", fence: "11", observations: []CompositeDomainObservationRequirement{{Domain: DomainAuthoritativeDNS, HealthEvidenceDigest: "invalid", MinimumSamples: "5", WindowSeconds: "120", RollbackBudgetSeconds: "300"}, validObservations[1]}},
		{name: "zero samples", decomposition: complete, generation: "7", fence: "11", observations: []CompositeDomainObservationRequirement{{Domain: DomainAuthoritativeDNS, HealthEvidenceDigest: md0Digest("7"), MinimumSamples: "0", WindowSeconds: "120", RollbackBudgetSeconds: "300"}, validObservations[1]}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := MaterializeCompositeReleasePlan(test.decomposition, test.generation, test.fence, test.observations); err == nil {
				t.Fatal("unsafe input was accepted")
			}
		})
	}

	tampered := complete
	tampered.Steps = canonicalCompositeDecompositionSteps(complete.Steps)
	tampered.Steps[0].Adapter = "manual"
	if _, err := MaterializeCompositeReleasePlan(tampered, "7", "11", validObservations); err == nil || !strings.Contains(err.Error(), "verify composite decomposition") {
		t.Fatalf("tampered evidence was not rejected at verification boundary: %v", err)
	}
}

func compositeMaterializerObservations() []CompositeDomainObservationRequirement {
	return []CompositeDomainObservationRequirement{
		{Domain: DomainAuthoritativeDNS, HealthEvidenceDigest: md0Digest("7"), MinimumSamples: "5", WindowSeconds: "120", RollbackBudgetSeconds: "300"},
		{Domain: DomainControlPlane, HealthEvidenceDigest: md0Digest("8"), MinimumSamples: "5", WindowSeconds: "120", RollbackBudgetSeconds: "300"},
	}
}
