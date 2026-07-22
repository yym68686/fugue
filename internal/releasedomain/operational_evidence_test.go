package releasedomain

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestOperationalDomainEvidenceReportsCompleteSingleAsActivationEligible(t *testing.T) {
	changed, imagePlan, plan := operationalEvidenceFixture(t,
		[]ChangedFile{{
			Status:           ChangeModified,
			Path:             "internal/runtime/objects.go",
			ConsumerDomains:  []Domain{DomainControlPlane},
			OutsideConsumers: []string{"cmd/fugue", "cmd/fugue-agent", "cmd/fugue-registry-maintenance"},
		}},
		[]string{"controller"},
		[]Domain{DomainControlPlane},
		nil,
	)

	report, err := BuildOperationalDomainEvidence(changed, imagePlan, plan)
	if err != nil {
		t.Fatal(err)
	}
	if report.Observation != OutcomeSingle || report.CandidateDomain != DomainControlPlane {
		t.Fatalf("unexpected report outcome: %#v", report)
	}
	if !report.AuthorizationEligible {
		t.Fatal("complete single-domain evidence was not activation eligible")
	}
	if report.ConservativeOutcome != OutcomeUnknown || report.ClassificationAgrees {
		t.Fatalf("conservative/operational comparison was not preserved: %#v", report)
	}
	if len(report.Issues) != 0 {
		t.Fatalf("unexpected operational issues: %v", report.Issues)
	}
	if !reflect.DeepEqual(report.IntersectionDomains, []Domain{DomainControlPlane}) {
		t.Fatalf("intersection = %v", report.IntersectionDomains)
	}
	if !reflect.DeepEqual(report.ImageTargets, operationalRolloutTargets([]string{"controller"})) {
		t.Fatalf("image targets = %v", report.ImageTargets)
	}

	encoded, err := MarshalOperationalDomainEvidence(report)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeAndVerifyOperationalDomainEvidence(bytes.NewReader(encoded), report.Digest)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, report) {
		t.Fatalf("decoded report drifted\n got=%#v\nwant=%#v", decoded, report)
	}
}

func TestActivationOperationalEvidenceUsesOnlyLiveRelativeActivations(t *testing.T) {
	changed, input, activationPlan, activationEvidence, activationRendered := operationalActivationV2Fixture(t, false, false)
	report, err := BuildOperationalDomainEvidenceFromActivation(
		changed, input.BuildPlan, activationPlan, activationEvidence, activationRendered,
		input.ReleasePlan.Digests.BaseManifest, input.ReleasePlan.Digests.TargetManifest,
		digestBytesSHA256(input.TargetManifest), input.ReleasePlan.Digests.Ownership, input.ReleasePlan,
	)
	if err != nil {
		t.Fatal(err)
	}
	if report.Policy != OperationalActivationEvidencePolicy || !report.AuthorizationEligible ||
		report.Observation != OutcomeSingle || report.CandidateDomain != DomainControlPlane ||
		!equalDomains(report.IntersectionDomains, []Domain{DomainControlPlane}) {
		t.Fatalf("activation report = %#v", report)
	}
	if len(report.ImageTargets) != 1 || report.ImageTargets[0].Name != "api" ||
		len(report.ActivationWitness) != 1 ||
		!reflect.DeepEqual(report.ActivationWitness[0].Evidence.BuiltOnlyArtifacts, []string{"edge"}) {
		t.Fatalf("built-only artifact entered activation domains: %#v", report)
	}
	encoded, err := MarshalOperationalDomainEvidence(report)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeAndVerifyOperationalDomainEvidence(bytes.NewReader(encoded), report.Digest)
	if err != nil || !reflect.DeepEqual(decoded, report) {
		t.Fatalf("activation report round trip\n got=%#v\nwant=%#v\nerr=%v", decoded, report, err)
	}

	mutated := report
	mutated.ActivationWitness = append([]OperationalActivationWitness(nil), report.ActivationWitness...)
	mutated.ActivationWitness[0].Evidence.BuiltOnlyArtifacts = []string{"api"}
	mutated.ActivationWitness[0].Evidence.Digest = imageActivationEvidenceDigest(mutated.ActivationWitness[0].Evidence)
	mutated.Digest = operationalEvidenceDigest(mutated)
	if err := VerifyOperationalDomainEvidence(mutated); err == nil {
		t.Fatal("mutated build/activation partition unexpectedly verified")
	}

	incomplete := report
	incomplete.ActivationWitness = append([]OperationalActivationWitness(nil), report.ActivationWitness...)
	incompleteEvidence := incomplete.ActivationWitness[0].Evidence
	incompleteEvidence.BuiltOnlyArtifacts = []string{}
	gap := md0bOwnershipGap()
	gap.ID = "edge-ownership"
	gap.Workload.Name = "fugue-edge"
	gap.Workload.Container = "edge"
	gap.LiveImageRef = "registry.example/edge:live"
	gap.TargetImageRef = "registry.example/edge@" + md0Digest("b")
	gap.ArtifactDigest = md0Digest("b")
	gap.MatchingBuildArtifacts = []string{"edge"}
	incompleteEvidence.Unresolved = []ImageActivationGap{gap}
	incompleteEvidence.Complete = false
	incompleteEvidence.Digest = imageActivationEvidenceDigest(incompleteEvidence)
	incomplete.ActivationWitness[0].Evidence = incompleteEvidence
	incomplete.Digest = operationalEvidenceDigest(incomplete)
	if err := VerifyOperationalDomainEvidence(incomplete); err == nil {
		t.Fatal("incomplete activation witness was accepted after digest recomputation")
	}
}

func TestActivationOperationalEvidenceKeepsIncompleteAndRealMultipleFailClosed(t *testing.T) {
	t.Run("unresolved", func(t *testing.T) {
		changed, input, activationPlan, activationEvidence, activationRendered := operationalActivationV2Fixture(t, false, true)
		report, err := BuildOperationalDomainEvidenceFromActivation(
			changed, input.BuildPlan, activationPlan, activationEvidence, activationRendered,
			input.ReleasePlan.Digests.BaseManifest, input.ReleasePlan.Digests.TargetManifest,
			digestBytesSHA256(input.TargetManifest), input.ReleasePlan.Digests.Ownership, input.ReleasePlan,
		)
		if err != nil {
			t.Fatal(err)
		}
		if report.AuthorizationEligible || report.Observation != OutcomeUnknown ||
			!containsOperationalIssue(report.Issues, "image activation evidence is incomplete") {
			t.Fatalf("unresolved activation did not fail closed: %#v", report)
		}
	})

	t.Run("multiple", func(t *testing.T) {
		changed, input, activationPlan, activationEvidence, activationRendered := operationalActivationV2Fixture(t, true, false)
		report, err := BuildOperationalDomainEvidenceFromActivation(
			changed, input.BuildPlan, activationPlan, activationEvidence, activationRendered,
			input.ReleasePlan.Digests.BaseManifest, input.ReleasePlan.Digests.TargetManifest,
			digestBytesSHA256(input.TargetManifest), input.ReleasePlan.Digests.Ownership, input.ReleasePlan,
		)
		if err != nil {
			t.Fatal(err)
		}
		if report.AuthorizationEligible || report.Observation != OutcomeMultiple ||
			!equalDomains(report.IntersectionDomains, []Domain{DomainAuthoritativeDNS, DomainControlPlane}) {
			t.Fatalf("real multi-domain activation was narrowed: %#v", report)
		}
	})
}

func TestActivateOperationalPlanRebuildsBlockedPlanAsSingleDomain(t *testing.T) {
	changed, imagePlan, conservative := operationalEvidenceFixture(t,
		[]ChangedFile{{
			Status: ChangeModified, Path: "internal/runtime/objects.go",
			ConsumerDomains: []Domain{DomainControlPlane},
		}},
		[]string{"controller"},
		[]Domain{DomainControlPlane},
		nil,
	)
	report, err := BuildOperationalDomainEvidence(changed, imagePlan, conservative)
	if err != nil {
		t.Fatal(err)
	}
	activated, err := ActivateOperationalPlan(conservative, report)
	if err != nil {
		t.Fatal(err)
	}
	if activated.Result != OutcomeSingle || activated.SelectedDomain != DomainControlPlane ||
		!reflect.DeepEqual(activated.Domains, []Domain{DomainControlPlane}) ||
		len(activated.OperationalEvidence) != 1 {
		t.Fatalf("activated plan = %#v", activated)
	}
	if activated.OperationalEvidence[0].PlanDigest != conservative.PlanDigest ||
		activated.PlanDigest == conservative.PlanDigest {
		t.Fatalf("activation digest chain was not preserved: %#v", activated)
	}
	if err := VerifyPlanDigest(activated); err != nil {
		t.Fatal(err)
	}

	conservative.Domains[0] = DomainBackup
	report.IntersectionDomains[0] = DomainBackup
	if activated.Domains[0] != DomainControlPlane ||
		activated.OperationalEvidence[0].IntersectionDomains[0] != DomainControlPlane {
		t.Fatal("activated plan retained mutable caller slices")
	}
}

func TestActivateOperationalPlanRejectsIncompleteOrNonBlockedEvidence(t *testing.T) {
	changed, imagePlan, conservative := operationalEvidenceFixture(t,
		[]ChangedFile{{Status: ChangeModified, Path: "internal/controller/controller.go", ConsumerDomains: []Domain{DomainControlPlane}}},
		[]string{"controller"},
		[]Domain{DomainControlPlane},
		nil,
	)
	report, err := BuildOperationalDomainEvidence(changed, imagePlan, conservative)
	if err != nil {
		t.Fatal(err)
	}
	mutated := report
	mutated.AuthorizationEligible = false
	mutated.Digest = operationalEvidenceDigest(mutated)
	if _, err := ActivateOperationalPlan(conservative, mutated); err == nil {
		t.Fatal("ineligible operational evidence unexpectedly activated")
	}

	conservative.Result = OutcomeSingle
	conservative.SelectedDomain = DomainControlPlane
	conservative.Domains = []Domain{DomainControlPlane}
	conservative.PlanDigest = computePlanDigest(conservative)
	report, err = BuildOperationalDomainEvidence(changed, imagePlan, conservative)
	if err != nil {
		t.Fatal(err)
	}
	if report.AuthorizationEligible {
		t.Fatal("already-authorized conservative plan became operational activation eligible")
	}
	if _, err := ActivateOperationalPlan(conservative, report); err == nil {
		t.Fatal("conservative single-domain plan unexpectedly reactivated")
	}
}

func TestOperationalDomainEvidenceReportsClassificationAgreement(t *testing.T) {
	changed, imagePlan, plan := operationalEvidenceFixture(t,
		[]ChangedFile{{Status: ChangeModified, Path: "internal/controller/controller.go", ConsumerDomains: []Domain{DomainControlPlane}}},
		[]string{"controller"},
		[]Domain{DomainControlPlane},
		nil,
	)
	plan.Result = OutcomeSingle
	plan.Domains = []Domain{DomainControlPlane}
	plan.SelectedDomain = DomainControlPlane
	plan.PlanDigest = computePlanDigest(plan)

	report, err := BuildOperationalDomainEvidence(changed, imagePlan, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !report.ClassificationAgrees || report.ConservativeOutcome != OutcomeSingle ||
		report.ConservativeDomain != DomainControlPlane {
		t.Fatalf("matching classifications did not agree: %#v", report)
	}

	report.ClassificationAgrees = false
	report.Digest = operationalEvidenceDigest(report)
	if err := VerifyOperationalDomainEvidence(report); err == nil {
		t.Fatal("mutated comparison flag unexpectedly verified")
	}
}

func TestOperationalAdapterBindingsMatchLiteralProductionDispatcher(t *testing.T) {
	source, err := os.ReadFile("../../scripts/lib/control_plane_release_domains.sh")
	if err != nil {
		t.Fatal(err)
	}
	for _, binding := range fixedOperationalBindings() {
		for _, phase := range []string{"prepare", "apply", "verify", "rollback"} {
			if !bytes.Contains(source, []byte(binding.Adapter+"_"+phase)) {
				t.Fatalf("production dispatcher is missing %s/%s binding", binding.Domain, phase)
			}
		}
	}
}

func TestOperationalDomainEvidencePreservesRealMultipleDomain(t *testing.T) {
	changed, imagePlan, plan := operationalEvidenceFixture(t,
		[]ChangedFile{{
			Status:          ChangeModified,
			Path:            "internal/model/model.go",
			ConsumerDomains: []Domain{DomainAuthoritativeDNS, DomainControlPlane},
		}},
		[]string{"controller", "edge"},
		[]Domain{DomainAuthoritativeDNS, DomainControlPlane},
		nil,
	)

	report, err := BuildOperationalDomainEvidence(changed, imagePlan, plan)
	if err != nil {
		t.Fatal(err)
	}
	if report.Observation != OutcomeMultiple || report.CandidateDomain != "" {
		t.Fatalf("real multi-domain change was narrowed: %#v", report)
	}
	if !reflect.DeepEqual(report.IntersectionDomains, []Domain{DomainAuthoritativeDNS, DomainControlPlane}) {
		t.Fatalf("intersection = %v", report.IntersectionDomains)
	}
}

func TestOperationalDomainEvidenceFailsClosedOnWitnessMismatch(t *testing.T) {
	changed, imagePlan, plan := operationalEvidenceFixture(t,
		[]ChangedFile{{
			Status:          ChangeModified,
			Path:            "internal/controller/controller.go",
			ConsumerDomains: []Domain{DomainControlPlane},
		}},
		[]string{"controller"},
		[]Domain{DomainAuthoritativeDNS},
		nil,
	)

	report, err := BuildOperationalDomainEvidence(changed, imagePlan, plan)
	if err != nil {
		t.Fatal(err)
	}
	if report.Observation != OutcomeUnknown || report.CandidateDomain != "" || len(report.Issues) == 0 {
		t.Fatalf("contradictory witnesses did not fail closed: %#v", report)
	}
}

func TestOperationalDomainEvidenceFailsClosedOnIncompleteSelectedTarget(t *testing.T) {
	changed, imagePlan, plan := operationalEvidenceFixture(t,
		[]ChangedFile{{
			Status:           ChangeModified,
			Path:             "internal/model/model.go",
			ConsumerDomains:  []Domain{DomainControlPlane},
			OutsideConsumers: []string{"cmd/fugue-telemetry-agent"},
		}},
		[]string{"controller", "telemetry_agent"},
		[]Domain{DomainControlPlane},
		nil,
	)

	report, err := BuildOperationalDomainEvidence(changed, imagePlan, plan)
	if err != nil {
		t.Fatal(err)
	}
	if report.Observation != OutcomeUnknown || !containsOperationalIssue(report.Issues, "telemetry_agent") {
		t.Fatalf("unmapped selected image target did not fail closed: %#v", report)
	}
}

func TestOperationalDomainEvidenceFailsClosedOnRenderedUnknown(t *testing.T) {
	changed, imagePlan, plan := operationalEvidenceFixture(t,
		[]ChangedFile{{Status: ChangeModified, Path: "internal/controller/controller.go", ConsumerDomains: []Domain{DomainControlPlane}}},
		[]string{"controller"},
		[]Domain{DomainControlPlane},
		[]Evidence{{Source: "rendered-object", Subject: "apps/v1 Deployment", Reason: "unowned field"}},
	)
	report, err := BuildOperationalDomainEvidence(changed, imagePlan, plan)
	if err != nil {
		t.Fatal(err)
	}
	if report.Observation != OutcomeUnknown || !containsOperationalIssue(report.Issues, "rendered-object evidence is incomplete") {
		t.Fatalf("rendered unknown did not fail closed: %#v", report)
	}
}

func TestOperationalDomainEvidenceFailsClosedOnRenderedPlanIntegrityFailure(t *testing.T) {
	changed, imagePlan, plan := operationalEvidenceFixture(t,
		[]ChangedFile{{Status: ChangeModified, Path: "internal/controller/controller.go", ConsumerDomains: []Domain{DomainControlPlane}}},
		[]string{"controller"},
		[]Domain{DomainControlPlane},
		nil,
	)
	plan.Digests.RepeatedTargetManifest = "sha256:" + strings.Repeat("9", 64)
	plan.PlanDigest = computePlanDigest(plan)

	report, err := BuildOperationalDomainEvidence(changed, imagePlan, plan)
	if err != nil {
		t.Fatal(err)
	}
	if report.Observation != OutcomeUnknown || report.CandidateDomain != "" || !containsOperationalIssue(report.Issues, "repeated target render digest differs") {
		t.Fatalf("rendered plan integrity failure did not fail closed: %#v", report)
	}
}

func TestOperationalImageRolloutPlanStrictBindings(t *testing.T) {
	base := strings.Repeat("a", 40)
	target := strings.Repeat("b", 40)
	changedDigest := "sha256:" + strings.Repeat("c", 64)
	plan, err := NewOperationalImageRolloutPlan(base, target, changedDigest, operationalRolloutTargets([]string{"edge", "controller"}))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan.Targets, operationalRolloutTargets([]string{"controller", "edge"})) {
		t.Fatalf("targets = %v", plan.Targets)
	}
	encoded, err := MarshalOperationalImageRolloutPlan(plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeAndVerifyOperationalImageRolloutPlan(bytes.NewReader(encoded), base, target, changedDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeAndVerifyOperationalImageRolloutPlan(bytes.NewReader(encoded), strings.Repeat("d", 40), target, changedDigest); err == nil {
		t.Fatal("trusted base drift unexpectedly accepted")
	}
	if _, err := NewOperationalImageRolloutPlan(base, target, changedDigest, operationalRolloutTargets([]string{"controller", "controller"})); err == nil {
		t.Fatal("duplicate target unexpectedly accepted")
	}
	if _, err := NewOperationalImageRolloutPlan(base, target, changedDigest, operationalRolloutTargets([]string{"manual-domain-hint"})); err == nil {
		t.Fatal("manual domain hint unexpectedly accepted as an image target")
	}
}

func TestOperationalEvidenceStrictDecodeRejectsMutationAndDuplicateFields(t *testing.T) {
	changed, imagePlan, plan := operationalEvidenceFixture(t,
		[]ChangedFile{{Status: ChangeModified, Path: "internal/controller/controller.go", ConsumerDomains: []Domain{DomainControlPlane}}},
		[]string{"controller"},
		[]Domain{DomainControlPlane},
		nil,
	)
	report, err := BuildOperationalDomainEvidence(changed, imagePlan, plan)
	if err != nil {
		t.Fatal(err)
	}
	report.AuthorizationEligible = false
	if err := VerifyOperationalDomainEvidence(report); err == nil {
		t.Fatal("authorizationEligible mutation unexpectedly accepted")
	}

	report.AuthorizationEligible = true
	report.ImagePlanDigest = "sha256:" + strings.Repeat("f", 64)
	report.Digest = operationalEvidenceDigest(report)
	mutatedImagePlan, err := MarshalOperationalDomainEvidence(report)
	if err == nil {
		t.Fatalf("image-plan digest mutation unexpectedly accepted: %s", mutatedImagePlan)
	}

	report.ImagePlanDigest = imagePlan.Digest
	report.Digest = operationalEvidenceDigest(report)
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := bytes.Replace(encoded, []byte(`"kind":`), []byte(`"kind":"OperationalDomainEvidence","kind":`), 1)
	if _, err := DecodeAndVerifyOperationalDomainEvidence(bytes.NewReader(duplicate), report.Digest); err == nil {
		t.Fatal("duplicate JSON field unexpectedly accepted")
	}

	encoded[len(encoded)/2] ^= 1
	if _, err := DecodeAndVerifyOperationalDomainEvidence(bytes.NewReader(encoded), report.Digest); err == nil {
		t.Fatal("mutated report unexpectedly accepted")
	}
}

func operationalEvidenceFixture(
	t *testing.T,
	changes []ChangedFile,
	targets []string,
	renderedDomains []Domain,
	renderedUnknown []Evidence,
) (ChangedFileEvidence, OperationalImageRolloutPlan, Plan) {
	t.Helper()
	base := strings.Repeat("a", 40)
	target := strings.Repeat("b", 40)
	changed := ChangedFileEvidence{
		baseCommit:   base,
		targetCommit: target,
		changes:      cloneChangedFiles(changes),
		digest:       "sha256:" + strings.Repeat("c", 64),
	}
	imagePlan, err := NewOperationalImageRolloutPlan(base, target, changed.digest, operationalRolloutTargets(targets))
	if err != nil {
		t.Fatal(err)
	}
	renderedEvidence := []Evidence{}
	if len(renderedDomains) != 0 {
		renderedEvidence = []Evidence{{
			Source: "rendered-object", Subject: "fixture", Domains: canonicalDomains(renderedDomains),
		}}
	}
	context, err := NewClassificationContextEvidence("fugue-system", map[string]string{
		"releaseName":      "fugue",
		"releaseNamespace": "fugue-system",
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	plan := BuildPlan(PlanInput{
		Digests: DigestEvidence{
			Base:                   "fixture-base",
			Target:                 "fixture-target",
			Live:                   "fixture-base",
			BaseManifest:           "sha256:" + strings.Repeat("1", 64),
			TargetManifest:         "sha256:" + strings.Repeat("2", 64),
			RepeatedTargetManifest: "sha256:" + strings.Repeat("2", 64),
			Ownership:              "sha256:" + strings.Repeat("3", 64),
			ChangedFiles:           changed.digest,
			ClassificationContext:  context,
		},
		Files: FileClassification{
			Domains:  []Domain{},
			Evidence: []Evidence{},
		},
		Rendered: RenderedClassification{
			Domains:  canonicalDomains(renderedDomains),
			Evidence: renderedEvidence,
			Unknown:  canonicalEvidence(renderedUnknown),
		},
	})
	return changed, imagePlan, plan
}

func operationalRolloutTargets(names []string) []OperationalImageRolloutTarget {
	targets := make([]OperationalImageRolloutTarget, 0, len(names))
	for _, name := range names {
		targets = append(targets, OperationalImageRolloutTarget{
			Name:             name,
			SourceBaseCommit: strings.Repeat("d", 40),
			ArtifactDigest:   "sha256:" + strings.Repeat("e", 64),
		})
	}
	return targets
}

func operationalActivationV2Fixture(t *testing.T, includeEdge, unresolved bool) (
	ChangedFileEvidence,
	ImageActivationPlanInput,
	ImageActivationPlan,
	ImageActivationEvidence,
	RenderedClassification,
) {
	t.Helper()
	apiDigest := md0Digest("a")
	requestedAPIDigest := apiDigest
	if unresolved {
		requestedAPIDigest = md0Digest("9")
	}
	base := md1Deployment("fugue-api", "api", "registry.example/api:live")
	target := md1Deployment("fugue-api", "api", "registry.example/api@"+requestedAPIDigest)
	rules := []md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}}
	artifacts := []BuildArtifact{{
		Name: "api", SourceBaseCommit: md0BaseCommit, ArtifactDigest: apiDigest,
		ProvenanceDigest: md0Digest("1"), PublishedImageRef: "registry.example/api@" + apiDigest,
	}}
	domains := []Domain{DomainControlPlane}
	if includeEdge {
		edgeDigest := md0Digest("b")
		base += "---\n" + md1Deployment("fugue-edge", "edge", "registry.example/edge:live")
		target += "---\n" + md1Deployment("fugue-edge", "edge", "registry.example/edge@"+edgeDigest)
		rules = append(rules, md1OwnershipRule{name: "fugue-edge", domain: DomainAuthoritativeDNS})
		artifacts = append(artifacts, BuildArtifact{
			Name: "edge", SourceBaseCommit: md0BaseCommit, ArtifactDigest: edgeDigest,
			ProvenanceDigest: md0Digest("1"), PublishedImageRef: "registry.example/edge@" + edgeDigest,
		})
		domains = []Domain{DomainAuthoritativeDNS, DomainControlPlane}
	} else {
		artifacts = append(artifacts, BuildArtifact{
			Name: "edge", SourceBaseCommit: md0BaseCommit, ArtifactDigest: md0Digest("b"),
			ProvenanceDigest: md0Digest("1"), PublishedImageRef: "registry.example/edge@" + md0Digest("b"),
		})
	}
	input := md1ActivationFixture(t, base, target, rules, artifacts)
	activationPlan, activationEvidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := LoadOwnership(bytes.NewReader(input.Ownership))
	if err != nil {
		t.Fatal(err)
	}
	rendered := ClassifyRendered(input.BaseManifest, input.TargetManifest, spec, RenderedOptions{
		DefaultNamespace: input.ReleasePlan.Digests.ClassificationContext.DefaultNamespace,
		Bindings:         input.ReleasePlan.Digests.ClassificationContext.BindingMap(),
	})
	changed := ChangedFileEvidence{
		baseCommit: md0BaseCommit, targetCommit: md0TargetCommit, digest: md0Digest("f"),
		changes: []ChangedFile{{
			Status: ChangeModified, Path: "internal/model/model.go", ConsumerDomains: domains,
		}},
	}
	return changed, input, activationPlan, activationEvidence, rendered
}

func containsOperationalIssue(issues []string, fragment string) bool {
	for _, issue := range issues {
		if strings.Contains(issue, fragment) {
			return true
		}
	}
	return false
}
