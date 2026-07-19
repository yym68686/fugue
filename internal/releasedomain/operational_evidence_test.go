package releasedomain

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestOperationalDomainEvidenceReportsSingleWithoutAuthorizing(t *testing.T) {
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
	if report.AuthorizationEligible {
		t.Fatal("report-only evidence became authorization eligible")
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
	report.AuthorizationEligible = true
	if err := VerifyOperationalDomainEvidence(report); err == nil {
		t.Fatal("authorizationEligible mutation unexpectedly accepted")
	}

	report.AuthorizationEligible = false
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
	plan := Plan{
		APIVersion: PlanAPIVersion,
		Kind:       PlanKind,
		Result:     OutcomeUnknown,
		Domains:    canonicalDomains(renderedDomains),
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
		Unknown: []Evidence{},
	}
	plan.PlanDigest = computePlanDigest(plan)
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

func containsOperationalIssue(issues []string, fragment string) bool {
	for _, issue := range issues {
		if strings.Contains(issue, fragment) {
			return true
		}
	}
	return false
}
