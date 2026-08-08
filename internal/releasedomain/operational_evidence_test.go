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
	reportOnly, err := BuildOperationalDomainEvidenceFromActivationReportOnly(
		changed, input.BuildPlan, activationPlan, activationEvidence, activationRendered,
		input.ReleasePlan.Digests.BaseManifest, input.ReleasePlan.Digests.TargetManifest,
		digestBytesSHA256(input.TargetManifest), input.ReleasePlan.Digests.Ownership, input.ReleasePlan,
	)
	if err != nil || !reportOnly.AuthorizationEligible || reportOnly.CandidateDomain != report.CandidateDomain ||
		len(reportOnly.RenderedOnlyObservations) != 1 || reportOnly.RenderedOnlyObservations[0].Applicable {
		t.Fatalf("report-only extension changed active authorization: report=%#v err=%v", reportOnly, err)
	}
	v2Plan, err := ActivateOperationalPlan(input.ReleasePlan, report)
	if err != nil {
		t.Fatal(err)
	}
	v3Plan, err := ActivateOperationalPlan(input.ReleasePlan, reportOnly)
	if err != nil {
		t.Fatal(err)
	}
	if v2Plan.Result != v3Plan.Result || v2Plan.SelectedDomain != v3Plan.SelectedDomain ||
		!equalDomains(v2Plan.Domains, v3Plan.Domains) {
		t.Fatalf("v2/v3 authorization decisions differ: v2=%#v v3=%#v", v2Plan, v3Plan)
	}
	drifted := reportOnly
	drifted.ActivationWitness = append([]OperationalActivationWitness(nil), reportOnly.ActivationWitness...)
	drifted.ActivationWitness[0].TargetManifestDigest = md0Digest("9")
	drifted.Digest = operationalEvidenceDigest(drifted)
	if err := VerifyOperationalDomainEvidence(drifted); err != nil {
		t.Fatalf("externally bound v3 witness should remain structurally verifiable: %v", err)
	}
	if _, err := ActivateOperationalPlan(input.ReleasePlan, drifted); err == nil {
		t.Fatal("v3 target-manifest witness drift bypassed conservative-plan binding")
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

func TestActivationOperationalEvidenceAuthorizesExactDeferredImageCacheArtifact(t *testing.T) {
	t.Parallel()

	componentBase := strings.Repeat("6", 40)
	changed, input, activationPlan, activationEvidence, rendered := operationalDeferredImageCacheFixture(t, componentBase)
	build := func(
		buildPlan BuildArtifactPlan,
		plan ImageActivationPlan,
		evidence ImageActivationEvidence,
		rendered RenderedClassification,
	) (OperationalDomainEvidence, error) {
		return BuildOperationalDomainEvidenceFromAuthorizedImageCacheConvergence(
			changed, buildPlan, plan, evidence, rendered,
			input.ReleasePlan.Digests.BaseManifest, input.ReleasePlan.Digests.TargetManifest,
			digestBytesSHA256(input.TargetManifest), input.ReleasePlan.Digests.Ownership, input.ReleasePlan,
		)
	}

	report, err := build(input.BuildPlan, activationPlan, activationEvidence, rendered)
	if err != nil {
		t.Fatal(err)
	}
	if report.Policy != OperationalImageCacheConvergencePolicy ||
		!report.AuthorizationEligible || report.Observation != OutcomeSingle ||
		report.CandidateDomain != DomainImageCache ||
		!equalDomains(report.ConsumerDomains, []Domain{DomainImageCache}) ||
		!equalDomains(report.IntersectionDomains, []Domain{DomainImageCache}) || len(report.Issues) != 0 {
		t.Fatalf("deferred image-cache report = %#v", report)
	}
	resolved, err := ActivateOperationalPlan(input.ReleasePlan, report)
	if err != nil || resolved.Result != OutcomeSingle || resolved.SelectedDomain != DomainImageCache ||
		!equalDomains(resolved.Domains, []Domain{DomainImageCache}) {
		t.Fatalf("deferred image-cache activation = %#v err=%v", resolved, err)
	}

	tampered := report
	tampered.ActivationWitness = append([]OperationalActivationWitness(nil), report.ActivationWitness...)
	tamperedPlan := tampered.ActivationWitness[0].Plan
	tamperedPlan.Activations = append([]ImageActivation(nil), tamperedPlan.Activations...)
	tamperedPlan.Activations[0].Workload.Kind = "Deployment"
	tamperedPlan.Digest = imageActivationPlanDigest(tamperedPlan)
	tamperedEvidence := tampered.ActivationWitness[0].Evidence
	tamperedEvidence.ResolvedImageActivationPlanDigest = tamperedPlan.Digest
	tamperedEvidence.Digest = imageActivationEvidenceDigest(tamperedEvidence)
	tampered.ActivationWitness[0].Plan = tamperedPlan
	tampered.ActivationWitness[0].Evidence = tamperedEvidence
	tampered.ImagePlanDigest = tamperedPlan.Digest
	tampered.RenderedOnlyObservations = []RenderedOnlyOperationalObservation{
		buildRenderedOnlyOperationalObservation(
			tampered.ActivationWitness[0].BuildPlan,
			tamperedPlan,
			tamperedEvidence,
			tampered.ActivationWitness[0].Rendered,
			fixedOperationalBindings(),
		),
	}
	tampered.Digest = operationalEvidenceDigest(tampered)
	if err := VerifyOperationalDomainEvidence(tampered); err == nil ||
		!strings.Contains(err.Error(), "authorized image-cache convergence witness is not exact") {
		t.Fatalf("special policy accepted a drifted workload witness: %v", err)
	}

	driftedContext, err := NewClassificationContextEvidence(
		"fugue-system",
		map[string]string{
			"imageCacheName":   "unbound-image-cache",
			"releaseNamespace": "fugue-system",
		},
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	driftedDigests := input.ReleasePlan.Digests
	driftedDigests.ClassificationContext = driftedContext
	driftedConservative := BuildPlan(PlanInput{
		Files: input.ReleasePlan.Files, Rendered: input.ReleasePlan.Rendered, Digests: driftedDigests,
	})
	driftedReport := report
	driftedReport.PlanDigest = driftedConservative.PlanDigest
	driftedReport.Digest = operationalEvidenceDigest(driftedReport)
	if err := VerifyOperationalDomainEvidence(driftedReport); err != nil {
		t.Fatalf("self-contained convergence report unexpectedly depends on external context: %v", err)
	}
	if _, err := ActivateOperationalPlan(driftedConservative, driftedReport); err == nil ||
		!strings.Contains(err.Error(), "classification context mismatch") {
		t.Fatalf("resolver accepted a workload outside the sealed imageCacheName binding: %v", err)
	}

	repeated, err := build(input.BuildPlan, activationPlan, activationEvidence, rendered)
	if err != nil || !reflect.DeepEqual(repeated, report) {
		t.Fatalf("prepare/apply report drifted\nfirst=%#v\nsecond=%#v\nerr=%v", report, repeated, err)
	}
	firstBytes, err := MarshalOperationalDomainEvidence(report)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := MarshalOperationalDomainEvidence(repeated)
	if err != nil || !bytes.Equal(firstBytes, secondBytes) {
		t.Fatalf("prepare/apply canonical bytes drifted: err=%v", err)
	}
	activation := report.ActivationWitness[0].Plan.Activations[0]
	if activation.ForwardRenderedDigest == "" || activation.ReverseRenderedDigest == "" ||
		activation.ForwardRenderedDigest == activation.ReverseRenderedDigest {
		t.Fatalf("forward/reverse image-cache evidence is incomplete: %#v", activation)
	}

	t.Run("ordinary activation cannot claim the deferred component baseline", func(t *testing.T) {
		t.Parallel()
		report, err := BuildOperationalDomainEvidenceFromRenderedOnlyActivation(
			changed, input.BuildPlan, activationPlan, activationEvidence, rendered,
			input.ReleasePlan.Digests.BaseManifest, input.ReleasePlan.Digests.TargetManifest,
			digestBytesSHA256(input.TargetManifest), input.ReleasePlan.Digests.Ownership, input.ReleasePlan,
		)
		if err != nil {
			t.Fatal(err)
		}
		if report.AuthorizationEligible || len(report.ConsumerDomains) != 0 ||
			!containsOperationalIssue(report.Issues, "changed-package consumers do not cover every activated production domain") {
			t.Fatalf("ordinary activation claimed convergence evidence: %#v", report)
		}
	})

	t.Run("same release baseline remains rejected", func(t *testing.T) {
		t.Parallel()
		changed, input, plan, evidence, rendered := operationalDeferredImageCacheFixture(t, md0BaseCommit)
		if report, err := BuildOperationalDomainEvidenceFromAuthorizedImageCacheConvergence(
			changed, input.BuildPlan, plan, evidence, rendered,
			input.ReleasePlan.Digests.BaseManifest, input.ReleasePlan.Digests.TargetManifest,
			digestBytesSHA256(input.TargetManifest), input.ReleasePlan.Digests.Ownership, input.ReleasePlan,
		); err == nil {
			t.Fatalf("same-baseline consumer gap emitted a convergence report: %#v", report)
		}
	})

	t.Run("artifact digest drift fails closed", func(t *testing.T) {
		t.Parallel()
		drifted := input.BuildPlan
		drifted.Artifacts = append([]BuildArtifact(nil), input.BuildPlan.Artifacts...)
		drifted.Artifacts[0].ArtifactDigest = md0Digest("9")
		drifted.Digest = buildArtifactPlanDigest(drifted)
		if _, err := build(drifted, activationPlan, activationEvidence, rendered); err == nil {
			t.Fatal("artifact digest drift unexpectedly authorized")
		}
	})

	t.Run("activation domain drift fails closed", func(t *testing.T) {
		t.Parallel()
		drifted := activationPlan
		drifted.Activations = append([]ImageActivation(nil), activationPlan.Activations...)
		drifted.Activations[0].Domain = DomainControlPlane
		drifted.Activations[0].Adapter, _ = fixedAdapterForDomain(DomainControlPlane)
		drifted.Digest = imageActivationPlanDigest(drifted)
		driftedEvidence := activationEvidence
		driftedEvidence.ResolvedImageActivationPlanDigest = drifted.Digest
		driftedEvidence.Digest = imageActivationEvidenceDigest(driftedEvidence)
		report, err := build(input.BuildPlan, drifted, driftedEvidence, rendered)
		if err == nil && report.AuthorizationEligible {
			t.Fatalf("activation domain drift unexpectedly authorized: %#v", report)
		}
	})

	t.Run("activation adapter drift fails closed", func(t *testing.T) {
		t.Parallel()
		drifted := activationPlan
		drifted.Activations = append([]ImageActivation(nil), activationPlan.Activations...)
		drifted.Activations[0].Adapter = "control_plane_release_adapter_control_plane"
		drifted.Digest = imageActivationPlanDigest(drifted)
		driftedEvidence := activationEvidence
		driftedEvidence.ResolvedImageActivationPlanDigest = drifted.Digest
		driftedEvidence.Digest = imageActivationEvidenceDigest(driftedEvidence)
		if report, err := build(input.BuildPlan, drifted, driftedEvidence, rendered); err == nil && report.AuthorizationEligible {
			t.Fatalf("activation adapter drift unexpectedly authorized: %#v", report)
		}
	})
}

func TestAuthorizedImageCacheConvergenceConsumerRequiresExactSingletonIdentity(t *testing.T) {
	t.Parallel()

	_, input, activationPlan, _, _ := operationalDeferredImageCacheFixture(t, strings.Repeat("6", 40))
	context := input.ReleasePlan.Digests.ClassificationContext
	assertDomains := func(
		t *testing.T,
		buildPlan BuildArtifactPlan,
		activations []ImageActivation,
		classificationContext ClassificationContextEvidence,
		authorized bool,
		want []Domain,
	) {
		t.Helper()
		issues := make([]string, 0)
		got := operationalConsumerDomainsFromActivations(
			nil, buildPlan, activations, classificationContext, authorized, &issues,
		)
		if !equalDomains(got, want) || len(issues) != 0 {
			t.Fatalf("consumer domains = %v issues=%v, want %v", got, issues, want)
		}
	}
	assertDomains(t, input.BuildPlan, activationPlan.Activations, context, true, []Domain{DomainImageCache})

	tests := []struct {
		name       string
		authorized bool
		mutate     func(*BuildArtifactPlan, *[]ImageActivation, *ClassificationContextEvidence)
	}{
		{
			name:       "ordinary dispatch",
			authorized: false,
		},
		{
			name:       "extra artifact",
			authorized: true,
			mutate: func(plan *BuildArtifactPlan, _ *[]ImageActivation, _ *ClassificationContextEvidence) {
				plan.Artifacts = append(plan.Artifacts, BuildArtifact{Name: "controller"})
			},
		},
		{
			name:       "extra activation",
			authorized: true,
			mutate: func(_ *BuildArtifactPlan, activations *[]ImageActivation, _ *ClassificationContextEvidence) {
				*activations = append(*activations, (*activations)[0])
			},
		},
		{
			name:       "non image cache divergent artifact",
			authorized: true,
			mutate: func(plan *BuildArtifactPlan, activations *[]ImageActivation, _ *ClassificationContextEvidence) {
				plan.Artifacts[0].Name = "controller"
				(*activations)[0].ArtifactName = "controller"
				(*activations)[0].Domain = DomainControlPlane
				(*activations)[0].Adapter, _ = fixedAdapterForDomain(DomainControlPlane)
			},
		},
		{
			name:       "same component baseline",
			authorized: true,
			mutate: func(plan *BuildArtifactPlan, _ *[]ImageActivation, _ *ClassificationContextEvidence) {
				plan.Artifacts[0].SourceBaseCommit = plan.BaseCommit
			},
		},
		{
			name:       "wrong workload api version",
			authorized: true,
			mutate: func(_ *BuildArtifactPlan, activations *[]ImageActivation, _ *ClassificationContextEvidence) {
				(*activations)[0].Workload.APIVersion = "extensions/v1beta1"
			},
		},
		{
			name:       "wrong workload kind",
			authorized: true,
			mutate: func(_ *BuildArtifactPlan, activations *[]ImageActivation, _ *ClassificationContextEvidence) {
				(*activations)[0].Workload.Kind = "Deployment"
			},
		},
		{
			name:       "wrong workload namespace",
			authorized: true,
			mutate: func(_ *BuildArtifactPlan, activations *[]ImageActivation, _ *ClassificationContextEvidence) {
				(*activations)[0].Workload.Namespace = "other-system"
			},
		},
		{
			name:       "wrong workload name with accepted suffix",
			authorized: true,
			mutate: func(_ *BuildArtifactPlan, activations *[]ImageActivation, _ *ClassificationContextEvidence) {
				(*activations)[0].Workload.Name = "unbound-image-cache"
			},
		},
		{
			name:       "wrong workload container",
			authorized: true,
			mutate: func(_ *BuildArtifactPlan, activations *[]ImageActivation, _ *ClassificationContextEvidence) {
				(*activations)[0].Workload.Container = "sidecar"
			},
		},
		{
			name:       "missing image cache binding",
			authorized: true,
			mutate: func(_ *BuildArtifactPlan, _ *[]ImageActivation, context *ClassificationContextEvidence) {
				context.Bindings = []ClassificationBinding{{Name: "releaseNamespace", Value: "fugue-system"}}
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			buildPlan := input.BuildPlan
			buildPlan.Artifacts = append([]BuildArtifact(nil), input.BuildPlan.Artifacts...)
			activations := append([]ImageActivation(nil), activationPlan.Activations...)
			classificationContext := context
			classificationContext.Bindings = append([]ClassificationBinding(nil), context.Bindings...)
			if test.mutate != nil {
				test.mutate(&buildPlan, &activations, &classificationContext)
			}
			assertDomains(t, buildPlan, activations, classificationContext, test.authorized, nil)
		})
	}
}

func TestResolveOperationalPlanAllowsExactBuiltOnlyZeroWithoutExecutionAuthorization(t *testing.T) {
	base := md1Deployment("fugue-image-cache", "image-cache", "registry.example/image-cache:live")
	input := md1ActivationFixture(
		t,
		base,
		base,
		[]md1OwnershipRule{{name: "fugue-image-cache", domain: DomainImageCache}},
		[]BuildArtifact{{
			Name: "image_cache", SourceBaseCommit: md0BaseCommit,
			ArtifactDigest: md0Digest("a"), ProvenanceDigest: md0Digest("b"),
			PublishedImageRef: "registry.example/image-cache@" + md0Digest("a"),
		}},
	)
	conservative := BuildPlan(PlanInput{
		Files: FileClassification{
			Domains:  []Domain{},
			Evidence: []Evidence{},
			Unknown: []Evidence{{
				Source: "file", Subject: "cmd/fugue-image-cache/main.go",
				Reason: "fixture source classification is intentionally conservative",
			}},
		},
		Rendered: input.ReleasePlan.Rendered,
		Digests:  input.ReleasePlan.Digests,
	})
	if conservative.Result != OutcomeUnknown {
		t.Fatalf("conservative result = %s, want unknown", conservative.Result)
	}
	input.ReleasePlan = conservative
	activationPlan, activationEvidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(activationPlan.Activations) != 0 || !activationEvidence.Complete ||
		!reflect.DeepEqual(activationEvidence.BuiltOnlyArtifacts, []string{"image_cache"}) {
		t.Fatalf("built-only activation partition = plan=%#v evidence=%#v", activationPlan, activationEvidence)
	}
	spec, err := LoadOwnership(bytes.NewReader(input.Ownership))
	if err != nil {
		t.Fatal(err)
	}
	rendered := ClassifyRendered(input.BaseManifest, input.TargetManifest, spec, RenderedOptions{
		DefaultNamespace: conservative.Digests.ClassificationContext.DefaultNamespace,
		Bindings:         conservative.Digests.ClassificationContext.BindingMap(),
	})
	changed := ChangedFileEvidence{
		baseCommit: md0BaseCommit, targetCommit: md0TargetCommit, digest: md0Digest("f"),
		changes: []ChangedFile{{
			Status: ChangeModified, Path: "cmd/fugue-image-cache/main.go",
			ConsumerDomains: []Domain{DomainImageCache},
		}},
	}
	report, err := BuildOperationalDomainEvidenceFromRenderedOnlyActivation(
		changed, input.BuildPlan, activationPlan, activationEvidence, rendered,
		conservative.Digests.BaseManifest, conservative.Digests.TargetManifest,
		digestBytesSHA256(input.TargetManifest), conservative.Digests.Ownership, conservative,
	)
	if err != nil {
		t.Fatal(err)
	}
	if report.Observation != OutcomeZero || report.AuthorizationEligible || !operationalZeroResolutionEligible(report) {
		t.Fatalf("built-only zero report = %#v", report)
	}

	resolved, err := ResolveOperationalPlan(conservative, report)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Result != OutcomeZero || resolved.SelectedDomain != "" || len(resolved.Domains) != 0 ||
		len(resolved.OperationalEvidence) != 1 {
		t.Fatalf("resolved built-only plan = %#v", resolved)
	}
	if err := VerifyPlanDigest(resolved); err != nil {
		t.Fatal(err)
	}
	if _, err := ActivateOperationalPlan(conservative, report); err == nil {
		t.Fatal("zero-write report created a single-domain execution authorization")
	}

	drifted := report
	drifted.ActivationWitness = append([]OperationalActivationWitness(nil), report.ActivationWitness...)
	drifted.ActivationWitness[0].TargetManifestDigest = md0Digest("9")
	drifted.Digest = operationalEvidenceDigest(drifted)
	if err := VerifyOperationalDomainEvidence(drifted); err != nil {
		t.Fatalf("externally bound digest drift should remain structurally verifiable: %v", err)
	}
	if _, err := ResolveOperationalPlan(conservative, drifted); err == nil {
		t.Fatal("built-only zero resolution ignored target-manifest drift")
	}
}

func TestResolveOperationalPlanAllowsExactEmptyNoOpReconciliation(t *testing.T) {
	base := md1Deployment("fugue-api", "api", "registry.example/api:live")
	input := md1ActivationFixture(
		t,
		base,
		base,
		[]md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}},
		nil,
	)
	conservative := BuildPlan(PlanInput{
		Files: FileClassification{
			Domains:  []Domain{},
			Evidence: []Evidence{},
			Unknown: []Evidence{{
				Source: "file", Subject: "internal/automation/registry.go",
				Reason: "fixture source classification is intentionally conservative",
			}},
		},
		Rendered: input.ReleasePlan.Rendered,
		Digests:  input.ReleasePlan.Digests,
	})
	if conservative.Result != OutcomeUnknown {
		t.Fatalf("conservative result = %s, want unknown", conservative.Result)
	}
	input.ReleasePlan = conservative
	activationPlan, activationEvidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(input.BuildPlan.Artifacts) != 0 || len(activationPlan.Activations) != 0 ||
		len(activationEvidence.BuiltOnlyArtifacts) != 0 || len(activationEvidence.Unresolved) != 0 ||
		!activationEvidence.Complete {
		t.Fatalf("empty no-op activation partition = plan=%#v evidence=%#v", activationPlan, activationEvidence)
	}
	spec, err := LoadOwnership(bytes.NewReader(input.Ownership))
	if err != nil {
		t.Fatal(err)
	}
	rendered := ClassifyRendered(input.BaseManifest, input.TargetManifest, spec, RenderedOptions{
		DefaultNamespace: conservative.Digests.ClassificationContext.DefaultNamespace,
		Bindings:         conservative.Digests.ClassificationContext.BindingMap(),
	})
	changed := ChangedFileEvidence{
		baseCommit: md0BaseCommit, targetCommit: md0TargetCommit, digest: md0Digest("f"),
		changes: []ChangedFile{{
			Status: ChangeModified, Path: "internal/automation/registry.go",
			ConsumerDomains: []Domain{DomainControlPlane},
		}},
	}
	report, err := BuildOperationalDomainEvidenceFromRenderedOnlyActivation(
		changed, input.BuildPlan, activationPlan, activationEvidence, rendered,
		conservative.Digests.BaseManifest, conservative.Digests.TargetManifest,
		digestBytesSHA256(input.TargetManifest), conservative.Digests.Ownership, conservative,
	)
	if err != nil {
		t.Fatal(err)
	}
	if report.Observation != OutcomeZero || report.AuthorizationEligible || report.ClassificationAgrees ||
		!equalDomains(report.ConsumerDomains, []Domain{DomainControlPlane}) ||
		!operationalZeroResolutionEligible(report) {
		t.Fatalf("empty no-op report = %#v", report)
	}

	resolved, err := ResolveOperationalPlan(conservative, report)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Result != OutcomeZero || resolved.SelectedDomain != "" || len(resolved.Domains) != 0 ||
		len(resolved.OperationalEvidence) != 1 {
		t.Fatalf("resolved empty no-op plan = %#v", resolved)
	}
	if err := VerifyPlanDigest(resolved); err != nil {
		t.Fatal(err)
	}
	if _, err := ActivateOperationalPlan(conservative, report); err == nil {
		t.Fatal("empty no-op report created a single-domain execution authorization")
	}

	for _, mutation := range []struct {
		name   string
		mutate func(*OperationalActivationWitness)
	}{
		{name: "target manifest", mutate: func(witness *OperationalActivationWitness) {
			witness.TargetManifestDigest = md0Digest("9")
		}},
		{name: "immutable target manifest", mutate: func(witness *OperationalActivationWitness) {
			witness.ImmutableTargetManifestDigest = md0Digest("9")
		}},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			drifted := report
			drifted.ActivationWitness = append([]OperationalActivationWitness(nil), report.ActivationWitness...)
			mutation.mutate(&drifted.ActivationWitness[0])
			drifted.Digest = operationalEvidenceDigest(drifted)
			if err := VerifyOperationalDomainEvidence(drifted); err != nil {
				t.Fatalf("externally bound digest drift should remain structurally verifiable: %v", err)
			}
			if _, err := ResolveOperationalPlan(conservative, drifted); err == nil {
				t.Fatal("empty no-op resolution ignored manifest drift")
			}
		})
	}
}

func TestActivationOperationalEvidenceReportsRenderedOnlyCandidateWithoutAuthorizingIt(t *testing.T) {
	base := md1Deployment("fugue-api", "api", "registry.example/api:live")
	target := strings.Replace(base,
		"    metadata:\n      labels:",
		"    metadata:\n      annotations:\n        fugue.pro/source-commit: "+md0TargetCommit+"\n      labels:",
		1,
	)
	input := md1ActivationFixture(
		t, base, target,
		[]md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}},
		nil,
	)
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
		changes: []ChangedFile{{Status: ChangeModified, Path: "deploy/helm/fugue/templates/deployment.yaml"}},
	}
	report, err := BuildOperationalDomainEvidenceFromActivationReportOnly(
		changed, input.BuildPlan, activationPlan, activationEvidence, rendered,
		input.ReleasePlan.Digests.BaseManifest, input.ReleasePlan.Digests.TargetManifest,
		digestBytesSHA256(input.TargetManifest), input.ReleasePlan.Digests.Ownership, input.ReleasePlan,
	)
	if err != nil {
		t.Fatal(err)
	}
	if report.Policy != OperationalActivationReportPolicy || report.AuthorizationEligible || len(report.RenderedOnlyObservations) != 1 {
		t.Fatalf("rendered-only report = %#v", report)
	}
	observation := report.RenderedOnlyObservations[0]
	if !observation.Applicable || observation.Observation != OutcomeSingle ||
		observation.CandidateDomain != DomainControlPlane ||
		!equalDomains(observation.IntersectionDomains, []Domain{DomainControlPlane}) || len(observation.Issues) != 0 {
		t.Fatalf("rendered-only report = %#v", report)
	}
	if !containsOperationalIssue(report.Issues, "image activation domains differ from immutable rendered-object domains") {
		t.Fatalf("active authorization inputs were unexpectedly relaxed: %#v", report)
	}

	v2, err := BuildOperationalDomainEvidenceFromActivation(
		changed, input.BuildPlan, activationPlan, activationEvidence, rendered,
		input.ReleasePlan.Digests.BaseManifest, input.ReleasePlan.Digests.TargetManifest,
		digestBytesSHA256(input.TargetManifest), input.ReleasePlan.Digests.Ownership, input.ReleasePlan,
	)
	if err != nil || v2.Policy != OperationalActivationEvidencePolicy || len(v2.RenderedOnlyObservations) != 0 {
		t.Fatalf("v2 compatibility drifted: report=%#v err=%v", v2, err)
	}

	encoded, err := MarshalOperationalDomainEvidence(report)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeAndVerifyOperationalDomainEvidence(bytes.NewReader(encoded), report.Digest)
	if err != nil || !reflect.DeepEqual(decoded, report) {
		t.Fatalf("rendered-only report round trip\n got=%#v\nwant=%#v\nerr=%v", decoded, report, err)
	}
	mutated := report
	mutated.RenderedOnlyObservations = append([]RenderedOnlyOperationalObservation(nil), report.RenderedOnlyObservations...)
	mutated.RenderedOnlyObservations[0].CandidateDomain = DomainBackup
	mutated.Digest = operationalEvidenceDigest(mutated)
	if err := VerifyOperationalDomainEvidence(mutated); err == nil {
		t.Fatal("mutated rendered-only observation unexpectedly verified")
	}
}

func TestActivationOperationalEvidenceAuthorizesRenderedOnlyCandidateOnlyWithV4(t *testing.T) {
	changed, input, activationPlan, activationEvidence, rendered := renderedOnlyOperationalActivationFixture(t)
	report, err := BuildOperationalDomainEvidenceFromRenderedOnlyActivation(
		changed, input.BuildPlan, activationPlan, activationEvidence, rendered,
		input.ReleasePlan.Digests.BaseManifest, input.ReleasePlan.Digests.TargetManifest,
		digestBytesSHA256(input.TargetManifest), input.ReleasePlan.Digests.Ownership, input.ReleasePlan,
	)
	if err != nil {
		t.Fatal(err)
	}
	if report.Policy != OperationalRenderedOnlyActivationPolicy || !report.AuthorizationEligible ||
		report.Observation != OutcomeUnknown || report.CandidateDomain != "" ||
		!reflect.DeepEqual(report.Issues, []string{operationalRenderedOnlyMismatchIssue}) {
		t.Fatalf("rendered-only activation report = %#v", report)
	}
	observation := report.RenderedOnlyObservations[0]
	if !observation.Applicable || observation.Observation != OutcomeSingle ||
		observation.CandidateDomain != DomainControlPlane ||
		!equalDomains(observation.IntersectionDomains, []Domain{DomainControlPlane}) || len(observation.Issues) != 0 {
		t.Fatalf("rendered-only activation observation = %#v", observation)
	}
	authorizedDomain, eligible := operationalAuthorizationCandidate(report)
	if !eligible || authorizedDomain != DomainControlPlane {
		t.Fatalf("rendered-only authorization candidate = %q %t", authorizedDomain, eligible)
	}
	activated, err := ActivateOperationalPlan(input.ReleasePlan, report)
	if err != nil {
		t.Fatal(err)
	}
	if activated.Result != OutcomeSingle || activated.SelectedDomain != DomainControlPlane ||
		!equalDomains(activated.Domains, []Domain{DomainControlPlane}) || len(activated.OperationalEvidence) != 1 {
		t.Fatalf("rendered-only activated plan = %#v", activated)
	}
	if err := VerifyPlanDigest(activated); err != nil {
		t.Fatal(err)
	}
	encoded, err := MarshalOperationalDomainEvidence(report)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeAndVerifyOperationalDomainEvidence(bytes.NewReader(encoded), report.Digest)
	if err != nil || !reflect.DeepEqual(decoded, report) {
		t.Fatalf("rendered-only activation round trip\n got=%#v\nwant=%#v\nerr=%v", decoded, report, err)
	}

	mutated := report
	mutated.Issues = canonicalOperationalStrings(append(mutated.Issues, "unexpected parallel issue"))
	mutated.AuthorizationEligible = operationalAuthorizationEligible(mutated)
	mutated.Digest = operationalEvidenceDigest(mutated)
	if err := VerifyOperationalDomainEvidence(mutated); err != nil {
		t.Fatalf("ineligible issue control did not verify: %v", err)
	}
	if mutated.AuthorizationEligible {
		t.Fatal("rendered-only activation ignored an additional issue")
	}
	if _, err := ActivateOperationalPlan(input.ReleasePlan, mutated); err == nil {
		t.Fatal("rendered-only activation accepted an additional issue")
	}
}

func TestObservedLiveActivationAuthorizesHelmStateAdoptionOnlyWithExactWitness(t *testing.T) {
	targetDigest := md0Digest("b")
	input := md1ActivationFixture(
		t,
		md1DaemonSet("fugue-dns", "dns", "registry.example/edge:helm-old"),
		md1DaemonSet("fugue-dns", "dns", "registry.example/edge@"+targetDigest),
		[]md1OwnershipRule{{name: "fugue-dns", domain: DomainAuthoritativeDNS, kind: "DaemonSet"}},
		nil,
	)
	input.ReleasePlan = BuildPlan(PlanInput{
		Files: FileClassification{Unknown: []Evidence{{
			Source: "file", Subject: "scripts/release_fugue_authoritative_dns.sh",
			Reason: "fixture keeps the source classification conservative",
		}}},
		Rendered: input.ReleasePlan.Rendered,
		Digests:  input.ReleasePlan.Digests,
	})
	if input.ReleasePlan.Result != OutcomeUnknown {
		t.Fatalf("conservative result = %s, want unknown", input.ReleasePlan.Result)
	}
	observed, err := MaterializeObservedLiveImageManifest(
		input.BaseManifest,
		[]byte(md1DaemonSet("fugue-dns", "dns", "registry.example/edge@"+targetDigest)),
		input.Ownership,
		"fugue-system",
	)
	if err != nil {
		t.Fatal(err)
	}
	input.ObservedLiveManifest = observed
	input.ImmutableTargetManifest, err = MaterializeObservedLiveRelativeTargetPublishedImageRefs(
		input.BaseManifest, observed, input.TargetManifest, input.Ownership,
		"fugue-system", input.BuildPlan.TargetCommit, input.BuildPlan, input.ReleasePlan,
	)
	if err != nil {
		t.Fatal(err)
	}
	activationPlan, activationEvidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatal(err)
	}
	if len(activationPlan.Activations) != 0 || !activationEvidence.Complete {
		t.Fatalf("already-active image was not complete: plan=%#v evidence=%#v", activationPlan, activationEvidence)
	}
	spec, err := LoadOwnership(bytes.NewReader(input.Ownership))
	if err != nil {
		t.Fatal(err)
	}
	rendered := ClassifyRendered(
		input.BaseManifest,
		input.ImmutableTargetManifest,
		spec,
		RenderedOptions{
			DefaultNamespace: input.ReleasePlan.Digests.ClassificationContext.DefaultNamespace,
			Bindings:         input.ReleasePlan.Digests.ClassificationContext.BindingMap(),
		},
	)
	changed := ChangedFileEvidence{
		baseCommit: md0BaseCommit, targetCommit: md0TargetCommit, digest: md0Digest("f"),
		changes: []ChangedFile{{Status: ChangeModified, Path: "scripts/release_fugue_authoritative_dns.sh"}},
	}
	report, err := BuildOperationalDomainEvidenceFromObservedLiveActivation(
		changed, input.BuildPlan, activationPlan, activationEvidence, rendered,
		input.ReleasePlan.Digests.BaseManifest, digestBytesSHA256(observed),
		input.ReleasePlan.Digests.TargetManifest, digestBytesSHA256(input.ImmutableTargetManifest),
		input.ReleasePlan.Digests.Ownership, input.ReleasePlan,
	)
	if err != nil {
		t.Fatal(err)
	}
	if report.Policy != OperationalObservedLiveActivationPolicy || !report.AuthorizationEligible ||
		report.ActivationWitness[0].ObservedLiveManifestDigest != digestBytesSHA256(observed) ||
		activationPlan.LiveStateDigest != digestBytesSHA256(observed) {
		t.Fatalf("observed-live report binding drifted: %#v", report)
	}
	if _, err := ResolveOperationalPlan(input.ReleasePlan, report); err == nil {
		t.Fatal("v5 report resolved without its observed-live manifest")
	}
	tampered := append([]byte(nil), observed...)
	tampered[len(tampered)-2] ^= 1
	if _, err := ResolveOperationalPlanWithObservedLiveManifest(input.ReleasePlan, report, tampered); err == nil {
		t.Fatal("v5 report resolved with a different observed-live manifest")
	}
	resolved, err := ResolveOperationalPlanWithObservedLiveManifest(input.ReleasePlan, report, observed)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Result != OutcomeSingle || resolved.SelectedDomain != DomainAuthoritativeDNS ||
		!equalDomains(resolved.Domains, []Domain{DomainAuthoritativeDNS}) {
		t.Fatalf("observed-live Helm state adoption resolved incorrectly: %#v", resolved)
	}
}

func TestRenderedOnlyV4KeepsZeroMultipleAndIncompleteEvidenceFailClosed(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		changed, input, activationPlan, activationEvidence, _ := renderedOnlyOperationalActivationFixture(t)
		rendered := RenderedClassification{Domains: []Domain{}, Evidence: []Evidence{}}
		report, err := BuildOperationalDomainEvidenceFromRenderedOnlyActivation(
			changed, input.BuildPlan, activationPlan, activationEvidence, rendered,
			input.ReleasePlan.Digests.BaseManifest, input.ReleasePlan.Digests.TargetManifest,
			digestBytesSHA256(input.TargetManifest), input.ReleasePlan.Digests.Ownership, input.ReleasePlan,
		)
		if err != nil {
			t.Fatal(err)
		}
		if report.AuthorizationEligible || report.RenderedOnlyObservations[0].Observation != OutcomeZero {
			t.Fatalf("zero rendered-only evidence authorized: %#v", report)
		}
	})

	t.Run("multiple", func(t *testing.T) {
		base := md1Deployment("fugue-api", "api", "registry.example/api:live") + "---\n" +
			md1Deployment("fugue-edge", "edge", "registry.example/edge:live")
		target := strings.ReplaceAll(base,
			"    metadata:\n      labels:",
			"    metadata:\n      annotations:\n        fugue.pro/source-commit: "+md0TargetCommit+"\n      labels:",
		)
		input := md1ActivationFixture(t, base, target, []md1OwnershipRule{
			{name: "fugue-api", domain: DomainControlPlane},
			{name: "fugue-edge", domain: DomainAuthoritativeDNS},
		}, nil)
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
			changes: []ChangedFile{{Status: ChangeModified, Path: "deploy/helm/fugue/templates/deployment.yaml"}},
		}
		report, err := BuildOperationalDomainEvidenceFromRenderedOnlyActivation(
			changed, input.BuildPlan, activationPlan, activationEvidence, rendered,
			input.ReleasePlan.Digests.BaseManifest, input.ReleasePlan.Digests.TargetManifest,
			digestBytesSHA256(input.TargetManifest), input.ReleasePlan.Digests.Ownership, input.ReleasePlan,
		)
		if err != nil {
			t.Fatal(err)
		}
		if report.AuthorizationEligible || report.RenderedOnlyObservations[0].Observation != OutcomeMultiple ||
			!equalDomains(report.RenderedOnlyObservations[0].IntersectionDomains,
				[]Domain{DomainAuthoritativeDNS, DomainControlPlane}) {
			t.Fatalf("multiple rendered-only evidence narrowed: %#v", report)
		}
	})

	t.Run("incomplete", func(t *testing.T) {
		changed, input, activationPlan, activationEvidence, rendered := renderedOnlyOperationalActivationFixture(t)
		rendered.Unknown = []Evidence{{Source: "rendered-object", Subject: "fixture gap"}}
		report, err := BuildOperationalDomainEvidenceFromRenderedOnlyActivation(
			changed, input.BuildPlan, activationPlan, activationEvidence, rendered,
			input.ReleasePlan.Digests.BaseManifest, input.ReleasePlan.Digests.TargetManifest,
			digestBytesSHA256(input.TargetManifest), input.ReleasePlan.Digests.Ownership, input.ReleasePlan,
		)
		if err != nil {
			t.Fatal(err)
		}
		if report.AuthorizationEligible || report.RenderedOnlyObservations[0].Observation != OutcomeUnknown ||
			!containsOperationalIssue(report.RenderedOnlyObservations[0].Issues,
				"rendered-only immutable rendered-object evidence is incomplete") {
			t.Fatalf("incomplete rendered-only evidence authorized: %#v", report)
		}
	})
}

func renderedOnlyOperationalActivationFixture(t *testing.T) (
	ChangedFileEvidence,
	ImageActivationPlanInput,
	ImageActivationPlan,
	ImageActivationEvidence,
	RenderedClassification,
) {
	t.Helper()
	base := md1Deployment("fugue-api", "api", "registry.example/api:live")
	target := strings.Replace(base,
		"    metadata:\n      labels:",
		"    metadata:\n      annotations:\n        fugue.pro/source-commit: "+md0TargetCommit+"\n      labels:",
		1,
	)
	input := md1ActivationFixture(
		t, base, target,
		[]md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}},
		nil,
	)
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
		changes: []ChangedFile{{Status: ChangeModified, Path: "deploy/helm/fugue/templates/deployment.yaml"}},
	}
	return changed, input, activationPlan, activationEvidence, rendered
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

func TestObservedLiveOperationalEvidenceDoesNotReintroduceDisabledWorkerGap(t *testing.T) {
	input := disabledDynamicWorkerActivationInput(t, "dynamic", DomainAuthoritativeDNS)
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
			Status: ChangeModified, Path: "internal/model/model.go",
			ConsumerDomains: []Domain{DomainAuthoritativeDNS},
		}},
	}
	report, err := BuildOperationalDomainEvidenceFromObservedLiveActivation(
		changed, input.BuildPlan, activationPlan, activationEvidence, rendered,
		input.ReleasePlan.Digests.BaseManifest, digestBytesSHA256(input.ObservedLiveManifest),
		input.ReleasePlan.Digests.TargetManifest, digestBytesSHA256(input.TargetManifest),
		input.ReleasePlan.Digests.Ownership, input.ReleasePlan,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !report.ActivationWitness[0].Evidence.Complete ||
		len(report.ActivationWitness[0].Evidence.Unresolved) != 0 ||
		containsOperationalIssue(report.Issues, "image activation evidence is incomplete") {
		t.Fatalf("operational evidence reintroduced disabled worker gap: %#v", report)
	}
	if !reflect.DeepEqual(report.ActivationWitness[0].Evidence.BuiltOnlyArtifacts, []string{"edge"}) {
		t.Fatalf("operational built-only partition drifted: %#v", report.ActivationWitness[0].Evidence)
	}
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

func operationalDeferredImageCacheFixture(t *testing.T, componentBase string) (
	ChangedFileEvidence,
	ImageActivationPlanInput,
	ImageActivationPlan,
	ImageActivationEvidence,
	RenderedClassification,
) {
	t.Helper()
	digest := md0Digest("a")
	base := md1DaemonSet(
		"fugue-fugue-image-cache",
		"image-cache",
		"registry.example/image-cache:live",
	)
	target := md1DaemonSet(
		"fugue-fugue-image-cache",
		"image-cache",
		"registry.example/image-cache@"+digest,
	)
	input := md1ActivationFixture(
		t,
		base,
		target,
		[]md1OwnershipRule{{
			name: "fugue-fugue-image-cache", domain: DomainImageCache, kind: "DaemonSet",
		}},
		[]BuildArtifact{{
			Name: "image_cache", SourceBaseCommit: componentBase, ArtifactDigest: digest,
			ProvenanceDigest: md0Digest("1"), PublishedImageRef: "registry.example/image-cache@" + digest,
		}},
	)
	context, err := NewClassificationContextEvidence(
		"fugue-system",
		map[string]string{
			"imageCacheName":   "fugue-fugue-image-cache",
			"releaseNamespace": "fugue-system",
		},
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	digests := input.ReleasePlan.Digests
	digests.ClassificationContext = context
	conservative := BuildPlan(PlanInput{
		Files: FileClassification{Unknown: []Evidence{{
			Source: "file", Subject: ".github/workflows/deploy-control-plane.yml",
			Reason: "fixture keeps the release-wide classification conservative",
		}}},
		Rendered: input.ReleasePlan.Rendered,
		Digests:  digests,
	})
	if conservative.Result != OutcomeUnknown {
		t.Fatalf("deferred image-cache conservative result = %s, want unknown", conservative.Result)
	}
	input.ReleasePlan = conservative
	activationPlan, activationEvidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := LoadOwnership(bytes.NewReader(input.Ownership))
	if err != nil {
		t.Fatal(err)
	}
	rendered := ClassifyRendered(input.BaseManifest, input.TargetManifest, spec, RenderedOptions{
		DefaultNamespace: conservative.Digests.ClassificationContext.DefaultNamespace,
		Bindings:         conservative.Digests.ClassificationContext.BindingMap(),
	})
	changed := ChangedFileEvidence{
		baseCommit: md0BaseCommit, targetCommit: md0TargetCommit, digest: md0Digest("f"),
		changes: []ChangedFile{{
			Status: ChangeModified, Path: ".github/workflows/deploy-control-plane.yml",
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
