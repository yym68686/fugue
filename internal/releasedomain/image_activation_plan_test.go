package releasedomain

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestBuildImageActivationPlanSeparatesBuildsFromActualWorkloadChanges(t *testing.T) {
	controllerDigest := md0Digest("c")
	input := md1ActivationFixture(
		t,
		md1Deployment("fugue-api", "api", "registry.test/api@"+md0Digest("a")),
		md1Deployment("fugue-api", "api", "registry.test/api@"+controllerDigest),
		[]md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}},
		[]BuildArtifact{
			{Name: "api", SourceBaseCommit: md0BaseCommit, ArtifactDigest: controllerDigest, ProvenanceDigest: md0Digest("f")},
			{Name: "edge", SourceBaseCommit: md0BaseCommit, ArtifactDigest: md0Digest("e"), ProvenanceDigest: md0Digest("f")},
		},
	)

	activation, err := BuildImageActivationPlanFromManifests(input)
	if err != nil {
		t.Fatalf("derive image activation plan: %v", err)
	}
	if len(activation.Activations) != 1 {
		t.Fatalf("activation count = %d, want 1: %#v", len(activation.Activations), activation.Activations)
	}
	got := activation.Activations[0]
	if got.ArtifactName != "api" || got.Domain != DomainControlPlane ||
		got.Adapter != "control_plane_release_adapter_control_plane" ||
		got.Workload.Name != "fugue-api" || got.Workload.Container != "api" ||
		got.TargetImageRef != "registry.test/api@"+controllerDigest {
		t.Fatalf("activation binding drifted: %#v", got)
	}
	if strings.Contains(got.ID, "edge") {
		t.Fatalf("built-only edge artifact entered activation plan: %#v", got)
	}
	if activation.BuildArtifactPlanDigest != input.BuildPlan.Digest || activation.LiveStateDigest != input.ReleasePlan.Digests.BaseManifest {
		t.Fatalf("activation plan digest binding drifted: %#v", activation)
	}
}

func TestBuildImageActivationPlanAssignsSharedImagePerRenderedWorkloadDomain(t *testing.T) {
	sharedDigest := md0Digest("c")
	base := strings.Join([]string{
		md1Deployment("fugue-api", "service", "registry.test/shared@"+md0Digest("a")),
		md1Deployment("fugue-dns", "service", "registry.test/shared@"+md0Digest("b")),
	}, "\n---\n")
	target := strings.Join([]string{
		md1Deployment("fugue-api", "service", "registry.test/shared@"+sharedDigest),
		md1Deployment("fugue-dns", "service", "registry.test/shared@"+sharedDigest),
	}, "\n---\n")
	input := md1ActivationFixture(
		t,
		base,
		target,
		[]md1OwnershipRule{
			{name: "fugue-api", domain: DomainControlPlane},
			{name: "fugue-dns", domain: DomainAuthoritativeDNS},
		},
		[]BuildArtifact{{Name: "shared", SourceBaseCommit: md0BaseCommit, ArtifactDigest: sharedDigest, ProvenanceDigest: md0Digest("f")}},
	)

	activation, err := BuildImageActivationPlanFromManifests(input)
	if err != nil {
		t.Fatalf("derive shared activation plan: %v", err)
	}
	if len(activation.Activations) != 2 {
		t.Fatalf("activation count = %d, want 2", len(activation.Activations))
	}
	domains := map[Domain]string{}
	for _, item := range activation.Activations {
		domains[item.Domain] = item.Adapter
	}
	if domains[DomainControlPlane] != "control_plane_release_adapter_control_plane" ||
		domains[DomainAuthoritativeDNS] != "control_plane_release_adapter_authoritative_dns" {
		t.Fatalf("shared image was not assigned by rendered workload ownership: %#v", domains)
	}
}

func TestBuildImageActivationReportKeepsOwnershipGapExplicit(t *testing.T) {
	telemetryDigest := md0Digest("d")
	input := md1ActivationFixture(
		t,
		md1Deployment("fugue-telemetry-agent", "telemetry-agent", "registry.test/telemetry@"+md0Digest("c")),
		md1Deployment("fugue-telemetry-agent", "telemetry-agent", "registry.test/telemetry@"+telemetryDigest),
		[]md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}},
		[]BuildArtifact{{Name: "telemetry_agent", SourceBaseCommit: md0BaseCommit, ArtifactDigest: telemetryDigest, ProvenanceDigest: md0Digest("f")}},
	)

	activation, evidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatalf("derive report-only activation evidence: %v", err)
	}
	if len(activation.Activations) != 0 || evidence.Complete || len(evidence.Unresolved) != 1 {
		t.Fatalf("ownership gap was hidden or promoted: plan=%#v evidence=%#v", activation, evidence)
	}
	gap := evidence.Unresolved[0]
	if gap.Reason != ImageActivationGapOwnershipMissing ||
		!reflect.DeepEqual(gap.MatchingBuildArtifacts, []string{"telemetry_agent"}) ||
		len(gap.OwnershipDomains) != 0 || len(evidence.BuiltOnlyArtifacts) != 0 ||
		evidence.ResolvedImageActivationPlanDigest != activation.Digest {
		t.Fatalf("ownership gap evidence drifted: %#v", evidence)
	}
}

func TestBuildImageActivationPlanFailsClosedOnIncompleteEvidence(t *testing.T) {
	targetDigest := md0Digest("d")
	valid := md1ActivationFixture(
		t,
		md1Deployment("fugue-api", "api", "registry.test/api@"+md0Digest("a")),
		md1Deployment("fugue-api", "api", "registry.test/api@"+targetDigest),
		[]md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}},
		[]BuildArtifact{{Name: "api", SourceBaseCommit: md0BaseCommit, ArtifactDigest: targetDigest, ProvenanceDigest: md0Digest("f")}},
	)

	tests := []struct {
		name   string
		mutate func(ImageActivationPlanInput) ImageActivationPlanInput
	}{
		{name: "manifest digest drift", mutate: func(input ImageActivationPlanInput) ImageActivationPlanInput {
			input.TargetManifest = append(append([]byte(nil), input.TargetManifest...), '\n')
			return input
		}},
		{name: "ambiguous artifact digest", mutate: func(input ImageActivationPlanInput) ImageActivationPlanInput {
			input.BuildPlan.Artifacts = append(input.BuildPlan.Artifacts, BuildArtifact{
				Name: "api-copy", SourceBaseCommit: md0BaseCommit,
				ArtifactDigest: targetDigest, ProvenanceDigest: md0Digest("e"),
			})
			input.BuildPlan.Artifacts = canonicalBuildArtifacts(input.BuildPlan.Artifacts)
			input.BuildPlan.Digest = buildArtifactPlanDigest(input.BuildPlan)
			return input
		}},
		{name: "unverified target image", mutate: func(input ImageActivationPlanInput) ImageActivationPlanInput {
			input.BuildPlan.Artifacts = []BuildArtifact{{
				Name: "edge", SourceBaseCommit: md0BaseCommit,
				ArtifactDigest: md0Digest("e"), ProvenanceDigest: md0Digest("f"),
			}}
			input.BuildPlan.Digest = buildArtifactPlanDigest(input.BuildPlan)
			return input
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := BuildImageActivationPlanFromManifests(test.mutate(valid)); err == nil {
				t.Fatal("incomplete evidence unexpectedly produced an activation plan")
			}
		})
	}
}

func TestBuildImageActivationPlanKeepsAbsentCreateOutOfImageReplacement(t *testing.T) {
	targetDigest := md0Digest("d")
	input := md1ActivationFixture(
		t,
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: preserved\n  namespace: fugue-system\ndata:\n  value: stable\n",
		md1Deployment("fugue-api", "api", "registry.test/api@"+targetDigest),
		[]md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}},
		[]BuildArtifact{{Name: "api", SourceBaseCommit: md0BaseCommit, ArtifactDigest: targetDigest, ProvenanceDigest: md0Digest("f")}},
	)
	if _, err := BuildImageActivationPlanFromManifests(input); err == nil || !strings.Contains(err.Error(), "absent-create") {
		t.Fatalf("absent-create boundary was not enforced: %v", err)
	}
}

func TestBuildImageActivationReportPreservesAbsentNonImmutableTarget(t *testing.T) {
	input := md1ActivationFixture(
		t,
		"apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: preserved\n  namespace: fugue-system\ndata:\n  value: stable\n",
		md1Deployment("fugue-api", "api", "registry.test/api:unreleased"),
		[]md1OwnershipRule{{name: "fugue-api", domain: DomainControlPlane}},
		[]BuildArtifact{{Name: "api", SourceBaseCommit: md0BaseCommit, ArtifactDigest: md0Digest("d"), ProvenanceDigest: md0Digest("f")}},
	)

	activation, evidence, err := BuildImageActivationReportFromManifests(input)
	if err != nil {
		t.Fatalf("derive report for absent non-immutable target: %v", err)
	}
	if len(activation.Activations) != 0 || evidence.Complete || len(evidence.Unresolved) != 1 {
		t.Fatalf("absent non-immutable target was hidden or promoted: plan=%#v evidence=%#v", activation, evidence)
	}
	gap := evidence.Unresolved[0]
	if gap.Reason != ImageActivationGapTargetNotImmutable || gap.LiveImageRef != "" ||
		gap.ReverseRenderedDigest != "" || gap.ArtifactDigest != "" ||
		len(gap.MatchingBuildArtifacts) != 0 {
		t.Fatalf("absent non-immutable target evidence drifted: %#v", gap)
	}
}

type md1OwnershipRule struct {
	name   string
	domain Domain
}

func md1ActivationFixture(t *testing.T, baseRaw, targetRaw string, rules []md1OwnershipRule, artifacts []BuildArtifact) ImageActivationPlanInput {
	t.Helper()
	ownership := md1Ownership(rules)
	spec, err := LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		t.Fatalf("load ownership: %v", err)
	}
	base, err := CanonicalizeRenderedManifest([]byte(baseRaw), spec, "fugue-system")
	if err != nil {
		t.Fatalf("canonicalize base: %v", err)
	}
	target, err := CanonicalizeRenderedManifest([]byte(targetRaw), spec, "fugue-system")
	if err != nil {
		t.Fatalf("canonicalize target: %v", err)
	}
	context, err := NewClassificationContextEvidence(
		"fugue-system",
		map[string]string{"releaseNamespace": "fugue-system"},
		false,
	)
	if err != nil {
		t.Fatalf("classification context: %v", err)
	}
	changedDigest := md0Digest("f")
	build, err := NewBuildArtifactPlan(md0BaseCommit, md0TargetCommit, changedDigest, artifacts)
	if err != nil {
		t.Fatalf("build artifact plan: %v", err)
	}
	rendered := ClassifyRendered(base, target, spec, RenderedOptions{
		DefaultNamespace: "fugue-system",
		Bindings:         context.BindingMap(),
	})
	plan := BuildPlan(PlanInput{
		Files:    FileClassification{Domains: []Domain{}, Evidence: []Evidence{}},
		Rendered: rendered,
		Digests: DigestEvidence{
			Base: md0Digest("1"), Target: md0Digest("2"), Live: md0Digest("1"),
			BaseManifest: digestBytesSHA256(base), TargetManifest: digestBytesSHA256(target),
			RepeatedTargetManifest: digestBytesSHA256(target), Ownership: digestBytesSHA256(ownership),
			ChangedFiles: changedDigest, ClassificationContext: context,
		},
	})
	return ImageActivationPlanInput{
		BuildPlan: build, ReleasePlan: plan, Ownership: ownership,
		BaseManifest: base, TargetManifest: target,
	}
}

func md1Ownership(rules []md1OwnershipRule) []byte {
	var result strings.Builder
	result.WriteString("apiVersion: release-domain.fugue.dev/v1\nkind: ReleaseDomainOwnership\ndomains:\n")
	for _, domain := range KnownDomains() {
		fmt.Fprintf(&result, "  - %s\n", domain)
	}
	result.WriteString("requiredBindings:\n  - releaseNamespace\nfileRules: []\nvalueRules: []\nobjectRules:\n")
	for index, rule := range rules {
		fmt.Fprintf(&result, "  - id: workload-%d\n    domain: %s\n    apiGroup: apps\n    version: v1\n    kind: Deployment\n    scope: Namespaced\n    namespace: ${releaseNamespace}\n    name: %s\n", index, rule.domain, rule.name)
	}
	return []byte(result.String())
}

func md1Deployment(name, container, image string) string {
	return fmt.Sprintf("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: %s\n  namespace: fugue-system\nspec:\n  selector:\n    matchLabels:\n      app: %s\n  template:\n    metadata:\n      labels:\n        app: %s\n    spec:\n      containers:\n        - name: %s\n          image: %s\n", name, name, name, container, image)
}
