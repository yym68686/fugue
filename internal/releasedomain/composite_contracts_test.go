package releasedomain

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

const (
	md0BaseCommit   = "1111111111111111111111111111111111111111"
	md0TargetCommit = "2222222222222222222222222222222222222222"
)

func TestBuildAndActivationPlansKeepUnactivatedArtifactsDormant(t *testing.T) {
	build, err := NewBuildArtifactPlan(md0BaseCommit, md0TargetCommit, md0Digest("a"), []BuildArtifact{
		{Name: "telemetry_agent", SourceBaseCommit: md0BaseCommit, ArtifactDigest: md0Digest("b"), ProvenanceDigest: md0Digest("c")},
		{Name: "controller", SourceBaseCommit: md0BaseCommit, ArtifactDigest: md0Digest("d"), ProvenanceDigest: md0Digest("e")},
	})
	if err != nil {
		t.Fatalf("new build artifact plan: %v", err)
	}
	if got := []string{build.Artifacts[0].Name, build.Artifacts[1].Name}; !reflect.DeepEqual(got, []string{"controller", "telemetry_agent"}) {
		t.Fatalf("build artifacts are not canonical: %v", got)
	}
	encoded, err := MarshalBuildArtifactPlan(build)
	if err != nil {
		t.Fatalf("marshal build artifact plan: %v", err)
	}
	decoded, err := DecodeAndVerifyBuildArtifactPlan(bytes.NewReader(encoded), build.Digest)
	if err != nil || !reflect.DeepEqual(decoded, build) {
		t.Fatalf("build artifact round trip failed: decoded=%#v err=%v", decoded, err)
	}

	activation, err := NewImageActivationPlan(md0BaseCommit, md0TargetCommit, build.Digest, md0Digest("f"), []ImageActivation{
		md0Activation("controller", "controller", DomainControlPlane, md0Digest("d")),
	})
	if err != nil {
		t.Fatalf("new image activation plan: %v", err)
	}
	if len(activation.Activations) != 1 || activation.Activations[0].ArtifactName != "controller" {
		t.Fatalf("unexpected activation inventory: %#v", activation.Activations)
	}
	for _, item := range activation.Activations {
		if item.ArtifactName == "telemetry_agent" {
			t.Fatal("a built-but-unactivated artifact entered the activation plan")
		}
	}

	empty, err := NewImageActivationPlan(md0BaseCommit, md0TargetCommit, build.Digest, md0Digest("0"), nil)
	if err != nil || empty.Activations == nil || len(empty.Activations) != 0 {
		t.Fatalf("empty dormant activation plan failed: %#v err=%v", empty, err)
	}
}

func TestBuildArtifactPlanPublishedImageRefIsOptionalAndSealed(t *testing.T) {
	artifactDigest := md0Digest("b")
	legacy, err := NewBuildArtifactPlan(md0BaseCommit, md0TargetCommit, md0Digest("a"), []BuildArtifact{{
		Name: "api", SourceBaseCommit: md0BaseCommit,
		ArtifactDigest: artifactDigest, ProvenanceDigest: md0Digest("c"),
	}})
	if err != nil {
		t.Fatalf("new legacy build artifact plan: %v", err)
	}
	const legacyDigest = "sha256:2020db02d62b2164eb31761657c56fd79c8757ead8b7681a7059bf29b6abddea"
	if legacy.Digest != legacyDigest {
		t.Fatalf("optional field changed the legacy digest: got %q want %q", legacy.Digest, legacyDigest)
	}
	legacyBytes, err := MarshalBuildArtifactPlan(legacy)
	if err != nil {
		t.Fatalf("marshal legacy build artifact plan: %v", err)
	}
	if bytes.Contains(legacyBytes, []byte("publishedImageRef")) {
		t.Fatalf("empty optional field changed legacy JSON: %s", legacyBytes)
	}
	decodedLegacy, err := DecodeAndVerifyBuildArtifactPlan(bytes.NewReader(legacyBytes), legacy.Digest)
	if err != nil || !reflect.DeepEqual(decodedLegacy, legacy) {
		t.Fatalf("legacy build artifact round trip failed: decoded=%#v err=%v", decodedLegacy, err)
	}

	publishedRef := "ghcr.io/yym68686/fugue-api@" + artifactDigest
	sealed, err := NewBuildArtifactPlan(md0BaseCommit, md0TargetCommit, md0Digest("a"), []BuildArtifact{{
		Name: "api", SourceBaseCommit: md0BaseCommit,
		ArtifactDigest: artifactDigest, ProvenanceDigest: md0Digest("c"), PublishedImageRef: publishedRef,
	}})
	if err != nil {
		t.Fatalf("new sealed build artifact plan: %v", err)
	}
	if sealed.Digest == legacy.Digest {
		t.Fatal("published image reference was not bound into the plan digest")
	}
	sealedBytes, err := MarshalBuildArtifactPlan(sealed)
	if err != nil || !bytes.Contains(sealedBytes, []byte(`"publishedImageRef": "`+publishedRef+`"`)) {
		t.Fatalf("published image reference was not serialized: bytes=%s err=%v", sealedBytes, err)
	}
	decodedSealed, err := DecodeAndVerifyBuildArtifactPlan(bytes.NewReader(sealedBytes), sealed.Digest)
	if err != nil || !reflect.DeepEqual(decodedSealed, sealed) {
		t.Fatalf("sealed build artifact round trip failed: decoded=%#v err=%v", decodedSealed, err)
	}

	for name, reference := range map[string]string{
		"mutable tag":       "ghcr.io/yym68686/fugue-api:latest",
		"mismatched digest": "ghcr.io/yym68686/fugue-api@" + md0Digest("d"),
		"whitespace":        " ghcr.io/yym68686/fugue-api@" + artifactDigest,
	} {
		t.Run(name, func(t *testing.T) {
			mutated := sealed
			mutated.Artifacts = append([]BuildArtifact(nil), sealed.Artifacts...)
			mutated.Artifacts[0].PublishedImageRef = reference
			mutated.Digest = buildArtifactPlanDigest(mutated)
			if err := VerifyBuildArtifactPlan(mutated); err == nil {
				t.Fatal("invalid published image reference must fail closed")
			}
		})
	}
}

func TestImageActivationPlanBindsWorkloadDomainAdapterAndRenderedEvidence(t *testing.T) {
	build, err := NewBuildArtifactPlan(md0BaseCommit, md0TargetCommit, md0Digest("1"), nil)
	if err != nil {
		t.Fatalf("new build artifact plan: %v", err)
	}
	plan, err := NewImageActivationPlan(md0BaseCommit, md0TargetCommit, build.Digest, md0Digest("2"), []ImageActivation{
		md0Activation("dns", "edge", DomainAuthoritativeDNS, md0Digest("3")),
		md0Activation("api", "api", DomainControlPlane, md0Digest("4")),
	})
	if err != nil {
		t.Fatalf("new image activation plan: %v", err)
	}
	if got := []string{plan.Activations[0].ID, plan.Activations[1].ID}; !reflect.DeepEqual(got, []string{"api", "dns"}) {
		t.Fatalf("activation ordering drifted: %v", got)
	}
	encoded, err := MarshalImageActivationPlan(plan)
	if err != nil {
		t.Fatalf("marshal image activation plan: %v", err)
	}
	decoded, err := DecodeAndVerifyImageActivationPlan(bytes.NewReader(encoded), plan.Digest)
	if err != nil || !reflect.DeepEqual(decoded, plan) {
		t.Fatalf("image activation round trip failed: decoded=%#v err=%v", decoded, err)
	}

	mutated := plan
	mutated.Activations = append([]ImageActivation(nil), plan.Activations...)
	mutated.Activations[0].Adapter = "manual-domain-hint"
	mutated.Digest = imageActivationPlanDigest(mutated)
	if err := VerifyImageActivationPlan(mutated); err == nil {
		t.Fatal("wrong fixed adapter must fail closed")
	}
	mutated = plan
	mutated.Activations = append([]ImageActivation(nil), plan.Activations...)
	mutated.Activations[1].Workload = mutated.Activations[0].Workload
	mutated.Digest = imageActivationPlanDigest(mutated)
	if err := VerifyImageActivationPlan(mutated); err == nil {
		t.Fatal("duplicate rendered workload must fail closed")
	}

	duplicate := bytes.Replace(encoded, []byte(`"kind":`), []byte(`"kind":"duplicate","kind":`), 1)
	if _, err := DecodeAndVerifyImageActivationPlan(bytes.NewReader(duplicate), plan.Digest); err == nil {
		t.Fatal("duplicate JSON field must fail closed")
	}
	unknown := bytes.Replace(encoded, []byte(`"kind":`), []byte(`"unknown":true,"kind":`), 1)
	if _, err := DecodeAndVerifyImageActivationPlan(bytes.NewReader(unknown), plan.Digest); err == nil {
		t.Fatal("unknown JSON field must fail closed")
	}
}

func TestCompositeReleasePlanCanonicalizesAndSealsOrderedDAG(t *testing.T) {
	plan, err := NewCompositeReleasePlan(md0CompositeFixture())
	if err != nil {
		t.Fatalf("new composite release plan: %v", err)
	}
	if got := []Domain{plan.BaseVersions[0].Domain, plan.BaseVersions[1].Domain}; !reflect.DeepEqual(got, []Domain{DomainAuthoritativeDNS, DomainControlPlane}) {
		t.Fatalf("version vector is not canonical: %v", got)
	}
	if !reflect.DeepEqual(plan.Steps[1].DependsOn, []string{"control-plane"}) {
		t.Fatalf("dependency ordering drifted: %v", plan.Steps[1].DependsOn)
	}
	encoded, err := MarshalCompositeReleasePlan(plan)
	if err != nil {
		t.Fatalf("marshal composite release plan: %v", err)
	}
	decoded, err := DecodeAndVerifyCompositeReleasePlan(bytes.NewReader(encoded), plan.Digest)
	if err != nil || !reflect.DeepEqual(decoded, plan) {
		t.Fatalf("composite round trip failed: decoded=%#v err=%v", decoded, err)
	}
}

func TestCompositeReleasePlanRejectsUnsafeStructure(t *testing.T) {
	valid, err := NewCompositeReleasePlan(md0CompositeFixture())
	if err != nil {
		t.Fatalf("new composite release plan: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*CompositeReleasePlan)
	}{
		{name: "zero generation", mutate: func(plan *CompositeReleasePlan) { plan.Generation = "0" }},
		{name: "wrong adapter", mutate: func(plan *CompositeReleasePlan) { plan.Steps[0].Adapter = "manual" }},
		{name: "future dependency", mutate: func(plan *CompositeReleasePlan) { plan.Steps[0].DependsOn = []string{"authoritative-dns"} }},
		{name: "duplicate dependency", mutate: func(plan *CompositeReleasePlan) { plan.Steps[1].DependsOn = []string{"control-plane", "control-plane"} }},
		{name: "duplicate activation", mutate: func(plan *CompositeReleasePlan) { plan.Steps[1].ActivationIDs = []string{"activate-api"} }},
		{name: "version mismatch", mutate: func(plan *CompositeReleasePlan) { plan.Steps[1].TargetVersion = md0Digest("9") }},
		{name: "noncanonical fencing epoch", mutate: func(plan *CompositeReleasePlan) { plan.FencingEpoch = "011" }},
		{name: "missing rollback budget", mutate: func(plan *CompositeReleasePlan) { plan.Steps[1].RollbackBudgetSeconds = "0" }},
		{name: "single domain", mutate: func(plan *CompositeReleasePlan) {
			plan.BaseVersions = []DomainVersion{plan.BaseVersions[1]}
			plan.TargetVersions = []DomainVersion{plan.TargetVersions[1]}
			plan.Steps = []CompositeReleaseStep{plan.Steps[0], plan.Steps[0]}
			plan.Steps[1].ID = "control-plane-two"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mutated := valid
			mutated.BaseVersions = append([]DomainVersion(nil), valid.BaseVersions...)
			mutated.TargetVersions = append([]DomainVersion(nil), valid.TargetVersions...)
			mutated.Steps = cloneCompositeSteps(valid.Steps)
			test.mutate(&mutated)
			mutated.Digest = compositeReleasePlanDigest(mutated)
			if err := VerifyCompositeReleasePlan(mutated); err == nil {
				t.Fatal("unsafe composite structure must fail closed")
			}
		})
	}
}

func TestCompositeReleasePlanConstructorRejectsDuplicateDependency(t *testing.T) {
	fixture := md0CompositeFixture()
	fixture.Steps[1].DependsOn = []string{"control-plane", "control-plane"}
	if _, err := NewCompositeReleasePlan(fixture); err == nil {
		t.Fatal("constructor must reject duplicate dependencies instead of normalizing them away")
	}
}

func md0Activation(id, artifact string, domain Domain, digest string) ImageActivation {
	adapter, ok := fixedAdapterForDomain(domain)
	if !ok {
		panic("unknown fixture domain")
	}
	return ImageActivation{
		ID: id, ArtifactName: artifact, ArtifactDigest: digest,
		Workload: ActivationWorkload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-" + id, Container: id},
		Domain:   domain, Adapter: adapter,
		LiveImageRef:          "ghcr.io/fugue/" + artifact + "@" + md0Digest("a"),
		TargetImageRef:        "ghcr.io/fugue/" + artifact + "@" + digest,
		ForwardRenderedDigest: md0Digest("b"), ReverseRenderedDigest: md0Digest("c"),
	}
}

func md0CompositeFixture() CompositeReleasePlan {
	controlAdapter, _ := fixedAdapterForDomain(DomainControlPlane)
	dnsAdapter, _ := fixedAdapterForDomain(DomainAuthoritativeDNS)
	return CompositeReleasePlan{
		BaseCommit: md0BaseCommit, TargetCommit: md0TargetCommit,
		ImageActivationPlanDigest: md0Digest("d"), Generation: "7", FencingEpoch: "11",
		BaseVersions: []DomainVersion{
			{Domain: DomainControlPlane, Version: md0Digest("e")},
			{Domain: DomainAuthoritativeDNS, Version: md0Digest("f")},
		},
		TargetVersions: []DomainVersion{
			{Domain: DomainControlPlane, Version: md0Digest("1")},
			{Domain: DomainAuthoritativeDNS, Version: md0Digest("2")},
		},
		Steps: []CompositeReleaseStep{
			{
				ID: "control-plane", Domain: DomainControlPlane, Adapter: controlAdapter,
				DependsOn: nil, ActivationIDs: []string{"activate-api"},
				BaseVersion: md0Digest("e"), TargetVersion: md0Digest("1"),
				ForwardRenderedDigest: md0Digest("3"), ReverseRenderedDigest: md0Digest("4"),
				Observation:           CompositeObservationPolicy{HealthEvidenceDigest: md0Digest("5"), MinimumSamples: "5", WindowSeconds: "120"},
				RollbackBudgetSeconds: "600",
			},
			{
				ID: "authoritative-dns", Domain: DomainAuthoritativeDNS, Adapter: dnsAdapter,
				DependsOn: []string{"control-plane"}, ActivationIDs: []string{"activate-dns"},
				BaseVersion: md0Digest("f"), TargetVersion: md0Digest("2"),
				ForwardRenderedDigest: md0Digest("6"), ReverseRenderedDigest: md0Digest("7"),
				Observation:           CompositeObservationPolicy{HealthEvidenceDigest: md0Digest("8"), MinimumSamples: "5", WindowSeconds: "180"},
				RollbackBudgetSeconds: "900",
			},
		},
	}
}

func md0Digest(digit string) string {
	return "sha256:" + strings.Repeat(digit, 64)
}
