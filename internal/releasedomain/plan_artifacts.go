package releasedomain

import (
	"bytes"
	"fmt"
)

const maxPlanOwnershipBytes = 4 << 20

// PlanArtifactInput is the complete side-effect-free input required to build
// a release-domain plan from durable B3 artifacts. The three manifests must be
// the exact canonical bytes produced for the base, target, and independently
// repeated target render.
type PlanArtifactInput struct {
	Ownership                 []byte
	ChangedFileEvidence       []byte
	TrustedBaseCommit         string
	TrustedTargetCommit       string
	BaseCanonicalManifest     []byte
	TargetCanonicalManifest   []byte
	RepeatedCanonicalManifest []byte
	SSoTBaseDigest            string
	SSoTTargetDigest          string
	SSoTLiveDigest            string
	DefaultNamespace          string
	Bindings                  map[string]string
	IgnoreHelmTestHooks       bool
}

// BuildPlanFromArtifacts validates the revision-bound changed-file evidence,
// loads the ownership SSoT, verifies that all render inputs are canonical, and
// applies the existing dual-evidence planner. A repeated-render mismatch is
// intentionally represented in DigestEvidence and reaches BuildPlan; it is
// therefore persisted as an unknown plan instead of being hidden by an early
// input error.
func BuildPlanFromArtifacts(input PlanArtifactInput) (Plan, error) {
	ownershipData := append([]byte(nil), input.Ownership...)
	if len(ownershipData) > maxPlanOwnershipBytes {
		return Plan{}, fmt.Errorf("ownership exceeds %d-byte limit", maxPlanOwnershipBytes)
	}
	spec, err := LoadOwnership(bytes.NewReader(ownershipData))
	if err != nil {
		return Plan{}, err
	}

	evidenceData := append([]byte(nil), input.ChangedFileEvidence...)
	evidence, err := DecodeAndVerifyChangedFileEvidence(
		bytes.NewReader(evidenceData),
		input.TrustedBaseCommit,
		input.TrustedTargetCommit,
	)
	if err != nil {
		return Plan{}, err
	}

	bindings := make(map[string]string, len(input.Bindings))
	for name, value := range input.Bindings {
		bindings[name] = value
	}
	classificationContext, err := NewClassificationContextEvidence(
		input.DefaultNamespace,
		bindings,
		input.IgnoreHelmTestHooks,
	)
	if err != nil {
		return Plan{}, fmt.Errorf("classification context: %w", err)
	}

	baseManifest := append([]byte(nil), input.BaseCanonicalManifest...)
	targetManifest := append([]byte(nil), input.TargetCanonicalManifest...)
	repeatedTargetManifest := append([]byte(nil), input.RepeatedCanonicalManifest...)
	for _, artifact := range []struct {
		name string
		data []byte
	}{
		{name: "base canonical manifest", data: baseManifest},
		{name: "target canonical manifest", data: targetManifest},
		{name: "repeated target canonical manifest", data: repeatedTargetManifest},
	} {
		canonical, err := CanonicalizeRenderedManifest(artifact.data, spec, classificationContext.DefaultNamespace)
		if err != nil {
			return Plan{}, fmt.Errorf("verify %s: %w", artifact.name, err)
		}
		if !bytes.Equal(canonical, artifact.data) {
			return Plan{}, fmt.Errorf("%s is not in canonical form", artifact.name)
		}
	}

	files := ClassifyFiles(evidence.Changes(), spec)
	renderedOptions := RenderedOptions{
		DefaultNamespace:    classificationContext.DefaultNamespace,
		Bindings:            classificationContext.BindingMap(),
		IgnoreHelmTestHooks: classificationContext.IgnoreHelmTestHooks,
	}
	rendered := ClassifyRendered(baseManifest, targetManifest, spec, renderedOptions)

	return BuildPlan(PlanInput{
		Files:    files,
		Rendered: rendered,
		Digests: DigestEvidence{
			Base:                   input.SSoTBaseDigest,
			Target:                 input.SSoTTargetDigest,
			Live:                   input.SSoTLiveDigest,
			BaseManifest:           digestBytesSHA256(baseManifest),
			TargetManifest:         digestBytesSHA256(targetManifest),
			RepeatedTargetManifest: digestBytesSHA256(repeatedTargetManifest),
			Ownership:              digestBytesSHA256(ownershipData),
			ChangedFiles:           evidence.Digest(),
			ClassificationContext:  classificationContext,
		},
	}), nil
}
