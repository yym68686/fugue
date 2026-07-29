package releasedomain

import "fmt"

// ImageActivationConvergence describes whether every image-cache artifact that
// requires an independent production activation is represented by this
// release's activation plan. Other known artifacts retain their existing
// release policy and remain audit-only when they are built-only.
type ImageActivationConvergence struct {
	PendingArtifacts []string
	Complete         bool
}

// EvaluateImageActivationConvergence verifies the complete build/activation
// partition before identifying mandatory independently activated artifacts
// that are still build-only. A caller must never advance the runtime baseline
// while the returned result is incomplete.
func EvaluateImageActivationConvergence(
	buildPlan BuildArtifactPlan,
	activationPlan ImageActivationPlan,
	activationEvidence ImageActivationEvidence,
) (ImageActivationConvergence, error) {
	if err := VerifyBuildArtifactPlan(buildPlan); err != nil {
		return ImageActivationConvergence{}, fmt.Errorf("verify convergence build plan: %w", err)
	}
	if err := VerifyImageActivationPlan(activationPlan); err != nil {
		return ImageActivationConvergence{}, fmt.Errorf("verify convergence activation plan: %w", err)
	}
	if err := VerifyImageActivationEvidence(activationEvidence); err != nil {
		return ImageActivationConvergence{}, fmt.Errorf("verify convergence activation evidence: %w", err)
	}
	if err := verifyOperationalActivationPartition(buildPlan, activationPlan, activationEvidence); err != nil {
		return ImageActivationConvergence{}, fmt.Errorf("verify convergence activation partition: %w", err)
	}
	if !activationEvidence.Complete || len(activationEvidence.Unresolved) != 0 {
		return ImageActivationConvergence{}, fmt.Errorf("image activation evidence is incomplete")
	}

	pending := make([]string, 0, len(activationEvidence.BuiltOnlyArtifacts))
	for _, artifact := range activationEvidence.BuiltOnlyArtifacts {
		domains, known := operationalImageTargetDomains[artifact]
		if !known {
			return ImageActivationConvergence{}, fmt.Errorf("built-only artifact %q has no known activation policy", artifact)
		}
		// image-cache is the only chart-managed artifact whose rollout is an
		// independent domain in this workflow. In particular, edge artifacts
		// must not turn an image-cache convergence release into a public data
		// plane activation.
		if artifact == "image_cache" && len(domains) != 0 {
			pending = append(pending, artifact)
		}
	}
	return ImageActivationConvergence{
		PendingArtifacts: pending,
		Complete:         len(pending) == 0,
	}, nil
}
