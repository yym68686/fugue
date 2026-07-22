package releasedomain

import "fmt"

// CompositeDomainObservationRequirement supplies the pre-write health and
// recovery budget for exactly one domain already present in verified
// decomposition evidence. It cannot select a domain, adapter, activation, or
// dependency that the evidence did not derive.
type CompositeDomainObservationRequirement struct {
	Domain                Domain
	HealthEvidenceDigest  string
	MinimumSamples        string
	WindowSeconds         string
	RollbackBudgetSeconds string
}

// MaterializeCompositeReleasePlan deterministically promotes one complete,
// report-only decomposition into the strict dormant composite plan contract.
// Generation and fencing epoch remain coordinator-controlled inputs. This
// function authorizes no transaction and exposes no adapter or write path.
func MaterializeCompositeReleasePlan(
	decomposition CompositeDecompositionEvidence,
	generation string,
	fencingEpoch string,
	observations []CompositeDomainObservationRequirement,
) (CompositeReleasePlan, error) {
	if err := VerifyCompositeDecompositionEvidence(decomposition); err != nil {
		return CompositeReleasePlan{}, fmt.Errorf("verify composite decomposition: %w", err)
	}
	if !decomposition.Complete || len(decomposition.Issues) != 0 || len(decomposition.Steps) < 2 {
		return CompositeReleasePlan{}, fmt.Errorf("composite decomposition is incomplete")
	}

	observationByDomain := make(map[Domain]CompositeDomainObservationRequirement, len(observations))
	for _, observation := range observations {
		if _, err := ParseDomain(string(observation.Domain)); err != nil {
			return CompositeReleasePlan{}, fmt.Errorf("composite observation domain: %w", err)
		}
		if _, duplicate := observationByDomain[observation.Domain]; duplicate {
			return CompositeReleasePlan{}, fmt.Errorf("composite observation domain is duplicated")
		}
		observationByDomain[observation.Domain] = observation
	}
	if len(observationByDomain) != len(decomposition.Steps) {
		return CompositeReleasePlan{}, fmt.Errorf("composite observations do not match decomposed domains")
	}

	baseVersions := make([]DomainVersion, 0, len(decomposition.Steps))
	targetVersions := make([]DomainVersion, 0, len(decomposition.Steps))
	steps := make([]CompositeReleaseStep, 0, len(decomposition.Steps))
	for _, decomposed := range decomposition.Steps {
		observation, ok := observationByDomain[decomposed.Domain]
		if !ok {
			return CompositeReleasePlan{}, fmt.Errorf("composite observation is missing for decomposed domain %q", decomposed.Domain)
		}
		delete(observationByDomain, decomposed.Domain)

		baseVersions = append(baseVersions, DomainVersion{
			Domain: decomposed.Domain, Version: decomposed.ReverseRenderedDigest,
		})
		targetVersions = append(targetVersions, DomainVersion{
			Domain: decomposed.Domain, Version: decomposed.ForwardRenderedDigest,
		})
		steps = append(steps, CompositeReleaseStep{
			ID:                    decomposed.ID,
			Domain:                decomposed.Domain,
			Adapter:               decomposed.Adapter,
			DependsOn:             append([]string(nil), decomposed.DependsOn...),
			ActivationIDs:         append([]string(nil), decomposed.ActivationIDs...),
			BaseVersion:           decomposed.ReverseRenderedDigest,
			TargetVersion:         decomposed.ForwardRenderedDigest,
			ForwardRenderedDigest: decomposed.ForwardRenderedDigest,
			ReverseRenderedDigest: decomposed.ReverseRenderedDigest,
			Observation: CompositeObservationPolicy{
				HealthEvidenceDigest: observation.HealthEvidenceDigest,
				MinimumSamples:       observation.MinimumSamples,
				WindowSeconds:        observation.WindowSeconds,
			},
			RollbackBudgetSeconds: observation.RollbackBudgetSeconds,
		})
	}
	if len(observationByDomain) != 0 {
		return CompositeReleasePlan{}, fmt.Errorf("composite observation contains a domain absent from decomposition")
	}

	plan, err := NewCompositeReleasePlan(CompositeReleasePlan{
		BaseCommit:                decomposition.BaseCommit,
		TargetCommit:              decomposition.TargetCommit,
		ImageActivationPlanDigest: decomposition.ImageActivationPlanDigest,
		Generation:                generation,
		FencingEpoch:              fencingEpoch,
		BaseVersions:              baseVersions,
		TargetVersions:            targetVersions,
		Steps:                     steps,
	})
	if err != nil {
		return CompositeReleasePlan{}, fmt.Errorf("materialize composite release plan: %w", err)
	}
	return plan, nil
}
