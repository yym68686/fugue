package releasedomain

import (
	"encoding/json"
	"fmt"
	"sort"
)

// BuildCompositeDecompositionEvidence groups verified image activations by
// their fixed operational domain. The result is report-only: it preserves
// unresolved activation gaps and cannot authorize or execute a mutation.
func BuildCompositeDecompositionEvidence(plan ImageActivationPlan, activationEvidence ImageActivationEvidence) (CompositeDecompositionEvidence, error) {
	if err := VerifyImageActivationPlan(plan); err != nil {
		return CompositeDecompositionEvidence{}, fmt.Errorf("verify image activation plan: %w", err)
	}
	if err := VerifyImageActivationEvidence(activationEvidence); err != nil {
		return CompositeDecompositionEvidence{}, fmt.Errorf("verify image activation evidence: %w", err)
	}
	if plan.BaseCommit != activationEvidence.BaseCommit || plan.TargetCommit != activationEvidence.TargetCommit ||
		plan.BuildArtifactPlanDigest != activationEvidence.BuildArtifactPlanDigest ||
		plan.Digest != activationEvidence.ResolvedImageActivationPlanDigest {
		return CompositeDecompositionEvidence{}, fmt.Errorf("image activation plan and evidence binding mismatch")
	}

	unresolved := make([]string, 0, len(activationEvidence.Unresolved))
	seen := make(map[string]struct{}, len(plan.Activations)+len(activationEvidence.Unresolved))
	grouped := make(map[Domain][]ImageActivation)
	for _, activation := range plan.Activations {
		seen[activation.ID] = struct{}{}
		grouped[activation.Domain] = append(grouped[activation.Domain], activation)
	}
	for _, gap := range activationEvidence.Unresolved {
		if _, collision := seen[gap.ID]; collision {
			return CompositeDecompositionEvidence{}, fmt.Errorf("resolved and unresolved activation identities overlap")
		}
		seen[gap.ID] = struct{}{}
		unresolved = append(unresolved, gap.ID)
	}

	steps := make([]CompositeDecompositionStep, 0, len(grouped))
	for _, domain := range KnownDomains() {
		activations := grouped[domain]
		if len(activations) == 0 {
			continue
		}
		adapter, ok := fixedAdapterForDomain(domain)
		if !ok {
			return CompositeDecompositionEvidence{}, fmt.Errorf("composite decomposition domain has no fixed adapter")
		}
		activationIDs := make([]string, 0, len(activations))
		for _, activation := range activations {
			if activation.Adapter != adapter {
				return CompositeDecompositionEvidence{}, fmt.Errorf("composite decomposition activation adapter binding mismatch")
			}
			activationIDs = append(activationIDs, activation.ID)
		}
		dependsOn := []string{}
		if len(steps) > 0 {
			dependsOn = []string{steps[len(steps)-1].ID}
		}
		steps = append(steps, CompositeDecompositionStep{
			ID:                    string(domain),
			Domain:                domain,
			Adapter:               adapter,
			DependsOn:             dependsOn,
			ActivationIDs:         canonicalContractStrings(activationIDs),
			ForwardRenderedDigest: compositeDecompositionAggregateDigest(domain, adapter, "forward", activations),
			ReverseRenderedDigest: compositeDecompositionAggregateDigest(domain, adapter, "reverse", activations),
		})
	}

	return NewCompositeDecompositionEvidence(CompositeDecompositionEvidence{
		BaseCommit:                    plan.BaseCommit,
		TargetCommit:                  plan.TargetCommit,
		ImageActivationPlanDigest:     plan.Digest,
		ImageActivationEvidenceDigest: activationEvidence.Digest,
		Steps:                         steps,
		UnresolvedActivationIDs:       unresolved,
	})
}

type compositeDecompositionAggregate struct {
	Domain      Domain                                      `json:"domain"`
	Adapter     string                                      `json:"adapter"`
	Direction   string                                      `json:"direction"`
	Activations []compositeDecompositionAggregateActivation `json:"activations"`
}

type compositeDecompositionAggregateActivation struct {
	ID             string `json:"id"`
	RenderedDigest string `json:"renderedDigest"`
}

func compositeDecompositionAggregateDigest(domain Domain, adapter, direction string, activations []ImageActivation) string {
	items := make([]compositeDecompositionAggregateActivation, 0, len(activations))
	for _, activation := range activations {
		digest := activation.ForwardRenderedDigest
		if direction == "reverse" {
			digest = activation.ReverseRenderedDigest
		}
		items = append(items, compositeDecompositionAggregateActivation{ID: activation.ID, RenderedDigest: digest})
	}
	sort.Slice(items, func(left, right int) bool { return items[left].ID < items[right].ID })
	encoded, err := json.Marshal(compositeDecompositionAggregate{
		Domain: domain, Adapter: adapter, Direction: direction, Activations: items,
	})
	if err != nil {
		panic(fmt.Sprintf("marshal composite decomposition aggregate: %v", err))
	}
	return digestOperationalBytes(encoded)
}
