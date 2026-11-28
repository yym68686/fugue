package releaseguardian

import (
	"bytes"
	"errors"
	"fmt"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	"github.com/google/go-containerregistry/pkg/name"
)

// RegistryRehomeSuccessorShadow compares a candidate with the proposed
// relocated baseline. It is not live prewrite evidence or an approval.
type RegistryRehomeSuccessorShadow struct {
	Baseline         productioncontrol.Version             `json:"proposedBaseline"`
	Scope            productioncontrol.Scope               `json:"scope"`
	Workload         declarativerelease.ResourceIdentity   `json:"workload"`
	Forward          declarativerelease.TargetIdentity     `json:"forward"`
	Rollback         declarativerelease.TargetIdentity     `json:"rollback"`
	ExecutionPlan    string                                `json:"executionPlanDigest"`
	ArtifactReceipt  string                                `json:"artifactReceiptDigest"`
	StaticCompanions []declarativerelease.ResourceIdentity `json:"staticCompanions,omitempty"`
}

// ShadowRegistryRehomeSuccessor verifies that Guardian's prepared candidate can
// use the exact relocated current manifest for rollback. The older relocated
// rollback is retained for recovery, not substituted as the next release's LKG.
// This offline comparison neither reads nor mutates Kubernetes or the journal.
func ShadowRegistryRehomeSuccessor(raw []byte, pin string, files map[string][]byte) (RegistryRehomeSuccessorShadow, error) {
	var none RegistryRehomeSuccessorShadow
	verified, err := verifyRegistryRehomeRecoveryBundle(raw, pin)
	if err != nil {
		return none, err
	}
	_, baseline, err := buildRegistryRehomeBaseline(verified)
	if err != nil {
		return none, err
	}
	current, input, err := registryRehomeReleaseInput(verified, "current")
	if err != nil {
		return none, err
	}
	bundle, err := DecodeExecutionBundle(files, baseline.Key)
	if err != nil {
		return none, err
	}
	release, prepared, artifact := bundle.Release, bundle.Prepared, bundle.Artifact
	source := verified.archive.forward.release
	if source.Delivery == nil || source.Delivery.Writer != "guardian" ||
		source.Delivery.Group != baseline.Key.Group ||
		!componentPattern.MatchString(source.Delivery.DependencyService) ||
		recoveryWorkloadKey(release.Workload) != recoveryWorkloadKey(source.Workload) ||
		release.Concurrency != source.Concurrency ||
		release.Workload.NodeParticipation != "" || release.Transition != nil ||
		len(release.RuntimeResourcesFromForward) != 0 || prepared.Prewrite.Participation != nil ||
		prepared.DegradedPredecessor || prepared.AlreadyConverged ||
		!prepared.Prewrite.Present || prepared.Prewrite.Primary != baseline.Workload ||
		release.Delivery.DependencyService != source.Delivery.DependencyService {
		return none, errors.New("registry rehome successor changes its enrolled workload or delegates a transition")
	}
	if artifact.PlanDigest != bundle.Plan.PlanDigest || artifact.IntentDigest != release.IntentDigest ||
		prepared.IntentDigest != release.IntentDigest {
		return none, errors.New("registry rehome successor artifact and execution do not bind the same candidate")
	}
	registry, err := name.NewDigest(input.Target.ImageRef, name.StrictValidation)
	if err != nil || registry.Context().Name() != artifact.Repository || prepared.Forward.ImageRef == input.Target.ImageRef {
		return none, errors.New("registry rehome successor must select a different immutable image in the same independent repository")
	}
	if prepared.LKG != baseline.Current.Target || !bytes.Equal(bundle.LKG, current) {
		return none, errors.New("registry rehome successor rollback differs from the exact relocated current release")
	}
	if _, err := declarativerelease.BindGuardianLKG(bundle.Plan, baseline.Key.Component, declarativerelease.RenderedManifests{Forward: bundle.Forward}, current); err != nil {
		return none, fmt.Errorf("registry rehome successor rollback is not consumable by Guardian: %w", err)
	}

	// Compare the inventory and declaration of every companion. The only
	// ignored fields are the three release-provenance stamps, never routing
	// labels, ports, roleRef, subjects or rules.
	before, err := declarativerelease.DecodeResourceSet(bytes.NewReader(current))
	if err != nil {
		return none, err
	}
	after, err := declarativerelease.DecodeResourceSet(bytes.NewReader(bundle.Forward))
	if err != nil {
		return none, err
	}
	a, err := declarativerelease.ResourceSetIdentities(current)
	if err != nil {
		return none, err
	}
	b, err := declarativerelease.ResourceSetIdentities(bundle.Forward)
	if err != nil {
		return none, err
	}
	if len(a) != len(b) {
		return none, errors.New("registry rehome successor changes the companion inventory")
	}
	for i, id := range a {
		if id != b[i] {
			return none, errors.New("registry rehome successor changes the companion inventory")
		}
		if id == baseline.Workload {
			continue
		}
		previous, err := registryRehomeCompanionShape(before.Items[i], id, baseline.Workload.Namespace)
		if err != nil {
			return none, err
		}
		next, err := registryRehomeCompanionShape(after.Items[i], id, baseline.Workload.Namespace)
		if err != nil || previous != next {
			return none, errors.Join(fmt.Errorf("registry rehome successor changes companion %s %s", id.Kind, id.Name), err)
		}
	}
	deployment, err := after.Primary(release.Workload)
	if err != nil {
		return none, err
	}
	spec, _ := deployment["spec"].(map[string]any)
	template, _ := spec["template"].(map[string]any)
	meta, _ := template["metadata"].(map[string]any)
	stamps, _ := meta["annotations"].(map[string]any)
	for k, v := range map[string]string{
		podArtifactImageAnnotation:          prepared.Forward.ImageRef,
		"fugue.pro/source-commit":           prepared.Forward.ManifestSHA,
		"fugue.pro/oci-revision":            prepared.Forward.OCIRevision,
		"fugue.pro/production-config-sha":   prepared.Forward.ConfigSHA,
		"fugue.pro/release-plan-digest":     bundle.Plan.PlanDigest,
		"fugue.pro/artifact-receipt-digest": artifact.ReceiptDigest,
	} {
		if stamps[k] != v {
			return none, errors.New("registry rehome successor Deployment provenance differs from its prepared artifact")
		}
	}
	after.Items = []map[string]any{deployment}
	projected, err := declarativerelease.CanonicalJSON(after)
	if err != nil {
		return none, err
	}
	if err := VerifyNativeDeploymentImages(projected, baseline.Workload, prepared.Forward); err != nil {
		return none, err
	}
	return RegistryRehomeSuccessorShadow{
		Baseline: baseline.Version(), Scope: baseline.Scope, Workload: baseline.Workload,
		Forward: prepared.Forward, Rollback: prepared.LKG, ExecutionPlan: prepared.PlanDigest,
		ArtifactReceipt: artifact.ReceiptDigest, StaticCompanions: input.Companions,
	}, nil
}
