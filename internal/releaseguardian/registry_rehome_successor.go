package releaseguardian

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	"github.com/google/go-containerregistry/pkg/name"
	"k8s.io/client-go/kubernetes"
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

// RegistryRehomeSuccessorPreflight binds Guardian's next execution plan to
// direct provider and artifact reads. The rehome is still pending; this is
// not a publishable intent, rollout permit, or terminal rehome receipt.
type RegistryRehomeSuccessorPreflight struct {
	Cutover      RegistryRehomeCutoverReadback           `json:"cutover"`
	Successor    RegistryRehomeSuccessorShadow           `json:"successor"`
	ForwardImage declarativerelease.RegistryVerification `json:"forwardImage"`
}

// ObserveRegistryRehomeSuccessorPreflight exercises the successor's real
// prewrite binding without treating the historical MonitorRecord as the
// relocated rollback version or issuing a provider dry-run write. Only the
// independent read-only observation client is needed.
func ObserveRegistryRehomeSuccessorPreflight(ctx context.Context, client kubernetes.Interface, journal *productioncontrol.Journal,
	cluster, intentDigest string, cutover KubeDeploymentCutover, provider productioncontrol.ParticipationKubernetesBinding,
	observer ed25519.PublicKey, raw []byte, pin string, registry *IndependentRegistry, files map[string][]byte) (RegistryRehomeSuccessorPreflight, error) {
	var out RegistryRehomeSuccessorPreflight
	var bundle ExecutionBundle
	read := func(verified verifiedRegistryRehomeRecoveryBundle, baseline RegistryRehomeBaseline, before RegistryRehomePendingObservation) error {
		var err error
		bundle, err = DecodeExecutionBundle(files, baseline.Key)
		if err != nil {
			return err
		}
		out.Successor, err = shadowRegistryRehomeSuccessor(verified, baseline, bundle)
		if err != nil {
			return err
		}
		if err := checkRegistryRehomeSuccessorPrewrite(bundle.Prepared.Prewrite, baseline, before.ProviderReadback, out.Successor.StaticCompanions); err != nil {
			return err
		}
		if err := validateRegistryRehomeSuccessorFreshness(bundle); err != nil {
			return err
		}
		out.ForwardImage, err = registry.VerifyRuntimeArtifact(ctx, bundle.Artifact)
		if err != nil {
			return fmt.Errorf("read successor image from the independent registry: %w", err)
		}
		return nil
	}
	var err error
	out.Cutover, err = observeRegistryRehomeCutover(ctx, client, journal, cluster, intentDigest, cutover, provider, observer, raw, pin, registry, read)
	if err != nil {
		return RegistryRehomeSuccessorPreflight{}, err
	}
	if err := validateRegistryRehomeSuccessorFreshness(bundle); err != nil {
		return RegistryRehomeSuccessorPreflight{}, err
	}
	return out, nil
}

func validateRegistryRehomeSuccessorFreshness(bundle ExecutionBundle) error {
	_, err := declarativerelease.DecodeExecutionPlan(bytes.NewReader(bundle.Files["execution-plan.json"]), bundle.Plan, bundle.Forward, bundle.LKG)
	return err
}

func checkRegistryRehomeSuccessorPrewrite(pre declarativerelease.Observation, baseline RegistryRehomeBaseline, live rehomePlacement, companions []declarativerelease.ResourceIdentity) error {
	target, dep := baseline.Current.Target, live.Deployment
	if !pre.Present || pre.Primary != baseline.Workload || pre.UID != dep.UID || pre.ResourceVersion != dep.ResourceVersion ||
		pre.Generation != live.Generation || pre.ObservedGeneration != live.Generation ||
		pre.ImageRef != target.ImageRef || pre.ConfigSHA != target.ConfigSHA || pre.ManifestSHA != target.ManifestSHA || pre.OCIRevision != target.OCIRevision ||
		pre.Desired < 1 || int64(pre.Desired) != int64(len(live.Pods)) || pre.Updated != pre.Desired ||
		pre.Ready != pre.Desired || pre.Available != pre.Desired || pre.Unavailable != 0 {
		return errors.New("successor prewrite differs from the directly observed relocated Deployment")
	}
	references := map[declarativerelease.ResourceIdentity]RecoveryRecordRef{baseline.Workload: dep}
	services := make(map[string]RecoveryRecordRef, len(live.Services))
	for _, svc := range live.Services {
		services[svc.Name] = svc
	}
	for _, id := range companions {
		ref := services[id.Name]
		if id.Kind != "Service" || id.APIVersion != "v1" || id.Namespace != baseline.Workload.Namespace || ref.UID == "" {
			return errors.New("successor prewrite includes a companion outside the executable Service scope")
		}
		references[id] = ref
	}
	if len(services) != len(companions) || len(pre.Resources) != len(references) {
		return errors.New("successor prewrite inventory differs from the directly observed resources")
	}
	for _, resource := range pre.Resources {
		ref, exists := references[resource.Identity]
		if !exists || !resource.Present || resource.UID != ref.UID || resource.ResourceVersion != ref.ResourceVersion ||
			(resource.Identity == baseline.Workload && resource.Generation != live.Generation) {
			return fmt.Errorf("successor prewrite differs from the observed %s %s", resource.Identity.Kind, resource.Identity.Name)
		}
		delete(references, resource.Identity)
	}
	return nil
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
	bundle, err := DecodeExecutionBundle(files, baseline.Key)
	if err != nil {
		return none, err
	}
	return shadowRegistryRehomeSuccessor(verified, baseline, bundle)
}

func shadowRegistryRehomeSuccessor(verified verifiedRegistryRehomeRecoveryBundle, baseline RegistryRehomeBaseline, bundle ExecutionBundle) (RegistryRehomeSuccessorShadow, error) {
	var none RegistryRehomeSuccessorShadow
	current, input, err := registryRehomeReleaseInput(verified, "current")
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
