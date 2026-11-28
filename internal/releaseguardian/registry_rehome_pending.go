package releaseguardian

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	registryRehomeOriginalPlacement    = "original_registry_observed"
	registryRehomeIndependentPlacement = "independent_registry_observed"
)

// RegistryRehomePendingObservation is observational only. It does not prove
// provider propagation, request drain, route health, or a terminal version.
type RegistryRehomePendingObservation struct {
	Claim            productioncontrol.Claim   `json:"claim"`
	AdmissionDigest  string                    `json:"admissionDigest"`
	ArchiveDigest    string                    `json:"archiveDigest"`
	LastVerified     productioncontrol.Version `json:"lastVerified"`
	Placement        string                    `json:"placement"`
	ProviderReadback rehomePlacement           `json:"providerReadback"`
	ReadbackDigest   string                    `json:"readbackDigest,omitempty"`
}

// RegistryRehomeCutoverReadback binds read-only placement to the recovery
// bundle and mirror. It is evidence for review, not a serving or LKG receipt.
type RegistryRehomeCutoverReadback struct {
	Placement RegistryRehomePendingObservation `json:"placement"`
	Recovery  RegistryRehomeRecoveryReport     `json:"recovery"`
	Baseline  RegistryRehomeBaseline           `json:"proposedBaseline"`
}

// ObserveRegistryRehomeCutover requires the same frozen workload, Pods,
// companions and pending claim on both sides of the independent image
// readback. It cannot install a new baseline or settle the journal.
func ObserveRegistryRehomeCutover(ctx context.Context, client kubernetes.Interface, journal *productioncontrol.Journal,
	cluster, intentDigest string, cutover KubeDeploymentCutover, provider productioncontrol.ParticipationKubernetesBinding,
	observer ed25519.PublicKey, recoveryRaw []byte, recoveryPin string, registry *IndependentRegistry) (RegistryRehomeCutoverReadback, error) {
	var none RegistryRehomeCutoverReadback
	if registry == nil || registry.transport == nil {
		return none, errors.New("registry rehome cutover readback requires a configured independent registry")
	}
	verified, err := verifyRegistryRehomeRecoveryBundle(recoveryRaw, recoveryPin)
	if err != nil {
		return none, err
	}
	report := verified.report
	if report.IntentDigest != intentDigest || verified.bundle.Review.Intent.ClusterID != cluster {
		return none, errors.New("registry rehome recovery bundle does not match the pending intent and cluster")
	}
	_, baseline, err := buildRegistryRehomeBaseline(verified)
	if err != nil {
		return none, err
	}
	observe := func() (RegistryRehomePendingObservation, error) {
		return ObserveFrozenRegistryRehomePending(ctx, client, journal, cluster, intentDigest, cutover, provider,
			observer, verified.bundle.Archive, report.ArchiveDigest)
	}
	before, err := observe()
	if err != nil {
		return none, err
	}
	if before.Placement != registryRehomeIndependentPlacement ||
		before.LastVerified != baseline.Predecessor || before.Claim.Scope != baseline.Scope {
		return none, errors.New("registry rehome cutover has not reached the independent placement")
	}
	if err := verifyRegistryRehomeRecoveryMirror(ctx, verified, registry); err != nil {
		return none, err
	}
	after, err := observe()
	if err != nil || after.ReadbackDigest != before.ReadbackDigest {
		return none, errors.Join(errors.New("registry rehome placement or claim changed during independent mirror verification"), err)
	}
	return RegistryRehomeCutoverReadback{Placement: after, Recovery: report, Baseline: baseline}, nil
}

// ObserveFrozenRegistryRehomePending resumes inspection after losing the
// PATCH response. Only an independently enrolled read-only client should be
// passed here. Restoring the plain freeze is a prerequisite, not a side
// effect. The policy readback is not a substitute for proving closure and
// drain at every API server, even when the original placement is observed.
func ObserveFrozenRegistryRehomePending(ctx context.Context, client kubernetes.Interface, journal *productioncontrol.Journal,
	cluster, intentDigest string, cutover KubeDeploymentCutover, provider productioncontrol.ParticipationKubernetesBinding,
	observer ed25519.PublicKey, archiveRaw []byte, archiveDigest string) (RegistryRehomePendingObservation, error) {
	var none RegistryRehomePendingObservation
	if client == nil || cutover.Phase != KubeCutoverFrozen || cutover.validate() != nil || provider.Validate() != nil || journal == nil {
		return none, errors.New("registry rehome readback requires an enrolled read-only provider and the plain Deployment freeze")
	}
	intent, payload, err := journal.ReadIntent(ctx, intentDigest)
	if err != nil {
		return none, err
	}
	review, err := InspectRegistryRehomeSubmission(productioncontrol.Submission{Intent: intent, Payload: payload})
	id := review.Draft.Workload
	if err != nil || intent.ClusterID != cluster || review.Source.Key != cutover.Key || cutover.MonitorComponent != cutover.Key.Component ||
		id.Namespace != cutover.Namespace || id.Name != cutover.Deployment {
		return none, errors.Join(errors.New("pending registry rehome does not match the frozen Deployment enrollment"), err)
	}
	archive, err := readReleaseRecoveryArchive(archiveRaw, archiveDigest)
	if err != nil {
		return none, err
	}
	if _, _, err := deriveRegistryRehomeRecovery(archive, review); err != nil {
		return none, err
	}
	scope := productioncontrol.Scope{ClusterID: cluster, ResourceID: intent.ResourceID}
	reader, err := productioncontrol.NewRegistryRehomeAdmissionReader(journal, scope, observer)
	if err != nil {
		return none, err
	}
	claim, result, err := journal.ReadExecution(ctx, scope, intentDigest)
	if err != nil || result != nil {
		return none, errors.Join(errors.New("registry rehome has no unsettled claim"), err)
	}
	_, admissionDigest, err := reader.ReadAdmission(ctx, claim)
	if err != nil {
		return none, err
	}
	readHead := func() (productioncontrol.ResourceState, error) {
		state, err := journal.ReadResource(ctx, scope)
		if err != nil || state.LastFence != claim.Fence || state.Pending != intentDigest || state.LastVerified != review.Source.Version {
			return productioncontrol.ResourceState{}, errors.Join(errors.New("registry rehome is not the pending fence over the reviewed predecessor"), err)
		}
		return state, nil
	}
	state, err := readHead()
	if err != nil {
		return none, err
	}
	place, kind, err := observeClosedRegistryRehomePlacement(ctx, client, review, cutover, provider, archive.report.Pair)
	if err != nil {
		return none, err
	}
	if next, err := readHead(); err != nil || next != state {
		return none, errors.Join(errors.New("registry rehome journal changed during readback"), err)
	}
	report := RegistryRehomePendingObservation{Claim: claim, AdmissionDigest: admissionDigest, ArchiveDigest: archiveDigest,
		LastVerified: state.LastVerified, Placement: kind, ProviderReadback: place}
	raw, err := declarativerelease.CanonicalJSON(struct {
		Cutover  KubeDeploymentCutover
		Provider productioncontrol.ParticipationKubernetesBinding
		Report   RegistryRehomePendingObservation
	}{cutover, provider, report})
	if err != nil {
		return none, err
	}
	report.ReadbackDigest = digest(append([]byte("fugue.registry-rehome.frozen-placement.v1\x00"), raw...))
	return report, nil
}

// SettleRegistryRehomeWithoutWrite closes only an independently proven
// no-write attempt. The caller supplies a read-only provider identity and
// a restricted append-only journal identity; no provider objects are mutated.
func SettleRegistryRehomeWithoutWrite(ctx context.Context, client kubernetes.Interface, journal *productioncontrol.Journal,
	cluster, actor, intentDigest string, cutover KubeDeploymentCutover, provider productioncontrol.ParticipationKubernetesBinding,
	observer ed25519.PublicKey, archive []byte, archiveDigest string, evidence []byte) (productioncontrol.Recorded, error) {
	if client == nil || journal == nil || cutover.Phase != KubeCutoverFrozen || cutover.validate() != nil || provider.Validate() != nil {
		return productioncontrol.Recorded{}, errors.New("registry rehome settlement requires an enrolled read-only provider and frozen boundary")
	}
	workload := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: cutover.Namespace, Name: cutover.Deployment}
	scope := productioncontrol.Scope{ClusterID: cluster, ResourceID: nativeKubernetesResourceID(workload)}
	check := func(ctx context.Context, proof productioncontrol.RegistryRehomeNoWriteEvidence) error {
		observed, err := ObserveFrozenRegistryRehomePending(ctx, client, journal, cluster, intentDigest, cutover, provider, observer, archive, archiveDigest)
		if err != nil {
			return err
		}
		if observed.Placement != registryRehomeOriginalPlacement || observed.Claim.Scope != scope ||
			observed.AdmissionDigest != proof.AdmissionDigest || observed.ArchiveDigest != proof.ArchiveDigest || observed.ReadbackDigest != proof.ReadbackDigest {
			return errors.New("no-write settlement requires fresh exact placement on the original registry")
		}
		return nil
	}
	settler, err := productioncontrol.NewRegistryRehomeNoWriteJournal(journal, scope, observer, check)
	if err != nil {
		return productioncontrol.Recorded{}, err
	}
	return settler.CloseWithoutWrite(ctx, actor, intentDigest, evidence)
}

func observeClosedRegistryRehomePlacement(ctx context.Context, client kubernetes.Interface, review RegistryRehomeReview,
	cutover KubeDeploymentCutover, provider productioncontrol.ParticipationKubernetesBinding, sealed SealedRecoveryPair) (rehomePlacement, string, error) {
	boundary := func() error {
		ns, err := client.CoreV1().Namespaces().Get(ctx, cutover.Namespace, metav1.GetOptions{})
		if err != nil || ns.DeletionTimestamp != nil || string(ns.UID) != provider.NamespaceUID {
			return errors.Join(errors.New("pending registry rehome namespace does not match the enrolled provider"), err)
		}
		if err := VerifyDeploymentCutoverPolicies(ctx, client, cutover); err != nil {
			return fmt.Errorf("plain Deployment freeze readback: %w", err)
		}
		pairs, err := DeploymentCutoverPolicies(cutover)
		if err != nil {
			return err
		}
		webhookName := pairs[1].Policy.Name + "-rehome"
		if _, err := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx, webhookName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			return errors.Join(errors.New("temporary registry rehome webhook exception is still installed"), err)
		}
		live, err := ObserveSealedRecoveryPair(ctx, client.CoreV1(), sealed.Namespace, sealed.Component)
		if err != nil || live.Pointer.UID != sealed.Pointer.UID || live.Forward != sealed.Forward || live.Rollback != sealed.Rollback {
			return errors.Join(errors.New("sealed release history changed during registry rehome"), err)
		}
		want := review.Source.Desired
		pointer, err := client.CoreV1().ConfigMaps(cutover.Namespace).Get(ctx, desiredName(cutover.Key), metav1.GetOptions{})
		if want == (RecoveryRecordRef{}) && apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil || want == (RecoveryRecordRef{}) || pointer.DeletionTimestamp != nil || recoveryRef(pointer) != want {
			return errors.Join(errors.New("Guardian desired pointer changed during registry rehome"), err)
		}
		return nil
	}
	if err := boundary(); err != nil {
		return rehomePlacement{}, "", err
	}
	id := review.Draft.Workload
	live, err := client.AppsV1().Deployments(id.Namespace).Get(ctx, id.Name, metav1.GetOptions{})
	if err != nil {
		return rehomePlacement{}, "", err
	}
	original, kind := live.Generation == review.Source.Deployment.Generation, registryRehomeIndependentPlacement
	if original {
		kind = registryRehomeOriginalPlacement
	}
	placement, err := observeRegistryRehomePlacementAt(ctx, client, review, original)
	if err != nil {
		return rehomePlacement{}, "", err
	}
	if err := boundary(); err != nil {
		return rehomePlacement{}, "", err
	}
	return placement, kind, nil
}
