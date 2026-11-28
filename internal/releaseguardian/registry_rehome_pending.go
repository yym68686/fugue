package releaseguardian

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"

	"fugue/internal/productioncontrol"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
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
	return RegistryRehomePendingObservation{claim, admissionDigest, archiveDigest, state.LastVerified, kind, place}, nil
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
	original, kind := live.Generation == review.Source.Deployment.Generation, "independent_registry_observed"
	if original {
		kind = "original_registry_observed"
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
