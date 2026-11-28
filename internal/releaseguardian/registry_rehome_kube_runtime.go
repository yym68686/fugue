package releaseguardian

import (
	"context"
	"crypto/ed25519"
	"errors"
	"net/http"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	admission "k8s.io/api/admissionregistration/v1"
	"k8s.io/client-go/rest"
)

// RenderRegistryRehomeKubeFence renders separate stages, not an applyable
// bundle. The operator must establish fail-closed webhook admission before
// replacing the plain freeze's workload policy with the exception.
func RenderRegistryRehomeKubeFence(enrollment RegistryRehomeKubeEnrollment) (RegistryRehomeFencePreview, error) {
	gate, err := enrollment.render()
	if err != nil {
		return RegistryRehomeFencePreview{}, err
	}
	return RegistryRehomeFencePreview{Webhook: gate.hook, Policies: gate.pairs}, nil
}

type RegistryRehomeFencePreview struct {
	Webhook  *admission.ValidatingWebhookConfiguration `json:"webhook"`
	Policies []KubeCutoverPolicy                       `json:"policies"`
}

// RegistryRehomeKubeEnrollment binds the only Kubernetes object which a
// one-shot registry rehome writer can update. The cutover must still be in
// its frozen phase; this enrollment grants no permissions and installs no
// admission policy.
type RegistryRehomeKubeEnrollment struct {
	Cutover                   KubeDeploymentCutover                            `json:"cutover"`
	Provider                  productioncontrol.ParticipationKubernetesBinding `json:"provider"`
	Writer                    string                                           `json:"writer"`
	DeploymentUID             string                                           `json:"deploymentUID"`
	DeploymentResourceVersion string                                           `json:"deploymentResourceVersion"`
	DeploymentGeneration      int64                                            `json:"deploymentGeneration"`
	AdmissionEndpoint         string                                           `json:"admissionEndpoint"`
	AdmissionCABundle         []byte                                           `json:"admissionCABundle,omitempty"`
}

// RegistryRehomeKubernetesDispatchResult reports a durable claim and whether
// this invocation submitted the single provider write. A submitted write is
// not proof of verified health or a settled LKG.
type RegistryRehomeKubernetesDispatchResult struct {
	Claim     productioncontrol.Claim `json:"claim"`
	Submitted bool                    `json:"submitted"`
	Workload  RecoveryRecordRef       `json:"workload,omitempty"`
}

// DispatchRegistryRehomeKubernetes constructs the exact provider fence from
// an independently enrolled frozen Deployment and attempts at most the
// journal's single-use write. It never creates or updates the fence itself.
func DispatchRegistryRehomeKubernetes(ctx context.Context, config *rest.Config, enrollment RegistryRehomeKubeEnrollment,
	cluster string, ledger *productioncontrol.Journal, observer ed25519.PublicKey, archive []byte, archiveDigest,
	actor, intentDigest, approvalDigest string, evidence []byte) (RegistryRehomeKubernetesDispatchResult, error) {
	var none RegistryRehomeKubernetesDispatchResult
	if config == nil || enrollment.Cutover.Phase != KubeCutoverFrozen {
		return none, errors.New("registry rehome dispatch requires an enrolled frozen Deployment")
	}
	gate, err := enrollment.render()
	if err != nil {
		return none, err
	}
	dispatcher, err := newRegistryRehomeDispatcher(config, gate, cluster, ledger, observer, archive, archiveDigest)
	if err != nil {
		return none, err
	}
	return dispatcher.dispatch(ctx, actor, intentDigest, approvalDigest, evidence)
}

// NewRegistryRehomeKubernetesAdmissionHandler creates the independent
// provider-side read-only admission check. The approved digests are fixed at
// construction time, never obtained from AdmissionReview; provider state is
// looked up using the journal reader each time a live write is admitted.
func NewRegistryRehomeKubernetesAdmissionHandler(enrollment RegistryRehomeKubeEnrollment, cluster string,
	submission productioncontrol.Submission, intentDigest, approvalDigest string,
	ledger *productioncontrol.Journal, observer ed25519.PublicKey) (http.Handler, error) {
	if ledger == nil || enrollment.Cutover.Phase != KubeCutoverFrozen || !digestPattern.MatchString(approvalDigest) {
		return nil, errors.New("registry rehome admission requires an enrolled frozen Deployment and read-only journal")
	}
	identity, err := submission.Intent.Digest()
	if err != nil || identity != intentDigest || submission.Intent.ClusterID != cluster ||
		submission.Intent.Operation != productioncontrol.ComponentRegistryMove ||
		submission.Intent.ResourceID != nativeKubernetesResourceID(declarativerelease.ResourceIdentity{
			APIVersion: "apps/v1", Kind: "Deployment", Namespace: enrollment.Cutover.Namespace, Name: enrollment.Cutover.Deployment,
		}) {
		return nil, errors.New("registry rehome admission submission does not match its fixed journal scope")
	}
	review, err := InspectRegistryRehomeSubmission(submission)
	if err != nil || review.Source.Key != enrollment.Cutover.Key || review.Draft.Component != enrollment.Cutover.MonitorComponent ||
		review.Draft.Workload.Namespace != enrollment.Cutover.Namespace ||
		review.Draft.Workload.Name != enrollment.Cutover.Deployment {
		return nil, errors.Join(errors.New("registry rehome admission submission differs from its exact workload enrollment"), err)
	}
	scope := productioncontrol.Scope{ClusterID: cluster, ResourceID: submission.Intent.ResourceID}
	reader, err := productioncontrol.NewRegistryRehomeAdmissionReader(ledger, scope, observer)
	if err != nil {
		return nil, err
	}
	gate, err := enrollment.render()
	if err != nil {
		return nil, err
	}
	return registryRehomeAdmissionServer(gate, review, func(ctx context.Context, policy, write string) error {
		return reader.VerifyPendingIntentWrite(ctx, intentDigest, approvalDigest, policy, write)
	})
}

func (enrollment RegistryRehomeKubeEnrollment) render() (registryRehomeKubeFence, error) {
	scope := registryRehomeKubeScope{Writer: enrollment.Writer, UID: enrollment.DeploymentUID,
		ResourceVersion: enrollment.DeploymentResourceVersion, Generation: enrollment.DeploymentGeneration}
	return renderRegistryRehomeKubeFence(enrollment.Cutover, enrollment.Provider, scope,
		enrollment.AdmissionEndpoint, enrollment.AdmissionCABundle)
}
