package releaseguardian

import (
	"context"
	"errors"
	"fmt"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	admission "k8s.io/api/admissionregistration/v1"
	authentication "k8s.io/api/authentication/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// DeploymentCutoverEvidence is independent of the submitted artifact and
// cluster policy objects. It must verify old credential revocation and
// request drainage, actual admission at the serving API endpoints, the
// native Guardian runtime, and availability of verified forward/LKG artifacts
// without GitHub. Finding a ValidatingAdmissionPolicy alone proves none of
// these. The verifier must reject missing, stale or mismatched observations.
type DeploymentCutoverEvidence interface {
	VerifyDeploymentCutoverEvidence(context.Context, KubeDeploymentCutover, NativePublicationBoundary) error
}

type kubeDeploymentAdmission struct {
	client   kubernetes.Interface
	config   KubeDeploymentCutover
	cluster  string
	evidence DeploymentCutoverEvidence
}

func DeploymentCutoverScope(config KubeDeploymentCutover, clusterID string) (productioncontrol.Scope, error) {
	if config.Phase != KubeCutoverNative || config.validate() != nil || !componentPattern.MatchString(clusterID) {
		return productioncontrol.Scope{}, errors.New("invalid native Deployment enrollment")
	}
	workload := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: config.Namespace, Name: config.Deployment}
	return productioncontrol.Scope{ClusterID: clusterID, ResourceID: nativeKubernetesResourceID(workload)}, nil
}

// NewKubeFencedNativePublisher uses exactly the same authenticated client for
// admission readback, publisher identity verification and eventual writes.
// It does not install RBAC, policies or credentials, or promote the phase.
func NewKubeFencedNativePublisher(store *KubeStore, journal *productioncontrol.ExecutionJournal, config KubeDeploymentCutover, clusterID string, evidence DeploymentCutoverEvidence, registry *IndependentRegistry) (*NativePublisher, error) {
	if store == nil || store.client == nil || evidence == nil || registry == nil || registry.transport == nil ||
		config.Phase != KubeCutoverNative || config.validate() != nil {
		return nil, errors.New("native publisher requires a native cutover scope, independent evidence verifier, and runtime registry")
	}
	if target, ok := store.targets[config.Key]; !ok || target.Namespace != config.Namespace ||
		target.MonitorComponent != config.MonitorComponent || config.MonitorComponent != config.Key.Component {
		return nil, errors.New("native publisher's Guardian target differs from its Kubernetes cutover scope")
	}
	provider := store.client.CoreV1().RESTClient()
	if provider == nil {
		return nil, errors.New("native publisher requires the authenticated Kubernetes REST writer")
	}
	publisher, err := newNativePublisher(store, journal, &kubeDeploymentAdmission{store.client, config, clusterID, evidence}, clusterID,
		nativeConfigMapWriter{provider})
	if err != nil {
		return nil, err
	}
	publisher.artifacts = registry
	return publisher, nil
}

func (gate *kubeDeploymentAdmission) VerifyNativePublication(ctx context.Context, boundary NativePublicationBoundary) error {
	config := gate.config
	if boundary.Scope.ClusterID != gate.cluster || boundary.Scope.ResourceID != nativeKubernetesResourceID(boundary.Workload) ||
		boundary.Namespace != config.Namespace || boundary.Workload.Namespace != config.Namespace || boundary.Workload.Name != config.Deployment ||
		boundary.Workload.Kind != "Deployment" || boundary.Workload.APIVersion != "apps/v1" ||
		boundary.MonitorComponent != config.MonitorComponent || boundary.ComponentLease != config.ComponentLease ||
		boundary.Predecessor.Key() != config.Key || boundary.Candidate.Key() != config.Key || boundary.Candidate.Validate() != nil ||
		boundary.DesiredObject != desiredName(config.Key) || boundary.RecordObject != releaseRecordName(config.Key, boundary.Candidate.RecordDigest) ||
		boundary.ExecutionObject != executionSnapshotName(config.Key, boundary.Candidate.Generation) {
		return errors.New("native release boundary differs from the enrolled Kubernetes cutover")
	}
	review, err := gate.client.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authentication.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("verify native publisher's Kubernetes identity: %w", err)
	}
	if review.Status.UserInfo.Username != config.Publisher {
		return errors.New("Kubernetes resolved a different publisher identity than the native cutover policy")
	}
	if err := VerifyDeploymentCutoverPolicies(ctx, gate.client, config); err != nil {
		return err
	}
	return gate.evidence.VerifyDeploymentCutoverEvidence(ctx, config, boundary)
}

// VerifyDeploymentCutoverPolicies reads the three exact policy/binding pairs.
// It does not install them, and is not proof that every API server already
// enforces the policy or that no request crossed admission before freeze.
func VerifyDeploymentCutoverPolicies(ctx context.Context, client kubernetes.Interface, config KubeDeploymentCutover) error {
	if client == nil {
		return errors.New("no Kubernetes client for native cutover policy readback")
	}
	expected, err := DeploymentCutoverPolicies(config)
	if err != nil {
		return err
	}
	registry := client.AdmissionregistrationV1()
	for _, pair := range expected {
		policy, err := registry.ValidatingAdmissionPolicies().Get(ctx, pair.Policy.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("read Kubernetes cutover policy %s: %w", pair.Policy.Name, err)
		}
		if policy.DeletionTimestamp != nil || !sameCutoverPolicySpec(policy.Spec, pair.Policy.Spec) ||
			(policy.Status.TypeChecking != nil && len(policy.Status.TypeChecking.ExpressionWarnings) > 0) {
			return fmt.Errorf("Kubernetes cutover policy %s is not the exact expected policy", pair.Policy.Name)
		}
		binding, err := registry.ValidatingAdmissionPolicyBindings().Get(ctx, pair.Binding.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("read Kubernetes cutover binding %s: %w", pair.Binding.Name, err)
		}
		if binding.DeletionTimestamp != nil || !apiequality.Semantic.DeepEqual(binding.Spec, pair.Binding.Spec) {
			return fmt.Errorf("Kubernetes cutover binding %s does not enforce the expected policy", pair.Binding.Name)
		}
	}
	return nil
}

func sameCutoverPolicySpec(actual, expected admission.ValidatingAdmissionPolicySpec) bool {
	// The API server defaults absent MatchResources selectors to empty
	// selectors. Empty selectors do not narrow the policy. Any other
	// selector, resource or expression difference must fail closed.
	if actual.MatchConstraints != nil {
		actual.MatchConstraints = actual.MatchConstraints.DeepCopy()
		if emptyCutoverSelector(actual.MatchConstraints.ObjectSelector) {
			actual.MatchConstraints.ObjectSelector = nil
		}
		if emptyCutoverSelector(actual.MatchConstraints.NamespaceSelector) {
			actual.MatchConstraints.NamespaceSelector = nil
		}
	}
	return apiequality.Semantic.DeepEqual(actual, expected)
}

func emptyCutoverSelector(selector *metav1.LabelSelector) bool {
	return selector != nil && len(selector.MatchLabels) == 0 && len(selector.MatchExpressions) == 0
}
