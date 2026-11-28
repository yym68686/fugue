package releaseguardian

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"

	"fugue/internal/declarativerelease"
	"fugue/internal/productioncontrol"
	admission "k8s.io/api/admissionregistration/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

const registryRehomeAdmissionPath = "/registry-rehome-admission"

var (
	registryRehomeKubeUID     = regexp.MustCompile(`^[a-zA-Z0-9-]{1,128}$`)
	registryRehomeKubeVersion = regexp.MustCompile(`^[a-zA-Z0-9_.:-]{1,128}$`)
)

type registryRehomeKubeScope struct {
	Writer          string
	UID             string
	ResourceVersion string
	Generation      int64
}

func (s registryRehomeKubeScope) validate(c KubeDeploymentCutover) error {
	if c.Phase != KubeCutoverFrozen || !cutoverServiceAccount(s.Writer) ||
		s.Writer == c.Publisher || s.Writer == c.Guardian || s.Writer == c.CanaryProber || s.Writer == c.DeploymentStatusWriter ||
		!registryRehomeKubeUID.MatchString(s.UID) || !registryRehomeKubeVersion.MatchString(s.ResourceVersion) || s.Generation < 1 {
		return errors.New("registry rehome requires a frozen workload, distinct writer and exact Deployment identity")
	}
	return nil
}

type registryRehomeKubeFence struct {
	cutover  KubeDeploymentCutover
	provider productioncontrol.ParticipationKubernetesBinding
	scope    registryRehomeKubeScope
	pairs    []KubeCutoverPolicy
	hook     *admission.ValidatingWebhookConfiguration
}

// renderRegistryRehomeKubeFence returns both gates as one internal bundle.
// Verify the fail-closed webhook first while the plain freeze is active.
// Do not install the exception until old credentials are revoked and old
// requests have drained. This renderer installs nothing.
func renderRegistryRehomeKubeFence(c KubeDeploymentCutover, provider productioncontrol.ParticipationKubernetesBinding, s registryRehomeKubeScope, endpoint string, ca []byte) (registryRehomeKubeFence, error) {
	if err := provider.Validate(); err != nil {
		return registryRehomeKubeFence{}, err
	}
	target, err := url.Parse(endpoint)
	if err != nil || target.Scheme != "https" || target.Hostname() == "" || target.User != nil || target.Opaque != "" ||
		target.Path != registryRehomeAdmissionPath || target.RawPath != "" || target.RawQuery != "" ||
		target.ForceQuery || target.Fragment != "" || target.String() != endpoint ||
		len(ca) == 0 || len(ca) > 128<<10 || !x509.NewCertPool().AppendCertsFromPEM(ca) {
		return registryRehomeKubeFence{}, errors.New("registry rehome webhook requires its explicit HTTPS path and trust bundle")
	}
	pairs, err := deploymentCutoverPolicies(c, &s)
	if err != nil {
		return registryRehomeKubeFence{}, err
	}
	name := pairs[1].Policy.Name + "-rehome"
	namespaced := admission.NamespacedScope
	hook := &admission.ValidatingWebhookConfiguration{
		TypeMeta:   metav1.TypeMeta{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Webhooks: []admission.ValidatingWebhook{{
			Name: name + ".admission.fugue.pro", ClientConfig: admission.WebhookClientConfig{URL: ptr.To(endpoint), CABundle: bytes.Clone(ca)},
			AdmissionReviewVersions: []string{"v1"}, FailurePolicy: ptr.To(admission.Fail), MatchPolicy: ptr.To(admission.Equivalent),
			SideEffects: ptr.To(admission.SideEffectClassNone), TimeoutSeconds: ptr.To[int32](4),
			NamespaceSelector: &metav1.LabelSelector{}, ObjectSelector: &metav1.LabelSelector{},
			Rules: []admission.RuleWithOperations{{
				Operations: []admission.OperationType{admission.Update},
				Rule: admission.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1"},
					Resources: []string{"deployments"}, Scope: &namespaced},
			}},
			MatchConditions: []admission.MatchCondition{
				{Name: "namespace", Expression: "request.namespace == " + strconv.Quote(c.Namespace)},
				{Name: "deployment", Expression: "request.name == " + strconv.Quote(c.Deployment)},
			},
		}},
	}
	return registryRehomeKubeFence{c, provider, s, pairs, hook}, nil
}

// verify reads the whole bundle. A web service that is reachable does not
// establish that an API server actually invokes it; the technical observer
// must verify that separately at every enrolled serving API endpoint.
func (g registryRehomeKubeFence) verify(ctx context.Context, client kubernetes.Interface) error {
	if client == nil || g.hook == nil {
		return errors.New("registry rehome admission fence is not configured")
	}
	hook, err := client.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx, g.hook.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("read registry rehome webhook: %w", err)
	}
	if hook.DeletionTimestamp != nil || !apiequality.Semantic.DeepEqual(hook.Webhooks, g.hook.Webhooks) {
		return errors.New("registry rehome webhook differs from its exact fail-closed policy")
	}
	return VerifyKubeCutoverPolicyPairs(ctx, client, g.pairs)
}

// digests bind the signed technical receipt to the exact effective policy
// template and the exact JSON Patch, including the resourceVersion check.
func (g registryRehomeKubeFence) digests(cas rehomeDeploymentCAS) (policy, write string, err error) {
	if g.hook == nil || len(g.pairs) != 3 || cas.source == nil || len(cas.patch) == 0 ||
		cas.source.Namespace != g.cutover.Namespace || cas.source.Name != g.cutover.Deployment ||
		string(cas.source.UID) != g.scope.UID || cas.source.Generation != g.scope.Generation ||
		cas.source.ResourceVersion != g.scope.ResourceVersion {
		return "", "", errors.New("registry rehome request does not match this exact fence")
	}
	policyBytes, err := declarativerelease.CanonicalJSON(struct {
		Provider productioncontrol.ParticipationKubernetesBinding
		Pairs    []KubeCutoverPolicy
		Hook     *admission.ValidatingWebhookConfiguration
	}{g.provider, g.pairs, g.hook})
	if err != nil {
		return "", "", err
	}
	writeBytes, err := declarativerelease.CanonicalJSON(struct {
		Namespace, Deployment string
		Patch                 []byte
	}{g.cutover.Namespace, g.cutover.Deployment, cas.patch})
	if err != nil {
		return "", "", err
	}
	return digest(append([]byte("fugue.registry-rehome.kube-policy.v1\x00"), policyBytes...)),
		digest(append([]byte("fugue.registry-rehome.kube-patch.v1\x00"), writeBytes...)), nil
}
