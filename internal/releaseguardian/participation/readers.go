package participation

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"fugue/internal/model/edgecohort"
	"fugue/internal/productioncontrol"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	admissionclient "k8s.io/client-go/kubernetes/typed/admissionregistration/v1"
	appsclient "k8s.io/client-go/kubernetes/typed/apps/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

const (
	readerRoleEnv     = "FUGUE_RELEASE_GUARDIAN_ROLE"
	readerBaselineEnv = "FUGUE_RELEASE_GUARDIAN_AUTHORITY_ACTIVATORS"
	readerRevision    = "fugue.pro/oci-revision"
	maxReaderReplicas = 32
)

var (
	readerImagePattern    = regexp.MustCompile(`^[a-z0-9./_-]+@sha256:[0-9a-f]{64}$`)
	readerRevisionPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
	baselineCountPattern  = regexp.MustCompile(`^[0-9]{1,3}$`)
)

// KubernetesObserver is a GET-only collector. It does not implement Node
// hold/recovery, read a serving transport, or accept a caller's baseline.
type KubernetesObserver struct {
	apps         appsclient.AppsV1Interface
	core         coreclient.CoreV1Interface
	admission    admissionclient.AdmissionregistrationV1Interface
	namespace    string
	namespaceUID string
}

func NewKubernetesObserver(config *rest.Config) (*KubernetesObserver, error) {
	if config == nil || config.Insecure || config.ExecProvider != nil || config.AuthProvider != nil || config.WrapTransport != nil ||
		config.Transport != nil || config.Proxy != nil {
		return nil, errors.New("participation observation requires an explicit verifiable Kubernetes TLS transport")
	}
	u, err := url.Parse(config.Host)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Path != "" ||
		u.RawQuery != "" || u.Fragment != "" || u.ForceQuery {
		return nil, errors.New("participation observation requires one HTTPS Kubernetes origin")
	}
	cfg := rest.CopyConfig(config)
	// Do not pass the observer credentials through an ambient proxy.
	cfg.Proxy = func(*http.Request) (*url.URL, error) { return nil, nil }
	client, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, err
	}
	// Copy: client-go may return the shared http.DefaultClient.
	observing := *client
	next := observing.Transport
	if next == nil {
		next = http.DefaultTransport
	}
	observing.Transport = getOnlyTransport{next}
	observing.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	apps, err := appsclient.NewForConfigAndClient(cfg, &observing)
	if err != nil {
		return nil, err
	}
	core, err := coreclient.NewForConfigAndClient(cfg, &observing)
	if err != nil {
		return nil, err
	}
	admission, err := admissionclient.NewForConfigAndClient(cfg, &observing)
	if err != nil {
		return nil, err
	}
	return &KubernetesObserver{apps: apps, core: core, admission: admission}, nil
}

type getOnlyTransport struct{ next http.RoundTripper }

func (t getOnlyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet || req.Body != nil || req.URL.Query().Has("watch") {
		return nil, errors.New("participation observation is restricted to finite Kubernetes GET/LIST reads")
	}
	return t.next.RoundTrip(req)
}

type ObjectVersion struct {
	Name            string `json:"name"`
	UID             string `json:"uid"`
	ResourceVersion string `json:"resourceVersion"`
}

type ReaderRollout struct {
	Role        string          `json:"role"`
	Deployment  ObjectVersion   `json:"deployment"`
	ReplicaSets []ObjectVersion `json:"replicaSets"`
	Pods        []ObjectVersion `json:"pods"`
	Image       string          `json:"image"`
	Revision    string          `json:"revision"`
}

type ReaderObservation struct {
	Namespace string          `json:"namespace"`
	Group     string          `json:"group"`
	Baseline  int             `json:"baseline"`
	Rollouts  []ReaderRollout `json:"rollouts"`
}

// ObserveReaderRollouts derives the legacy reader baseline from the live
// Guardian Deployment and verifies both sets of release readers. It repeats
// the check for drift; the result does not assert an atomic cluster snapshot
// or independently authorize releases.
func (o *KubernetesObserver) ObserveReaderRollouts(ctx context.Context, policy productioncontrol.ParticipationPolicy, node string) (ReaderObservation, error) {
	group := ""
	for _, member := range policy.Members {
		if member.Node == node {
			if group != "" || member.Group == "" {
				return ReaderObservation{}, errors.New("ambiguous participation topology member")
			}
			group = member.Group
		}
	}
	if o == nil || o.apps == nil || o.core == nil || group == "" || policy.Namespace == "" ||
		policy.Readers.Guardian == "" || policy.Readers.CanaryProber == "" || policy.Readers.Guardian == policy.Readers.CanaryProber {
		return ReaderObservation{}, errors.New("participation requires an enrolled member and two distinct Kubernetes readers")
	}
	observe := func() (ReaderObservation, error) {
		guardian, count, err := o.readRollout(ctx, policy.Namespace, "guardian", policy.Readers.Guardian, group)
		if err != nil {
			return ReaderObservation{}, err
		}
		canary, _, err := o.readRollout(ctx, policy.Namespace, "canary-prober", policy.Readers.CanaryProber, group)
		if err != nil {
			return ReaderObservation{}, err
		}
		return ReaderObservation{Namespace: policy.Namespace, Group: group, Baseline: count, Rollouts: []ReaderRollout{guardian, canary}}, nil
	}
	first, err := observe()
	if err != nil {
		return ReaderObservation{}, err
	}
	second, err := observe()
	if err != nil {
		return ReaderObservation{}, err
	}
	if first.Baseline != second.Baseline || !slices.EqualFunc(first.Rollouts, second.Rollouts, func(a, b ReaderRollout) bool {
		return a.Role == b.Role && a.Deployment == b.Deployment && a.Image == b.Image && a.Revision == b.Revision &&
			slices.Equal(a.ReplicaSets, b.ReplicaSets) && slices.Equal(a.Pods, b.Pods)
	}) {
		return ReaderObservation{}, errors.New("reader rollout changed during participation observation")
	}
	return second, nil
}

func (o *KubernetesObserver) readRollout(ctx context.Context, ns, role, name, group string) (ReaderRollout, int, error) {
	fail := func(msg string) (ReaderRollout, int, error) {
		return ReaderRollout{}, 0, fmt.Errorf("participation %s reader: %s", role, msg)
	}
	deployment, err := o.apps.Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil || !exactLiveObject(deployment, ns, name) || deployment.Generation < 1 ||
		deployment.Annotations[edgecohort.ReleaseHoldCapabilityAnnotation] != "v1" {
		return fail("cannot verify Deployment identity or release-hold capability")
	}
	expected := int32(1)
	if deployment.Spec.Replicas != nil {
		expected = *deployment.Spec.Replicas
	}
	s := deployment.Status
	if expected < 1 || expected > maxReaderReplicas || s.ObservedGeneration != deployment.Generation ||
		s.Replicas != expected || s.UpdatedReplicas != expected || s.ReadyReplicas != expected || s.AvailableReplicas != expected {
		return fail("Deployment rollout is not completely ready")
	}
	rev := deployment.Spec.Template.Annotations[readerRevision]
	c, ok := rolloutReaderContainer(deployment, role)
	if !ok || !readerRevisionPattern.MatchString(rev) || !readerImagePattern.MatchString(c.Image) {
		return fail("reader executable is not uniquely pinned")
	}
	count := 0
	if role == "guardian" {
		count, ok = declaredGroupBaseline(c.Env, group)
		if !ok {
			return fail("Guardian lacks an unambiguous group baseline")
		}
	}
	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil || selector.Empty() || deployment.Spec.Selector == nil ||
		(len(deployment.Spec.Selector.MatchLabels) == 0 && len(deployment.Spec.Selector.MatchExpressions) == 0) {
		return fail("Deployment selector is absent or invalid")
	}
	pods, err := o.core.Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: selector.String(), Limit: maxReaderReplicas + 1})
	if err != nil || pods.Continue != "" || pods.ResourceVersion == "" || len(pods.Items) != int(expected) {
		return fail("Pod inventory is incomplete")
	}
	report := ReaderRollout{Role: role, Deployment: describeObject(deployment), Image: c.Image, Revision: rev}
	seenPods := make(map[string]bool, len(pods.Items))
	sets := make(map[string]ObjectVersion)
	for i := range pods.Items {
		pod := &pods.Items[i]
		if !exactLiveObject(pod, ns, pod.Name) || seenPods[string(pod.UID)] ||
			!selector.Matches(labels.Set(pod.Labels)) || !readyReaderPod(pod, c, role, rev) {
			return fail("Pod identity, health, revision or runtime image differs")
		}
		seenPods[string(pod.UID)] = true
		rsRef := soleController(pod)
		if rsRef == nil || rsRef.APIVersion != "apps/v1" || rsRef.Kind != "ReplicaSet" {
			return fail("Pod is not controlled by a ReplicaSet")
		}
		proof, found := sets[rsRef.Name]
		if !found {
			rs, err := o.apps.ReplicaSets(ns).Get(ctx, rsRef.Name, metav1.GetOptions{})
			if err != nil || !exactLiveObject(rs, ns, rsRef.Name) || rs.Spec.Template.Annotations[readerRevision] != rev {
				return fail("ReplicaSet does not bind the observed revision")
			}
			owner := soleController(rs)
			if owner == nil || owner.APIVersion != "apps/v1" || owner.Kind != "Deployment" ||
				owner.Name != name || owner.UID != deployment.UID {
				return fail("ReplicaSet is owned by a different Deployment")
			}
			proof = describeObject(rs)
			sets[rsRef.Name] = proof
			report.ReplicaSets = append(report.ReplicaSets, proof)
		}
		if string(rsRef.UID) != proof.UID {
			return fail("Pod refers to a replaced ReplicaSet")
		}
		report.Pods = append(report.Pods, describeObject(pod))
	}
	sortObjectVersions(report.Pods)
	sortObjectVersions(report.ReplicaSets)
	return report, count, nil
}

func exactLiveObject(m metav1.Object, ns, name string) bool {
	return name != "" && m.GetName() == name && m.GetNamespace() == ns &&
		m.GetUID() != "" && m.GetResourceVersion() != "" && m.GetDeletionTimestamp() == nil
}

func describeObject(m metav1.Object) ObjectVersion {
	return ObjectVersion{m.GetName(), string(m.GetUID()), m.GetResourceVersion()}
}

func sortObjectVersions(objects []ObjectVersion) {
	slices.SortFunc(objects, func(a, b ObjectVersion) int { return strings.Compare(a.UID, b.UID) })
}

func soleController(m metav1.Object) *metav1.OwnerReference {
	var owner *metav1.OwnerReference
	for _, r := range m.GetOwnerReferences() {
		if r.Controller == nil || !*r.Controller {
			continue
		}
		if owner != nil || r.Name == "" || r.UID == "" {
			return nil
		}
		owner = &r
	}
	return owner
}

func exactEnv(env []corev1.EnvVar, name string) (string, bool) {
	value, hits := "", 0
	for _, e := range env {
		if e.Name == name {
			hits++
			if e.ValueFrom != nil {
				return "", false
			}
			value = e.Value
		}
	}
	return value, hits == 1
}

func rolloutReaderContainer(d *appsv1.Deployment, role string) (corev1.Container, bool) {
	var found corev1.Container
	for _, c := range d.Spec.Template.Spec.Containers {
		if value, ok := exactEnv(c.Env, readerRoleEnv); ok && value == role {
			if found.Name != "" || c.Name == "" {
				return corev1.Container{}, false
			}
			found = c
		}
	}
	imageCount := 0
	for _, c := range d.Spec.Template.Spec.Containers {
		if c.Image == found.Image {
			imageCount++
		}
	}
	return found, found.Name != "" && imageCount == 1
}

func declaredGroupBaseline(env []corev1.EnvVar, group string) (int, bool) {
	config, ok := exactEnv(env, readerBaselineEnv)
	if !ok {
		return 0, false
	}
	count := 0
	for _, entry := range strings.Split(config, ";") {
		fields := strings.Split(entry, ",")
		if fields[0] != group {
			continue
		}
		if count != 0 || len(fields) != 10 || !baselineCountPattern.MatchString(fields[3]) {
			return 0, false
		}
		count, _ = strconv.Atoi(fields[3])
		if count < 1 || count > 100 {
			return 0, false
		}
	}
	return count, count != 0
}

func readyReaderPod(p *corev1.Pod, c corev1.Container, role, revision string) bool {
	if p.Annotations[readerRevision] != revision || p.Status.Phase != corev1.PodRunning {
		return false
	}
	readiness, specs, statuses := 0, 0, 0
	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady {
			if cond.Status != corev1.ConditionTrue {
				return false
			}
			readiness++
		}
	}
	for _, container := range p.Spec.Containers {
		if container.Name == c.Name {
			gotRole, ok := exactEnv(container.Env, readerRoleEnv)
			if container.Image != c.Image || !ok || gotRole != role {
				return false
			}
			if role == "guardian" {
				want, _ := exactEnv(c.Env, readerBaselineEnv)
				got, ok := exactEnv(container.Env, readerBaselineEnv)
				if !ok || got != want {
					return false
				}
			}
			specs++
		}
	}
	_, imageDigest, _ := strings.Cut(c.Image, "@")
	for _, status := range p.Status.ContainerStatuses {
		if status.Name == c.Name {
			if !status.Ready || status.RestartCount != 0 || status.State.Running == nil ||
				!strings.HasSuffix(status.ImageID, "@"+imageDigest) {
				return false
			}
			statuses++
		}
	}
	return readiness == 1 && specs == 1 && statuses == 1
}
