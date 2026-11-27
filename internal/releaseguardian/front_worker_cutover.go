package releaseguardian

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
)

// FrontWorkerCutover describes the shared Kubernetes mutation boundary of a
// node-local handoff. WorkerDaemonSets and ComponentLeases are the staged
// Worker controllers and their release leases that can rearm or retire slots.
type FrontWorkerCutover struct {
	Namespace             string           `json:"namespace"`
	Group                 string           `json:"group"`
	Phase                 KubeCutoverPhase `json:"phase"`
	Guardian              string           `json:"guardian"`
	DaemonSetStatusWriter string           `json:"daemonSetStatusWriter"`
	WorkerDaemonSets      []string         `json:"workerDaemonSets"`
	ComponentLeases       []string         `json:"componentLeases"`
}

// FrontWorkerGroupLeaseName is shared by ordinary promotion, Front repair,
// and node-local handoffs. Taking this lease alone never authorizes a write.
func FrontWorkerGroupLeaseName(group string) string {
	return "fugue-authority-" + group
}

// FrontWorkerCutoverPolicies renders the frozen or native phase of the same
// provider admission policies. The native identity must use new credentials.
// Its enrollment follows confirmed RBAC revocation and drainage of old writes.
func FrontWorkerCutoverPolicies(config FrontWorkerCutover) ([]KubeCutoverPolicy, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	group, namespace := config.Group, config.Namespace
	id := strings.TrimPrefix(digest([]byte("fugue.cutover.front-worker.v1\n"+namespace+"\n"+group)), "sha256:")[:20]
	prefix := "fugue-front-worker-cutover-" + id
	inNames := func(names ...string) string {
		quoted := make([]string, 0, len(names))
		for _, name := range names {
			quoted = append(quoted, strconv.Quote(name))
		}
		return "variables.targetName in [" + strings.Join(quoted, ", ") + "]"
	}
	labeledGroup := func(object string) string {
		return object + " != null && has(" + object + ".metadata.labels) && 'fugue.pro/group' in " + object +
			".metadata.labels && " + object + ".metadata.labels['fugue.pro/group'] == " + strconv.Quote(group)
	}
	shapedName := func(family string) string {
		return "variables.targetName.matches(" + strconv.Quote("^fugue-node-worker-("+family+")-[0-9a-f]{32}$") + ")"
	}
	sharedFence := inNames(FrontRepairFenceName(group))
	activatedFence := inNames(transitionActivatedJournalName(group))
	marker := inNames(NodeWorkerModeName(group))
	index := inNames(NodeWorkerIndexName(group))
	immutableRecords := shapedName("armed|connected|result|retirement|retired|archive")
	projections := shapedName("projection|drain")
	groupRecords := joinCEL(" && ",
		joinCEL(" || ", immutableRecords, projections), joinCEL(" || ", labeledGroup("oldObject"), labeledGroup("object")))
	mapTarget := joinCEL(" || ", sharedFence, activatedFence, marker, index, groupRecords)
	mapPermit, leasePermit := "false", "false"
	username := "request.userInfo.username == " + strconv.Quote(config.Guardian)
	update := "request.operation == 'UPDATE'"
	createOrUpdate := "(request.operation == 'CREATE' || request.operation == 'UPDATE')"
	immutableCreate := "request.operation == 'CREATE' && object != null && has(object.immutable) && object.immutable"
	daemonStatus := joinCEL(" && ", "has(request.subResource)", "request.subResource == 'status'", update,
		"request.userInfo.username == "+strconv.Quote(config.DaemonSetStatusWriter))
	daemonPermit := daemonStatus
	if config.Phase == KubeCutoverNative {
		// A native transaction creates and removes the shared handoff
		// fence. Prepared whole-group transitions stay closed.
		isWorkerFence := func(object string) string {
			return joinCEL(" && ", labeledGroup(object), "'fugue.pro/authority-kind' in "+object+".metadata.labels",
				object+".metadata.labels['fugue.pro/authority-kind'] == 'node-worker-journal'")
		}
		nativeMarker := func(subject string) string {
			annotations := subject + ".metadata.annotations"
			fieldMatches := func(name, pattern string) string {
				key := strconv.Quote(name)
				return "(" + key + " in " + annotations + " && " + annotations + "[" + key + "].matches(" + strconv.Quote(pattern) + "))"
			}
			return joinCEL(" && ", "has("+annotations+")",
				fieldMatches(nativeIntentAnnotation, "^sha256:[0-9a-f]{64}$"), fieldMatches(nativeWorkerApprovalAnnotation, "^sha256:[0-9a-f]{64}$"),
				fieldMatches(nativeWorkerClusterAnnotation, nativeWorkerClusterExpression),
				fieldMatches(nativeFenceAnnotation, "^[1-9][0-9]{0,18}$"))
		}
		fenceOperations := joinCEL(" || ",
			joinCEL(" && ", immutableCreate, isWorkerFence("object"), nativeMarker("object")),
			joinCEL(" && ", "request.operation == 'DELETE'", isWorkerFence("oldObject"), nativeMarker("oldObject")))
		mapPermit = joinCEL(" && ", username,
			joinCEL(" || ",
				joinCEL(" && ", sharedFence, fenceOperations),
				joinCEL(" && ", joinCEL(" || ", marker, immutableRecords), immutableCreate, labeledGroup("object")),
				joinCEL(" && ", joinCEL(" || ", index, projections), createOrUpdate, labeledGroup("object"))))
		leasePermit = joinCEL(" && ", username, createOrUpdate)
		daemonPermit = joinCEL(" || ", daemonStatus,
			joinCEL(" && ", username, "(!has(request.subResource) || request.subResource == '')", update))
	}
	return []KubeCutoverPolicy{
		makeCutoverPolicy(prefix+"-maps", namespace, "", []string{"configmaps"}, mapTarget, mapPermit, nil),
		makeCutoverPolicy(prefix+"-leases", namespace, "coordination.k8s.io", []string{"leases"},
			inNames(append([]string{FrontWorkerGroupLeaseName(group)}, config.ComponentLeases...)...), leasePermit, nil),
		makeCutoverPolicy(prefix+"-workers", namespace, "apps", []string{"daemonsets", "daemonsets/status"},
			inNames(config.WorkerDaemonSets...), daemonPermit, nil),
	}, nil
}

func (config FrontWorkerCutover) validate() error {
	if len(validation.IsDNS1123Label(config.Namespace)) != 0 || !groupPattern.MatchString(config.Group) || len(config.Group) > 63 ||
		(config.Phase != KubeCutoverFrozen && config.Phase != KubeCutoverNative) || !cutoverServiceAccount(config.Guardian) {
		return errors.New("Front/Worker cutover requires a namespace, group, phase and dedicated Guardian service account")
	}
	if config.DaemonSetStatusWriter == config.Guardian ||
		(config.DaemonSetStatusWriter != "system:kube-controller-manager" && !cutoverServiceAccount(config.DaemonSetStatusWriter)) {
		return errors.New("Front/Worker cutover requires a separate DaemonSet status writer when it guards Workers")
	}
	if len(config.WorkerDaemonSets) == 0 || len(config.WorkerDaemonSets) > 32 ||
		len(config.ComponentLeases) == 0 || len(config.ComponentLeases) > 32 {
		return errors.New("Front/Worker cutover requires the enrolled Worker DaemonSets and component Leases")
	}
	for _, names := range [][]string{config.WorkerDaemonSets, append([]string{FrontWorkerGroupLeaseName(config.Group)}, config.ComponentLeases...)} {
		seen := make(map[string]bool, len(names))
		for _, name := range names {
			if len(validation.IsDNS1123Subdomain(name)) != 0 || seen[name] {
				return errors.New("Front/Worker cutover object names must be valid and distinct")
			}
			seen[name] = true
		}
	}
	return nil
}

type FrontWorkerPolicyObservation struct {
	PolicyName      string `json:"policyName"`
	PolicyUID       string `json:"policyUID"`
	PolicyRevision  string `json:"policyRevision"`
	BindingUID      string `json:"bindingUID"`
	BindingRevision string `json:"bindingRevision"`
}

type FrontWorkerLeaseObservation struct {
	Name     string `json:"name"`
	UID      string `json:"uid,omitempty"`
	Revision string `json:"revision,omitempty"`
}

type FrozenFrontWorkerObservation struct {
	Policies []FrontWorkerPolicyObservation `json:"policies"`
	Leases   []FrontWorkerLeaseObservation  `json:"leases"`
}

// ObserveFrozenFrontWorkerBoundary checks that both shared transaction
// objects are absent and that every configured Lease is absent or has
// already been released. Provider request drainage requires separate proof.
func ObserveFrozenFrontWorkerBoundary(ctx context.Context, client kubernetes.Interface, config FrontWorkerCutover) (FrozenFrontWorkerObservation, error) {
	if client == nil || config.Phase != KubeCutoverFrozen {
		return FrozenFrontWorkerObservation{}, errors.New("Front/Worker boundary requires the frozen phase and a Kubernetes client")
	}
	policies, err := FrontWorkerCutoverPolicies(config)
	if err != nil {
		return FrozenFrontWorkerObservation{}, err
	}
	read := func() (FrozenFrontWorkerObservation, error) {
		policyRevisions, err := observeFrontWorkerPolicyRevisions(ctx, client, policies)
		if err != nil {
			return FrozenFrontWorkerObservation{}, err
		}
		configs := client.CoreV1().ConfigMaps(config.Namespace)
		for _, name := range []string{FrontRepairFenceName(config.Group), transitionActivatedJournalName(config.Group)} {
			_, err := configs.Get(ctx, name, metav1.GetOptions{})
			if err == nil {
				return FrozenFrontWorkerObservation{}, fmt.Errorf("Front/Worker transaction fence %s is still present", name)
			}
			if !apierrors.IsNotFound(err) {
				return FrozenFrontWorkerObservation{}, fmt.Errorf("read Front/Worker transaction fence %s: %w", name, err)
			}
		}
		leases, err := observeReleasedFrontWorkerLeases(ctx, client, config)
		return FrozenFrontWorkerObservation{Policies: policyRevisions, Leases: leases}, err
	}
	before, err := read()
	if err != nil {
		return FrozenFrontWorkerObservation{}, err
	}
	after, err := read()
	if err != nil {
		return FrozenFrontWorkerObservation{}, err
	}
	if !slices.Equal(before.Policies, after.Policies) || !slices.Equal(before.Leases, after.Leases) {
		return FrozenFrontWorkerObservation{}, errors.New("frozen Front/Worker policies or Leases changed during readback")
	}
	return after, nil
}

func observeReleasedFrontWorkerLeases(ctx context.Context, client kubernetes.Interface, config FrontWorkerCutover) ([]FrontWorkerLeaseObservation, error) {
	leases := client.CoordinationV1().Leases(config.Namespace)
	names := append([]string{FrontWorkerGroupLeaseName(config.Group)}, config.ComponentLeases...)
	observations := make([]FrontWorkerLeaseObservation, 0, len(names))
	for _, name := range names {
		observed := FrontWorkerLeaseObservation{Name: name}
		lease, err := leases.Get(ctx, name, metav1.GetOptions{})
		if !apierrors.IsNotFound(err) {
			if err != nil {
				return nil, fmt.Errorf("read Front/Worker Lease %s: %w", name, err)
			}
			if lease.Name != name || lease.Namespace != config.Namespace ||
				lease.UID == "" || lease.ResourceVersion == "" || lease.DeletionTimestamp != nil ||
				(lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "") {
				return nil, fmt.Errorf("Front/Worker Lease %s still has a holder or its release is unproven", name)
			}
			observed.UID, observed.Revision = string(lease.UID), lease.ResourceVersion
		}
		observations = append(observations, observed)
	}
	return observations, nil
}

// ObserveFrontWorkerCutover reads the exact admission policy and binding
// revisions for this phase. Endpoint enforcement and drain evidence come
// from the provider independently of these object reads.
func ObserveFrontWorkerCutover(ctx context.Context, client kubernetes.Interface, config FrontWorkerCutover) ([]FrontWorkerPolicyObservation, error) {
	if client == nil {
		return nil, errors.New("Front/Worker cutover observation requires a Kubernetes client")
	}
	policies, err := FrontWorkerCutoverPolicies(config)
	if err != nil {
		return nil, err
	}
	first, err := observeFrontWorkerPolicyRevisions(ctx, client, policies)
	if err != nil {
		return nil, err
	}
	last, err := observeFrontWorkerPolicyRevisions(ctx, client, policies)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(first, last) {
		return nil, errors.New("Front/Worker policy or binding revisions changed during readback")
	}
	return last, nil
}

func observeFrontWorkerPolicyRevisions(ctx context.Context, client kubernetes.Interface, policies []KubeCutoverPolicy) ([]FrontWorkerPolicyObservation, error) {
	registry := client.AdmissionregistrationV1()
	observed := make([]FrontWorkerPolicyObservation, 0, len(policies))
	for _, pair := range policies {
		policy, err := registry.ValidatingAdmissionPolicies().Get(ctx, pair.Policy.Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("read Front/Worker admission policy %s: %w", pair.Policy.Name, err)
		}
		if policy.UID == "" || policy.ResourceVersion == "" || policy.DeletionTimestamp != nil ||
			!sameCutoverPolicySpec(policy.Spec, pair.Policy.Spec) || policy.Generation < 1 || policy.Status.TypeChecking == nil ||
			policy.Status.ObservedGeneration != policy.Generation || len(policy.Status.TypeChecking.ExpressionWarnings) > 0 {
			return nil, fmt.Errorf("Front/Worker policy %s is not exact and type-checked", pair.Policy.Name)
		}
		binding, err := registry.ValidatingAdmissionPolicyBindings().Get(ctx, pair.Binding.Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("read Front/Worker admission binding %s: %w", pair.Binding.Name, err)
		}
		bindingSpec, expectedBindingSpec := binding.Spec, pair.Binding.Spec
		bindingSpec.ValidationActions = slices.Clone(bindingSpec.ValidationActions)
		expectedBindingSpec.ValidationActions = slices.Clone(expectedBindingSpec.ValidationActions)
		slices.Sort(bindingSpec.ValidationActions)
		slices.Sort(expectedBindingSpec.ValidationActions)
		if binding.UID == "" || binding.ResourceVersion == "" || binding.DeletionTimestamp != nil ||
			!apiequality.Semantic.DeepEqual(bindingSpec, expectedBindingSpec) {
			return nil, fmt.Errorf("Front/Worker binding %s differs from the expected enforcing binding", pair.Binding.Name)
		}
		observed = append(observed, FrontWorkerPolicyObservation{
			PolicyName: policy.Name, PolicyUID: string(policy.UID), PolicyRevision: policy.ResourceVersion,
			BindingUID: string(binding.UID), BindingRevision: binding.ResourceVersion,
		})
	}
	return observed, nil
}
