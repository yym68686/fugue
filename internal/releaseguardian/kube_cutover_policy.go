package releaseguardian

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"fugue/internal/productioncontrol"
	admission "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
)

type KubeCutoverPhase string

const (
	KubeCutoverFrozen KubeCutoverPhase = "frozen"
	KubeCutoverNative KubeCutoverPhase = "native"
)

// KubeDeploymentCutover describes a single existing Deployment with no
// autoscaler or delegated writes. Principals are Kubernetes authenticated
// usernames, not field managers, pod labels, or GitHub runner names. Native
// service accounts must be new identities with no usable legacy credentials.
type KubeDeploymentCutover struct {
	Key                    Key              `json:"key"`
	Namespace              string           `json:"namespace"`
	Deployment             string           `json:"deployment"`
	MonitorComponent       string           `json:"monitorComponent"`
	ComponentLease         string           `json:"componentLease"`
	Phase                  KubeCutoverPhase `json:"phase"`
	Publisher              string           `json:"publisher"`
	Guardian               string           `json:"guardian"`
	CanaryProber           string           `json:"canaryProber"`
	DeploymentStatusWriter string           `json:"deploymentStatusWriter"`
}

type KubeCutoverPolicy struct {
	Policy  *admission.ValidatingAdmissionPolicy
	Binding *admission.ValidatingAdmissionPolicyBinding
}

// StorageAuditProjectionFence permits retiring a legacy policy key but
// forbids restoring it, including through binaryData or object replacement.
func StorageAuditProjectionFence(config productioncontrol.StorageAuditShadowBinding) (KubeCutoverPolicy, error) {
	if err := config.Validate(); err != nil {
		return KubeCutoverPolicy{}, err
	}
	const key = "'storage-lifecycle.json'"
	name := "fugue-storage-audit-retired-" + digest([]byte(config.Namespace + "/" + config.ConfigMap))[7:27]
	target := "variables.targetName == " + strconv.Quote(config.ConfigMap)
	permit := "request.operation == 'UPDATE' && oldObject != null && object != null" +
		" && oldObject.metadata.uid == " + strconv.Quote(config.ConfigMapUID) +
		" && object.metadata.uid == oldObject.metadata.uid" +
		" && (!has(object.data) || !(" + key + " in object.data))" +
		" && (!has(object.binaryData) || !(" + key + " in object.binaryData))"
	return makeCutoverPolicy(name, config.Namespace, "", []string{"configmaps"}, target, "("+permit+")", nil), nil
}

// DeploymentCutoverPolicies only renders. Freeze and native use the same
// policy names and deny every unrecognized identity in both phases. A caller
// must not enable native before old requests already admitted have drained,
// legacy credentials/RBAC are removed, and the native runtime and independent
// artifact store are verified. Admission by itself does not prove those facts.
func DeploymentCutoverPolicies(config KubeDeploymentCutover) ([]KubeCutoverPolicy, error) {
	return deploymentCutoverPolicies(config, nil)
}

func deploymentCutoverPolicies(config KubeDeploymentCutover, rehome *registryRehomeKubeScope) ([]KubeCutoverPolicy, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if rehome != nil {
		if err := rehome.validate(config); err != nil {
			return nil, err
		}
	}
	// Own one actual workload regardless of a lane's alias or component key.
	id := strings.TrimPrefix(digest([]byte("fugue.cutover.deployment.v1\n"+config.Namespace+"\n"+config.Deployment)), "sha256:")[:20]
	prefix := "fugue-deployment-cutover-" + id
	nameIs := func(name string) string { return "variables.targetName == " + strconv.Quote(name) }
	nameMatches := func(pattern string) string { return "variables.targetName.matches(" + strconv.Quote(pattern) + ")" }
	username := func(identity string) string { return "request.userInfo.username == " + strconv.Quote(identity) }
	record := nameMatches("^" + regexp.QuoteMeta(objectName("fugue-guardian-record", config.Key)) + "-[0-9a-f]{16}$")
	execution := nameMatches("^" + regexp.QuoteMeta(objectName("fugue-guardian-execution", config.Key)) + "-[1-9][0-9]{0,18}$")
	monitorRecord := nameMatches("^" + regexp.QuoteMeta("fugue-release-record-"+config.MonitorComponent) + "-[0-9a-f]{16}$")
	pointer, monitor := nameIs(desiredName(config.Key)), nameIs("fugue-release-monitor-"+config.MonitorComponent)
	status, canary := nameIs(statusName(config.Key)), nameIs(canaryName(config.Key))
	mapTarget := joinCEL(" || ", record, execution, monitorRecord, pointer, monitor, status, canary)
	workloadTarget := nameIs(config.Deployment)
	leaseTarget := nameIs(config.ComponentLease)
	mapPermit, leasePermit := "false", "false"
	workloadPermit := joinCEL(" && ", "has(request.subResource)", "request.subResource == 'status'", "request.operation == 'UPDATE'", username(config.DeploymentStatusWriter))
	var mapVariables []admission.Variable
	if config.Phase == KubeCutoverNative {
		mutating := "(request.operation == 'CREATE' || request.operation == 'UPDATE')"
		immutableCreate := "(request.operation == 'CREATE' && object != null && has(object.immutable) && object.immutable)"
		mapVariables = cutoverClaimVariables()
		mapPermit = joinCEL(" || ",
			joinCEL(" && ", joinCEL(" || ", record, execution), immutableCreate, username(config.Publisher)),
			joinCEL(" && ", monitorRecord, immutableCreate, username(config.Guardian)),
			joinCEL(" && ", joinCEL(" || ", monitor, status), mutating, username(config.Guardian)),
			joinCEL(" && ", canary, mutating, username(config.CanaryProber)),
			joinCEL(" && ", pointer, mutating, "variables.pointerEnvelope",
				joinCEL(" || ",
					joinCEL(" && ", username(config.Publisher),
						joinCEL(" || ", "!variables.oldMarked", "(variables.oldValid && variables.newFence > variables.oldFence)")),
					joinCEL(" && ", username(config.Guardian), "request.operation == 'UPDATE'", "variables.oldValid",
						"variables.newFence == variables.oldFence", "variables.newIntent == variables.oldIntent"))))
		workloadPermit = joinCEL(" || ", workloadPermit,
			joinCEL(" && ", "(!has(request.subResource) || request.subResource == '')", "request.operation == 'UPDATE'", username(config.Guardian)))
		leasePermit = joinCEL(" && ", mutating, username(config.Guardian))
	}
	if rehome != nil {
		// Only the dedicated bundle includes this exception. The exact
		// projection, expiration and durable claim are independently
		// enforced by the fail-closed validating webhook.
		workloadPermit = joinCEL(" || ", workloadPermit, joinCEL(" && ",
			"(!has(request.subResource) || request.subResource == '')", "request.operation == 'UPDATE'",
			username(rehome.Writer), "oldObject != null && object != null",
			"oldObject.metadata.uid == "+strconv.Quote(rehome.UID),
			"oldObject.metadata.resourceVersion == "+strconv.Quote(rehome.ResourceVersion),
			"oldObject.metadata.generation == "+strconv.FormatInt(rehome.Generation, 10)))
	}
	return []KubeCutoverPolicy{
		makeCutoverPolicy(prefix+"-maps", config.Namespace, "", []string{"configmaps"}, mapTarget, mapPermit, mapVariables),
		makeCutoverPolicy(prefix+"-workload", config.Namespace, "apps", []string{"deployments", "deployments/status", "deployments/scale"}, workloadTarget, workloadPermit, nil),
		makeCutoverPolicy(prefix+"-lease", config.Namespace, "coordination.k8s.io", []string{"leases"}, leaseTarget, leasePermit, nil),
	}, nil
}

func (config KubeDeploymentCutover) validate() error {
	if config.Key.Validate() != nil || !componentPattern.MatchString(config.MonitorComponent) ||
		len(validation.IsDNS1123Label(config.Namespace)) != 0 || len(validation.IsDNS1123Subdomain(config.Deployment)) != 0 ||
		len(validation.IsDNS1123Subdomain(config.ComponentLease)) != 0 ||
		(config.Phase != KubeCutoverFrozen && config.Phase != KubeCutoverNative) ||
		len(validation.IsDNS1123Subdomain(executionSnapshotName(config.Key, 9223372036854775807))) != 0 ||
		len(validation.IsDNS1123Subdomain(monitorRecordNameFromDigest(config.MonitorComponent, "sha256:"+strings.Repeat("0", 64)))) != 0 {
		return errors.New("invalid Kubernetes Deployment cutover scope or phase")
	}
	seen := map[string]bool{}
	for index, identity := range []string{config.Publisher, config.Guardian, config.CanaryProber, config.DeploymentStatusWriter} {
		controlPlane := index == 3 && identity == "system:kube-controller-manager"
		if (!controlPlane && !cutoverServiceAccount(identity)) || seen[identity] {
			return errors.New("Kubernetes cutover requires distinct, explicit authenticated writer identities")
		}
		seen[identity] = true
	}
	return nil
}

func cutoverServiceAccount(identity string) bool {
	path, found := strings.CutPrefix(identity, "system:serviceaccount:")
	namespace, name, separate := strings.Cut(path, ":")
	return found && separate && len(validation.IsDNS1123Label(namespace)) == 0 &&
		len(validation.IsDNS1123Subdomain(name)) == 0 && name != "default"
}

func joinCEL(operator string, expressions ...string) string {
	return "(" + strings.Join(expressions, operator) + ")"
}

func cutoverClaimVariables() []admission.Variable {
	hasAttr := func(subject, key string) string {
		return fmt.Sprintf("(%s != null && has(%s.metadata.annotations) && %s in %s.metadata.annotations)", subject, subject, strconv.Quote(key), subject)
	}
	attr := func(subject, key string) string { return subject + ".metadata.annotations[" + strconv.Quote(key) + "]" }
	oldIntent, oldFence := hasAttr("oldObject", nativeIntentAnnotation), hasAttr("oldObject", nativeFenceAnnotation)
	newIntent, newFence := hasAttr("object", nativeIntentAnnotation), hasAttr("object", nativeFenceAnnotation)
	intentPattern, fencePattern := strconv.Quote("^sha256:[0-9a-f]{64}$"), strconv.Quote("^[1-9][0-9]{0,18}$")
	return []admission.Variable{
		{Name: "oldMarked", Expression: joinCEL(" || ", oldIntent, oldFence)},
		{Name: "oldIntent", Expression: oldIntent + " ? " + attr("oldObject", nativeIntentAnnotation) + " : ''"},
		{Name: "oldFenceString", Expression: oldFence + " ? " + attr("oldObject", nativeFenceAnnotation) + " : ''"},
		{Name: "oldValid", Expression: "variables.oldIntent.matches(" + intentPattern + ") && variables.oldFenceString.matches(" + fencePattern + ")"},
		{Name: "oldFence", Expression: "int(variables.oldFenceString)"},
		{Name: "newIntent", Expression: newIntent + " ? " + attr("object", nativeIntentAnnotation) + " : ''"},
		{Name: "newFenceString", Expression: newFence + " ? " + attr("object", nativeFenceAnnotation) + " : ''"},
		{Name: "newFence", Expression: "int(variables.newFenceString)"},
		{Name: "pointerEnvelope", Expression: "object != null && has(object.data) && size(object.data) == 1 && 'desired.json' in object.data && " +
			"variables.newIntent.matches(" + intentPattern + ") && variables.newFenceString.matches(" + fencePattern + ") && variables.newFence > 0"},
	}
}

func makeCutoverPolicy(name, namespace, group string, resources []string, target, permit string, variables []admission.Variable) KubeCutoverPolicy {
	fail, equivalent, namespaced := admission.Fail, admission.Equivalent, admission.NamespacedScope
	return KubeCutoverPolicy{
		Policy: &admission.ValidatingAdmissionPolicy{
			TypeMeta:   metav1.TypeMeta{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingAdmissionPolicy"},
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: admission.ValidatingAdmissionPolicySpec{
				FailurePolicy: &fail,
				MatchConstraints: &admission.MatchResources{
					MatchPolicy: &equivalent,
					ResourceRules: []admission.NamedRuleWithOperations{{RuleWithOperations: admission.RuleWithOperations{
						Operations: []admission.OperationType{admission.Create, admission.Update, admission.Delete},
						Rule:       admission.Rule{APIGroups: []string{group}, APIVersions: []string{"v1"}, Resources: resources, Scope: &namespaced},
					}}},
				},
				MatchConditions: []admission.MatchCondition{{Name: "cutover-namespace", Expression: "request.namespace == " + strconv.Quote(namespace)}},
				Variables: append([]admission.Variable{{Name: "targetName", Expression: "has(request.name) && request.name != '' ? request.name : " +
					"(object != null && has(object.metadata.name) ? object.metadata.name : (oldObject != null && has(oldObject.metadata.name) ? oldObject.metadata.name : ''))"}}, variables...),
				Validations: []admission.Validation{{
					Expression: "variables.targetName != '' && (!" + joinCEL("", target) + " || " + permit + ")",
					Message:    "Fugue cutover denies mutation of this protected resource or an unnamed request",
					Reason:     ptr.To(metav1.StatusReasonForbidden),
				}},
			},
		},
		Binding: &admission.ValidatingAdmissionPolicyBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingAdmissionPolicyBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: admission.ValidatingAdmissionPolicyBindingSpec{
				PolicyName: name, ValidationActions: []admission.ValidationAction{admission.Deny, admission.Audit},
			},
		},
	}
}
