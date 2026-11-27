package participation

import (
	"errors"
	"strconv"
	"strings"

	"fugue/internal/model/edgecohort"
	"fugue/internal/productioncontrol"
	admission "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
)

// FreezeNodePolicy renders a fail-closed fence, scoped by Node name so
// recreating the Node cannot bypass it. It grants nobody a new write path.
// It is not a fence for Pod exec/delete, Leases, or writes already admitted.
func FreezeNodePolicy(node string) (*admission.ValidatingAdmissionPolicy, *admission.ValidatingAdmissionPolicyBinding, error) {
	return nodePolicy(node, "", "")
}

// RenderOfflineHoldGuard derives a provider-side admission constraint, not
// permission to install it. It admits only the reviewed offline hold or its
// exact inverse at the reviewed resourceVersion; success consumes that version.
// Recovering holds are deliberately unsupported: their stabilization clock
// must start at actual application, not draft time.
func RenderOfflineHoldGuard(s productioncontrol.Submission, writer string) (*admission.ValidatingAdmissionPolicy, *admission.ValidatingAdmissionPolicyBinding, error) {
	hold, err := decodeHold(s)
	path, prefix := strings.CutPrefix(writer, "system:serviceaccount:")
	ns, name, split := strings.Cut(path, ":")
	if err != nil || !exactHoldPhase(hold.Phase) || !prefix || !split ||
		len(validation.IsDNS1123Label(ns)) != 0 || len(validation.IsDNS1123Subdomain(name)) != 0 || name == "default" ||
		len(hold.ResourceVersion) > 128 || len(hold.To.UID) > 128 {
		return nil, nil, errors.New("invalid offline hold admission boundary or dedicated writer")
	}
	annotationEquals := func(subject, key string, value *string) string {
		path, k := subject+".metadata.annotations", strconv.Quote(key)
		present := "(has(" + path + ") && " + k + " in " + path + ")"
		if value == nil {
			return "!" + present
		}
		return "(" + present + " && " + path + "[" + k + "] == " + strconv.Quote(*value) + ")"
	}
	// Absent and empty maps have the same set of unrelated keys. Compare
	// both directions to reject removals as well as replacements/insertions.
	unchangedOtherAnnotations := func(a, b string) string {
		x, y := a+".metadata.annotations", b+".metadata.annotations"
		return "(!has(" + x + ") || " + x + ".all(k, k == " + strconv.Quote(participationAnnotation) +
			" || k == " + strconv.Quote(edgecohort.ReleaseHoldAnnotation) +
			" || (has(" + y + ") && k in " + y + " && " + x + "[k] == " + y + "[k])))"
	}
	checks := []string{
		"request.userInfo.username == " + strconv.Quote(writer),
		"(!has(request.subResource) || request.subResource == '')",
		"oldObject.metadata.uid == " + strconv.Quote(hold.To.UID),
		"oldObject.metadata.resourceVersion == " + strconv.Quote(hold.ResourceVersion),
		"object.metadata.resourceVersion == oldObject.metadata.resourceVersion",
		"!has(oldObject.metadata.deletionTimestamp)", "!has(object.metadata.deletionTimestamp)",
		"has(oldObject.metadata.labels) && " + strconv.Quote(participationGroup) + " in oldObject.metadata.labels",
		"oldObject.metadata.labels[" + strconv.Quote(participationGroup) + "] == " + strconv.Quote(hold.To.Group),
		strconv.Quote(participationRole) + " in oldObject.metadata.labels && oldObject.metadata.labels[" + strconv.Quote(participationRole) + "] == 'true'",
		"object.spec == oldObject.spec",
		unchangedOtherAnnotations("oldObject", "object"), unchangedOtherAnnotations("object", "oldObject"),
	}
	// Managed fields are server-owned. Do not allow changes to user-owned
	// metadata or Node spec alongside the reviewed annotations.
	for _, field := range []string{"labels", "ownerReferences", "finalizers", "deletionGracePeriodSeconds"} {
		old, next := "oldObject.metadata."+field, "object.metadata."+field
		checks = append(checks, "(has("+old+") ? (has("+next+") && "+old+" == "+next+") : !has("+next+"))")
	}
	for _, key := range []string{participationAnnotation, edgecohort.ReleaseHoldAnnotation} {
		checks = append(checks, annotationEquals("oldObject", key, hold.From.Annotations[key]),
			annotationEquals("object", key, hold.To.Annotations[key]))
	}
	return nodePolicy(hold.To.Node, writer, "("+strings.Join(checks, " && ")+")")
}

func nodePolicy(node, writer, guardedTransition string) (*admission.ValidatingAdmissionPolicy, *admission.ValidatingAdmissionPolicyBinding, error) {
	if len(validation.IsDNS1123Subdomain(node)) != 0 {
		return nil, nil, errors.New("invalid participation freeze Node name")
	}
	name := "fugue-participation-freeze-" + strings.TrimPrefix(digest([]byte("fugue.participation.node.v1\n"+node)), "sha256:")[:20]
	unchanged := func(field, key string) string {
		k := strconv.Quote(key)
		old, next := "oldObject.metadata."+field, "object.metadata."+field
		oldHas, newHas := "has("+old+") && "+k+" in "+old, "has("+next+") && "+k+" in "+next
		return "((" + oldHas + ") ? ((" + newHas + ") && " + old + "[" + k + "] == " + next + "[" + k + "]) : !(" + newHas + "))"
	}
	unchangedMetadata := []string{
		unchanged("annotations", participationAnnotation), unchanged("annotations", edgecohort.ReleaseHoldAnnotation)}
	for _, label := range []string{participationGroup, participationRole, participationEnrollment, "fugue.io/schedulable", "fugue.io/edge-workload"} {
		unchangedMetadata = append(unchangedMetadata, unchanged("labels", label))
	}
	common := "request.operation == 'UPDATE' && object != null && oldObject != null && object.metadata.uid == oldObject.metadata.uid"
	permit := common + " && " + strings.Join(unchangedMetadata, " && ")
	if guardedTransition != "" {
		permit = common + " && ((request.userInfo.username != " + strconv.Quote(writer) +
			" && " + strings.Join(unchangedMetadata, " && ") + ") || " + guardedTransition + ")"
	}
	// A DELETE collection has no caller-supplied object name; check oldObject
	// when the API evaluates each deletion. If no name can be resolved, deny.
	policy := &admission.ValidatingAdmissionPolicy{
		TypeMeta:   metav1.TypeMeta{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingAdmissionPolicy"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: admission.ValidatingAdmissionPolicySpec{
			FailurePolicy: ptr.To(admission.Fail),
			MatchConstraints: &admission.MatchResources{
				MatchPolicy: ptr.To(admission.Equivalent),
				ResourceRules: []admission.NamedRuleWithOperations{{RuleWithOperations: admission.RuleWithOperations{
					Operations: []admission.OperationType{admission.Create, admission.Update, admission.Delete},
					Rule: admission.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"nodes", "nodes/status"},
						Scope: ptr.To(admission.ClusterScope)},
				}}},
			},
			Variables: []admission.Variable{{Name: "target", Expression: "has(request.name) && request.name != '' ? request.name : " +
				"(object != null && has(object.metadata.name) ? object.metadata.name : (oldObject != null && has(oldObject.metadata.name) ? oldObject.metadata.name : ''))"}},
			Validations: []admission.Validation{{
				Expression: "variables.target != '' && (variables.target != " + strconv.Quote(node) +
					" || (" + permit + "))",
				Message: "Fugue participation freeze denies mutation of the enrolled Node identity or participation metadata",
				Reason:  ptr.To(metav1.StatusReasonForbidden),
			}},
		},
	}
	binding := &admission.ValidatingAdmissionPolicyBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingAdmissionPolicyBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       admission.ValidatingAdmissionPolicyBindingSpec{PolicyName: name, ValidationActions: []admission.ValidationAction{admission.Deny, admission.Audit}},
	}
	return policy, binding, nil
}
