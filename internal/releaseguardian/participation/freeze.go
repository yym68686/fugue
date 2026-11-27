package participation

import (
	"errors"
	"strconv"
	"strings"

	"fugue/internal/model/edgecohort"
	admission "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/utils/ptr"
)

// FreezeNodePolicy renders a fail-closed fence, scoped by Node name so
// recreating the Node cannot bypass it. It grants nobody a new write path.
// It is not a fence for Pod exec/delete, Leases, or writes already admitted.
func FreezeNodePolicy(node string) (*admission.ValidatingAdmissionPolicy, *admission.ValidatingAdmissionPolicyBinding, error) {
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
	checks := []string{"request.operation == 'UPDATE'", "object != null", "oldObject != null",
		"object.metadata.uid == oldObject.metadata.uid",
		unchanged("annotations", participationAnnotation), unchanged("annotations", edgecohort.ReleaseHoldAnnotation)}
	for _, label := range []string{participationGroup, participationRole, participationEnrollment, "fugue.io/schedulable", "fugue.io/edge-workload"} {
		checks = append(checks, unchanged("labels", label))
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
					" || (" + strings.Join(checks, " && ") + "))",
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
