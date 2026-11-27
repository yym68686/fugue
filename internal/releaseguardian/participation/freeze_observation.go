package participation

import (
	"context"
	"errors"
	"slices"

	"fugue/internal/productioncontrol"
	admission "k8s.io/api/admissionregistration/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type FreezeObservation struct {
	Node               string        `json:"node"`
	Policy             ObjectVersion `json:"policy"`
	Binding            ObjectVersion `json:"binding"`
	CurrentTypeChecked bool          `json:"currentTypeChecked"`
}

// ObserveNodeFreeze checks the stored configuration twice. This does not
// check endpoint enforcement, old credential revocation or request drainage.
func (o *KubernetesObserver) ObserveNodeFreeze(ctx context.Context, topology productioncontrol.ParticipationPolicy, node string) (FreezeObservation, error) {
	if _, err := enrolledMember(topology, node); err != nil {
		return FreezeObservation{}, err
	}
	if o == nil || o.admission == nil {
		return FreezeObservation{}, errors.New("no Kubernetes participation admission reader")
	}
	expectedPolicy, expectedBinding, err := FreezeNodePolicy(node)
	if err != nil {
		return FreezeObservation{}, err
	}
	observe := func() (FreezeObservation, error) {
		p, err := o.admission.ValidatingAdmissionPolicies().Get(ctx, expectedPolicy.Name, metav1.GetOptions{})
		if err != nil || !exactLiveObject(p, "", expectedPolicy.Name) || !sameFreezePolicy(p.Spec, expectedPolicy.Spec) ||
			(p.Status.TypeChecking != nil && len(p.Status.TypeChecking.ExpressionWarnings) != 0) {
			return FreezeObservation{}, errors.New("stored participation freeze policy is missing, differs, or has expression warnings")
		}
		b, err := o.admission.ValidatingAdmissionPolicyBindings().Get(ctx, expectedBinding.Name, metav1.GetOptions{})
		if err != nil || !exactLiveObject(b, "", expectedBinding.Name) || !sameFreezeBinding(b.Spec, expectedBinding.Spec) {
			return FreezeObservation{}, errors.New("stored participation freeze binding is missing or differs")
		}
		return FreezeObservation{Node: node, Policy: describeObject(p), Binding: describeObject(b),
			CurrentTypeChecked: p.Generation > 0 && p.Status.ObservedGeneration == p.Generation && p.Status.TypeChecking != nil}, nil
	}
	before, err := observe()
	if err != nil {
		return FreezeObservation{}, err
	}
	after, err := observe()
	if err != nil {
		return FreezeObservation{}, err
	}
	if before != after {
		return FreezeObservation{}, errors.New("participation freeze objects changed during readback")
	}
	return after, nil
}

func sameFreezePolicy(actual, expected admission.ValidatingAdmissionPolicySpec) bool {
	// Kubernetes defaults the absent selectors to empty, unrestricted
	// selectors. Do not normalize any selector that can narrow the policy.
	if actual.MatchConstraints != nil {
		match := actual.MatchConstraints.DeepCopy()
		empty := func(s *metav1.LabelSelector) bool {
			return s != nil && len(s.MatchLabels) == 0 && len(s.MatchExpressions) == 0
		}
		if empty(match.ObjectSelector) {
			match.ObjectSelector = nil
		}
		if empty(match.NamespaceSelector) {
			match.NamespaceSelector = nil
		}
		actual.MatchConstraints = match
	}
	return apiequality.Semantic.DeepEqual(actual, expected)
}

func sameFreezeBinding(actual, expected admission.ValidatingAdmissionPolicyBindingSpec) bool {
	// ValidationActions is a set: retain all actions but ignore ordering.
	actual.ValidationActions = slices.Clone(actual.ValidationActions)
	expected.ValidationActions = slices.Clone(expected.ValidationActions)
	slices.Sort(actual.ValidationActions)
	slices.Sort(expected.ValidationActions)
	return apiequality.Semantic.DeepEqual(actual, expected)
}
