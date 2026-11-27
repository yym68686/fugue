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
	expectedPolicy, expectedBinding, err := FreezeNodePolicy(node)
	if err != nil {
		return FreezeObservation{}, err
	}
	return o.observeNodeGuard(ctx, node, expectedPolicy, expectedBinding)
}

// ObserveOfflineHoldGuard checks that the installed policy is exactly the
// one rendered for this submission and the designated writer. It does not
// prove propagation to other API servers, credential revocation, or drainage.
func (o *KubernetesObserver) ObserveOfflineHoldGuard(ctx context.Context, topology productioncontrol.VerifiedParticipationTopology, s productioncontrol.Submission, writer string) (FreezeObservation, error) {
	hold, err := decodeHold(s)
	if err != nil {
		return FreezeObservation{}, err
	}
	member, err := enrolledMember(topology.Policy, hold.To.Node)
	if err != nil || topology.Policy.Schema != productioncontrol.BoundParticipationPolicySchema ||
		topology.Version != hold.Topology || topology.ApprovalDigest != hold.TopologyApproval ||
		member.NodeUID != hold.To.UID || member.Group != hold.To.Group {
		return FreezeObservation{}, errors.New("offline hold guard does not match the current enrollment or topology approval")
	}
	expectedPolicy, expectedBinding, err := RenderOfflineHoldGuard(s, writer)
	if err != nil {
		return FreezeObservation{}, err
	}
	return o.observeNodeGuard(ctx, member.Node, expectedPolicy, expectedBinding)
}

func (o *KubernetesObserver) observeNodeGuard(ctx context.Context, node string, expectedPolicy *admission.ValidatingAdmissionPolicy, expectedBinding *admission.ValidatingAdmissionPolicyBinding) (FreezeObservation, error) {
	if o == nil || o.admission == nil {
		return FreezeObservation{}, errors.New("no Kubernetes participation admission reader")
	}
	observe := func() (FreezeObservation, error) {
		p, err := o.admission.ValidatingAdmissionPolicies().Get(ctx, expectedPolicy.Name, metav1.GetOptions{})
		if err != nil || !exactLiveObject(p, "", expectedPolicy.Name) || !sameFreezePolicy(p.Spec, expectedPolicy.Spec) ||
			(p.Status.TypeChecking != nil && len(p.Status.TypeChecking.ExpressionWarnings) != 0) {
			return FreezeObservation{}, errors.New("stored participation admission policy is missing, differs, or has expression warnings")
		}
		b, err := o.admission.ValidatingAdmissionPolicyBindings().Get(ctx, expectedBinding.Name, metav1.GetOptions{})
		if err != nil || !exactLiveObject(b, "", expectedBinding.Name) || !sameFreezeBinding(b.Spec, expectedBinding.Spec) {
			return FreezeObservation{}, errors.New("stored participation admission binding is missing or differs")
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
		return FreezeObservation{}, errors.New("participation admission objects changed during readback")
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
