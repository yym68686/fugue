package participation

import (
	"context"
	"errors"

	"fugue/internal/productioncontrol"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type OfflineHoldReadbackState string

const (
	OfflineHoldTargetVisible      OfflineHoldReadbackState = "target_visible"
	OfflineHoldPredecessorVisible OfflineHoldReadbackState = "predecessor_visible"
	OfflineHoldConditionChanged   OfflineHoldReadbackState = "condition_changed"
)

// OfflineHoldReadback is a point-in-time observation, not a terminal
// execution result. It neither proves which actor changed the annotations
// nor that all prior writes have drained. No state authorizes another attempt.
type OfflineHoldReadback struct {
	State                   OfflineHoldReadbackState `json:"state"`
	Node                    ObjectVersion            `json:"node"`
	ExpectedResourceVersion string                   `json:"expectedResourceVersion"`
	PredecessorDigest       string                   `json:"predecessorDigest"`
	TargetDigest            string                   `json:"targetDigest"`
	ObservedDigest          string                   `json:"observedDigest"`
}

// ObserveOfflineHoldReadback uses only fresh GETs from the approved
// Kubernetes identity, never a cached PATCH response. It rejects an
// unavailable, replaced or changing Node rather than manufacturing a
// terminal receipt. The caller must also revalidate the journal topology.
func (o *KubernetesObserver) ObserveOfflineHoldReadback(ctx context.Context, topology productioncontrol.VerifiedParticipationTopology, s productioncontrol.Submission) (OfflineHoldReadback, error) {
	p, err := decodeHold(s)
	if err != nil {
		return OfflineHoldReadback{}, err
	}
	member, err := enrolledMember(topology.Policy, p.To.Node)
	if err != nil || p.Phase != ParticipationOffline || o == nil || o.core == nil ||
		topology.Policy.Schema != productioncontrol.BoundParticipationPolicySchema ||
		topology.Policy.Kubernetes == nil || o.binding != *topology.Policy.Kubernetes ||
		o.namespace != topology.Policy.Namespace || o.namespaceUID != topology.Policy.Kubernetes.NamespaceUID ||
		p.Topology != topology.Version || p.TopologyApproval != topology.ApprovalDigest ||
		member.NodeUID != p.To.UID || member.Group != p.To.Group {
		return OfflineHoldReadback{}, errors.New("offline hold readback does not match the approved cluster and enrollment")
	}
	if err := o.VerifyNamespace(ctx); err != nil {
		return OfflineHoldReadback{}, err
	}
	read := func() (OfflineHoldReadback, error) {
		n, err := o.core.Nodes().Get(ctx, member.Node, metav1.GetOptions{})
		if err != nil || !exactLiveObject(n, "", member.Node) || string(n.UID) != member.NodeUID ||
			n.Labels[participationGroup] != member.Group || n.Labels[participationRole] != "true" {
			return OfflineHoldReadback{}, errors.New("offline hold Node identity is unavailable or no longer enrolled")
		}
		r := OfflineHoldReadback{
			Node: describeObject(n), ExpectedResourceVersion: p.ResourceVersion,
			PredecessorDigest: s.Intent.CurrentDigest, TargetDigest: s.Intent.TargetDigest,
			ObservedDigest: HoldStateDigest(n.Name, string(n.UID), member.Group, n.Annotations),
		}
		switch {
		case n.ResourceVersion == p.ResourceVersion:
			if r.ObservedDigest != r.PredecessorDigest {
				return OfflineHoldReadback{}, errors.New("offline hold readback contradicts the reviewed resourceVersion")
			}
			r.State = OfflineHoldPredecessorVisible
		case r.ObservedDigest == r.TargetDigest:
			r.State = OfflineHoldTargetVisible
		default:
			r.State = OfflineHoldConditionChanged
		}
		return r, nil
	}
	first, err := read()
	if err != nil {
		return OfflineHoldReadback{}, err
	}
	second, err := read()
	if err != nil {
		return OfflineHoldReadback{}, err
	}
	if first != second {
		return OfflineHoldReadback{}, errors.New("offline hold Node changed during readback")
	}
	if err := o.VerifyNamespace(ctx); err != nil {
		return OfflineHoldReadback{}, err
	}
	return second, nil
}
