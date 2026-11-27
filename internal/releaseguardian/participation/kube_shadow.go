package participation

import (
	"context"
	"errors"
	"reflect"
	"time"

	"fugue/internal/productioncontrol"
	corev1 "k8s.io/api/core/v1"
)

type KubernetesShadow struct {
	Member  MemberObservation     `json:"member"`
	Readers *ReaderObservation    `json:"readers,omitempty"`
	Waiting string                `json:"waiting,omitempty"`
	Preview *ParticipationPreview `json:"preview,omitempty"`
}

// ShadowKubernetes compares the Kubernetes inputs around the reader
// observation. It cannot distinguish active and offline transport states
// using Kubernetes alone when the Node is Ready and has a serving Service.
// Neither a preview nor a waiting report can authorize a write.
func (o *KubernetesObserver) ShadowKubernetes(ctx context.Context, topology productioncontrol.ParticipationPolicy, node string, now func() time.Time) (KubernetesShadow, error) {
	if now == nil {
		return KubernetesShadow{}, errors.New("no participation observation clock")
	}
	member, err := enrolledMember(topology, node)
	if err != nil || o == nil || o.core == nil || o.apps == nil {
		return KubernetesShadow{}, errors.New("no exact participation member or Kubernetes observation client")
	}
	before, err := o.readMemberObjects(ctx, topology.Namespace, member)
	if err != nil {
		return KubernetesShadow{}, err
	}
	var readers *ReaderObservation
	if !requiresDNSObservation(before.report) {
		value, err := o.ObserveReaderRollouts(ctx, topology, node)
		if err != nil {
			return KubernetesShadow{}, err
		}
		readers = &value
	}
	after, err := o.readMemberObjects(ctx, topology.Namespace, member)
	if err != nil {
		return KubernetesShadow{}, err
	}
	if !reflect.DeepEqual(before.report, after.report) {
		return KubernetesShadow{}, errors.New("Kubernetes participation objects changed during the shadow read")
	}
	return evaluateKubernetesShadow(topology, node, after, readers, now())
}

func requiresDNSObservation(member MemberObservation) bool {
	return member.Readiness == corev1.ConditionTrue && member.Service != nil
}

func evaluateKubernetesShadow(policy productioncontrol.ParticipationPolicy, node string, current memberCapture, readers *ReaderObservation, now time.Time) (KubernetesShadow, error) {
	out := KubernetesShadow{Member: current.report, Readers: readers}
	if requiresDNSObservation(current.report) {
		out.Waiting = "independent_dns_and_heartbeat_observation_required"
		return out, nil
	}
	if readers == nil || readers.Baseline < 1 {
		return KubernetesShadow{}, errors.New("no verified live participation reader baseline")
	}
	snapshot := current.snapshot
	snapshot.ValidatedReaderBaseline = readers.Baseline
	if service := current.report.Service; service != nil {
		// In this branch Node NotReady is sufficient to choose offline.
		// Carry only Service identity to select the correct inventory check;
		// never treat missing DNS data as a negative DNS/heartbeat result.
		snapshot.Transport = &ParticipationTransport{Node: node, Group: readers.Group, ExternalIPs: []string{service.Address}}
	}
	preview, err := PreviewParticipation(policy, node, snapshot, now)
	if err != nil {
		return KubernetesShadow{}, err
	}
	out.Preview = &preview
	if preview.Action == ParticipationReviewRecovery {
		out.Waiting = "independent_front_worker_and_network_recovery_observation_required"
	}
	return out, nil
}
