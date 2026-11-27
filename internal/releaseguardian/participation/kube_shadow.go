package participation

import (
	"context"
	"errors"
	"reflect"
	"time"

	"fugue/internal/model"
	"fugue/internal/productioncontrol"
	corev1 "k8s.io/api/core/v1"
)

type KubernetesShadow struct {
	Member  MemberObservation                   `json:"member"`
	Readers *ReaderObservation                  `json:"readers,omitempty"`
	DNS     *model.EdgeParticipationObservation `json:"dns,omitempty"`
	Waiting string                              `json:"waiting,omitempty"`
	Preview *ParticipationPreview               `json:"preview,omitempty"`
}

// ShadowKubernetes compares the Kubernetes inputs around the reader
// observation. It cannot distinguish active and offline transport states
// using Kubernetes alone when the Node is Ready and has a serving Service.
// Neither a preview nor a waiting report can authorize a write.
func (o *KubernetesObserver) ShadowKubernetes(ctx context.Context, topology productioncontrol.ParticipationPolicy, node string, now func() time.Time) (KubernetesShadow, error) {
	return o.shadowKubernetes(ctx, topology, node, nil, now)
}

// ShadowWithDNS checks the narrow Fugue observation endpoint as well as the
// enrolled Kubernetes objects. These reads help explain a phase choice but
// do not supply independent provider, recovery, or cutover evidence.
func (o *KubernetesObserver) ShadowWithDNS(ctx context.Context, topology productioncontrol.ParticipationPolicy, node string, dns *DNSObserver, now func() time.Time) (KubernetesShadow, error) {
	if dns == nil || dns.origin != topology.Origin {
		return KubernetesShadow{}, errors.New("participation DNS observer is not bound to the approved origin")
	}
	return o.shadowKubernetes(ctx, topology, node, dns, now)
}

func (o *KubernetesObserver) shadowKubernetes(ctx context.Context, topology productioncontrol.ParticipationPolicy, node string, dns *DNSObserver, now func() time.Time) (KubernetesShadow, error) {
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
	needDNS := requiresDNSObservation(before.report)
	var first model.EdgeParticipationObservation
	if needDNS && dns != nil {
		first, err = dns.Observe(ctx, node)
		if err != nil {
			return KubernetesShadow{}, err
		}
		if _, err := bindDNSObservation(first, node, member.Group, before.report.Service.Address, now()); err != nil {
			return KubernetesShadow{}, err
		}
	}
	var readers *ReaderObservation
	if !needDNS || dns != nil {
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
	var fact *model.EdgeParticipationObservation
	if needDNS && dns != nil {
		last, err := dns.Observe(ctx, node)
		if err != nil {
			return KubernetesShadow{}, err
		}
		if last.EvaluatedAt.Before(first.EvaluatedAt) || !sameDNSObservation(first, last) {
			return KubernetesShadow{}, errors.New("participation DNS or heartbeat changed during the shadow read")
		}
		// Both observations must still be fresh after all network reads.
		at := now()
		if _, err := bindDNSObservation(first, node, member.Group, after.report.Service.Address, at); err != nil {
			return KubernetesShadow{}, err
		}
		fact = &last
		return evaluateKubernetesShadow(topology, node, after, readers, fact, at)
	}
	return evaluateKubernetesShadow(topology, node, after, readers, fact, now())
}

func requiresDNSObservation(member MemberObservation) bool {
	return member.Readiness == corev1.ConditionTrue && member.Service != nil
}

func evaluateKubernetesShadow(policy productioncontrol.ParticipationPolicy, node string, current memberCapture, readers *ReaderObservation, fact *model.EdgeParticipationObservation, now time.Time) (KubernetesShadow, error) {
	out := KubernetesShadow{Member: current.report, Readers: readers}
	if requiresDNSObservation(current.report) && fact == nil {
		out.Waiting = "independent_dns_and_heartbeat_observation_required"
		return out, nil
	}
	member, err := enrolledMember(policy, node)
	if err != nil {
		return KubernetesShadow{}, err
	}
	if readers == nil || readers.Baseline < 1 || readers.Namespace != policy.Namespace || readers.Group != member.Group {
		return KubernetesShadow{}, errors.New("no verified live participation reader baseline")
	}
	snapshot := current.snapshot
	snapshot.ValidatedReaderBaseline = readers.Baseline
	if fact != nil {
		if !requiresDNSObservation(current.report) {
			return KubernetesShadow{}, errors.New("unexpected DNS observation without a Ready serving member")
		}
		snapshot.Transport, err = bindDNSObservation(*fact, node, member.Group, current.report.Service.Address, now)
		if err != nil {
			return KubernetesShadow{}, err
		}
		out.Member.DNSObserved, out.DNS = true, fact
	} else if service := current.report.Service; service != nil {
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
