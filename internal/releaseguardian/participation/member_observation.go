package participation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/netip"
	"reflect"
	"strconv"

	"fugue/internal/productioncontrol"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	frontManagerKey = "app.kubernetes.io/managed-by"
	frontManager    = "fugue-front-serving-transport"
	frontNodeKey    = "fugue.io/edge-node"
	frontPhase      = "transport.fugue.dev/phase"
	frontGeneration = "transport.fugue.dev/generation"
	frontDigest     = "transport.fugue.dev/digest"
	maxServices     = 256
	maxGroupNodes   = 1000
)

type ServingServiceObservation struct {
	Object     ObjectVersion `json:"object"`
	Generation int64         `json:"generation"`
	Digest     string        `json:"digest"`
	Address    string        `json:"address"`
}

type MemberObservation struct {
	Node        ObjectVersion              `json:"node"`
	Nodes       []ObjectVersion            `json:"groupNodes"`
	Retained    []ObjectVersion            `json:"retainedWorkloads,omitempty"`
	Service     *ServingServiceObservation `json:"service,omitempty"`
	Readiness   corev1.ConditionStatus     `json:"readiness"`
	DNSObserved bool                       `json:"dnsObserved"`
}

// ObserveMemberObjects checks the Kubernetes half of the phase inputs twice.
// A missing service means that the bounded manager-scoped search did not
// find one, not proof that DNS is withdrawn or that the member can rejoin.
func (o *KubernetesObserver) ObserveMemberObjects(ctx context.Context, policy productioncontrol.ParticipationPolicy, node string) (MemberObservation, error) {
	member, err := enrolledMember(policy, node)
	if err != nil || o == nil || o.core == nil || o.apps == nil {
		return MemberObservation{}, errors.New("no exact participation member or Kubernetes observation client")
	}
	first, err := o.readMemberObjects(ctx, policy.Namespace, member)
	if err != nil {
		return MemberObservation{}, err
	}
	second, err := o.readMemberObjects(ctx, policy.Namespace, member)
	if err != nil {
		return MemberObservation{}, err
	}
	if !reflect.DeepEqual(first, second) {
		return MemberObservation{}, errors.New("Kubernetes participation objects changed during observation")
	}
	return second, nil
}

func enrolledMember(policy productioncontrol.ParticipationPolicy, node string) (productioncontrol.ParticipationMember, error) {
	var result productioncontrol.ParticipationMember
	for _, m := range policy.Members {
		if m.Node == node {
			if result.Node != "" {
				return productioncontrol.ParticipationMember{}, errors.New("ambiguous participation member")
			}
			result = m
		}
	}
	if result.Node == "" || result.NodeUID == "" || result.Group == "" || policy.Namespace == "" {
		return productioncontrol.ParticipationMember{}, errors.New("participation member lacks a physical identity")
	}
	return result, nil
}

func (o *KubernetesObserver) readMemberObjects(ctx context.Context, ns string, m productioncontrol.ParticipationMember) (MemberObservation, error) {
	n, err := o.core.Nodes().Get(ctx, m.Node, metav1.GetOptions{})
	if err != nil || !exactLiveObject(n, "", m.Node) || string(n.UID) != m.NodeUID ||
		n.Labels[participationGroup] != m.Group || n.Labels[participationRole] != "true" {
		return MemberObservation{}, errors.New("physical participation Node changed or is unavailable")
	}
	status, err := observedReadiness(n)
	if err != nil {
		return MemberObservation{}, err
	}
	group, err := o.core.Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set{participationGroup: m.Group}.AsSelector().String(), Limit: maxGroupNodes + 1})
	if err != nil || group.Continue != "" || group.ResourceVersion == "" || len(group.Items) > maxGroupNodes {
		return MemberObservation{}, errors.New("group Node inventory is incomplete")
	}
	result := MemberObservation{Node: describeObject(n), Readiness: status}
	names, uids := make(map[string]bool), make(map[string]bool)
	for i := range group.Items {
		item := &group.Items[i]
		if item.Labels[participationRole] != "true" {
			continue
		}
		if !exactLiveObject(item, "", item.Name) || item.Labels[participationGroup] != m.Group || names[item.Name] || uids[string(item.UID)] {
			return MemberObservation{}, errors.New("group Node inventory has an ambiguous physical identity")
		}
		if item.Name == n.Name && (!reflect.DeepEqual(item.ObjectMeta, n.ObjectMeta) || !reflect.DeepEqual(item.Status, n.Status)) {
			return MemberObservation{}, errors.New("participation Node changed while listing its group")
		}
		names[item.Name], uids[string(item.UID)] = true, true
		result.Nodes = append(result.Nodes, describeObject(item))
	}
	if !names[n.Name] {
		return MemberObservation{}, errors.New("participation Node absent from its group inventory")
	}
	sortObjectVersions(result.Nodes)
	result.Service, err = o.readServingService(ctx, ns, m.Node)
	if err != nil {
		return MemberObservation{}, err
	}
	if result.Service == nil {
		names, uids := map[string]bool{}, map[string]bool{}
		for _, name := range []string{m.Front, m.Workers.A, m.Workers.B} {
			if name == "" || names[name] {
				return MemberObservation{}, errors.New("retained workload references are missing or duplicated")
			}
			workload, err := o.apps.DaemonSets(ns).Get(ctx, name, metav1.GetOptions{})
			if err != nil || !exactLiveObject(workload, ns, name) || uids[string(workload.UID)] ||
				workload.Spec.UpdateStrategy.Type != appsv1.OnDeleteDaemonSetStrategyType || workload.Spec.Template.Labels[participationGroup] != m.Group {
				return MemberObservation{}, errors.New("retained Front or Worker is not a live group-owned OnDelete DaemonSet")
			}
			names[name], uids[string(workload.UID)] = true, true
			result.Retained = append(result.Retained, describeObject(workload))
		}
	}
	return result, nil
}

func observedReadiness(n *corev1.Node) (corev1.ConditionStatus, error) {
	status := corev1.ConditionStatus("")
	for _, c := range n.Status.Conditions {
		if c.Type != corev1.NodeReady {
			continue
		}
		if status != "" || (c.Status != corev1.ConditionTrue && c.Status != corev1.ConditionFalse && c.Status != corev1.ConditionUnknown) {
			return "", errors.New("ambiguous physical Node readiness")
		}
		status = c.Status
	}
	if status == "" {
		return "", errors.New("missing physical Node readiness")
	}
	return status, nil
}

func (o *KubernetesObserver) readServingService(ctx context.Context, ns, node string) (*ServingServiceObservation, error) {
	services, err := o.core.Services(ns).List(ctx, metav1.ListOptions{LabelSelector: frontManagerKey + "=" + frontManager, Limit: maxServices + 1})
	if err != nil || services.Continue != "" || services.ResourceVersion == "" || len(services.Items) > maxServices {
		return nil, errors.New("serving Front Service inventory is incomplete")
	}
	var selected *corev1.Service
	for i := range services.Items {
		s := &services.Items[i]
		if s.Labels[frontManagerKey] != frontManager {
			return nil, errors.New("serving Front Service list returned an unexpected owner")
		}
		if s.Spec.Selector[frontNodeKey] == node && s.Annotations[frontPhase] == "serving" {
			if selected != nil || !exactLiveObject(s, ns, s.Name) {
				return nil, errors.New("serving Front Service is ambiguous or unavailable")
			}
			selected = s
		}
	}
	if selected == nil {
		return nil, nil
	}
	// Read JSON rather than round-tripping through typed structs: the old
	// projection hashes the exact JSON shape of the API's Service port fields.
	body, err := o.core.RESTClient().Get().Namespace(ns).Resource("services").Name(selected.Name).
		SetHeader("Accept", "application/json").Stream(ctx)
	if err != nil {
		return nil, errors.New("cannot read the exact serving Front Service")
	}
	defer body.Close()
	raw, err := io.ReadAll(io.LimitReader(body, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		return nil, errors.New("cannot read the exact serving Front Service")
	}
	obj, err := verifyServingService(raw, selected, node)
	if err != nil {
		return nil, err
	}
	return obj, nil
}

func verifyServingService(raw []byte, selected *corev1.Service, node string) (*ServingServiceObservation, error) {
	fail := errors.New("serving Front Service does not match its exact node, version and spec digest")
	var s corev1.Service
	if json.Unmarshal(raw, &s) != nil || !exactLiveObject(&s, selected.Namespace, selected.Name) ||
		describeObject(&s) != describeObject(selected) || s.Spec.Selector[frontNodeKey] != node ||
		s.Labels[frontManagerKey] != frontManager || s.Annotations[frontPhase] != "serving" ||
		s.Spec.Type != corev1.ServiceTypeClusterIP || s.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyLocal ||
		s.Spec.PublishNotReadyAddresses || len(s.Spec.ExternalIPs) != 1 || len(s.Spec.Ports) == 0 ||
		s.Annotations[frontDigest] != selected.Annotations[frontDigest] || s.Annotations[frontGeneration] != selected.Annotations[frontGeneration] {
		return nil, fail
	}
	ip, err := netip.ParseAddr(s.Spec.ExternalIPs[0])
	if err != nil || !ip.Is4() {
		return nil, fail
	}
	generation, err := strconv.ParseInt(s.Annotations[frontGeneration], 10, 64)
	if err != nil || generation < 1 || strconv.FormatInt(generation, 10) != s.Annotations[frontGeneration] {
		return nil, fail
	}
	var object struct {
		Spec map[string]any `json:"spec"`
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&object) != nil {
		return nil, fail
	}
	projection := map[string]any{"publishNotReadyAddresses": s.Spec.PublishNotReadyAddresses}
	for _, key := range []string{"type", "selector", "ports", "externalIPs", "externalTrafficPolicy"} {
		projection[key] = object.Spec[key]
	}
	canonical, err := json.Marshal(projection)
	if err != nil || digest(canonical) != s.Annotations[frontDigest] {
		return nil, fail
	}
	return &ServingServiceObservation{Object: describeObject(&s), Generation: generation,
		Digest: s.Annotations[frontDigest], Address: ip.String()}, nil
}
