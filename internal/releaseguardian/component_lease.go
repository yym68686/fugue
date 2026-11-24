package releaseguardian

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// ComponentLeaseDurationSeconds covers the longest ordinary executor (the
// 12-minute whole-group Edge A/B transition) and is shared by both writers.
const ComponentLeaseDurationSeconds = 900

func NewComponentLease(namespace, name, component, holder string, now time.Time) *coordinationv1.Lease {
	value := &coordinationv1.Lease{
		TypeMeta: metav1.TypeMeta{APIVersion: coordinationv1.SchemeGroupVersion.String(), Kind: "Lease"},
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: namespace,
			Labels: map[string]string{"app.kubernetes.io/managed-by": "fugue-declarative-release", "fugue.pro/component": component},
		},
	}
	SetComponentLease(value, holder, now)
	return value
}

func SetComponentLease(value *coordinationv1.Lease, holder string, now time.Time) {
	duration := int32(ComponentLeaseDurationSeconds)
	renewTime := metav1.NewMicroTime(now.UTC().Truncate(time.Microsecond))
	value.Spec = coordinationv1.LeaseSpec{
		HolderIdentity: &holder, LeaseDurationSeconds: &duration, RenewTime: &renewTime,
	}
	if holder != "" {
		acquireTime := renewTime
		value.Spec.AcquireTime = &acquireTime
	}
}

func ReadComponentLease(value *coordinationv1.Lease) (string, int64, time.Time, error) {
	if value == nil || value.GetUID() == "" || value.GetResourceVersion() == "" {
		return "", 0, time.Time{}, errors.New("component lease metadata is invalid")
	}
	if value.Spec.LeaseDurationSeconds == nil || *value.Spec.LeaseDurationSeconds != ComponentLeaseDurationSeconds {
		return "", 0, time.Time{}, errors.New("component lease spec is invalid")
	}
	if value.Spec.HolderIdentity == nil || *value.Spec.HolderIdentity == "" {
		return "", ComponentLeaseDurationSeconds, time.Time{}, nil
	}
	if value.Spec.RenewTime == nil || value.Spec.RenewTime.IsZero() {
		return "", 0, time.Time{}, errors.New("held component lease lacks a renewal timestamp")
	}
	return *value.Spec.HolderIdentity, ComponentLeaseDurationSeconds, value.Spec.RenewTime.Time, nil
}

// NodeWorkerComponent identifies the whole-group component that can replace
// the old slot's workloads. It is resolved from the configured Guardian
// targets' immutable stable monitor records, not from a pilot journal.
type NodeWorkerComponent struct {
	Namespace string
	Component string
	LeaseName string
}

func (store *AuthorityStore) FindNodeWorkerComponent(ctx context.Context, targets []TargetConfig, group string) (NodeWorkerComponent, error) {
	if !groupPattern.MatchString(group) {
		return NodeWorkerComponent{}, errors.New("node Worker component group is invalid")
	}
	read := &KubeStore{client: store.client}
	var out NodeWorkerComponent
	for _, target := range targets {
		if target.Namespace != store.namespace || target.Validate() != nil || target.MonitorComponent != target.Key.Component {
			return NodeWorkerComponent{}, errors.New("node Worker component target scope is invalid")
		}
		record, err := read.loadStableMonitorMap(ctx, target)
		if err != nil {
			return NodeWorkerComponent{}, fmt.Errorf("resolve Guardian component %s: %w", target.Key.Component, err)
		}
		plan, _, prepared, monitor, _, _, err := decodeStableRecord(record.Data)
		release, ok := releaseForComponent(plan, target.Key.Component)
		if err != nil || !ok || prepared.Component != target.Key.Component || monitor.Component != target.Key.Component {
			return NodeWorkerComponent{}, errors.Join(errors.New("node Worker component monitor is not bound to the configured component"), err)
		}
		if t := release.Transition; t == nil || t.Type != "edge-group-ab" || t.EdgeGroupAB == nil || t.EdgeGroupAB.GroupID != group {
			continue
		}
		if out.LeaseName != "" || !componentPattern.MatchString(release.Concurrency) || release.Workload.Namespace != store.namespace ||
			release.Delivery == nil || release.Delivery.Writer != "guardian" || release.Delivery.Group != target.Key.Group ||
			release.Delivery.DependencyService != target.DependencyService {
			return NodeWorkerComponent{}, errors.New("ambiguous or invalid whole-group Worker component lease")
		}
		out = NodeWorkerComponent{Namespace: store.namespace, Component: release.ComponentID, LeaseName: release.Concurrency}
	}
	if out.LeaseName == "" {
		return out, errors.New("no configured whole-group Worker component matches node Worker authority")
	}
	return out, nil
}

type NodeWorkerComponentHold struct {
	client     kubernetes.Interface
	binding    NodeWorkerComponent
	holder, rv string
	uid        types.UID
}

func AcquireNodeWorkerComponent(ctx context.Context, client kubernetes.Interface, binding NodeWorkerComponent, podUID, journalDigest string, now time.Time) (NodeWorkerComponentHold, error) {
	if client == nil || !componentPattern.MatchString(binding.Namespace) || !componentPattern.MatchString(binding.Component) ||
		!componentPattern.MatchString(binding.LeaseName) || podUID == "" || len(podUID) > 80 || strings.ContainsAny(podUID, "\r\n:\x00") ||
		!digestPattern.MatchString(journalDigest) || now.IsZero() {
		return NodeWorkerComponentHold{}, errors.New("node Worker component Lease identity is invalid")
	}
	// Unlike an ordinary Guardian holder, this holder is never eligible for
	// orphan-Pod takeover. Lease expiry is required after a lost claim reply.
	holder := "node-worker:" + podUID + ":" + journalDigest[7:]
	leases := client.CoordinationV1().Leases(binding.Namespace)
	object, err := leases.Get(ctx, binding.LeaseName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		object, err = leases.Create(ctx, NewComponentLease(binding.Namespace, binding.LeaseName, binding.Component, holder, now), metav1.CreateOptions{})
	} else if err == nil {
		current, duration, renewed, readErr := readBoundComponentLease(object, binding)
		if readErr != nil {
			return NodeWorkerComponentHold{}, readErr
		}
		if current != "" && now.Before(renewed.Add(time.Duration(duration)*time.Second)) {
			return NodeWorkerComponentHold{}, errors.New("whole-group Worker component Lease is already held")
		}
		object = object.DeepCopy()
		SetComponentLease(object, holder, now)
		object, err = leases.Update(ctx, object, metav1.UpdateOptions{})
	}
	if err != nil {
		return NodeWorkerComponentHold{}, fmt.Errorf("claim whole-group Worker component Lease: %w", err)
	}
	got, _, _, err := readBoundComponentLease(object, binding)
	if err != nil || got != holder {
		return NodeWorkerComponentHold{}, errors.Join(errors.New("whole-group Worker component Lease claim is unproven"), err)
	}
	return NodeWorkerComponentHold{client: client, binding: binding, holder: holder, uid: object.UID, rv: object.ResourceVersion}, nil
}

func (h NodeWorkerComponentHold) Release(ctx context.Context, now time.Time) error {
	leases := h.client.CoordinationV1().Leases(h.binding.Namespace)
	object, err := leases.Get(ctx, h.binding.LeaseName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	holder, _, _, err := readBoundComponentLease(object, h.binding)
	if err != nil || holder != h.holder || object.UID != h.uid || object.ResourceVersion != h.rv {
		return errors.Join(errors.New("whole-group Worker component Lease changed before release"), err)
	}
	object = object.DeepCopy()
	SetComponentLease(object, "", now)
	if _, err := leases.Update(ctx, object, metav1.UpdateOptions{}); err != nil {
		got, readErr := leases.Get(ctx, h.binding.LeaseName, metav1.GetOptions{})
		current, _, _, parseErr := readBoundComponentLease(got, h.binding)
		if readErr != nil || parseErr != nil || got.UID != h.uid || current != "" {
			return errors.Join(errors.New("whole-group Worker component Lease release is unproven"), err, readErr, parseErr)
		}
	}
	return nil
}

func readBoundComponentLease(value *coordinationv1.Lease, binding NodeWorkerComponent) (string, int64, time.Time, error) {
	if value == nil || value.Namespace != binding.Namespace || value.Name != binding.LeaseName || value.DeletionTimestamp != nil ||
		value.Labels["app.kubernetes.io/managed-by"] != "fugue-declarative-release" || value.Labels["fugue.pro/component"] != binding.Component {
		return "", 0, time.Time{}, errors.New("whole-group Worker component Lease binding is invalid")
	}
	return ReadComponentLease(value)
}
