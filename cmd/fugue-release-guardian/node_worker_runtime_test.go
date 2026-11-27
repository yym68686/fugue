package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/releaseguardian"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
)

func nodeWorkerRuntimeFixture(t *testing.T) (*frontAuthorityActivator, *fake.Clientset, edgegroupfront.NodeWorkerJournal) {
	t.Helper()
	const namespace, group, node, ip = "fugue-system", "edge-group-fixture", "edge-pilot", "192.0.2.7"
	uid := func(i int) types.UID { return types.UID(fmt.Sprintf("00000000-0000-4000-8000-%012d", i)) }
	digest := func(x string) string { return "sha256:" + strings.Repeat(x, 64) }
	source := func(x string) string { return strings.Repeat(x, 40) }
	n := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: node, UID: uid(1), ResourceVersion: "1", Labels: map[string]string{"fugue.io/edge-group-id": group, "fugue.io/role.edge": "true"}},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
	}
	s := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "node-public", Namespace: namespace, UID: uid(2), ResourceVersion: "2",
			Labels: map[string]string{"app.kubernetes.io/managed-by": "fugue-front-serving-transport"}},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal,
			ExternalIPs: []string{ip}, Selector: map[string]string{"app": "front", "fugue.io/edge-node": node},
			Ports: []corev1.ServicePort{{Name: "http", Port: 80, TargetPort: intstr.FromInt(80), Protocol: corev1.ProtocolTCP},
				{Name: "https", Port: 443, TargetPort: intstr.FromInt(443), Protocol: corev1.ProtocolTCP}}},
	}
	now := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)
	localState := corev1.Volume{Name: "activation", VolumeSource: corev1.VolumeSource{
		HostPath: &corev1.HostPathVolumeSource{Path: "/var/lib/fugue/edge-owned/" + group + "/front"},
	}}
	mount := func(readOnly bool) corev1.VolumeMount {
		return corev1.VolumeMount{Name: "activation", MountPath: "/var/lib/fugue-edge-front", ReadOnly: readOnly}
	}
	status := func(name, pod string) corev1.ContainerStatus {
		return corev1.ContainerStatus{Name: name, Ready: true, ContainerID: "containerd://" + pod + "-" + name,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(now)}}}
	}
	base := func(i int, name string) corev1.Pod {
		return corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: uid(i), ResourceVersion: "3",
				Labels: map[string]string{"fugue.io/edge-group-id": group, "fugue.io/edge-node": node}},
			Spec: corev1.PodSpec{NodeName: node, Volumes: []corev1.Volume{localState}},
			Status: corev1.PodStatus{HostIP: ip, PodIP: fmt.Sprintf("10.0.0.%d", i), Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
		}
	}
	f := base(3, "selected-front")
	f.Labels["app"] = "front"
	f.Spec.Containers = []corev1.Container{{Name: "edge-front", VolumeMounts: []corev1.VolumeMount{
		mount(false), {Name: "journal", MountPath: "/var/run/fugue-worker-transaction", ReadOnly: true},
	}}}
	f.Spec.Volumes = append(f.Spec.Volumes, corev1.Volume{Name: "journal", VolumeSource: corev1.VolumeSource{
		ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: releaseguardian.NodeWorkerProjectionName(group, node)},
			Items: []corev1.KeyToPath{{Key: "journal.json", Path: "journal.json"}}, Optional: func() *bool { v := true; return &v }()},
	}})
	f.Status.ContainerStatuses = []corev1.ContainerStatus{status("edge-front", f.Name)}
	worker := func(i int, name, slot, code string, port int32) corev1.Pod {
		p := base(i, name)
		p.Labels["fugue.io/edge-slot"] = slot
		p.Annotations = map[string]string{"fugue.pro/source-commit": source(code)}
		image := "example.test/worker@" + digest(code)
		p.Spec.Containers = []corev1.Container{
			{Name: "edge", Image: image, VolumeMounts: []corev1.VolumeMount{mount(true)}},
			{Name: "caddy", Ports: []corev1.ContainerPort{{Name: "https-slot", HostPort: port, ContainerPort: port, Protocol: corev1.ProtocolTCP}}},
		}
		p.Status.ContainerStatuses = []corev1.ContainerStatus{status("edge", p.Name), status("caddy", p.Name)}
		p.Status.ContainerStatuses[0].ImageID = image
		return p
	}
	old, next := worker(4, "prior", "b", "b", 28443), worker(5, "next", "a", "a", 38443)
	endpoint := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "primary-1", Namespace: namespace, ResourceVersion: "4",
			Labels:          map[string]string{discoveryv1.LabelServiceName: s.Name},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(s, corev1.SchemeGroupVersion.WithKind("Service"))}},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports: []discoveryv1.EndpointPort{
			{Name: localPointer("http"), Port: localPointer(int32(80)), Protocol: localPointer(corev1.ProtocolTCP)},
			{Name: localPointer("https"), Port: localPointer(int32(443)), Protocol: localPointer(corev1.ProtocolTCP)},
		},
		Endpoints: []discoveryv1.Endpoint{{Addresses: []string{f.Status.PodIP}, NodeName: localPointer(node),
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: namespace, Name: f.Name, UID: f.UID},
			Conditions: discoveryv1.EndpointConditions{Ready: localPointer(true), Serving: localPointer(true), Terminating: localPointer(false)}}},
	}
	journal, err := (edgegroupfront.NodeWorkerJournal{
		Schema:    edgegroupfront.NodeWorkerJournalSchema,
		Recipient: edgegroupfront.NativeRecipient{PodUID: string(f.UID), Node: node, Group: group, Incarnation: digest("d")},
		NodeUID:   string(n.UID), ServiceUID: string(s.UID), PredecessorWorkerUID: string(old.UID), CandidateWorkerUID: string(next.UID),
		PredecessorProcess: edgegroupfront.NodeWorkerProcessDigest(old.Status.ContainerStatuses[0].ContainerID, old.Status.ContainerStatuses[1].ContainerID),
		CandidateProcess:   edgegroupfront.NodeWorkerProcessDigest(next.Status.ContainerStatuses[0].ContainerID, next.Status.ContainerStatuses[1].ContainerID),
		CandidateComponent: "node-worker-candidate", ReleaseDigest: digest("e"),
		Expected: edgegroupfront.ActivationState{
			Schema: edgegroupfront.ActivationStateSchemaV1, GroupID: group, Generation: 912, ActiveSlot: "b",
			BundleGeneration: "test.p20.r1", WorkerSourceCommit: source("b"), WorkerImageDigest: digest("b"),
			Authority: edgegroupfront.ActivationAuthority, Operation: edgegroupfront.ActivationOperationInit, Reason: "observed physical predecessor", UpdatedAt: now,
		},
		Candidate: edgegroupfront.ActivationRepairRelease{Slot: "a", BundleGeneration: "test.p21.r1", WorkerSourceCommit: source("a"), WorkerImageDigest: digest("a")},
		CreatedAt: now.Add(time.Second),
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	client := fake.NewSimpleClientset(n, s, &f, &old, &next, endpoint)
	return &frontAuthorityActivator{client: client, config: frontAuthorityConfig{Namespace: namespace, GroupID: group}}, client, journal
}

func TestBoundedDrainRejectsAnotherIngressButNotAnUnselectedFront(t *testing.T) {
	_, client, j := nodeWorkerRuntimeFixture(t)
	ctx := context.Background()
	core := client.CoreV1()
	front, err := core.Pods("fugue-system").Get(ctx, "selected-front", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	public, err := core.Services("fugue-system").Get(ctx, "node-public", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	probe := public.DeepCopy()
	probe.Name, probe.UID = "node-probe", "another-service"
	r := nodeWorkerRuntime{front: *front, service: *public, probe: *probe}
	legacy := front.DeepCopy()
	legacy.Name, legacy.UID, legacy.Labels["app"] = "legacy-front", "legacy", "old-front"
	legacy.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: "https", ContainerPort: 443}}
	services := []corev1.Service{*public, *probe}
	if err := requireExclusiveNodeWorkerIngress(services, []corev1.Pod{*front, *legacy}, r); err != nil {
		t.Fatalf("unselected old Front with no ingress blocked cutover of %s: %v", j.Recipient.Node, err)
	}
	legacy.Spec.Containers[0].Ports[0].HostPort = 443
	if err := requireExclusiveNodeWorkerIngress(services, []corev1.Pod{*front, *legacy}, r); err == nil {
		t.Fatal("accepted the legacy hostPort as an unowned path")
	}
	legacy.Spec.Containers[0].Ports[0].HostPort = 0
	extra := public.DeepCopy()
	extra.Name, extra.UID, extra.Spec.Selector = "other-ingress", "other-service", map[string]string{"app": "old-front"}
	extra.Spec.Ports = []corev1.ServicePort{{Name: "https-alt", Port: 16443, TargetPort: intstr.FromString("https")}}
	if err := requireExclusiveNodeWorkerIngress(append(services, *extra), []corev1.Pod{*front, *legacy}, r); err == nil {
		t.Fatal("accepted another Service exposing the old Front")
	}
}

func localPointer[T any](in T) *T { return &in }

func TestIndependentRuntimeRejectsDriftInFullPublicSelectorAndPhysicalOwner(t *testing.T) {
	ctx := context.Background()
	test := func(t *testing.T, mutate func(*fake.Clientset, nodeWorkerRuntime) error) {
		a, client, j := nodeWorkerRuntimeFixture(t)
		r, err := a.observeNodeWorkerRuntime(ctx, j, true, true)
		if err != nil || r.prior.target != "192.0.2.7:28443" || r.next.target != "192.0.2.7:38443" {
			t.Fatalf("exact one-node observation failed: %v", err)
		}
		if err := mutate(client, r); err != nil {
			t.Fatal(err)
		}
		if _, err := a.observeNodeWorkerRuntime(ctx, j, true, true); err == nil {
			t.Fatal("unsafe scope was accepted")
		}
	}
	t.Run("remote-selected-Pod", func(t *testing.T) {
		test(t, func(c *fake.Clientset, r nodeWorkerRuntime) error {
			extra := r.front.DeepCopy()
			extra.Name, extra.UID, extra.Spec.NodeName = "another-front", "foreign", "other-node"
			extra.DeletionTimestamp = localPointer(metav1.Now())
			_, err := c.CoreV1().Pods(extra.Namespace).Create(ctx, extra, metav1.CreateOptions{})
			return err
		})
	})
	t.Run("candidate process restart without replacing Pod", func(t *testing.T) {
		test(t, func(c *fake.Clientset, r nodeWorkerRuntime) error {
			restart := r.next.pod.DeepCopy()
			restart.Status.ContainerStatuses[1].ContainerID = "containerd://new-caddy"
			_, err := c.CoreV1().Pods(restart.Namespace).UpdateStatus(ctx, restart, metav1.UpdateOptions{})
			return err
		})
	})
	t.Run("unrelated service owns same public IP", func(t *testing.T) {
		test(t, func(c *fake.Clientset, r nodeWorkerRuntime) error {
			other := r.service.DeepCopy()
			other.Name, other.UID = "conflicting-service", "foreign"
			other.Labels = nil
			_, err := c.CoreV1().Services(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
			return err
		})
	})
	t.Run("candidate host port owned twice", func(t *testing.T) {
		test(t, func(c *fake.Clientset, r nodeWorkerRuntime) error {
			other := r.next.pod.DeepCopy()
			other.Name, other.UID = "other-port-owner", "foreign"
			_, err := c.CoreV1().Pods(other.Namespace).Create(ctx, other, metav1.CreateOptions{})
			return err
		})
	})
}

func TestFailedCandidateMayMakeFrontUnreadyButCannotPreventSafeRecovery(t *testing.T) {
	ctx := context.Background()
	a, client, j := nodeWorkerRuntimeFixture(t)
	r, err := a.observeNodeWorkerRuntime(ctx, j, true, true)
	if err != nil {
		t.Fatal(err)
	}
	ns := r.front.Namespace
	if err := client.CoreV1().Pods(ns).Delete(ctx, r.next.pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	f := r.front.DeepCopy()
	f.Status.ContainerStatuses[0].Ready = false
	f.Status.Conditions[0].Status = corev1.ConditionFalse
	if _, err := client.CoreV1().Pods(ns).UpdateStatus(ctx, f, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	s := r.slices[0].DeepCopy()
	s.Endpoints[0].Conditions.Ready, s.Endpoints[0].Conditions.Serving = localPointer(false), localPointer(false)
	if _, err := client.DiscoveryV1().EndpointSlices(ns).Update(ctx, s, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.observeNodeWorkerRuntime(ctx, j, false, true); err == nil {
		t.Fatal("reported readiness while Front and Endpoints were unready")
	}
	if next, err := a.observeNodeWorkerRuntime(ctx, j, false, false); err != nil || next.prior.target != "192.0.2.7:28443" {
		t.Fatalf("wrongly excluded exact original listener from recovery: %+v %v", next.prior.target, err)
	}
	s.Endpoints[0].Conditions.Terminating = localPointer(true)
	if _, err := client.DiscoveryV1().EndpointSlices(ns).Update(ctx, s, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.observeNodeWorkerRuntime(ctx, j, false, false); err == nil {
		t.Fatal("accepted an unrelated endpoint teardown as an unready recovery target")
	}
}

func TestNodeWorkerConnectionWitnessRejectsReconnectAndWrongUpperPort(t *testing.T) {
	now := time.Date(2026, 11, 25, 1, 0, 0, 0, time.UTC)
	original := releaseguardian.NodeWorkerConnection{FrontUID: "front", ID: "one", StartedAt: now, Slot: "b", Target: "192.0.2.7:28443"}
	fresh := releaseguardian.NodeWorkerConnection{FrontUID: "front", ID: "two", StartedAt: now.Add(5 * time.Second), Slot: "a", Target: "192.0.2.7:38443"}
	encode := func(facts ...releaseguardian.NodeWorkerConnection) []byte {
		items := make([]map[string]any, 0, len(facts))
		for _, x := range facts {
			items = append(items, map[string]any{"id": x.ID, "started_at": x.StartedAt, "protocol": "https", "slot": x.Slot, "target": x.Target})
		}
		b, err := json.Marshal(map[string]any{"count": len(items), "active": items})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	refs := []releaseguardian.NodeWorkerConnection{original, fresh}
	matches := func(raw []byte) bool {
		view, err := decodeNodeWorkerInventory(raw)
		return err == nil && nodeWorkerConnectionInventoryMatches(view, "front", refs)
	}
	if !matches(encode(original, fresh)) {
		t.Fatal("exact original connection and new listener were not accepted")
	}
	reconnected, wrongWorker := original, fresh
	reconnected.StartedAt = fresh.StartedAt
	wrongWorker.Target = "192.0.2.7:18443"
	for _, b := range [][]byte{encode(reconnected, fresh), encode(original, wrongWorker), encode(original, fresh, fresh)} {
		if matches(b) {
			t.Fatal("false held-connection or physical listener proof was accepted")
		}
	}
}
