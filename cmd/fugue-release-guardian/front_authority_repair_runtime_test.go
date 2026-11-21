package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"testing"

	"fugue/internal/model/edgecohort"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

type repairRuntimeExecFixture func(context.Context, string, string, string, ...string) ([]byte, error)

func (exec repairRuntimeExecFixture) Exec(ctx context.Context, namespace, pod, container string, command ...string) ([]byte, error) {
	return exec(ctx, namespace, pod, container, command...)
}

func TestFrontRepairRuntimeBindsRealCohortAndRejectsSamePodProcessReplacement(t *testing.T) {
	const namespace = "fixture-system"
	ctx := context.Background()
	before, cohort, fixture := partialFrontRollbackFixture()
	var objects []runtime.Object
	var podsByName = map[string]frontRepairWitness{}
	var endpoints = map[string]frontRepairWitness{}
	container := func(name string, worker bool) corev1.Container {
		c := corev1.Container{Name: name, Image: "example.test/worker@" + before.CurrentWorkerImageDigest}
		if name != "caddy" {
			c.VolumeMounts = []corev1.VolumeMount{{Name: "activation", MountPath: path.Dir(frontActivationStatePath), ReadOnly: !worker}}
		}
		if name == "edge-front" {
			c.Ports = []corev1.ContainerPort{{HostPort: 443}}
		}
		return c
	}
	for i, row := range fixture {
		identity := row.Node
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: identity.Name, UID: types.UID(identity.UID), ResourceVersion: "2",
			Labels: map[string]string{"fugue.io/edge-group-id": before.GroupID, "fugue.io/role.edge": "true", "fugue.io/edge-workload": "dynamic"}},
			Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}}
		if identity.Admitted {
			node.Labels[edgecohort.PhaseLabel] = edgecohort.Admitted
		}
		newPod := func(role, uid, ip string, containers ...corev1.Container) *corev1.Pod {
			owner := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: identity.Name + "-" + role, UID: types.UID(uid),
				Labels: map[string]string{"fugue.io/edge-group-id": before.GroupID}, Annotations: map[string]string{"fugue.pro/source-commit": before.CurrentWorkerSourceSHA}},
				Spec: corev1.PodSpec{NodeName: identity.Name, Containers: containers, Volumes: []corev1.Volume{{Name: "activation", VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{Path: "/var/lib/fixture-activation"}}}}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: ip, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
			ids := map[string]string{"edge-front": row.FrontProcessID, "edge": row.TargetWorkerProcessID, "caddy": row.TargetCaddyProcessID}
			for _, c := range containers {
				owner.Status.ContainerStatuses = append(owner.Status.ContainerStatuses, corev1.ContainerStatus{Name: c.Name, ImageID: c.Image, ContainerID: ids[c.Name], Ready: true,
					State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}})
			}
			return owner
		}
		front := newPod("front", row.FrontPodUID, fmt.Sprintf("192.0.2.%d", 10+i), container("edge-front", false))
		worker := newPod("worker", row.TargetWorkerPodUID, fmt.Sprintf("192.0.2.%d", 20+i), container("edge", true), container("caddy", true))
		worker.Labels["fugue.io/edge-slot"] = string(before.CurrentWorkerSlot)
		podsByName[worker.Name] = row
		endpoints[repairHealthEndpoint(front.Status.PodIP, frontHealthPort, "/readyz")] = row
		endpoints[repairHealthEndpoint(worker.Status.PodIP, workerHealthPort, "/healthz")] = row
		objects = append(objects, node, front, worker)
	}
	client := fake.NewSimpleClientset(objects...)
	activator, err := newFrontAuthorityActivator(client, repairRuntimeExecFixture(func(_ context.Context, ns, name, container string, args ...string) ([]byte, error) {
		row, exists := podsByName[name]
		if ns != namespace || !exists || container != "edge" || strings.Join(args, " ") != "cat "+frontActivationStatePath {
			return nil, fmt.Errorf("unexpected or mutating exec for %s", name)
		}
		return json.Marshal(row.Activation)
	}), frontAuthorityConfig{Namespace: namespace, GroupID: before.GroupID, ExpectedNodes: 2}, "fixture-holder")
	if err != nil {
		t.Fatal(err)
	}
	reader := newFrontRepairRuntime(activator)
	reader.read = func(_ context.Context, endpoint string, value any) error {
		row, exists := endpoints[endpoint]
		if !exists {
			return fmt.Errorf("unexpected probe %s", endpoint)
		}
		switch result := value.(type) {
		case *observedFront:
			*result = row.LiveFront
		case *authorityWorkerHealth:
			*result = row.TargetWorkerHealth
		default:
			return fmt.Errorf("unexpected health destination %T", value)
		}
		return nil
	}
	if _, err := reader.observe(ctx, before, cohort[1:]); err == nil {
		t.Fatal("allowed enrollment subset excluding the admitted member")
	}
	result, err := reader.observe(ctx, before, cohort)
	if err != nil || len(result) != len(cohort) {
		t.Fatalf("could not read the whole serving cohort: %v", err)
	}
	witnesses := make([]frontRepairWitness, 0, len(result))
	for _, member := range result {
		witnesses = append(witnesses, member.frontRepairWitness)
	}
	if _, err := planPartialFrontRollbackRepair(before, "pointer-uid", "101", cohort, witnesses); err != nil {
		t.Fatalf("trusted reads do not bind an exact partial rollback: %v", err)
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "get" && action.GetVerb() != "list" {
			t.Fatalf("observer performed a mutating Kubernetes action: %v", action)
		}
	}
	kept := result[0]
	pod := kept.worker.DeepCopy()
	pod.Status.ContainerStatuses[1].ContainerID = "replacement-caddy-process"
	if _, err := client.CoreV1().Pods(namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := reader.requireSameMember(ctx, kept); err == nil {
		t.Fatal("same Pod UID with changed Caddy process was accepted")
	}
}

func TestFrontRepairHealthReadDoesNotFollowRedirects(t *testing.T) {
	read := newFrontRepairRuntime(nil).read
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/healthz", http.StatusFound)
			return
		}
		_, _ = w.Write([]byte(`{"status":"ok","edge_group_id":"fixture","stale_cache":false,"another_worker_health_field":1337}`))
	}))
	defer server.Close()
	var result authorityWorkerHealth
	if err := read(context.Background(), server.URL+"/healthz", &result); err != nil || result.StaleCache == nil || *result.StaleCache {
		t.Fatalf("current Worker health schema was not readable: %v", err)
	}
	if err := read(context.Background(), server.URL+"/redirect", &result); err == nil {
		t.Fatal("health proof followed an untrusted redirect")
	}
}
