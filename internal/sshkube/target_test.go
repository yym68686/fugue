package sshkube

import (
	"context"
	"testing"

	"fugue/internal/managedssh"
	"fugue/internal/model"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
)

func TestResolveTargetUsesServingPodAndExplicitContainer(test *testing.T) {
	ready := true
	spec := model.ManagedSSHIntentSpec{Enabled: true, IdentityID: "identity", Container: "workspace", User: "app", WorkingDirectory: "/workspace", MaxConnections: 8, MaxConnectionsPerIP: 4, MaxChannels: 4}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "workspace", Namespace: "workloads", UID: "service-uid", Labels: map[string]string{"fugue.pro/app-id": "app", "fugue.pro/tenant-id": "tenant"}}, Spec: corev1.ServiceSpec{Selector: map[string]string{"fugue.pro/app-id": "app", "release": "active"}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "workspace-a", Namespace: "workloads", UID: "pod-a", Labels: map[string]string{"fugue.pro/app-id": "app", "fugue.pro/tenant-id": "tenant", "release": "active"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "workspace"}, {Name: "storage-sidecar"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "workspace", Ready: true, ContainerID: "container-a", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}, {Name: "storage-sidecar", Ready: true, ContainerID: "sidecar-a", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	slice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "workspace", Namespace: "workloads", Labels: map[string]string{"kubernetes.io/service-name": "workspace"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: service.Name, UID: service.UID}}}, AddressType: discoveryv1.AddressTypeIPv4, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"192.0.2.10"}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}, TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}}}}
	resolve := func(test *testing.T, service *corev1.Service, pod *corev1.Pod, slice *discoveryv1.EndpointSlice, extra ...runtime.Object) (managedssh.Target, error) {
		test.Helper()
		objects := append([]runtime.Object{service, pod, slice}, extra...)
		client := fake.NewClientset(objects...)
		result, err := ResolveTarget(context.Background(), client, "tenant", "app", "workloads", "workspace", spec, managedssh.Target{})
		for _, action := range client.Actions() {
			if action.GetVerb() != "get" && action.GetVerb() != "list" {
				test.Fatal("target resolution changed application resources", action)
			}
		}
		return result, err
	}
	target, err := resolve(test, service, pod, slice)
	if err != nil || target.PodUID != string(pod.UID) || target.Container != "workspace" || target.ContainerID != "container-a" {
		test.Fatal("selected wrong execution target", target, err)
	}
	for name, mutate := range map[string]func(*corev1.Service, *corev1.Pod, *discoveryv1.EndpointSlice){
		"stale endpoint UID": func(service *corev1.Service, pod *corev1.Pod, slice *discoveryv1.EndpointSlice) {
			pod.UID = "replacement"
		},
		"unselected rollout": func(service *corev1.Service, pod *corev1.Pod, slice *discoveryv1.EndpointSlice) {
			pod.Labels["release"] = "candidate"
		},
		"cross tenant pod": func(service *corev1.Service, pod *corev1.Pod, slice *discoveryv1.EndpointSlice) {
			pod.Labels["fugue.pro/tenant-id"] = "other"
		},
		"cross tenant service": func(service *corev1.Service, pod *corev1.Pod, slice *discoveryv1.EndpointSlice) {
			service.Labels["fugue.pro/tenant-id"] = "other"
		},
		"stale endpoint owner": func(service *corev1.Service, pod *corev1.Pod, slice *discoveryv1.EndpointSlice) {
			service.UID = "replacement"
		},
		"not ready": func(service *corev1.Service, pod *corev1.Pod, slice *discoveryv1.EndpointSlice) {
			slice.Endpoints[0].Conditions.Ready = nil
		},
		"terminating": func(service *corev1.Service, pod *corev1.Pod, slice *discoveryv1.EndpointSlice) {
			slice.Endpoints[0].Conditions.Terminating = &ready
		},
		"missing container": func(service *corev1.Service, pod *corev1.Pod, slice *discoveryv1.EndpointSlice) {
			pod.Spec.Containers = pod.Spec.Containers[1:]
		},
	} {
		test.Run(name, func(test *testing.T) {
			candidateService, candidatePod, candidateSlice := service.DeepCopy(), pod.DeepCopy(), slice.DeepCopy()
			mutate(candidateService, candidatePod, candidateSlice)
			if _, err := resolve(test, candidateService, candidatePod, candidateSlice); err == nil {
				test.Fatal("unverified execution target selected")
			}
		})
	}
}
