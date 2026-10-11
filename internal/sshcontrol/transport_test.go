package sshcontrol

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
)

func TestRetirementRequiresPublicServiceSelectingExactAcknowledgedPod(test *testing.T) {
	image := "registry.example/front@sha256:" + strings.Repeat("a", 64)
	edge := EdgeConsumer{EdgeID: "edge-a", EdgeGroupID: "group-a", Namespace: "platform", PodLabels: map[string]string{"slot": "front-a"}, PublicAddress: "192.0.2.10", PublicService: "public-ssh", ServiceUID: "service-a", Image: image}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "front-a", Namespace: edge.Namespace, UID: "pod-a"}, Spec: corev1.PodSpec{NodeName: edge.EdgeID, Containers: []corev1.Container{{Image: image}}}, Status: corev1.PodStatus{PodIP: "10.1.1.2", ContainerStatuses: []corev1.ContainerStatus{{ImageID: image, Ready: true}}}}
	local := corev1.ServiceInternalTrafficPolicyLocal
	service := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: edge.PublicService, Namespace: edge.Namespace, UID: "service-a", Labels: map[string]string{"app.kubernetes.io/managed-by": "fugue-managed-ssh-transport"}, Annotations: map[string]string{"ssh.fugue.dev/transport-phase": "serving"}}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Selector: edge.PodLabels, ExternalIPs: []string{edge.PublicAddress}, ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal, InternalTrafficPolicy: &local, Ports: []corev1.ServicePort{{Name: "ssh-24000", Protocol: corev1.ProtocolTCP, Port: 24000, TargetPort: intstr.FromInt32(24000)}}}}
	ready, controller := true, true
	port, portName, protocol := int32(24000), "ssh-24000", corev1.ProtocolTCP
	slice := discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{Name: "selected", Namespace: edge.Namespace, Labels: map[string]string{"kubernetes.io/service-name": service.Name}, OwnerReferences: []metav1.OwnerReference{{Kind: "Service", UID: service.UID, Controller: &controller}}}, AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Name: &portName, Protocol: &protocol, Port: &port}}, Endpoints: []discoveryv1.Endpoint{{NodeName: &edge.EdgeID, Addresses: []string{pod.Status.PodIP}, TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}}}}
	for _, scenario := range []struct {
		name   string
		mutate func(*corev1.Service, *discoveryv1.EndpointSlice)
	}{
		{"valid", func(*corev1.Service, *discoveryv1.EndpointSlice) {}},
		{"private stage", func(service *corev1.Service, slice *discoveryv1.EndpointSlice) { service.Spec.ExternalIPs = nil }},
		{"compensated", func(service *corev1.Service, slice *discoveryv1.EndpointSlice) {
			service.Annotations["ssh.fugue.dev/transport-phase"] = "compensated"
		}},
		{"foreign service", func(service *corev1.Service, slice *discoveryv1.EndpointSlice) { service.UID = "replacement" }},
		{"different selector", func(service *corev1.Service, slice *discoveryv1.EndpointSlice) {
			service.Spec.Selector = map[string]string{"slot": "another"}
		}},
		{"missing binding", func(service *corev1.Service, slice *discoveryv1.EndpointSlice) { service.Spec.Ports[0].Port = 24001 }},
		{"wrong target", func(service *corev1.Service, slice *discoveryv1.EndpointSlice) {
			service.Spec.Ports[0].TargetPort = intstr.FromInt32(22)
		}},
		{"foreign slice", func(service *corev1.Service, slice *discoveryv1.EndpointSlice) {
			slice.OwnerReferences[0].UID = "other-service"
		}},
		{"previous Pod", func(service *corev1.Service, slice *discoveryv1.EndpointSlice) {
			slice.Endpoints[0].TargetRef.UID = "old-pod"
		}},
		{"wrong node", func(service *corev1.Service, slice *discoveryv1.EndpointSlice) {
			value := "edge-b"
			slice.Endpoints[0].NodeName = &value
		}},
		{"unready", func(service *corev1.Service, slice *discoveryv1.EndpointSlice) {
			value := false
			slice.Endpoints[0].Conditions.Ready = &value
		}},
		{"ambiguous", func(service *corev1.Service, slice *discoveryv1.EndpointSlice) {
			slice.Endpoints = append(slice.Endpoints, slice.Endpoints[0])
		}},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			candidate, endpoints := service.DeepCopy(), slice.DeepCopy()
			scenario.mutate(candidate, endpoints)
			runtime := &KubernetesRuntime{client: fake.NewClientset(candidate, endpoints)}
			err := runtime.verifyEdgeTransport(context.Background(), edge, pod, 24000)
			if (err == nil) != (scenario.name == "valid") {
				test.Fatal("public serving evidence incorrectly accepted or rejected", err)
			}
		})
	}
	runtime := &KubernetesRuntime{client: fake.NewClientset(service.DeepCopy(), slice.DeepCopy())}
	for _, wrong := range []EdgeConsumer{{}, {PublicService: edge.PublicService}, {PublicService: edge.PublicService, ServiceUID: edge.ServiceUID, Image: "wrong"}} {
		if runtime.verifyEdgeTransport(context.Background(), wrong, pod, 24000) == nil {
			test.Fatal("Pod-network retirement accepted without complete transport declaration")
		}
	}
}
