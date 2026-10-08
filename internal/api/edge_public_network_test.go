package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/frontnetwork"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/store"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func publicNetworkFixture() (model.EdgeNode, corev1.Service, corev1.Pod, discoveryv1.EndpointSliceList) {
	node := model.EdgeNode{ID: "edge-a", EdgeGroupID: "group-a", PublicIPv4: "203.0.113.10"}
	service := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "public", Namespace: "system", UID: "service-a", ResourceVersion: "7", Labels: map[string]string{"app.kubernetes.io/managed-by": "fugue-front-serving-transport"}, Annotations: map[string]string{"transport.fugue.dev/generation": "2", "transport.fugue.dev/phase": "serving"}}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, ExternalIPs: []string{node.PublicIPv4}, ExternalTrafficPolicy: corev1.ServiceExternalTrafficPolicyLocal, Selector: map[string]string{"app": "public-front"}, Ports: []corev1.ServicePort{{Name: "https", Protocol: corev1.ProtocolTCP, Port: 443, TargetPort: intstr.FromInt32(443)}}}}
	raw, _ := json.Marshal(map[string]any{"type": service.Spec.Type, "selector": service.Spec.Selector, "ports": service.Spec.Ports, "externalIPs": service.Spec.ExternalIPs, "externalTrafficPolicy": service.Spec.ExternalTrafficPolicy, "publishNotReadyAddresses": false})
	var canonical map[string]any
	json.Unmarshal(raw, &canonical)
	service.Annotations["transport.fugue.dev/digest"], _ = platformconfig.Digest(canonical)
	image := "registry.example.test/front@sha256:" + strings.Repeat("a", 64)
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "front-a", Namespace: "system", UID: "pod-a", ResourceVersion: "8", Labels: map[string]string{"app": "public-front", "fugue.io/edge-group-id": node.EdgeGroupID}}, Spec: corev1.PodSpec{NodeName: node.ID, Containers: []corev1.Container{{Name: "edge-front", Image: image, Command: []string{"/usr/local/bin/fugue-edge-front"}, Ports: []corev1.ContainerPort{{Name: "health", ContainerPort: 7831, Protocol: corev1.ProtocolTCP}}}}}, Status: corev1.PodStatus{PodIP: "10.0.0.3", PodIPs: []corev1.PodIP{{IP: "10.0.0.3"}}, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "edge-front", ImageID: image, Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
	name, protocol, port, ready := "https", corev1.ProtocolTCP, int32(443), true
	slices := discoveryv1.EndpointSliceList{Items: []discoveryv1.EndpointSlice{{ObjectMeta: metav1.ObjectMeta{Name: "slice-a", Namespace: "system", UID: "slice-a", ResourceVersion: "9", Labels: map[string]string{discoveryv1.LabelServiceName: service.Name}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: service.Name, UID: service.UID, Controller: &ready}}}, AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Name: &name, Protocol: &protocol, Port: &port}}, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{pod.Status.PodIP}, NodeName: &node.ID, TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: pod.Name, Namespace: pod.Namespace, UID: pod.UID}, Conditions: discoveryv1.EndpointConditions{Ready: &ready}}}}}}
	return node, service, pod, slices
}

func TestPublicFrontObservationSelectsRuntimeTransportNotLegacyName(t *testing.T) {
	for _, scenario := range []string{"valid", "service_changed", "pod_changed", "endpoints_changed", "foreign_peer", "duplicate_public_owner", "unready"} {
		t.Run(scenario, func(t *testing.T) {
			node, service, pod, slices := publicNetworkFixture()
			query := frontnetwork.Request{Schema: frontnetwork.Schema, Nonce: strings.Repeat("b", 32), EdgeID: node.ID, GroupID: node.EdgeGroupID, Slot: "b", RemoteAddr: "198.51.100.1:40000"}
			serviceReads, sliceReads := 0, 0
			kube := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Header.Get("Authorization") != "Bearer metadata-reader" {
					t.Error("missing Kubernetes credential")
				}
				var response any
				switch request.URL.Path {
				case "/api/v1/namespaces/system/services":
					serviceReads++
					value := *service.DeepCopy()
					if scenario == "service_changed" && serviceReads > 1 {
						value.ResourceVersion = "99"
					}
					items := []corev1.Service{value}
					if scenario == "duplicate_public_owner" {
						items = append(items, value)
					}
					response = corev1.ServiceList{Items: items}
				case "/api/v1/namespaces/system/pods":
					value := *pod.DeepCopy()
					if scenario == "unready" {
						value.Status.Conditions = nil
					}
					legacy := *pod.DeepCopy()
					legacy.Name = "old-front"
					legacy.UID = "other"
					legacy.Labels["app"] = "old-front"
					response = corev1.PodList{Items: []corev1.Pod{legacy, value}}
				case "/api/v1/namespaces/system/pods/front-a":
					value := *pod.DeepCopy()
					if scenario == "pod_changed" {
						value.UID = "replacement"
					}
					response = value
				case "/apis/discovery.k8s.io/v1/namespaces/system/endpointslices":
					sliceReads++
					value := *slices.DeepCopy()
					if scenario == "endpoints_changed" && sliceReads > 1 {
						value.Items[0].ResourceVersion = "99"
					}
					response = value
				case "/api/v1/namespaces/system/pods/front-a:7831/proxy/edge/tcp-connections":
					remote := query.RemoteAddr
					if scenario == "foreign_peer" {
						remote = "198.51.100.2:40000"
					}
					response = frontnetwork.LiveConnections{Count: 1, Active: []frontnetwork.LiveConnection{{ID: "connection-a", Protocol: "https", Slot: query.Slot, DownstreamRemote: remote, StartedAt: time.Now().Add(-time.Minute), ProxyProtocol: true, TCPInfo: map[string]json.RawMessage{"tcp_info_available": json.RawMessage("false")}}}}
				default:
					t.Error("unexpected read", request.URL.String())
					http.NotFound(writer, request)
					return
				}
				json.NewEncoder(writer).Encode(response)
			}))
			defer kube.Close()
			server := &Server{controlPlaneNamespace: "system", newClusterNodeClient: func() (*clusterNodeClient, error) {
				return &clusterNodeClient{client: kube.Client(), baseURL: kube.URL, bearerToken: "metadata-reader"}, nil
			}}
			response, err := server.readPublicFrontNetwork(context.Background(), node, query)
			if (err == nil) != (scenario == "valid") {
				t.Fatal(scenario, response, err)
			}
			if err == nil && (response.Sample.Backend == nil || response.Sample.Backend.PodUID != "pod-a" || response.Sample.RTTMS != nil) {
				t.Fatal("transport witness missing or RTT fabricated", response)
			}
		})
	}
}

func TestPublicFrontObservationBudgetIsBounded(t *testing.T) {
	server := &Server{}
	now := time.Now()
	if !server.allowPublicNetworkObservation("edge-a", now) || server.allowPublicNetworkObservation("edge-a", now.Add(time.Millisecond)) || !server.allowPublicNetworkObservation("edge-a", now.Add(time.Second)) {
		t.Fatal("budget not enforced")
	}
	for index := 0; index < 256; index++ {
		server.frontNetworkObservationLast[string(rune(index))] = now
	}
	if server.allowPublicNetworkObservation("extra", now) || !server.allowPublicNetworkObservation("extra", now.Add(2*time.Minute)) {
		t.Fatal("cardinality bound or expiry failed")
	}
}

func TestPublicFrontObservationRejectsForeignNodeCredential(t *testing.T) {
	state := store.New(t.TempDir() + "/state.json")
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	_, token, err := state.CreateEdgeNodeToken(model.EdgeNode{ID: "edge-a", EdgeGroupID: "group-a"})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(state, auth.New(state, "admin"), nil, ServerConfig{})
	query := frontnetwork.Request{Schema: frontnetwork.Schema, Nonce: strings.Repeat("b", 32), EdgeID: "edge-b", GroupID: "group-a", Slot: "b", RemoteAddr: "198.51.100.1:40000"}
	response := performJSONRequest(t, server, http.MethodPost, "/v1/edge/network-observation", token, query)
	if response.Code != http.StatusForbidden {
		t.Fatal(response.Code, response.Body.String())
	}
	response = performJSONRequest(t, server, http.MethodPost, "/v1/edge/network-observation", "admin", query)
	if response.Code != http.StatusForbidden {
		t.Fatal("unscoped credential accepted", response.Code)
	}
}
