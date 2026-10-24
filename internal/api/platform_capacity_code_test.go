package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/edgegroupfront"
	"fugue/internal/platformproducer"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func testCapacityCode() platformproducer.CapacityCodeIdentity {
	return platformproducer.CapacityCodeIdentity{NodeUID: "node-uid", FrontPodUID: "pod-a", WorkerPodUID: "worker-uid", ServiceUID: "service-a", AuthorityUID: "authority-uid", AuthorityRecordDigest: "sha256:" + strings.Repeat("c", 64), WorkerSourceSHA: strings.Repeat("d", 40), WorkerImageDigest: "sha256:" + strings.Repeat("e", 64), ProxyImageDigest: "sha256:" + strings.Repeat("f", 64), FrontImageDigest: "sha256:" + strings.Repeat("a", 64), Slot: "a", Fence: 9, Bundle: "release-a"}
}

func TestCapacityCodeUsesSelectedPublicFrontAndItsExactCurrentWorker(t *testing.T) {
	for _, scenario := range []string{"valid", "old_worker_image", "wrong_source", "wrong_group", "foreign_target", "duplicate_target", "wrong_slot", "false_runtime_image", "unready_worker", "unready_front", "deleting_node", "no_authority_owner", "bad_authority_digest", "transition_in_progress", "front_old_authority", "front_target_loopback", "missing_endpoints", "truncated_inventory", "false_caddy_image", "wrong_caddy_port", "node_hostport", "foreign_node_hostport", "wrong_hostport", "bound_other_host"} {
		t.Run(scenario, func(t *testing.T) {
			node, service, front, endpoints := publicNetworkFixture()
			code := testCapacityCode()
			worker := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-a", Namespace: "system", UID: "worker-uid", ResourceVersion: "10", Labels: map[string]string{"fugue.io/edge-group-id": node.EdgeGroupID, "fugue.io/edge-slot": "a"}, Annotations: map[string]string{"fugue.pro/source-commit": code.WorkerSourceSHA}}, Spec: corev1.PodSpec{NodeName: node.ID, Containers: []corev1.Container{{Name: "edge", Image: "registry.example.test/worker@" + code.WorkerImageDigest, Ports: []corev1.ContainerPort{{Name: "https", ContainerPort: 18443, Protocol: corev1.ProtocolTCP}}}}}, Status: corev1.PodStatus{PodIP: "10.0.0.4", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "edge", ImageID: "registry.example.test/worker@" + code.WorkerImageDigest, Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}}}
			proxyImage := "registry.example.test/caddy@" + code.ProxyImageDigest
			worker.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: "health", ContainerPort: 7832, Protocol: corev1.ProtocolTCP}}
			worker.Spec.Containers = append(worker.Spec.Containers, corev1.Container{Name: "caddy", Image: proxyImage, Ports: []corev1.ContainerPort{{Name: "https-slot", ContainerPort: 18443, Protocol: corev1.ProtocolTCP}}})
			worker.Status.ContainerStatuses = append(worker.Status.ContainerStatuses, corev1.ContainerStatus{Name: "caddy", ImageID: proxyImage, Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}})
			state := capacityFrontState{Status: "ok", Slot: "a", Target: "10.0.0.4:18443", Fence: code.Fence, Bundle: code.Bundle, Source: code.WorkerSourceSHA, Image: code.WorkerImageDigest, Authority: edgegroupfront.ActivationAuthority}
			current := edgeEnrollmentAuthority{GroupID: node.EdgeGroupID, Record: code.AuthorityRecordDigest, Slot: code.Slot, Fence: code.Fence, Bundle: code.Bundle, Source: code.WorkerSourceSHA, Image: code.WorkerImageDigest}
			kubeNode := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: node.ID, UID: "node-uid"}}
			switch scenario {
			case "node_hostport", "foreign_node_hostport", "wrong_hostport", "bound_other_host":
				state.Target = node.PublicIPv4 + ":18443"
				kubeNode.Status.Addresses = []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: node.PublicIPv4}}
				worker.Spec.Containers[1].Ports[0].HostPort = 18443
				if scenario == "foreign_node_hostport" {
					kubeNode.Status.Addresses[0].Address = "203.0.113.11"
				}
				if scenario == "wrong_hostport" {
					worker.Spec.Containers[1].Ports[0].HostPort = 28443
				}
				if scenario == "bound_other_host" {
					worker.Spec.Containers[1].Ports[0].HostIP = "203.0.113.11"
				}
			case "false_caddy_image":
				worker.Status.ContainerStatuses[1].ImageID = "unverified"
			case "wrong_caddy_port":
				worker.Spec.Containers[1].Ports[0].ContainerPort = 28443
			case "old_worker_image":
				worker.Spec.Containers[0].Image = "registry.example.test/worker@sha256:" + strings.Repeat("f", 64)
			case "wrong_source":
				worker.Annotations["fugue.pro/source-commit"] = strings.Repeat("f", 40)
			case "wrong_group":
				worker.Labels["fugue.io/edge-group-id"] = "another-group"
			case "foreign_target":
				state.Target = "10.0.0.5:18443"
			case "wrong_slot":
				worker.Labels["fugue.io/edge-slot"] = "b"
			case "false_runtime_image":
				worker.Status.ContainerStatuses[0].ImageID = "registry.example.test/worker@sha256:" + strings.Repeat("f", 64)
			case "unready_worker":
				worker.Status.Conditions = nil
			case "unready_front":
				front.Status.Conditions = nil
			case "deleting_node":
				at := metav1.Now()
				kubeNode.DeletionTimestamp = &at
			case "bad_authority_digest":
				current.Record = "unverified"
			case "front_old_authority":
				state.Fence--
			case "front_target_loopback":
				state.Target = "127.0.0.1:18443"
			case "missing_endpoints":
				endpoints.Items = nil
			}
			kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Error("recovery code read mutated cluster", r.Method)
				}
				var response any
				switch r.URL.Path {
				case "/api/v1/nodes/edge-a":
					response = kubeNode
				case "/api/v1/namespaces/system/configmaps/fugue-current-authority-group-a":
					raw, _ := json.Marshal(current)
					cm := corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "authority-uid", Labels: map[string]string{"fugue.pro/authority-store": "true"}}, Data: map[string]string{"authority.json": string(raw)}}
					if scenario == "no_authority_owner" {
						cm.Labels = nil
					}
					response = cm
				case "/api/v1/namespaces/system/configmaps":
					journals := corev1.ConfigMapList{}
					if scenario == "transition_in_progress" {
						journals.Items = []corev1.ConfigMap{{}}
					}
					response = journals
				case "/api/v1/namespaces/system/services":
					response = corev1.ServiceList{Items: []corev1.Service{service}}
				case "/api/v1/namespaces/system/pods":
					// A same-node legacy Front/Worker pair is not public-serving authority.
					legacy := *front.DeepCopy()
					legacy.Name = "legacy-front"
					legacy.UID = "legacy-uid"
					legacy.Labels["app"] = "legacy"
					oldWorker := *worker.DeepCopy()
					oldWorker.Name = "legacy-worker"
					oldWorker.UID = "legacy-worker-uid"
					oldWorker.Status.PodIP = "10.0.0.8"
					oldWorker.Spec.Containers[1].Ports[0].HostPort = 0
					pods := corev1.PodList{Items: []corev1.Pod{legacy, oldWorker, front, worker}}
					if scenario == "duplicate_target" {
						pods.Items = append(pods.Items, worker)
					}
					if scenario == "truncated_inventory" {
						pods.Continue = "more"
					}
					response = pods
				case "/apis/discovery.k8s.io/v1/namespaces/system/endpointslices":
					response = endpoints
				case "/api/v1/namespaces/system/pods/front-a:7831/proxy/healthz":
					response = state
				default:
					t.Error("unexpected metadata access", r.URL.String())
					http.NotFound(w, r)
					return
				}
				json.NewEncoder(w).Encode(response)
			}))
			defer kube.Close()
			s := &Server{controlPlaneNamespace: "system", newClusterNodeClient: func() (*clusterNodeClient, error) {
				return &clusterNodeClient{client: kube.Client(), baseURL: kube.URL, bearerToken: "metadata-reader"}, nil
			}}
			got, err := s.readCapacityCode(context.Background(), node)
			if (err == nil) != (scenario == "valid" || scenario == "node_hostport") {
				t.Fatal(scenario, got, err)
			}
			if err == nil && got != code {
				t.Fatal("physical code identity drift", got, code)
			}
		})
	}
}

func TestCapacityCodeCancellationBoundsUnavailableNode(t *testing.T) {
	node, _, _, _ := publicNetworkFixture()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	s := &Server{controlPlaneNamespace: "system", newClusterNodeClient: func() (*clusterNodeClient, error) {
		return &clusterNodeClient{client: server.Client(), baseURL: server.URL}, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := s.readCapacityCode(ctx, node)
	if err == nil || time.Since(started) > time.Second {
		t.Fatal("unavailable metadata blocked recovery", err)
	}
}
