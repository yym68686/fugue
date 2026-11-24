package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestParseWorkerFrontBindings(t *testing.T) {
	valid := `[{"node":"node-a","service_uid":"12345678-1234-4234-8234-123456789abc"}]`
	got, err := ParseWorkerFrontBindings(valid)
	if err != nil || got["node-a"].uid == "" {
		t.Fatal(got, err)
	}
	for _, raw := range []string{
		`[{"node":"node-a","service_uid":"12345678-1234-4234-8234-123456789abc"},{"node":"node-a","service_uid":"22345678-1234-4234-8234-123456789abc"}]`,
		`[{"node":"node-a","service_uid":"12345678-1234-4234-8234-123456789abc"},{"node":"node-b","service_uid":"12345678-1234-4234-8234-123456789abc"}]`,
		`[{"node":"node-a","service_uid":"12345678-1234-4234-8234-123456789abc","operator":"self-review"}]`,
		`null`,
	} {
		if _, err := ParseWorkerFrontBindings(raw); err == nil {
			t.Fatalf("accepted invalid binding: %s", raw)
		}
	}
	if _, err := ParseWorkerFrontBindings(""); err != nil {
		t.Fatal(err)
	}
}

func workerFrontFixture() (workerFrontHealth, corev1.Pod, corev1.Pod, platformcontrol.PlatformComponentIdentityClaims) {
	const uid = "12345678-1234-4234-8234-123456789abc"
	image := "registry.example.test/fugue-edge@sha256:" + strings.Repeat("a", 64)
	annotation := `{"version":"v1","component":"edge-worker","scope_key":"global","artifact_kinds":["edge_route_bundle","caddy_route_config"]}`
	front := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "system", Labels: map[string]string{"fugue.io/edge-group-id": "group-a"}}, Spec: corev1.PodSpec{NodeName: "node-a"}, Status: corev1.PodStatus{HostIP: "192.0.2.10"}}
	owner := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "worker-b", Namespace: "system", UID: uid, ResourceVersion: "7", Labels: map[string]string{"fugue.io/edge-slot": "b", "fugue.io/edge-group-id": "group-a"}, Annotations: map[string]string{platformConsumerIdentityAnnotation: annotation, "fugue.pro/source-commit": strings.Repeat("b", 40)}}, Spec: corev1.PodSpec{NodeName: "node-a", ServiceAccountName: "worker-b", Containers: []corev1.Container{{Name: "caddy", Ports: []corev1.ContainerPort{{Name: "https-slot", Protocol: corev1.ProtocolTCP, ContainerPort: 28443, HostPort: 28443}}}, {Name: "edge", Image: image}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, HostIP: "192.0.2.10", Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "edge", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}, ImageID: "docker-pullable://" + image}}}}
	claims := platformcontrol.PlatformComponentIdentityClaims{CredentialID: "kubernetes:system:worker-b:" + uid, Component: model.PlatformConsumerComponentEdgeWorker, NodeID: "node-a", ScopeKey: "global", ArtifactKinds: []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig}}
	health := workerFrontHealth{Status: "ok", ActiveSlot: "b", Target: "192.0.2.10:28443", Generation: 912, Bundle: "912", Source: strings.Repeat("b", 40), Image: "sha256:" + strings.Repeat("a", 64), Authority: "edge-control"}
	return health, front, owner, claims
}

func TestPhysicalWorkerReporterRequiresSelectedLiveOwner(t *testing.T) {
	for _, scenario := range []string{"selected", "previous success", "previous failure", "stale source", "ambiguous listener", "wrong host", "different image", "unready positive", "unready negative", "different group", "same slot different port"} {
		t.Run(scenario, func(t *testing.T) {
			h, front, owner, claims := workerFrontFixture()
			positive, want := true, http.StatusOK
			pods := []corev1.Pod{owner}
			switch scenario {
			case "previous success", "previous failure":
				claims.CredentialID = "kubernetes:system:worker-b:aaaaaaaa-1234-4234-8234-123456789abc"
				positive = scenario == "previous success"
				want = http.StatusConflict
			case "stale source":
				owner.Annotations["fugue.pro/source-commit"] = strings.Repeat("c", 40)
				want = http.StatusServiceUnavailable
			case "ambiguous listener":
				peer := owner.DeepCopy()
				peer.UID = "other"
				pods = append(pods, *peer)
				want = http.StatusServiceUnavailable
			case "wrong host":
				h.Target = "192.0.2.11:28443"
				want = http.StatusServiceUnavailable
			case "different image":
				pods[0].Status.ContainerStatuses[0].ImageID = "repo@sha256:" + strings.Repeat("d", 64)
				want = http.StatusServiceUnavailable
			case "unready positive", "unready negative":
				pods[0].Status.Conditions = nil
				positive = scenario == "unready positive"
				if positive {
					want = http.StatusServiceUnavailable
				}
			case "different group":
				claims.AuthorityID = "group-b"
				want = http.StatusForbidden
			case "same slot different port":
				peer := owner.DeepCopy()
				peer.UID = "other"
				peer.Spec.Containers[0].Ports[0].HostPort = 38443
				pods = append(pods, *peer)
				claims.CredentialID = "kubernetes:system:worker-b:other"
				want = http.StatusConflict
			}
			_, status := physicalWorkerOwner(front, h, pods, claims, positive)
			if status != want {
				t.Fatalf("status %d, want %d", status, want)
			}
		})
	}
}

func TestPhysicalWorkerTransportRechecksDuringRollback(t *testing.T) {
	for _, scenario := range []string{"stable", "rollback during read", "replaced service", "changed worker", "stale endpoint", "partial inventory", "busy or unavailable metadata", "wrong pin"} {
		t.Run(scenario, func(t *testing.T) {
			node, svc, front, ep := publicNetworkFixture()
			h, _, owner, claims := workerFrontFixture()
			front.Status.HostIP = owner.Status.HostIP
			owner.Spec.NodeName = node.ID
			claims.NodeID = node.ID
			healthReads, serviceReads := 0, 0
			k := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				var reply any
				switch r.URL.Path {
				case "/api/v1/namespaces/system/services":
					serviceReads++
					s := svc.DeepCopy()
					if scenario == "replaced service" && serviceReads > 1 {
						s.UID = "replaced"
					}
					reply = corev1.ServiceList{Items: []corev1.Service{*s}}
				case "/api/v1/namespaces/system/pods":
					if r.URL.Query().Get("fieldSelector") != "spec.nodeName="+node.ID {
						t.Error("inventory not node-bound")
					}
					list := corev1.PodList{Items: []corev1.Pod{front}}
					if r.URL.Query().Get("labelSelector") == "" {
						list.Items = append(list.Items, owner)
						if scenario == "partial inventory" {
							list.Continue = "more"
						}
					}
					reply = list
				case "/api/v1/namespaces/system/pods/front-a":
					reply = front
				case "/api/v1/namespaces/system/pods/worker-b":
					o := owner.DeepCopy()
					o.Kind = "Pod"
					if scenario == "changed worker" {
						o.ResourceVersion = "999"
					}
					reply = o
				case "/api/v1/namespaces/system/pods/front-a:7831/proxy/healthz":
					healthReads++
					hh := h
					if scenario == "busy or unavailable metadata" {
						w.WriteHeader(503)
						return
					}
					if scenario == "rollback during read" && healthReads == 2 {
						hh.Generation++
						hh.ActiveSlot = "a"
						hh.Target = "192.0.2.10:18443"
					}
					reply = hh
				case "/apis/discovery.k8s.io/v1/namespaces/system/endpointslices":
					e := ep.DeepCopy()
					if scenario == "stale endpoint" {
						e.Items[0].Endpoints[0].TargetRef.UID = "wrong"
					}
					reply = e
				default:
					t.Error("unexpected request", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				if r.Method != "GET" {
					t.Errorf("mutation %s", r.Method)
				}
				if err := json.NewEncoder(w).Encode(reply); err != nil {
					t.Error(err)
				}
			}))
			defer k.Close()
			s := &Server{controlPlaneNamespace: "system", newClusterNodeClient: func() (*clusterNodeClient, error) {
				return &clusterNodeClient{client: k.Client(), baseURL: k.URL, bearerToken: "test"}, nil
			}}
			pin := string(svc.UID)
			if scenario == "wrong pin" {
				pin = "different"
			}
			_, _, status := s.inspectPhysicalWorker(t.Context(), node, claims, pin, true)
			if (status == http.StatusOK) != (scenario == "stable") {
				t.Fatalf("status %d, health reads %d", status, healthReads)
			}
			if scenario == "stable" && healthReads != 2 {
				t.Fatalf("expected activation recheck; got %d reads", healthReads)
			}
		})
	}
}
