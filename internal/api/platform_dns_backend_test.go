package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformcontrol"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

type dnsBackendFixture struct {
	pod          corev1.Pod
	svc          corev1.Service
	node         corev1.Node
	slices       discoveryv1.EndpointSliceList
	claims       platformcontrol.PlatformComponentIdentityClaims
	failPath     string
	changePath   string
	extraService bool
	podListDelay time.Duration
	proxyHandler http.HandlerFunc
}

func newDNSBackendFixture(t *testing.T) *dnsBackendFixture {
	t.Helper()
	f := &dnsBackendFixture{}
	// Golden output from scripts.reconcile_dns_transport.service; this digest
	// checks the Go verifier against the independently implemented publisher.
	if err := json.Unmarshal([]byte(`{"apiVersion":"v1","kind":"Service","metadata":{"name":"dns-public","namespace":"platform-system-test","labels":{"app.kubernetes.io/managed-by":"fugue-dns-transport"},"annotations":{"transport.fugue.dev/generation":"1","transport.fugue.dev/digest":"sha256:cf21b9d48d0bc31d887311266983b968ae3a36d57a6175f7d965379da1251799"}},"spec":{"type":"ClusterIP","externalIPs":["192.0.2.10"],"externalTrafficPolicy":"Local","internalTrafficPolicy":"Local","publishNotReadyAddresses":false,"selector":{"app":"resolver"},"ports":[{"name":"dns-udp","protocol":"UDP","port":53,"targetPort":53},{"name":"dns-tcp","protocol":"TCP","port":53,"targetPort":53}]}}`), &f.svc); err != nil {
		t.Fatal(err)
	}
	f.svc.UID, f.svc.ResourceVersion = "service-uid", "20"
	f.pod = corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "backend", Namespace: f.svc.Namespace, UID: "pod-uid", ResourceVersion: "10", Labels: map[string]string{"app": "resolver"}, Annotations: map[string]string{platformConsumerIdentityAnnotation: `{"version":"v1","component":"dns-server","scope_key":"global","artifact_kinds":["dns_answer_bundle"]}`}}, Spec: corev1.PodSpec{NodeName: "dns-node", ServiceAccountName: "dns-account"}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.0.0.2", PodIPs: []corev1.PodIP{{IP: "10.0.0.2"}}, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	f.node = corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "dns-node", UID: "node-uid"}, Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeExternalIP, Address: "192.0.2.10"}}}}
	f.slices.Items = []discoveryv1.EndpointSlice{{ObjectMeta: metav1.ObjectMeta{Name: "public-backends", Namespace: f.svc.Namespace, UID: "slice-uid", ResourceVersion: "30", Labels: map[string]string{discoveryv1.LabelServiceName: f.svc.Name}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Service", Name: f.svc.Name, UID: f.svc.UID, Controller: ptr.To(true)}}}, AddressType: discoveryv1.AddressTypeIPv4,
		Ports:     []discoveryv1.EndpointPort{{Name: ptr.To("dns-udp"), Protocol: ptr.To(corev1.ProtocolUDP), Port: ptr.To(int32(53))}, {Name: ptr.To("dns-tcp"), Protocol: ptr.To(corev1.ProtocolTCP), Port: ptr.To(int32(53))}},
		Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.0.0.2"}, NodeName: ptr.To(f.pod.Spec.NodeName), TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: f.pod.Namespace, Name: f.pod.Name, UID: f.pod.UID}, Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true), Serving: ptr.To(true), Terminating: ptr.To(false)}}}}}
	f.claims = platformcontrol.PlatformComponentIdentityClaims{CredentialID: "kubernetes:" + f.pod.Namespace + ":" + f.pod.Spec.ServiceAccountName + ":" + string(f.pod.UID), Component: model.PlatformConsumerComponentDNSServer, NodeID: f.pod.Spec.NodeName, ScopeKey: "global", ArtifactKinds: []string{model.PlatformArtifactKindDNSAnswerBundle}}
	return f
}

func (f *dnsBackendFixture) install(t *testing.T, server *Server) {
	t.Helper()
	server.controlPlaneNamespace = f.pod.Namespace
	sliceReads := 0
	kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer metadata-only-reader" {
			t.Error("backend validation must only read metadata with its own credential")
		}
		if strings.HasSuffix(r.URL.Path, f.failPath) && f.failPath != "" {
			w.WriteHeader(500)
			w.Write([]byte("sensitive-kubernetes-error"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		base := "/api/v1/namespaces/" + f.pod.Namespace
		switch r.URL.Path {
		case base + "/pods":
			time.Sleep(f.podListDelay)
			if r.URL.Query().Get("fieldSelector") != "spec.nodeName="+f.claims.NodeID {
				t.Error("node selector missing")
			}
			json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{f.pod}})
		case base + "/pods/" + f.pod.Name:
			pod := *f.pod.DeepCopy()
			if f.changePath == "pod" {
				pod.ResourceVersion = "11"
				pod.DeletionTimestamp = ptr.To(metav1.Now())
			}
			json.NewEncoder(w).Encode(pod)
		case "/api/v1/nodes/" + f.claims.NodeID:
			json.NewEncoder(w).Encode(f.node)
		case base + "/services":
			if r.URL.Query().Get("labelSelector") != "app.kubernetes.io/managed-by="+dnsTransportManager {
				t.Error("transport selector missing")
			}
			items := []corev1.Service{f.svc}
			if f.extraService {
				items = append(items, *f.svc.DeepCopy())
			}
			json.NewEncoder(w).Encode(corev1.ServiceList{Items: items})
		case base + "/services/" + f.svc.Name:
			svc := *f.svc.DeepCopy()
			if f.changePath == "service" {
				svc.ResourceVersion = "21"
			}
			json.NewEncoder(w).Encode(svc)
		case "/apis/discovery.k8s.io/v1/namespaces/" + f.pod.Namespace + "/endpointslices":
			if r.URL.Query().Get("labelSelector") != discoveryv1.LabelServiceName+"="+f.svc.Name {
				t.Error("service selector missing")
			}
			items := f.slices.DeepCopy()
			sliceReads++
			if f.changePath == "slice" && sliceReads > 1 {
				items.Items[0].ResourceVersion = "31"
				items.Items[0].Endpoints = nil
			}
			json.NewEncoder(w).Encode(items)
		case base + "/pods/" + f.pod.Name + ":8081/proxy/runtime-facts":
			if f.proxyHandler == nil {
				t.Error("unexpected Pod proxy request")
				w.WriteHeader(500)
				return
			}
			f.proxyHandler(w, r)
		default:
			t.Errorf("unexpected backend metadata request %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(kube.Close)
	server.newClusterNodeClient = func() (*clusterNodeClient, error) {
		return &clusterNodeClient{client: kube.Client(), baseURL: kube.URL, bearerToken: "metadata-only-reader"}, nil
	}
}

func TestDNSHeartbeatPublicBackendBinding(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		mutate func(*dnsBackendFixture, *platformcontrol.PlatformConsumerHeartbeatEnvelope)
	}{
		{"selected ready backend", 200, nil},
		{"candidate UID", 403, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.claims.CredentialID += "-candidate"
		}},
		{"pod terminating", 403, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.pod.DeletionTimestamp = ptr.To(metav1.Now())
		}},
		{"pod node mismatch", 403, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.pod.Spec.NodeName = "other"
		}},
		{"pod account mismatch", 403, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.pod.Spec.ServiceAccountName = "other"
		}},
		{"revoked authorization", 403, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.pod.Annotations = nil
		}},
		{"unready positive", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.pod.Status.Conditions = nil
		}},
		{"unready negative retained", 200, func(f *dnsBackendFixture, h *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.pod.Status.Conditions = nil
			f.slices.Items[0].Endpoints[0].Conditions.Ready = ptr.To(false)
			f.slices.Items[0].Endpoints[0].Conditions.Serving = ptr.To(false)
			h.ProbeStatus = "failed"
		}},
		{"node address mismatch", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.node.Status.Addresses = nil
		}},
		{"foreign manager", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) { f.svc.Labels = nil }},
		{"service drift", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.svc.Spec.Ports[0].TargetPort.IntVal = 5353
		}},
		{"bad spec digest", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.svc.Annotations["transport.fugue.dev/digest"] = "sha256:bad"
		}},
		{"foreign selector", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.pod.Labels["app"] = "candidate"
		}},
		{"multiple public services", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.extraService = true
		}},
		{"no endpoint", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) { f.slices.Items = nil }},
		{"foreign owner", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.slices.Items[0].OwnerReferences[0].UID = "other"
		}},
		{"foreign endpoint UID", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.slices.Items[0].Endpoints[0].TargetRef.UID = "candidate"
		}},
		{"foreign endpoint address", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.slices.Items[0].Endpoints[0].Addresses = []string{"10.0.0.8"}
		}},
		{"wrong endpoint port", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.slices.Items[0].Ports[0].Port = ptr.To(int32(5353))
		}},
		{"remote endpoint only", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.slices.Items[0].Endpoints[0].NodeName = ptr.To("remote")
		}},
		{"endpoint terminating", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.slices.Items[0].Endpoints[0].Conditions.Terminating = ptr.To(true)
		}},
		{"endpoint unready", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.slices.Items[0].Endpoints[0].Conditions.Ready = nil
		}},
		{"duplicate endpoint", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.slices.Items[0].Endpoints = append(f.slices.Items[0].Endpoints, f.slices.Items[0].Endpoints[0])
		}},
		{"second selected Pod cannot mask failure", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			e := f.slices.Items[0].Endpoints[0].DeepCopy()
			e.TargetRef.UID = "candidate"
			e.Conditions.Ready = ptr.To(false)
			f.slices.Items[0].Endpoints = append(f.slices.Items[0].Endpoints, *e)
		}},
		{"pod lookup unavailable", 503, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) { f.failPath = "/pods" }},
		{"slice lookup unavailable", 503, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.failPath = "/endpointslices"
		}},
		{"pod changed during read", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) { f.changePath = "pod" }},
		{"service changed during read", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.changePath = "service"
		}},
		{"slice changed during read", 409, func(f *dnsBackendFixture, _ *platformcontrol.PlatformConsumerHeartbeatEnvelope) {
			f.changePath = "slice"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDNSBackendFixture(t)
			h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ApplyStatus: "applied", ProbeStatus: "passed"}
			if tc.mutate != nil {
				tc.mutate(f, &h)
			}
			s := &Server{}
			f.install(t, s)
			if status := s.validateDNSHeartbeatBackend(context.Background(), f.claims, h); status != tc.status {
				t.Fatalf("want %d got %d", tc.status, status)
			}
		})
	}
}

func TestDNSBackendRejectionDoesNotAdvanceFactsOrAudit(t *testing.T) {
	f := newDNSBackendFixture(t)
	state, server, _, _, _, _ := setupAppDomainTestServerWithDomains(t, "example.test")
	f.install(t, server)
	ring := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "test", Keys: map[string]string{"test": "test-component-secret"}}
	server.auth.PlatformComponentIdentityKeyring = ring
	server.heartbeatAuditKeyring = trustedHeartbeatAuditTestKeyring()
	now := time.Now().UTC()
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: "dns-release", ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, ScopeKey: "global", Generation: "generation", PreparedAt: now, Topology: platformcontrol.ExpectedConsumerTopology{DNSNodes: []model.DNSNode{{ID: f.claims.NodeID}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = state.CreatePlatformExpectedConsumerSet(set); err != nil {
		t.Fatal(err)
	}
	token, err := platformcontrol.IssuePlatformComponentIdentity(ring, f.claims, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(ring, token, now)
	if err != nil {
		t.Fatal(err)
	}
	h := trustedPlatformHeartbeatRequest(t, claims, set, now, 1, 1, 1, "dns-backend-original-nonce")
	response := performJSONRequest(t, server, "POST", "/v1/platform-state/consumers/trusted-heartbeat", token, h)
	if response.Code != 200 {
		t.Fatalf("initial heartbeat %d %s", response.Code, response.Body.String())
	}
	before, _ := state.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
	auditBefore, _ := state.ListAuditEvents("", true, 100)
	h = trustedPlatformHeartbeatRequest(t, claims, set, now, 2, 1, 1, "dns-backend-rejected-nonce")
	f.slices.Items[0].Endpoints[0].TargetRef.UID = "replacement"
	response = performJSONRequest(t, server, "POST", "/v1/platform-state/consumers/trusted-heartbeat", token, h)
	if response.Code != 409 {
		t.Fatalf("stale backend accepted: %d %s", response.Code, response.Body.String())
	}
	after, _ := state.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
	auditAfter, _ := state.ListAuditEvents("", true, 100)
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(auditBefore, auditAfter) {
		t.Fatal("rejected backend modified existing facts/cursor/audit")
	}
	f.slices.Items[0].Endpoints[0].TargetRef.UID = f.pod.UID
	response = performJSONRequest(t, server, "POST", "/v1/platform-state/consumers/trusted-heartbeat", token, h)
	if response.Code != 200 {
		t.Fatalf("rejected cursor was consumed: %d %s", response.Code, response.Body.String())
	}
}
