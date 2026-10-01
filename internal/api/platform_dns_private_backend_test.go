package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
	"fugue/internal/testfixture/celldns"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

func TestPrivateDNSReceiptRequiresSignedCurrentDNSOnlyMember(t *testing.T) {
	for _, scenario := range []string{"valid", "unbound set", "wrong fence", "wrong generation", "wrong parent", "foreign node", "tampered membership", "untrusted parent", "legacy role"} {
		t.Run(scenario, func(t *testing.T) {
			c := celldns.Compile(t, celldns.Request(t))
			now := time.Now().UTC()
			r := model.PlatformArtifactRelease{ID: "dns-shadow", ArtifactID: c.ReleaseArtifact.ID, ArtifactKind: c.ReleaseArtifact.ArtifactKind, Scope: c.ReleaseArtifact.Scope, ScopeKey: c.ReleaseArtifact.ScopeKey, Generation: c.ReleaseArtifact.Generation, ReleaseChannel: "shadow", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now, LaneKey: platformsafety.ReleaseLaneKey(c.ReleaseArtifact.ArtifactKind, c.ReleaseArtifact.ScopeKey, "shadow")}
			topology, _, err := platformcontrol.DeclaredTrafficConsumerTopology(c.ReleaseArtifact)
			if err != nil {
				t.Fatal(err)
			}
			set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: c.ReleaseArtifact.ID, ArtifactReleaseID: r.ID, ArtifactKind: c.DNSArtifact.ArtifactKind, Generation: c.DNSArtifact.Generation, Scope: c.ReleaseArtifact.Scope, ScopeKey: c.ReleaseArtifact.ScopeKey, Revision: 1, PreparedAt: now, Topology: topology})
			if err != nil {
				t.Fatal(err)
			}
			claims := platformcontrol.PlatformComponentIdentityClaims{Component: model.PlatformConsumerComponentDNSServer, NodeID: "dns-a", AuthorityID: "cell-dns", ScopeKey: c.ReleaseArtifact.ScopeKey, ArtifactKinds: []string{model.PlatformArtifactKindDNSAnswerBundle}}
			h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ReleaseSetID: c.ReleaseArtifact.ID, ExpectedConsumerSetID: set.ID, FencingToken: 1, GenerationSequence: c.DNSArtifact.GenerationSequence}
			switch scenario {
			case "unbound set":
				set.ArtifactReleaseID = ""
			case "wrong fence":
				h.FencingToken++
			case "wrong generation":
				h.GenerationSequence++
			case "wrong parent":
				h.ReleaseSetID = "foreign"
			case "foreign node":
				claims.NodeID = "foreign"
			case "tampered membership":
				set.Consumers[0].Required = false
			case "untrusted parent":
				c.ReleaseArtifact.Provenance.Signature = "forged"
			case "legacy role":
				delete(c.ReleaseArtifact.Content, "publication_role")
			}
			state := model.State{PlatformArtifacts: []model.PlatformArtifact{c.ReleaseArtifact, c.DNSArtifact}, PlatformArtifactReleases: []model.PlatformArtifactRelease{r}, ExpectedConsumerSets: []model.PlatformExpectedConsumerSet{set}, PlatformReleaseLanes: []model.PlatformReleaseLane{{LaneKey: r.LaneKey, ArtifactKind: r.ArtifactKind, ScopeKey: r.ScopeKey, ReleaseChannel: r.ReleaseChannel, FencingToken: r.FencingToken, ActiveReleaseID: r.ID}}}
			path := t.TempDir() + "/state.json"
			raw, _ := json.Marshal(state)
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			st := store.New(path)
			st.ConfigurePlatformArtifactSigning(celldns.Keys())
			s := &Server{store: st}
			allowed, status := s.privateDNSReceiptMember(claims, h, []model.PlatformExpectedConsumerSet{set})
			if (scenario == "valid") != allowed || allowed && status != http.StatusOK {
				t.Fatal("private membership verdict", allowed, status)
			}
		})
	}
}

func TestDeclaredPrivateDNSServicesMatchExecutorIdentityAndDigest(t *testing.T) {
	var registry struct {
		Components []struct {
			ManifestPath string `json:"manifestPath"`
		} `json:"components"`
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "releases", "components.json"))
	if err != nil || json.Unmarshal(raw, &registry) != nil {
		t.Fatal("registry unavailable", err)
	}
	seenMembers, seenClaims := map[string]bool{}, map[string]bool{}
	count := 0
	for _, component := range registry.Components {
		raw, err := os.ReadFile(filepath.Join("..", "..", component.ManifestPath))
		if err != nil {
			t.Fatal(err)
		}
		var list struct {
			Items []json.RawMessage `json:"items"`
		}
		if json.Unmarshal(raw, &list) != nil {
			t.Fatal("invalid manifest")
		}
		var deployments []appsv1.Deployment
		var services []corev1.Service
		for _, item := range list.Items {
			var meta metav1.TypeMeta
			json.Unmarshal(item, &meta)
			switch meta.Kind {
			case "Deployment":
				var d appsv1.Deployment
				if json.Unmarshal(item, &d) != nil {
					t.Fatal("invalid Deployment")
				}
				deployments = append(deployments, d)
			case "Service":
				var s corev1.Service
				if json.Unmarshal(item, &s) != nil {
					t.Fatal("invalid Service")
				}
				if s.Labels["app.kubernetes.io/managed-by"] == dnsPrivateTransportManager {
					services = append(services, s)
				}
			}
		}
		for _, svc := range services {
			count++
			var owner *corev1.Pod
			for _, d := range deployments {
				pod := &corev1.Pod{ObjectMeta: d.Spec.Template.ObjectMeta, Spec: d.Spec.Template.Spec}
				pod.Namespace = d.Namespace
				pod.Spec.NodeName = pod.Spec.NodeSelector["kubernetes.io/hostname"]
				if dnsServiceSelectsPod(svc, *pod) {
					if owner != nil {
						t.Fatal("ambiguous private DNS executor")
					}
					owner = pod
				}
			}
			if owner == nil || owner.Spec.NodeName == "" || owner.Spec.HostNetwork || owner.Annotations["fugue.pro/authority-transition-role"] != "isolated-candidate" {
				t.Fatal("private DNS executor is not independently declared")
			}
			policy, valid := decodePlatformConsumerIdentityPolicy(owner.Annotations[platformConsumerIdentityAnnotation])
			if !valid || policy.Component != model.PlatformConsumerComponentDNSServer || policy.AuthorityID == "" || policy.ScopeKey != platformconfig.AuthorityCellScope(policy.AuthorityID) {
				t.Fatal("private DNS component identity differs")
			}
			svc.UID, svc.ResourceVersion = "observed-service", "1"
			if !validDNSPrivateTransport(svc, owner.Namespace, policy.AuthorityID, owner.Spec.NodeName) {
				t.Fatal("private DNS declaration differs from transport digest/identity", component.ManifestPath)
			}
			if _, ok := dnsBackendObservationPort(*owner, svc); !ok {
				t.Fatal("private DNS observation has no unique socket owner")
			}
			member := policy.ScopeKey + "/" + owner.Spec.NodeName
			if seenMembers[member] {
				t.Fatal("duplicate private DNS member")
			}
			seenMembers[member] = true
			stateClaims := 0
			for _, v := range owner.Spec.Volumes {
				if v.PersistentVolumeClaim != nil {
					stateClaims++
					claim := owner.Namespace + "/" + v.PersistentVolumeClaim.ClaimName
					if seenClaims[claim] {
						t.Fatal("private DNS executors share mutable recovery state")
					}
					seenClaims[claim] = true
				}
			}
			if stateClaims != 1 {
				t.Fatal("private DNS requires one independent recovery PVC")
			}
		}
	}
	if count == 0 {
		t.Fatal("no private DNS declaration was exercised")
	}
}

func privateDNSServiceFixture(t *testing.T, pod corev1.Pod) corev1.Service {
	t.Helper()
	svc := corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "dns-validation", Namespace: pod.Namespace, UID: "private-service", ResourceVersion: "40", Labels: map[string]string{"app.kubernetes.io/managed-by": dnsPrivateTransportManager}, Annotations: map[string]string{"transport.fugue.dev/authority-id": "cell-dns", "transport.fugue.dev/node-id": pod.Spec.NodeName, "transport.fugue.dev/generation": "1"}}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, InternalTrafficPolicy: ptr.To(corev1.ServiceInternalTrafficPolicyLocal), Selector: map[string]string{"app": "candidate"}, Ports: []corev1.ServicePort{{Name: "dns-udp", Protocol: corev1.ProtocolUDP, Port: 5353, TargetPort: intstr.FromInt32(5353)}, {Name: "dns-tcp", Protocol: corev1.ProtocolTCP, Port: 5353, TargetPort: intstr.FromInt32(5353)}}}}
	spec := map[string]any{"type": "ClusterIP", "internalTrafficPolicy": "Local", "publishNotReadyAddresses": false, "selector": svc.Spec.Selector, "ports": []map[string]any{{"name": "dns-udp", "protocol": "UDP", "port": 5353, "targetPort": 5353}, {"name": "dns-tcp", "protocol": "TCP", "port": 5353, "targetPort": 5353}}}
	raw, _ := json.Marshal(map[string]any{"authority_id": "cell-dns", "node_id": pod.Spec.NodeName, "spec": spec})
	sum := sha256.Sum256(raw)
	svc.Annotations["transport.fugue.dev/digest"] = "sha256:" + hex.EncodeToString(sum[:])
	return svc
}

func TestPrivateDNSBackendNeverReplacesPublicAuthority(t *testing.T) {
	for _, scenario := range []string{"staged", "positive", "negative", "no public service", "public facts stay public", "same public authority", "unlabelled candidate", "host network", "host port", "external private IP", "node port", "wrong service node", "wrong service authority", "changed service digest", "duplicate private service", "private sibling appeared", "private selection replaced", "foreign private endpoint", "unready positive", "replaced candidate", "changed private service", "changed public service", "changed public pod", "changed node", "private lookup failed"} {
		t.Run(scenario, func(t *testing.T) {
			f := newDNSBackendFixture(t)
			f.node.ResourceVersion = "node-version"
			publicPod := *f.pod.DeepCopy()
			pod := *f.pod.DeepCopy()
			pod.Name = "candidate"
			pod.UID = "candidate-uid"
			pod.Labels = map[string]string{"app": "candidate", "fugue.io/edge-group-id": "cell-dns"}
			pod.Annotations = map[string]string{"fugue.pro/authority-transition-role": "isolated-candidate", platformConsumerIdentityAnnotation: `{"version":"v1","component":"dns-server","scope_key":"authority-cell:cell-dns","authority_id":"cell-dns","artifact_kinds":["dns_answer_bundle"]}`}
			pod.Spec.Containers = []corev1.Container{{Name: "dns", Ports: []corev1.ContainerPort{{Name: "dns-udp", Protocol: corev1.ProtocolUDP, ContainerPort: 5353}, {Name: "dns-tcp", Protocol: corev1.ProtocolTCP, ContainerPort: 5353}, {Name: "health", Protocol: corev1.ProtocolTCP, ContainerPort: 8081}}, ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromString("health")}}}}}
			svc := privateDNSServiceFixture(t, pod)
			endpoints := f.slices.DeepCopy()
			slice := &endpoints.Items[0]
			slice.Name = "candidate-slice"
			slice.UID = "candidate-slice-uid"
			slice.Labels[discoveryv1.LabelServiceName] = svc.Name
			slice.OwnerReferences[0].Name = svc.Name
			slice.OwnerReferences[0].UID = svc.UID
			for i := range slice.Ports {
				slice.Ports[i].Port = ptr.To(int32(5353))
			}
			slice.Endpoints[0].TargetRef.Name = pod.Name
			slice.Endpoints[0].TargetRef.UID = pod.UID
			claims := f.claims
			claims.AuthorityID = "cell-dns"
			claims.ScopeKey = "authority-cell:cell-dns"
			claims.CredentialID = "kubernetes:" + pod.Namespace + ":" + pod.Spec.ServiceAccountName + ":" + string(pod.UID)
			h := platformcontrol.PlatformConsumerHeartbeatEnvelope{ApplyStatus: "staged", ProbeStatus: "shadow_validated"}
			want := http.StatusConflict
			switch scenario {
			case "staged", "no public service":
				want = http.StatusOK
			case "positive":
				h.ApplyStatus, h.ProbeStatus = "applied", "passed"
				want = http.StatusOK
			case "negative":
				h.ApplyStatus, h.ProbeStatus = "applied", "failed"
				pod.Status.Conditions[0].Status = corev1.ConditionFalse
				want = http.StatusOK
			case "same public authority":
				publicPod.Annotations[platformConsumerIdentityAnnotation] = pod.Annotations[platformConsumerIdentityAnnotation]
			case "unlabelled candidate":
				delete(pod.Annotations, "fugue.pro/authority-transition-role")
			case "host network":
				pod.Spec.HostNetwork = true
			case "host port":
				pod.Spec.Containers[0].Ports[0].HostPort = 53
			case "external private IP":
				svc.Spec.ExternalIPs = []string{"192.0.2.10"}
			case "node port":
				svc.Spec.Ports[0].NodePort = 30535
			case "wrong service node":
				svc.Annotations["transport.fugue.dev/node-id"] = "other-node"
			case "wrong service authority":
				svc.Annotations["transport.fugue.dev/authority-id"] = "cell-other"
			case "changed service digest":
				svc.Annotations["transport.fugue.dev/digest"] = "forged"
			case "foreign private endpoint":
				slice.Endpoints[0].TargetRef.UID = "foreign"
			case "unready positive":
				h.ApplyStatus, h.ProbeStatus = "applied", "passed"
				pod.Status.Conditions[0].Status = corev1.ConditionFalse
			case "private lookup failed":
				want = http.StatusServiceUnavailable
			}
			publicLists, privateLists, nodeReads, podLists := 0, 0, 0, 0
			kube := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer metadata-only-reader" {
					t.Error("metadata-only observation required")
					w.WriteHeader(403)
					return
				}
				base := "/api/v1/namespaces/" + pod.Namespace
				switch r.URL.Path {
				case base + "/pods":
					podLists++
					if podLists == 1 && scenario != "public facts stay public" && r.URL.Query().Get("fieldSelector") != "spec.nodeName="+pod.Spec.NodeName {
						t.Error("private validation must retain public backends under other service accounts")
					}
					json.NewEncoder(w).Encode(corev1.PodList{Items: []corev1.Pod{pod, publicPod}})
				case base + "/pods/" + pod.Name:
					p := *pod.DeepCopy()
					if scenario == "replaced candidate" {
						p.UID = types.UID("replacement")
					}
					json.NewEncoder(w).Encode(p)
				case base + "/pods/" + publicPod.Name:
					p := *publicPod.DeepCopy()
					if scenario == "changed public pod" {
						p.ResourceVersion = "different"
					}
					json.NewEncoder(w).Encode(p)
				case "/api/v1/nodes/" + pod.Spec.NodeName:
					n := *f.node.DeepCopy()
					nodeReads++
					if scenario == "changed node" && nodeReads > 1 {
						n.ResourceVersion = "different"
					}
					json.NewEncoder(w).Encode(n)
				case base + "/services":
					var items []corev1.Service
					if r.URL.Query().Get("labelSelector") == "app.kubernetes.io/managed-by="+dnsPrivateTransportManager {
						privateLists++
						if scenario == "private lookup failed" {
							w.WriteHeader(500)
							return
						}
						items = []corev1.Service{svc}
						if scenario == "private selection replaced" && privateLists > 1 {
							items[0].UID = "new-service"
						}
						if scenario == "duplicate private service" || scenario == "private sibling appeared" && privateLists > 1 {
							items = append(items, *svc.DeepCopy())
						}
					} else {
						publicLists++
						v := *f.svc.DeepCopy()
						if scenario == "changed public service" && publicLists > 1 {
							v.ResourceVersion = "different"
						}
						if scenario != "no public service" {
							items = []corev1.Service{v}
						}
					}
					json.NewEncoder(w).Encode(corev1.ServiceList{Items: items})
				case base + "/services/" + svc.Name:
					v := *svc.DeepCopy()
					if scenario == "changed private service" {
						v.ResourceVersion = "different"
					}
					json.NewEncoder(w).Encode(v)
				case "/apis/discovery.k8s.io/v1/namespaces/" + pod.Namespace + "/endpointslices":
					if r.URL.Query().Get("labelSelector") != discoveryv1.LabelServiceName+"="+svc.Name {
						t.Error("wrong Service observation")
					}
					json.NewEncoder(w).Encode(endpoints)
				default:
					t.Errorf("unexpected observation %s", r.URL)
					w.WriteHeader(404)
				}
			}))
			defer kube.Close()
			s := &Server{controlPlaneNamespace: pod.Namespace, newClusterNodeClient: func() (*clusterNodeClient, error) {
				return &clusterNodeClient{client: kube.Client(), baseURL: kube.URL, bearerToken: "metadata-only-reader"}, nil
			}}
			var got int
			if scenario == "public facts stay public" {
				got = s.inspectDNSBackend(context.Background(), claims, h, nil)
			} else {
				got = s.inspectDNSBackendTransport(context.Background(), claims, h, nil, true)
			}
			if got != want {
				t.Fatal("private/public transport binding", got, want)
			}
			if scenario == "positive" {
				testPrivateDNSHeartbeatPersistence(t, s, claims, func() { svc.Annotations["transport.fugue.dev/digest"] = "changed" })
			}
		})
	}
}

func testPrivateDNSHeartbeatPersistence(t *testing.T, transport *Server, claims platformcontrol.PlatformComponentIdentityClaims, invalidate func()) {
	t.Helper()
	req := celldns.Request(t)
	req.Intent.DNSConsumers[0].NodeID = claims.NodeID
	req.Policy.DNSAuthorities[0].NodeID = claims.NodeID
	req.Policy.DNSClientPolicies[0].NodeID = claims.NodeID
	req.Policy.DNSAnswerRules[0].NodeID = claims.NodeID
	req.RuntimeSnapshot.DNSConsumers[0].NodeID = claims.NodeID
	req.RuntimeSnapshot.DNSSelections[0].NodeID = claims.NodeID
	declared, err := platformconfig.TrafficConsumerTopologyFromIntent(req.Intent)
	if err != nil {
		t.Fatal(err)
	}
	req.Policy.ConsumerTopologyDigest, _ = platformconfig.Digest(declared)
	c := celldns.Compile(t, req)
	now := time.Now().UTC()
	r := model.PlatformArtifactRelease{ID: "dns-gray", ArtifactID: c.ReleaseArtifact.ID, ArtifactKind: c.ReleaseArtifact.ArtifactKind, Scope: c.ReleaseArtifact.Scope, ScopeKey: c.ReleaseArtifact.ScopeKey, Generation: c.ReleaseArtifact.Generation, ReleaseChannel: "gray", CanaryRuleRef: "cohort=complete", FencingToken: 1, Status: model.PlatformArtifactReleaseStatusActive, ReleasedAt: now, LaneKey: platformsafety.ReleaseLaneKey(c.ReleaseArtifact.ArtifactKind, c.ReleaseArtifact.ScopeKey, "gray")}
	topology, _, err := platformcontrol.DeclaredTrafficConsumerTopology(c.ReleaseArtifact)
	if err != nil {
		t.Fatal(err)
	}
	set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: c.ReleaseArtifact.ID, ArtifactReleaseID: r.ID, ArtifactKind: c.DNSArtifact.ArtifactKind, Generation: c.DNSArtifact.Generation, Scope: c.ReleaseArtifact.Scope, ScopeKey: c.ReleaseArtifact.ScopeKey, Revision: 1, PreparedAt: now, Topology: topology})
	if err != nil {
		t.Fatal(err)
	}
	state := model.State{PlatformArtifacts: []model.PlatformArtifact{c.ReleaseArtifact, c.DNSArtifact}, PlatformArtifactReleases: []model.PlatformArtifactRelease{r}, ExpectedConsumerSets: []model.PlatformExpectedConsumerSet{set}, PlatformReleaseLanes: []model.PlatformReleaseLane{{LaneKey: r.LaneKey, ArtifactKind: r.ArtifactKind, ScopeKey: r.ScopeKey, ReleaseChannel: r.ReleaseChannel, FencingToken: r.FencingToken, ActiveReleaseID: r.ID}}}
	path := t.TempDir() + "/state.json"
	raw, _ := json.Marshal(state)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	st := store.New(path)
	keys := celldns.Keys()
	s := NewServer(st, auth.New(st, "synthetic-private-admin"), nil, ServerConfig{BundleSigningKey: keys.PrimaryKey, BundleSigningKeyID: keys.PrimaryKeyID})
	s.controlPlaneNamespace, s.newClusterNodeClient = transport.controlPlaneNamespace, transport.newClusterNodeClient
	ring := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "identity", Keys: map[string]string{"identity": "synthetic-private-dns-identity"}}
	s.auth.PlatformComponentIdentityKeyring = ring
	s.heartbeatAuditKeyring = trustedHeartbeatAuditTestKeyring()
	token, err := platformcontrol.IssuePlatformComponentIdentity(ring, claims, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err = platformcontrol.ParsePlatformComponentIdentity(ring, token, now)
	if err != nil {
		t.Fatal(err)
	}
	heartbeat := func(sequence int64) platformcontrol.PlatformConsumerHeartbeatEnvelope {
		h := trustedPlatformHeartbeatRequest(t, claims, set, time.Now().UTC(), sequence, c.DNSArtifact.GenerationSequence, r.FencingToken, model.NewID("nonce"))
		h.LKGGeneration = c.DNSArtifact.Generation
		h.CompatibilityCapabilities = []string{platformcontrol.TrafficReleaseCapabilityV1, platformcontrol.CellDNSCapabilityV1}
		h.EvidenceHash, _ = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
		return h
	}
	response := performJSONRequest(t, s, http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", token, heartbeat(1))
	if response.Code != http.StatusOK {
		t.Fatalf("private heartbeat rejected: %d %s", response.Code, response.Body.String())
	}
	before, err := st.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 1 || !before[0].IdentityVerified {
		t.Fatal("private scoped receipt missing")
	}
	if result := s.evaluateLiveConsumerConvergence(context.Background(), set, before, s.platformConvergenceBinding(set)); !result.Pass {
		t.Fatal("private local convergence rejected", result)
	}
	if result := s.inspectDNSBackend(context.Background(), claims, platformcontrol.PlatformConsumerHeartbeatEnvelope{}, nil); result == http.StatusOK {
		t.Fatal("private receipt claimed public transport")
	}
	auditBefore, err := st.ListAuditEvents("", true, 100)
	if err != nil {
		t.Fatal(err)
	}
	invalidate()
	response = performJSONRequest(t, s, http.MethodPost, "/v1/platform-state/consumers/trusted-heartbeat", token, heartbeat(2))
	if response.Code != http.StatusConflict {
		t.Fatalf("changed validation transport accepted: %d %s", response.Code, response.Body.String())
	}
	after, err := st.ListPlatformConsumers(set.ArtifactKind, set.ScopeKey)
	if err != nil {
		t.Fatal(err)
	}
	auditAfter, err := st.ListAuditEvents("", true, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(auditBefore, auditAfter) {
		t.Fatal("rejected private transport changed facts, cursor or audit")
	}
	if result := s.evaluateLiveConsumerConvergence(context.Background(), set, after, s.platformConvergenceBinding(set)); result.Pass {
		t.Fatal("retained private receipt hid changed transport")
	}
}
