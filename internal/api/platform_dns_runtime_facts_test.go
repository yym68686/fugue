package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/dnsfacts"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeprobe"
	"fugue/internal/store"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestDNSRuntimeFactsReadBindsPodProxyAndCurrentAssignment(t *testing.T) {
	path := t.TempDir() + "/state.json"
	st := store.New(path)
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	s := NewServer(st, auth.New(st, "facts-admin"), nil, ServerConfig{BundleSigningKey: "synthetic-facts-key", BundleSigningKeyID: "key"})
	f := newDNSBackendFixture(t)
	if _, err := st.UpdateDNSHeartbeat(model.DNSNode{ID: f.claims.NodeID, EdgeGroupID: "edge-group-a", Zone: "example.test", PublicIPv4: "8.8.8.8"}); err != nil {
		t.Fatal(err)
	}
	seedVerifiedDNSDelegationFixture(t, s, "example.test")
	parent, release, found, err := st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, "global", "gray")
	if err != nil || !found {
		t.Fatal(err)
	}
	sets, err := s.currentReleaseSetExpectations(parent)
	if err != nil {
		t.Fatal(err)
	}
	var set model.PlatformExpectedConsumerSet
	for _, item := range sets {
		if item.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle {
			set = item
		}
	}
	child, err := s.consumerAssignmentChild(parent, set.ArtifactKind)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	ring := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "test", Keys: map[string]string{"test": "synthetic-identity"}}
	token, err := platformcontrol.IssuePlatformComponentIdentity(ring, f.claims, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	claims, err := platformcontrol.ParsePlatformComponentIdentity(ring, token, now)
	if err != nil {
		t.Fatal(err)
	}
	h := trustedPlatformHeartbeatRequest(t, claims, set, now, 20, child.GenerationSequence, release.FencingToken, "reader-original-receipt")
	h.LKGGeneration = child.Generation
	h.EvidenceHash, _ = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
	if _, err := st.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, h, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
		t.Fatal(err)
	}
	f.pod.Spec.Containers = []corev1.Container{{Name: "resolver", Ports: []corev1.ContainerPort{{ContainerPort: 53, Protocol: corev1.ProtocolTCP}, {ContainerPort: 53, Protocol: corev1.ProtocolUDP}, {Name: "observation", ContainerPort: 8081, Protocol: corev1.ProtocolTCP}}, ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromString("observation")}}}}}
	var snapshot dnsfacts.Snapshot
	reads := 0
	f.proxyHandler = func(w http.ResponseWriter, r *http.Request) { reads++; json.NewEncoder(w).Encode(snapshot) }
	f.install(t, s)
	source, err := s.currentDNSFactSource(f.claims.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := platformconfig.Digest(source.payload.ReadinessPlan)
	snapshot = dnsfacts.Snapshot{Schema: dnsfacts.Schema, NodeID: f.claims.NodeID, EdgeGroupID: source.group, Assignment: source.lookup.Assignment, ParentDigest: source.parent.ContentHash, RouteArtifactID: source.routeID, PlanDigest: digest, ObservedAt: now, EvaluatedAt: now, CheckpointValidUntil: now.Add(time.Hour), Ready: true, Facts: []dnsfacts.Probe{}}
	baseline, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := "/v1/admin/platform-state/dns-runtime-facts/" + f.claims.NodeID
	for i := 0; i < 2; i++ {
		r := performJSONRequest(t, s, "GET", endpoint, "facts-admin", nil)
		var response platformDNSRuntimeFactsResponse
		mustDecodeJSON(t, r, &response)
		if r.Code != 200 || !response.Ready || response.Backend.PodUID != string(f.pod.UID) || response.Backend.ServiceUID != string(f.svc.UID) || !reflect.DeepEqual(response.Snapshot, snapshot) || r.Header().Get("Cache-Control") != "private, no-store" {
			t.Fatal(r.Code, r.Body.String())
		}
	}
	if reads != 2 {
		t.Fatal("node observations were cached", reads)
	}
	legacy := model.DNSNode{ID: f.claims.NodeID, PhysicalNodeID: f.claims.NodeID, EdgeGroupID: "edge-group-a", Zone: "example.test", PublicIPv4: "8.8.8.8", Status: model.EdgeHealthDegraded, Healthy: false, LastError: "old unselected Pod: HTTP 409", QueryCount: 19, QueryErrorCount: 3, CacheWriteErrors: 2, DNSBundleVersion: "old-generation", ServingGeneration: "old-generation", LKGGeneration: "old-lkg", LastHeartbeatAt: &now}
	projected, err := s.dnsInventoryServingFacts(context.Background(), []model.DNSNode{legacy, legacy})
	if err != nil || len(projected) != 2 || reads != 3 {
		t.Fatal("inventory projection did not deduplicate physical node reads", err, reads)
	}
	for _, node := range projected {
		if !node.Healthy || node.Status != model.EdgeHealthHealthy || node.LastError != "" || node.DNSBundleVersion != child.Generation || node.LKGGeneration != child.Generation || node.ServingObservation == nil || node.ServingObservation.BackendPodUID != string(f.pod.UID) || node.ServingObservation.State != "ready" || node.QueryCount != legacy.QueryCount || node.CacheWriteErrors != legacy.CacheWriteErrors || node.LastHeartbeatAt != legacy.LastHeartbeatAt || !node.ServingObservation.ObservedAt.Equal(snapshot.ObservedAt) {
			t.Fatalf("selected backend did not replace legacy health while preserving history: %+v", node)
		}
	}
	if legacy.Status != model.EdgeHealthDegraded || legacy.ServingObservation != nil {
		t.Fatal("read mutated the source inventory")
	}
	getNode := performJSONRequest(t, s, "GET", "/v1/dns/nodes/"+f.claims.NodeID, "facts-admin", nil)
	var nodeResponse struct {
		Node model.DNSNode `json:"node"`
	}
	mustDecodeJSON(t, getNode, &nodeResponse)
	if getNode.Code != 200 || !nodeResponse.Node.Healthy || nodeResponse.Node.ServingObservation == nil || nodeResponse.Node.ServingObservation.BackendPodUID != string(f.pod.UID) {
		t.Fatal("DNS node API did not expose the selected backend projection", getNode.Code, getNode.Body.String())
	}
	after, _ := os.ReadFile(path)
	if !reflect.DeepEqual(baseline, after) {
		t.Fatal("read mutated durable state")
	}
	originalProxy := f.proxyHandler
	for _, scenario := range []string{"bad schema", "bad plan", "foreign assignment", "pod changed", "service changed", "endpoint changed", "truncated", "unknown field", "oversized", "redirect", "proxy failure", "publication changed", "artifact corrupt", "no HTTP port", "ambiguous container", "unready", "negative", "expired"} {
		t.Run(scenario, func(t *testing.T) {
			oldSnapshot, oldPod := snapshot, *f.pod.DeepCopy()
			defer func() {
				snapshot = oldSnapshot
				f.pod = oldPod
				f.changePath = ""
				f.proxyHandler = originalProxy
				if err := os.WriteFile(path, baseline, 0600); err != nil {
					t.Fatal(err)
				}
			}()
			want := 503
			switch scenario {
			case "bad schema":
				snapshot.Schema = "unknown"
			case "bad plan":
				snapshot.PlanDigest = "sha256:" + strings.Repeat("0", 64)
			case "foreign assignment":
				snapshot.Assignment.FencingToken++
			case "pod changed":
				f.changePath = "pod"
			case "service changed":
				f.changePath = "service"
			case "endpoint changed":
				f.changePath = "slice"
			case "truncated":
				f.proxyHandler = func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(`{"schema":`)) }
			case "unknown field":
				f.proxyHandler = func(w http.ResponseWriter, _ *http.Request) {
					raw, _ := json.Marshal(snapshot)
					w.Write(append(raw[:len(raw)-1], []byte(`,"unexpected":true}`)...))
				}
			case "oversized":
				f.proxyHandler = func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(strings.Repeat(" ", (8<<20)+1))) }
			case "redirect":
				f.proxyHandler = func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/credential-trap", 302) }
			case "proxy failure":
				f.proxyHandler = func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(500)
					w.Write([]byte("private-node-error"))
				}
			case "publication changed", "artifact corrupt":
				f.proxyHandler = func(w http.ResponseWriter, _ *http.Request) {
					var state model.State
					json.Unmarshal(baseline, &state)
					if scenario == "publication changed" {
						for i := range state.PlatformArtifactReleases {
							if state.PlatformArtifactReleases[i].ID == release.ID {
								state.PlatformArtifactReleases[i].FencingToken++
							}
						}
					} else {
						for i := range state.PlatformArtifacts {
							if state.PlatformArtifacts[i].ID == child.ID {
								state.PlatformArtifacts[i].Provenance.Signature = "bad"
							}
						}
					}
					raw, _ := json.Marshal(state)
					os.WriteFile(path, raw, 0600)
					json.NewEncoder(w).Encode(snapshot)
				}
			case "no HTTP port":
				f.pod.Spec.Containers[0].ReadinessProbe = nil
			case "ambiguous container":
				f.pod.Spec.Containers = append(f.pod.Spec.Containers, f.pod.Spec.Containers[0])
			case "unready":
				want = 200
				f.pod.Status.Conditions = nil
			case "negative":
				want = 200
				snapshot.Ready = false
			case "expired":
				want = 200
				snapshot.CheckpointValidUntil = now.Add(-time.Second)
			}
			r := performJSONRequest(t, s, "GET", endpoint, "facts-admin", nil)
			if r.Code != want || strings.Contains(r.Body.String(), "private-node-error") {
				t.Fatal(r.Code, r.Body.String())
			}
			if want == 200 {
				var response platformDNSRuntimeFactsResponse
				mustDecodeJSON(t, r, &response)
				if response.Ready {
					t.Fatal("negative/expired/unready snapshot became ready")
				}
			}
			// Legacy inventory health cannot hide failed, expired or replaced
			// current backend evidence, even if its last heartbeat was fresh.
			projected, projectionErr := s.dnsInventoryServingFacts(context.Background(), []model.DNSNode{legacy})
			if projectionErr != nil || len(projected) != 1 || projected[0].Healthy || projected[0].ServingObservation == nil {
				t.Fatal("unavailable backend reused legacy health or dropped membership", projectionErr, projected)
			}
			if len(freshDNSNodes(projected, now.Add(24*time.Hour))) != 1 {
				t.Fatal("unavailable enrolled node disappeared with stale inventory")
			}
			if scenario != "publication changed" && scenario != "artifact corrupt" {
				after, _ := os.ReadFile(path)
				if !reflect.DeepEqual(baseline, after) {
					t.Fatal("failure mutated durable state")
				}
			}
		})
	}
	if r := performJSONRequest(t, s, "GET", endpoint, "", nil); r.Code != 401 {
		t.Fatal("anonymous read", r.Code)
	}
	if r := performJSONRequest(t, s, "GET", "/v1/admin/platform-state/dns-runtime-facts/INVALID", "facts-admin", nil); r.Code != 400 {
		t.Fatal("invalid node", r.Code)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.readPlatformDNSRuntimeFacts(ctx, f.claims.NodeID); err == nil {
		t.Fatal("canceled read accepted")
	}
}

func TestDNSRuntimeSnapshotValidatesProofsWithoutRenewingThem(t *testing.T) {
	now := time.Now().UTC()
	policy := platformconfig.PolicySnapshot{MaxStaleSeconds: 3600, DNSReadiness: &platformconfig.DNSReadinessPolicy{FactFreshnessSeconds: 120}}
	p := platformconfig.DNSReadinessProbe{ID: "probe", EdgeID: "edge", EdgeGroupID: "group", RouteDigest: "route-digest"}
	plan := &platformconfig.DNSReadinessPlan{Probes: []platformconfig.DNSReadinessProbe{p}, Records: []platformconfig.DNSReadinessRecord{{MinimumHealthyEdges: 1, Targets: []platformconfig.DNSReadinessTarget{{EdgeID: p.EdgeID, Family: "A", ProbeIDs: []string{p.ID}}}}}}
	digest, _ := platformconfig.Digest(plan)
	assignment := model.PlatformConsumerAssignment{ReleaseSetID: "parent", ArtifactReleaseID: "release", ReleaseChannel: "full", FencingToken: 3, ScopeKey: "global"}
	lineage := platformconfig.Lineage{IntentDigest: "intent", PolicyDigest: "policy", InputSnapshotDigest: "input"}
	source := dnsFactSource{claims: platformcontrol.PlatformComponentIdentityClaims{NodeID: "node"}, group: "group", routeID: "route", parent: model.PlatformArtifact{ID: "parent", ContentHash: "parent-digest"}, lookup: consumerArtifactLookup{Assignment: assignment}, payload: platformDNSArtifactPayload{Policy: policy, ReadinessPlan: plan, Lineage: lineage}}
	proof := routeprobe.Proof{Digest: p.RouteDigest, EdgeID: p.EdgeID, GroupID: p.EdgeGroupID, Version: "v1", CheckedAt: now.Add(-time.Second), ValidUntil: now.Add(time.Minute), TrafficRelease: &model.TrafficReleaseBinding{ReleaseSetID: "parent", ReleaseSetDigest: "parent-digest", RouteArtifactID: "route", ReleaseID: "release", ReleaseChannel: "full", FencingToken: 3, ScopeKey: "global", IntentDigest: "intent", PolicyDigest: "policy", InputSnapshotDigest: "input"}}
	source.trafficBinding = proof.TrafficRelease
	base := dnsfacts.Snapshot{Schema: dnsfacts.Schema, NodeID: "node", EdgeGroupID: "group", Assignment: assignment, ParentDigest: "parent-digest", RouteArtifactID: "route", PlanDigest: digest, ObservedAt: now.Add(-time.Second), EvaluatedAt: now, CheckpointValidUntil: now.Add(time.Hour), Ready: true, Facts: []dnsfacts.Probe{{ProbeID: p.ID, Ready: true, Proof: proof}}}
	for _, scenario := range []string{"valid", "expired proof", "negative", "missing", "duplicate", "unknown probe", "wrong digest", "wrong edge", "wrong group", "wrong state", "foreign release", "future", "renewed validity", "checkpoint extended", "dualstack absent"} {
		t.Run(scenario, func(t *testing.T) {
			raw, _ := json.Marshal(base)
			var snapshot dnsfacts.Snapshot
			json.Unmarshal(raw, &snapshot)
			input := source
			wantReady, wantErr := false, false
			switch scenario {
			case "valid":
				wantReady = true
			case "expired proof":
				snapshot.Facts[0].Proof.ValidUntil = now.Add(-time.Millisecond)
			case "negative":
				snapshot.Facts[0].Ready = false
			case "missing":
				snapshot.Facts = nil
			case "duplicate":
				snapshot.Facts = append(snapshot.Facts, snapshot.Facts[0])
				wantErr = true
			case "unknown probe":
				snapshot.Facts[0].ProbeID = "unknown"
				wantErr = true
			case "wrong digest":
				snapshot.Facts[0].Proof.Digest = "wrong"
				wantErr = true
			case "wrong edge":
				snapshot.Facts[0].Proof.EdgeID = "wrong"
				wantErr = true
			case "wrong group":
				snapshot.Facts[0].Proof.GroupID = "wrong"
				wantErr = true
			case "wrong state":
				snapshot.Facts[0].Proof.State = "disabled"
				wantErr = true
			case "foreign release":
				snapshot.Facts[0].Proof.TrafficRelease.FencingToken++
				wantErr = true
			case "future":
				snapshot.Facts[0].Proof.CheckedAt = now.Add(time.Second)
				wantErr = true
			case "renewed validity":
				snapshot.Facts[0].Proof.ValidUntil = now.Add(time.Hour)
				wantErr = true
			case "checkpoint extended":
				snapshot.CheckpointValidUntil = now.Add(2 * time.Hour)
				wantErr = true
			case "dualstack absent":
				planCopy := *plan
				planCopy.Records = append([]platformconfig.DNSReadinessRecord(nil), plan.Records...)
				planCopy.Records[0].RequireDualStack = true
				input.payload.ReadinessPlan = &planCopy
				snapshot.PlanDigest, _ = platformconfig.Digest(planCopy)
			}
			before, _ := json.Marshal(snapshot)
			ids, ready, err := evaluateDNSRuntimeSnapshot(snapshot, input, now)
			if (err != nil) != wantErr || ready != wantReady {
				t.Fatal(ids, ready, err)
			}
			after, _ := json.Marshal(snapshot)
			if string(before) != string(after) {
				t.Fatal("evaluation rewrote original proof")
			}
		})
	}
}

func TestDNSPodSnapshotRejectsRedirectWithoutForwardingCredential(t *testing.T) {
	called := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true; t.Error("followed untrusted redirect") }))
	defer target.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 302) }))
	defer proxy.Close()
	c := &clusterNodeClient{client: proxy.Client(), baseURL: proxy.URL, bearerToken: "private-reader"}
	var snapshot dnsfacts.Snapshot
	if err := readDNSPodSnapshot(context.Background(), c, "/proxy", &snapshot); err == nil || called {
		t.Fatal("redirect accepted")
	}
}

func TestDNSRuntimePortRejectsForeignAndAmbiguousMetadata(t *testing.T) {
	f := newDNSBackendFixture(t)
	f.pod.Spec.Containers = []corev1.Container{{Ports: []corev1.ContainerPort{{ContainerPort: 53, Protocol: corev1.ProtocolTCP}, {ContainerPort: 53, Protocol: corev1.ProtocolUDP}}, ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromInt32(8123)}}}}}
	if p, ok := dnsBackendObservationPort(f.pod, f.svc); !ok || p != 8123 {
		t.Fatal(p, ok)
	}
	for _, mutate := range []func(*corev1.Pod){func(p *corev1.Pod) { p.Spec.Containers[0].ReadinessProbe.HTTPGet.Host = "foreign" }, func(p *corev1.Pod) { p.Spec.Containers[0].ReadinessProbe.HTTPGet.Scheme = corev1.URISchemeHTTPS }, func(p *corev1.Pod) { p.Spec.Containers[0].ReadinessProbe.HTTPGet.Port = intstr.FromString("missing") }, func(p *corev1.Pod) { p.Spec.Containers[0].Ports = p.Spec.Containers[0].Ports[:1] }} {
		p := f.pod.DeepCopy()
		mutate(p)
		if _, ok := dnsBackendObservationPort(*p, f.svc); ok {
			t.Fatal("invalid observation port accepted")
		}
	}
}
