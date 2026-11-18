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
	"fugue/internal/edgetopology"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformcontrol"
	"fugue/internal/routeprobe"
	"fugue/internal/store"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestDNSRuntimeFactsChoosePublicBackendAcrossRetainedAuthorities(t *testing.T) {
	for _, tc := range []struct{ group, scope string }{{"edge-group-old", "global"}, {"cell-a", "global"}, {"cell-b", "global"}, {"cell-a", "authority-cell:cell-a"}} {
		t.Run(tc.group+"/"+tc.scope, func(t *testing.T) {
			selectedGroup := tc.group
			path := t.TempDir() + "/state.json"
			st := store.New(path)
			if err := st.Init(); err != nil {
				t.Fatal(err)
			}
			s := NewServer(st, auth.New(st, "facts-admin"), nil, ServerConfig{BundleSigningKey: "synthetic-facts-key", BundleSigningKeyID: "key"})
			f := newDNSBackendFixture(t)
			f.setAuthority(platformcontrol.ConsumerAuthorityID(selectedGroup))
			f.claims.ScopeKey = tc.scope
			f.pod.Annotations[platformConsumerIdentityAnnotation] = strings.ReplaceAll(f.pod.Annotations[platformConsumerIdentityAnnotation], `"scope_key":"global"`, `"scope_key":"`+tc.scope+`"`)
			if _, err := st.UpdateDNSHeartbeat(model.DNSNode{ID: f.claims.NodeID, EdgeGroupID: selectedGroup, Zone: "example.test", PublicIPv4: "8.8.8.8"}); err != nil {
				t.Fatal(err)
			}
			if tc.scope == "global" {
				seedVerifiedDNSDelegationFixture(t, s, "example.test")
			} else {
				seedScopedDNSFactsFixture(t, s, f.claims.NodeID, selectedGroup)
			}
			parent, release, found, err := st.GetActivePlatformArtifact(model.PlatformArtifactKindReleaseSet, tc.scope, "gray")
			if err != nil || !found {
				t.Fatal("missing publication", err)
			}
			sets, err := s.currentReleaseSetExpectations(parent)
			if err != nil {
				t.Fatal(err)
			}
			var selectedSet model.PlatformExpectedConsumerSet
			for _, set := range sets {
				if set.ArtifactKind == model.PlatformArtifactKindDNSAnswerBundle {
					selectedSet = set
				}
			}
			child, err := s.consumerAssignmentChild(parent, selectedSet.ArtifactKind)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			ring := platformcontrol.PlatformComponentIdentityKeyring{ActiveKeyID: "test", Keys: map[string]string{"test": "synthetic-identity"}}
			accept := func(claims platformcontrol.PlatformComponentIdentityClaims, set model.PlatformExpectedConsumerSet, sequence, generation, fence int64) {
				t.Helper()
				token, err := platformcontrol.IssuePlatformComponentIdentity(ring, claims, now, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				claims, err = platformcontrol.ParsePlatformComponentIdentity(ring, token, now)
				if err != nil {
					t.Fatal(err)
				}
				h := trustedPlatformHeartbeatRequest(t, claims, set, now, sequence, generation, fence, "independent-authority-receipt")
				h.LKGGeneration = set.ExpectedGeneration
				h.CompatibilityCapabilities = []string{platformcontrol.TrafficReleaseCapabilityV1}
				h.EvidenceHash, _ = platformcontrol.ComputePlatformConsumerHeartbeatEvidenceHash(h)
				if _, err := st.AcceptTrustedPlatformConsumerHeartbeat(claims, set.ID, h, now, platformcontrol.PlatformConsumerHeartbeatValidationPolicy{}); err != nil {
					t.Fatal(err)
				}
			}
			accept(f.claims, selectedSet, 20, child.GenerationSequence, release.FencingToken)
			for _, retained := range []struct{ group, scope string }{{"edge-group-old", "global"}, {"cell-a", "global"}, {"cell-a", "authority-cell:cell-a"}, {"cell-b", "global"}, {"cell-b", "authority-cell:cell-b"}} {
				group := retained.group
				if group == selectedGroup && retained.scope == tc.scope {
					continue
				}
				candidate := newDNSBackendFixture(t)
				candidate.setAuthority(platformcontrol.ConsumerAuthorityID(group))
				candidate.claims.ScopeKey = retained.scope
				candidate.pod.Annotations[platformConsumerIdentityAnnotation] = strings.ReplaceAll(candidate.pod.Annotations[platformConsumerIdentityAnnotation], `"scope_key":"global"`, `"scope_key":"`+retained.scope+`"`)
				suffix := "-global"
				if retained.scope != "global" {
					suffix = "-scoped"
				}
				candidate.pod.Name = "candidate-" + group + suffix
				candidate.pod.UID = types.UID(candidate.pod.Name)
				candidate.pod.Labels["app"] = "candidate"
				candidate.claims.CredentialID = "kubernetes:" + candidate.pod.Namespace + ":" + candidate.pod.Spec.ServiceAccountName + ":" + string(candidate.pod.UID)
				f.extraPods = append(f.extraPods, candidate.pod)
				set, err := platformcontrol.BuildExpectedConsumerSet(platformcontrol.ExpectedConsumerSetBuildRequest{ReleaseSetID: "retained-" + group + suffix, ArtifactKind: model.PlatformArtifactKindDNSAnswerBundle, Generation: "retained-generation", ScopeKey: candidate.claims.ScopeKey, PreparedAt: now, Topology: platformcontrol.ExpectedConsumerTopology{DNSNodes: []model.DNSNode{{ID: f.claims.NodeID, EdgeGroupID: group}}}})
				if err != nil {
					t.Fatal(err)
				}
				if _, err = st.CreatePlatformExpectedConsumerSet(set); err != nil {
					t.Fatal(err)
				}
				// A larger independent cursor never supersedes the selected Pod.
				accept(candidate.claims, set, 100, 100, 100)
			}
			f.pod.Spec.Containers = []corev1.Container{{Name: "resolver", Ports: []corev1.ContainerPort{{ContainerPort: 53, Protocol: corev1.ProtocolTCP}, {ContainerPort: 53, Protocol: corev1.ProtocolUDP}, {Name: "observation", ContainerPort: 8081, Protocol: corev1.ProtocolTCP}}, ReadinessProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromString("observation")}}}}}
			var snapshot dnsfacts.Snapshot
			reads := 0
			f.proxyHandler = func(w http.ResponseWriter, r *http.Request) { reads++; json.NewEncoder(w).Encode(snapshot) }
			f.install(t, s)
			source, err := s.currentDNSFactSource(context.Background(), f.claims.NodeID)
			if err != nil || source.claims.AuthorityID != f.claims.AuthorityID || source.group != selectedGroup {
				t.Fatal("wrong authority selected", source.group, err)
			}
			digest, _ := platformconfig.Digest(source.payload.ReadinessPlan)
			snapshot = dnsfacts.Snapshot{Schema: dnsfacts.Schema, NodeID: f.claims.NodeID, EdgeGroupID: selectedGroup, Assignment: source.lookup.Assignment, ParentDigest: source.parent.ContentHash, RouteArtifactID: source.routeID, PlanDigest: digest, ObservedAt: now, EvaluatedAt: now, CheckpointValidUntil: now.Add(time.Hour), Ready: true, Facts: []dnsfacts.Probe{}}
			before, _ := os.ReadFile(path)
			consumers, err := st.ListPlatformConsumers(selectedSet.ArtifactKind, selectedSet.ScopeKey)
			if err != nil {
				t.Fatal(err)
			}
			binding := s.platformConvergenceBinding(selectedSet)
			if status := s.evaluateLiveConsumerConvergence(context.Background(), selectedSet, consumers, binding); !status.Pass || status.RequiredPassing != 1 {
				t.Fatal("selected authority did not converge", status)
			}
			response, err := s.readPlatformDNSRuntimeFacts(context.Background(), f.claims.NodeID)
			if err != nil || !response.Ready || response.Backend.PodUID != string(f.pod.UID) || reads != 1 {
				t.Fatal("selected backend facts unavailable", response.Backend, err, reads)
			}
			inventory := []model.DNSNode{{ID: f.claims.NodeID, EdgeGroupID: selectedGroup, Healthy: true, ServingGeneration: "legacy"}}
			projected, err := s.dnsInventoryServingFacts(context.Background(), inventory)
			if err != nil || len(projected) != 1 || !projected[0].Healthy || projected[0].ServingObservation == nil || projected[0].ServingObservation.BackendPodUID != string(f.pod.UID) || reads != 2 {
				t.Fatal("inventory missed the selected scoped consumer", projected, err)
			}
			f.pod.Labels["app"] = "unselected"
			if status := s.evaluateLiveConsumerConvergence(context.Background(), selectedSet, consumers, binding); status.Pass || status.RequiredPassing != 0 {
				t.Fatal("unselected authority retained convergence", status)
			}
			if _, err := s.readPlatformDNSRuntimeFacts(context.Background(), f.claims.NodeID); err == nil {
				t.Fatal("no selected backend reused retained receipt")
			}
			rows := captureReceipts(t, s)
			foundSourceRejection := false
			for _, event := range rows[len(rows)-1].Events {
				foundSourceRejection = foundSourceRejection || event.Stage == "source_rejected" && event.Rejection == "no_current_source"
			}
			if !foundSourceRejection {
				t.Fatal("source selection failure lost its bounded reason")
			}
			projected, err = s.dnsInventoryServingFacts(context.Background(), inventory)
			if err != nil || len(projected) != 1 || projected[0].Healthy || projected[0].ServingGeneration != "" || projected[0].ServingObservation == nil || projected[0].ServingObservation.State != "unknown" {
				t.Fatal("unavailable scoped consumer fell back to legacy health", projected, err)
			}
			if reads != 2 {
				t.Fatal("unselected Pod was proxied")
			}
			after, _ := os.ReadFile(path)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("read changed persistent facts or cursors")
			}
		})
	}
}

func seedScopedDNSFactsFixture(t *testing.T, s *Server, node, cell string) {
	t.Helper()
	now := time.Now().UTC()
	scope := platformconfig.AuthorityCellScope(cell)
	intent := platformconfig.PlatformIntent{Scope: scope, AuthorityCellID: cell, Generation: "cell-facts-intent",
		EdgeTopology: &edgetopology.Intent{SchemaVersion: edgetopology.SchemaVersion, Cells: []edgetopology.AuthorityCell{{ID: cell}}, Pools: []edgetopology.ServingPool{{ID: "pool-public"}}, Edges: []edgetopology.Edge{{ID: "edge-a", AuthorityCellID: cell, ServingPoolIDs: []string{"pool-public"}, Capabilities: []string{"http", "tls"}, FailureDomains: map[string]string{"host": "node-a"}}}},
		DNSConsumers: []platformconfig.DNSConsumerIntent{{NodeID: node, EdgeGroupID: cell, Zones: []string{"example.test"}, ProbeLabel: "probe", ProbeTTL: 60}},
		Routes:       []platformconfig.RouteIntent{{Hostname: "disabled.example.test", UpstreamURL: "http://origin:8080", Enabled: false}}}
	topology, err := platformconfig.TrafficConsumerTopologyFromIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := platformconfig.Digest(topology)
	policy := platformconfig.PolicySnapshot{Scope: scope, AuthorityCellID: cell, ConsumerTopologyDigest: digest, Generation: "cell-facts-policy",
		TrafficRolloutCohorts: []platformconfig.TrafficRolloutCohort{{ID: "complete", EdgeGroupIDs: []string{cell}}},
		DNSReadiness:          &platformconfig.ReadinessProbePolicy{ProbeIntervalSeconds: 30, ProbeTimeoutSeconds: 5, FactFreshnessSeconds: 120, MaxConcurrency: 8, MaxProbes: 4096},
		DNSAuthorities:        []platformconfig.DNSAuthorityPolicy{{NodeID: node, Zone: "example.test", Nameservers: []string{"ns.example.test"}, TTLSeconds: 60, RefreshSeconds: 300, RetrySeconds: 60, ExpireSeconds: 3600}},
		DNSClientPolicies:     []platformconfig.DNSClientPolicy{{NodeID: node}}}
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: intent, Policy: policy, RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &now, DNSConsumers: []platformconfig.DNSConsumerObservation{{NodeID: node, EdgeGroupID: cell, A: []string{"8.8.8.8"}, ObservedAt: now}}}})
	if err != nil {
		t.Fatal(err)
	}
	c, err := s.materializePlatformCompilation(context.Background(), compiled, platformProducerPrincipal(), nil)
	if err != nil {
		t.Fatal(err)
	}
	_, gray, _, _, err := s.store.ReleasePlatformArtifact(c.ReleaseArtifact.ID, model.PlatformArtifactReleaseRequest{ReleaseChannel: "gray", CanaryRuleRef: "cohort=complete"}, platformProducerPrincipal())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.preparePlatformReleaseSetConsumers(context.Background(), model.Principal{}, c.ReleaseArtifact, gray); err != nil {
		t.Fatal(err)
	}
}

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
	source, err := s.currentDNSFactSource(context.Background(), f.claims.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("assignment rejection receipts", func(t *testing.T) {
		baseline, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			reason string
			change func(*model.PlatformConsumerInstance)
		}{
			{"", func(*model.PlatformConsumerInstance) {}},
			{"fact_expected_set", func(f *model.PlatformConsumerInstance) { f.ExpectedConsumerSetID = "private-foreign-set" }},
			{"fact_release_set", func(f *model.PlatformConsumerInstance) { f.ReleaseSetID = "private-foreign-parent" }},
			{"fact_fence", func(f *model.PlatformConsumerInstance) { f.FencingToken++ }},
			{"fact_generation_sequence", func(f *model.PlatformConsumerInstance) { f.GenerationSequence++ }},
			{"fact_heartbeat_missing", func(f *model.PlatformConsumerInstance) { f.LastHeartbeatAt = time.Time{} }},
			{"fact_heartbeat_future", func(f *model.PlatformConsumerInstance) { f.LastHeartbeatAt = time.Now().Add(time.Hour) }},
			{"fact_heartbeat_stale", func(f *model.PlatformConsumerInstance) { f.LastHeartbeatAt = time.Now().Add(-24 * time.Hour) }},
		} {
			t.Run(tc.reason, func(t *testing.T) {
				fact := source.consumer
				tc.change(&fact)
				ctx, trace := s.beginCaptureTrace(context.Background(), "dns-runtime-facts")
				_, err := s.dnsFactSourceForConsumer(ctx, f.claims.NodeID, fact, source.claims)
				s.finishCaptureTrace(trace, "test", err, 0, 0)
				if (err != nil) != (tc.reason != "") || err != nil && err != errDNSRuntimeFacts {
					t.Fatal("diagnostics changed the rejection contract", err)
				}
				rows := captureReceipts(t, s)
				events := rows[len(rows)-1].Events
				if tc.reason == "" {
					if len(events) != 0 {
						t.Fatal("successful source invented a rejection", events)
					}
				} else if len(events) != 1 || events[0].Stage != "source_assignment_rejected" || events[0].Rejection != tc.reason {
					t.Fatal("assignment failure lost its fixed reason", events)
				}
				if tc.reason != "" && !reflect.DeepEqual(events[0].DNSAuthority, captureDNSAuthorityEvidence(source.lookup.Assignment, fact)) {
					t.Fatal("assignment rejection did not retain the exact compared heartbeat and expectation", events)
				}
				raw, _ := json.Marshal(events)
				if strings.Contains(string(raw), "private-foreign") {
					t.Fatal("receipt retained a private identity")
				}
			})
		}
		after, err := os.ReadFile(path)
		if err != nil || !reflect.DeepEqual(baseline, after) {
			t.Fatal("diagnostics mutated durable state", err)
		}
	})
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
		rows := captureReceipts(t, s)
		phases := map[string]uint64{}
		for _, timing := range rows[len(rows)-1].DNSRead {
			if timing.Stage == "total" {
				phases[timing.Phase] += timing.Calls
			}
			if !timing.HasDeadline || timing.ElapsedMillis < 0 || timing.MaxMillis < 0 || timing.LastFinishOffset < timing.FirstStartOffset {
				t.Fatal("invalid same-request timing", timing)
			}
		}
		if !reflect.DeepEqual(phases, map[string]uint64{"initial_source": 1, "backend": 1, "authority_recheck": 1, "validation": 1}) {
			t.Fatal("successful read lost a phase", phases)
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
			rows := captureReceipts(t, s)
			if len(rows[len(rows)-1].DNSRead) == 0 {
				t.Fatal("failure or known not-ready result lost timing", scenario)
			}
			if scenario == "publication changed" || scenario == "artifact corrupt" {
				rows := captureReceipts(t, s)
				found := false
				for _, event := range rows[len(rows)-1].Events {
					found = found || event.Stage == "authority_recheck_rejected" && event.Rejection == "source_unavailable"
				}
				if !found {
					t.Fatal("recheck lost the failed source predicate", rows[len(rows)-1])
				}
			}
			if reason := (map[string]string{"bad schema": "schema", "bad plan": "plan_digest", "foreign assignment": "assignment"})[scenario]; reason != "" {
				rows := captureReceipts(t, s)
				found := false
				for _, event := range rows[len(rows)-1].Events {
					found = found || event.Stage == "snapshot_validation_rejected" && event.Rejection == reason
					if scenario == "foreign assignment" && event.Stage == "snapshot_validation_rejected" && !reflect.DeepEqual(event.Mismatches, []string{"fencing_token"}) {
						t.Fatal("assignment mismatch lost its exact field", event)
					}
					if scenario == "foreign assignment" && event.Stage == "snapshot_validation_rejected" {
						pair := event.DNSAuthority
						if pair == nil || pair.Expected != captureDNSAssignmentReference(source.lookup.Assignment) || pair.Snapshot == nil || *pair.Snapshot != captureDNSAssignmentReference(snapshot.Assignment) {
							t.Fatal("snapshot rejection lost its exact compared assignments", pair)
						}
					}
				}
				if !found {
					t.Fatal("HTTP failure lost its bounded validation reason", reason)
				}
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

func TestDNSAssignmentMismatchFieldsCoverEveryFieldWithoutValues(t *testing.T) {
	var expected model.PlatformConsumerAssignment
	typ := reflect.TypeOf(expected)
	if typ.NumField() != 17 {
		t.Fatal("new assignment fields need bounded diagnostic coverage", typ.NumField())
	}
	if fields := dnsAssignmentMismatchFields(expected, expected); len(fields) != 0 {
		t.Fatal("equal assignment invented differences", fields)
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		t.Run(field.Name, func(t *testing.T) {
			observed := expected
			value := reflect.ValueOf(&observed).Elem().Field(i)
			switch value.Kind() {
			case reflect.String:
				value.SetString("private-assignment-value")
			case reflect.Int64:
				value.SetInt(1)
			case reflect.Slice:
				value.Set(reflect.ValueOf([]string{})) // nil and empty are not DeepEqual.
			default:
				value.Set(reflect.ValueOf(time.Now().UTC()))
			}
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if fields := dnsAssignmentMismatchFields(observed, expected); !reflect.DeepEqual(fields, []string{name}) {
				t.Fatal("missing, misnamed or private mismatch field", fields)
			}
			initial := dnsFactSource{lookup: consumerArtifactLookup{Assignment: expected}}
			current := dnsFactSource{lookup: consumerArtifactLookup{Assignment: observed}}
			event := dnsAuthorityRecheckRejection(context.Background(), initial, current, nil)
			if event.Rejection != "assignment" || !reflect.DeepEqual(event.Mismatches, []string{name}) {
				t.Fatal("authority recheck lost an exact assignment difference", event)
			}
		})
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
	for _, scenario := range []string{"valid", "expired proof", "negative", "missing", "duplicate", "unknown probe", "wrong digest", "wrong edge", "wrong group", "wrong state", "foreign release", "future", "renewed validity", "checkpoint extended", "dualstack absent", "schema", "node_identity", "group_identity", "parent_digest", "route_artifact", "plan_digest", "assignment", "observed_at_missing", "evaluated_before_observation", "evaluated_in_future", "checkpoint_deadline_missing", "fact_count_budget", "readiness_contract_missing", "proof_version_missing", "proof_observed_at_missing", "proof_publication_missing"} {
		t.Run(scenario, func(t *testing.T) {
			raw, _ := json.Marshal(base)
			var snapshot dnsfacts.Snapshot
			json.Unmarshal(raw, &snapshot)
			input := source
			wantReady, wantErr := false, false
			switch scenario {
			case "schema":
				snapshot.Schema = "private-invalid-schema"
				wantErr = true
			case "node_identity":
				snapshot.NodeID = "private-other-node"
				wantErr = true
			case "group_identity":
				snapshot.EdgeGroupID = "private-other-group"
				wantErr = true
			case "parent_digest":
				snapshot.ParentDigest = "private-other-parent"
				wantErr = true
			case "route_artifact":
				snapshot.RouteArtifactID = "private-other-route"
				wantErr = true
			case "plan_digest":
				snapshot.PlanDigest = "private-other-plan"
				wantErr = true
			case "assignment":
				snapshot.Assignment.FencingToken++
				wantErr = true
			case "observed_at_missing":
				snapshot.ObservedAt = time.Time{}
				wantErr = true
			case "evaluated_before_observation":
				snapshot.EvaluatedAt = snapshot.ObservedAt.Add(-time.Second)
				wantErr = true
			case "evaluated_in_future":
				snapshot.EvaluatedAt = now.Add(time.Second)
				wantErr = true
			case "checkpoint_deadline_missing":
				snapshot.CheckpointValidUntil = time.Time{}
				wantErr = true
			case "fact_count_budget":
				snapshot.Facts = make([]dnsfacts.Probe, 4097)
				wantErr = true
			case "readiness_contract_missing":
				input.payload.ReadinessPlan = nil
				wantErr = true
			case "proof_version_missing":
				snapshot.Facts[0].Proof.Version = ""
				wantErr = true
			case "proof_observed_at_missing":
				snapshot.Facts[0].Proof.CheckedAt = time.Time{}
				wantErr = true
			case "proof_publication_missing":
				snapshot.Facts[0].Proof.TrafficRelease = nil
				wantErr = true
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
			var reasons []string
			observedIDs, observedReady, observedErr := evaluateDNSRuntimeSnapshotWithRejection(snapshot, input, now, func(reason string) { reasons = append(reasons, reason) })
			after, _ := json.Marshal(snapshot)
			if !reflect.DeepEqual(ids, observedIDs) || ready != observedReady || err != observedErr || !reflect.DeepEqual(before, after) {
				t.Fatal("rejection diagnostics changed validation or evidence")
			}
			if wantErr {
				wantReason := map[string]string{"duplicate": "duplicate_probe", "unknown probe": "probe_not_in_plan", "wrong digest": "proof_requirement", "wrong edge": "proof_requirement", "wrong group": "proof_requirement", "wrong state": "proof_state", "foreign release": "proof_publication_binding", "future": "proof_observed_after_evaluation", "renewed validity": "proof_deadline_exceeds_policy", "checkpoint extended": "checkpoint_deadline_exceeds_policy"}[scenario]
				if wantReason == "" {
					wantReason = scenario
				}
				if !reflect.DeepEqual(reasons, []string{wantReason}) {
					t.Fatalf("imprecise or unsafe rejection: got %v, want %s", reasons, wantReason)
				}
			} else if len(reasons) != 0 {
				t.Fatal("valid, negative or expired evidence invented a validation rejection", reasons)
			}
			if (err != nil) != wantErr || ready != wantReady {
				t.Fatal(ids, ready, err)
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
