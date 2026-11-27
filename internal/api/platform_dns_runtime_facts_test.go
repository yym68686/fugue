package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func TestDecodePodObservationBody(t *testing.T) {
	const valid = `{"ready":true}`
	for _, tc := range []struct {
		name, input, failure string
		budget               int64
		transport            bool
	}{
		{name: "exact budget", input: valid, budget: int64(len(valid))},
		{name: "exact budget with trailing whitespace", input: valid + " \r\n", budget: int64(len(valid) + 3)},
		{name: "oversized whitespace", input: valid + " ", budget: int64(len(valid)), failure: "response_budget"},
		{name: "unknown field", input: `{"ready":true,"extra":1}`, budget: 100, failure: "decode"},
		{name: "trailing value", input: valid + " {}", budget: 100, failure: "decode"},
		{name: "truncated JSON", input: valid[:len(valid)-1], budget: 100, failure: "decode"},
		{name: "oversized malformed body", input: "!" + strings.Repeat(" ", 2048), budget: 1024, failure: "response_budget"},
		{name: "truncated body transport", input: valid, budget: 100, transport: true, failure: captureErrorClass(io.ErrUnexpectedEOF)},
		{name: "malformed body with transport error", input: "!", budget: 100, transport: true, failure: captureErrorClass(io.ErrUnexpectedEOF)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out struct {
				Ready bool `json:"ready"`
			}
			var body io.Reader = strings.NewReader(tc.input)
			if tc.transport {
				body = io.MultiReader(body, observationTruncatedReader{})
			}
			if failure := decodePodObservationBody(body, &out, tc.budget); failure != tc.failure {
				t.Fatalf("failure = %q, want %q", failure, tc.failure)
			} else if failure == "" && !out.Ready {
				t.Fatal("ready not decoded")
			}
		})
	}
}

type observationTruncatedReader struct{}

func (observationTruncatedReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestReadPodObservationAppliesBudgetAfterGzip(t *testing.T) {
	payload := `{"ready":true}` + strings.Repeat(" ", 512)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := io.WriteString(writer, payload); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept-Encoding") != "gzip" {
			t.Error("transport did not negotiate gzip")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(compressed.Bytes())
	}))
	defer upstream.Close()
	c := &clusterNodeClient{baseURL: upstream.URL, client: upstream.Client()}
	var out struct {
		Ready bool `json:"ready"`
	}
	if err := readPodObservation(t.Context(), c, "/", &out, int64(len(payload)), nil); err != nil || !out.Ready {
		t.Fatalf("exact decoded limit did not accept gzip response: %v", err)
	}
	err := readPodObservation(t.Context(), c, "/", &out, int64(len(payload)-1), nil)
	var failure *dnsObservationReadFailure
	if !errors.As(err, &failure) || failure.class != "response_budget" {
		t.Fatalf("decoded limit did not reject gzip response: %v", err)
	}
}

func BenchmarkDecodePodObservationBody(b *testing.B) {
	// Representative proxy response size, without production data.
	payload := `{"facts":[` + strings.Repeat(`{"ready":true,"value":"synthetic sample"},`, 3200) + `{"ready":true}]}`
	type response struct {
		Facts []struct {
			Ready bool   `json:"ready"`
			Value string `json:"value"`
		} `json:"facts"`
	}
	const budget = 32 << 20
	b.Run("read-all", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(payload)))
		for b.Loop() {
			raw, err := io.ReadAll(io.LimitReader(strings.NewReader(payload), budget+1))
			if err != nil || len(raw) > budget {
				b.Fatal(err)
			}
			var out response
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&out); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
				b.Fatal(err)
			}
		}
	})
	b.Run("bounded-decoder", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(payload)))
		for b.Loop() {
			var out response
			if failure := decodePodObservationBody(strings.NewReader(payload), &out, budget); failure != "" {
				b.Fatal(failure)
			}
		}
	})
}

func TestDNSRuntimeFactsChoosePublicBackendAcrossRetainedAuthorities(t *testing.T) {
	for _, tc := range []struct{ group, scope string }{{"edge-group-old", "global"}, {"cell-a", "global"}, {"cell-b", "global"}, {"cell-a", "authority-cell:cell-a"}} {
		t.Run(tc.group+"/"+tc.scope, func(t *testing.T) {
			selectedGroup := tc.group
			const path = "dns-runtime-facts"
			st := newStoreTestDatabase(t, path)
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
			db := openStoreTestSQL(t, path)
			before := convergenceStoreFacts(t, db)
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
			var foundSourceRejection, foundSelectorRejection, foundTruncation bool
			refusals := 0
			for _, event := range rows[len(rows)-1].Events {
				foundSourceRejection = foundSourceRejection || event.Stage == "source_rejected" && event.Rejection == "no_current_source"
				if event.Stage == "source_backend_rejected" {
					refusals++
					foundSelectorRejection = foundSelectorRejection || event.Rejection == "public_service_selector_mismatch" && event.HTTPStatus == http.StatusConflict
				}
				foundTruncation = foundTruncation || event.Stage == "source_backend_rejections_truncated"
			}
			if !foundSourceRejection || !foundSelectorRejection || !foundTruncation || refusals != 4 {
				t.Fatal("source selection failure lost its bounded rejection sample", rows[len(rows)-1].Events)
			}
			projected, err = s.dnsInventoryServingFacts(context.Background(), inventory)
			if err != nil || len(projected) != 1 || projected[0].Healthy || projected[0].ServingGeneration != "" || projected[0].ServingObservation == nil || projected[0].ServingObservation.State != "unknown" {
				t.Fatal("unavailable scoped consumer fell back to legacy health", projected, err)
			}
			if reads != 2 {
				t.Fatal("unselected Pod was proxied")
			}
			after := convergenceStoreFacts(t, db)
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
