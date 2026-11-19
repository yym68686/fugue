package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/agentedge"
	"fugue/internal/dnsfacts"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
)

func captureReceipts(t *testing.T, s *Server) []captureReceipt {
	t.Helper()
	value, err := s.OperationSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return value.(map[string]any)["captures"].(map[string]any)["receipts"].([]captureReceipt)
}

func TestAgentPublicationRecheckDoesNotFabricateObservedBinding(t *testing.T) {
	source := agentCellSource{parent: model.PlatformArtifact{ID: "private-parent", ContentHash: "private-digest"},
		release: model.PlatformArtifactRelease{ID: "private-release", ReleaseChannel: "full", FencingToken: 9}}
	for _, tc := range []struct {
		found      bool
		err        error
		rejection  string
		errorClass string
	}{
		{true, context.Canceled, "selection_read_error", "canceled"},
		{false, nil, "selection_missing", ""},
	} {
		event := agentPublicationRecheckFailure("edge-a", source, source.parent, source.release, tc.found, tc.err)
		if event.Stage != "cell_publication_changed" || event.Rejection != tc.rejection || event.ErrorClass != tc.errorClass ||
			event.Publication == nil || event.Publication.Observed != nil || event.Publication.Expected.FencingToken != 9 {
			t.Fatal("incorrect unsuccessful selection receipt", event)
		}
		raw, err := json.Marshal(event)
		if err != nil || strings.Contains(string(raw), "private") || !strings.Contains(string(raw), captureIdentityHash(source.parent.ContentHash)) {
			t.Fatal("selection receipt leaked raw input or lost the exact digest fingerprint", string(raw), err)
		}
	}
}

func TestDNSCaptureFailuresSurviveSuccessfulReadsWithDetachedBoundedEvidence(t *testing.T) {
	s := &Server{}
	now := time.Now().UTC()
	a := model.PlatformConsumerAssignment{ExpectedConsumerSetID: "private-expectation", ReleaseSetID: "private-parent",
		ArtifactReleaseID: "private-release", ArtifactID: "private-artifact", ReleaseChannel: "full", Revision: 8,
		GenerationSequence: 20, FencingToken: 7, HeartbeatDeadline: now.Add(time.Minute), ConvergenceDeadline: now.Add(2 * time.Minute)}
	fact := model.PlatformConsumerInstance{ExpectedConsumerSetID: "private-old-expectation", ReleaseSetID: a.ReleaseSetID,
		GenerationSequence: 19, FencingToken: 6, LastHeartbeatAt: now, CredentialID: "private-credential", Nonce: "private-nonce", LastError: "private-error"}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range dnsCaptureFailureLimit {
				ctx, trace := s.beginCaptureTrace(context.Background(), "dns-runtime-facts")
				pair := captureDNSAuthorityEvidence(a, fact)
				snapshot := captureDNSAssignmentReference(a)
				snapshot.FencingToken--
				pair.Snapshot = &snapshot
				observeCapture(ctx, captureEvent{Stage: "snapshot_validation_rejected", Rejection: "assignment", DNSAuthority: pair})
				pair.Expected.Revision, pair.Heartbeat.FencingToken, snapshot.FencingToken = 999, 999, 999
				s.finishCaptureTrace(trace, "snapshot_validation", errDNSRuntimeFacts, 0, 0)
				_, _ = s.OperationSnapshot(context.Background())
			}
		}()
	}
	wg.Wait()
	for range captureReceiptLimit + 1 {
		_, trace := s.beginCaptureTrace(context.Background(), "dns-runtime-facts")
		s.finishCaptureTrace(trace, "ready", nil, 0, 0)
	}
	snapshot := s.captureSnapshot().(map[string]any)
	failures := snapshot["dns_failures"].(map[string]any)
	rows := failures["receipts"].([]captureReceipt)
	if failures["total"] != uint64(4*dnsCaptureFailureLimit) || failures["evicted"] != uint64(3*dnsCaptureFailureLimit) || len(rows) != dnsCaptureFailureLimit {
		t.Fatal("DNS failure retention lost accounting or bounds", failures)
	}
	for _, row := range rows {
		pair := row.Events[0].DNSAuthority
		if pair.Expected != captureDNSAssignmentReference(a) || pair.Heartbeat.ExpectedSetHash != captureIdentityHash(fact.ExpectedConsumerSetID) ||
			pair.Heartbeat.FencingToken != 6 || !pair.Heartbeat.LastHeartbeatAt.Equal(now) || pair.Snapshot == nil || pair.Snapshot.FencingToken != 6 {
			t.Fatal("failed comparison evidence changed after capture", pair)
		}
	}
	for _, row := range snapshot["receipts"].([]captureReceipt) {
		if row.ErrorClass != "" {
			t.Fatal("successful reads failed to exercise ordinary-ring eviction")
		}
	}
	raw, _ := json.Marshal(snapshot)
	if len(raw) > 128<<10 || strings.Contains(string(raw), "private-") {
		t.Fatal("DNS metadata leaked raw inputs or exceeded budget", len(raw))
	}
	sequence := rows[0].Sequence
	_, trace := s.beginCaptureTrace(context.Background(), "dns-runtime-facts")
	s.finishCaptureTrace(trace, "source", errDNSRuntimeFacts, 0, 0)
	if rows[0].Sequence != sequence {
		t.Fatal("DNS snapshot shares eviction backing array")
	}
}

func TestCaptureReceiptsBoundedDetachedAndConcurrent(t *testing.T) {
	s := &Server{}
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iteration := range 16 {
				ctx, trace := s.beginCaptureTrace(context.Background(), "agent-request")
				for range captureEventLimit + 2 {
					capacity := &captureCapacity{NodeUID: strings.Repeat("x", 129), CPUNanoCores: 123}
					publication := &capturePublicationPair{Expected: capturePublicationReference("expected-set", "expected-route", "expected-release", "full", 9),
						Observed: &capturePublication{ReleaseHash: captureIdentityHash("observed-release"), Channel: "gray", FencingToken: 8}}
					observeCapture(ctx, captureEvent{Stage: "synthetic", Node: strings.Repeat("n", 254), Capacity: capacity, Publication: publication})
					capacity.CPUNanoCores = 999
					publication.Expected.FencingToken, publication.Observed.FencingToken = 999, 999
				}
				s.finishCaptureTrace(trace, "unavailable", errors.New(fmt.Sprintf("private-secret-%d-%d", worker, iteration)), 0, 1)
				_, _ = s.OperationSnapshot(context.Background())
			}
		}()
	}
	wg.Wait()
	snapshot := s.captureSnapshot().(map[string]any)
	rows := snapshot["receipts"].([]captureReceipt)
	if len(rows) != captureReceiptLimit || snapshot["started"] != uint64(128) || snapshot["evicted"] != uint64(128-captureReceiptLimit) {
		t.Fatal("unbounded or unaccounted retention", len(rows), snapshot["started"], snapshot["evicted"])
	}
	failures := snapshot["agent_failures"].(map[string]any)
	if len(failures["receipts"].([]captureReceipt)) != captureReceiptLimit || failures["total"] != uint64(128) || failures["evicted"] != uint64(128-captureReceiptLimit) {
		t.Fatal("concurrent failure retention is unbounded or unaccounted", failures)
	}
	for _, row := range rows {
		if len(row.Events) != captureEventLimit || row.Dropped != 2 || row.Events[0].Capacity.CPUNanoCores != 123 || row.ErrorClass != "unavailable" {
			t.Fatal("event copy or budget lost", row)
		}
		if row.Events[0].Publication.Expected.FencingToken != 9 || row.Events[0].Publication.Observed.FencingToken != 8 {
			t.Fatal("publication pair shares mutable input")
		}
	}
	raw, _ := json.Marshal(snapshot)
	if len(raw) > 1<<20 || strings.Contains(string(raw), "private-secret") || strings.Contains(string(raw), strings.Repeat("x", 129)) {
		t.Fatal("receipt response budget or privacy violated", len(raw))
	}
	oldSequence := rows[0].Sequence
	_, trace := s.beginCaptureTrace(context.Background(), "synthetic")
	s.finishCaptureTrace(trace, "ok", nil, 1, 1)
	if rows[0].Sequence != oldSequence {
		t.Fatal("snapshot shares eviction backing array")
	}
}

func TestAgentRejectedProofRetainsSameReadPublicationPairWithoutChangingDecision(t *testing.T) {
	for _, scenario := range []string{"ok", "different_publication", "transport_error_with_partial_proof", "missing_binding"} {
		t.Run(scenario, func(t *testing.T) {
			f := setupAgentAuthorityFixture(t)
			original := f.s.agentEdgeProbe
			f.s.agentEdgeProbe = func(ctx context.Context, host, path, address, state string, timeout time.Duration) (routeprobe.Proof, error) {
				proof, err := original(ctx, host, path, address, state, timeout)
				if err != nil || scenario == "ok" {
					return proof, err
				}
				if scenario == "missing_binding" {
					proof.TrafficRelease = nil
					return proof, nil
				}
				copy := *proof.TrafficRelease
				copy.ReleaseSetID, copy.RouteArtifactID, copy.ReleaseID = "private-set-identity", "private-route-identity", "private-release-identity"
				copy.ReleaseChannel, copy.FencingToken = "full", 123
				proof.TrafficRelease = &copy
				if scenario == "transport_error_with_partial_proof" {
					return proof, errors.New("private-transport-error")
				}
				return proof, nil
			}
			response := performJSONRequest(t, f.s, http.MethodGet, "/v1/agent/edge-candidates", f.runtimeKey, nil)
			wantStatus := http.StatusServiceUnavailable
			if scenario == "ok" {
				wantStatus = http.StatusOK
			}
			rows := captureReceipts(t, f.s)
			if response.Code != wantStatus || len(rows) != 1 {
				t.Fatal("observation changed grant decision", response.Code, len(rows))
			}
			rejected := 0
			for _, event := range rows[0].Events {
				if event.Stage != "route_proof_rejected" {
					continue
				}
				rejected++
				if event.Publication == nil {
					t.Fatal("same-read publication evidence missing")
				}
				cell := f.topology.Cells[0]
				if event.Node == f.topology.Edges[1].ID {
					cell = f.topology.Cells[1]
				}
				source, err := f.s.agentCellAuthorization(f.policy, f.topology, cell)
				if err != nil {
					t.Fatal(err)
				}
				want := capturePublicationReference(source.parent.ID, source.route.ID, source.release.ID, source.release.ReleaseChannel, source.release.FencingToken)
				if event.Publication.Expected != want {
					t.Fatal("expected evidence is not the captured publication", event.Publication.Expected, want)
				}
				if scenario != "different_publication" {
					if event.Publication.Observed != nil {
						t.Fatal("unavailable proof labeled as observed")
					}
					continue
				}
				observed := event.Publication.Observed
				if observed == nil || observed.ReleaseSetHash != captureIdentityHash("private-set-identity") ||
					observed.RouteArtifactHash != captureIdentityHash("private-route-identity") || observed.ReleaseHash != captureIdentityHash("private-release-identity") ||
					observed.Channel != "full" || observed.FencingToken != 123 {
					t.Fatal("observed evidence is not the rejected proof", observed)
				}
			}
			if scenario == "ok" && rejected != 0 || scenario != "ok" && rejected == 0 {
				t.Fatal("rejection evidence coverage changed", rejected)
			}
			raw, _ := json.Marshal(rows)
			for _, secret := range []string{"private-set-identity", "private-route-identity", "private-release-identity", "private-transport-error", f.runtimeKey, "api.example.test"} {
				if strings.Contains(string(raw), secret) {
					t.Fatal("publication receipt retained private input")
				}
			}
		})
	}
}

func TestPublicationEvidenceHashesAndBoundsUntrustedValues(t *testing.T) {
	if got := captureIdentityHash("abc"); got != "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatal("identity hash contract changed", got)
	}
	if captureIdentityHash("") != "" || captureIdentityHash(strings.Repeat("x", 257)) != "identity_exceeds_budget" || len(captureIdentityHash(strings.Repeat("x", 256))) != 71 {
		t.Fatal("identity hash bounds changed")
	}
	source := agentCellSource{parent: model.PlatformArtifact{ID: "expected-set"}, route: model.PlatformArtifact{ID: "expected-route"},
		release: model.PlatformArtifactRelease{ID: "expected-release", ReleaseChannel: "gray", FencingToken: 10}}
	proof := &model.TrafficReleaseBinding{ReleaseSetID: strings.Repeat("private", 100), RouteArtifactID: "private-route", ReleaseID: "private-release", ReleaseChannel: "private-channel", FencingToken: 11}
	pair := agentPublicationEvidence(source, proof, true)
	if pair.Observed.ReleaseSetHash != "identity_exceeds_budget" || pair.Observed.Channel != "unrecognized" {
		t.Fatal("wire values exceeded diagnostic bounds", pair.Observed)
	}
	raw, _ := json.Marshal(pair)
	if len(raw) > 1024 || strings.Contains(string(raw), "private") {
		t.Fatal("untrusted publication metadata leaked or exceeded bounds", len(raw))
	}
}

func TestAgentCaptureReceiptsPreserveActualRequestFailures(t *testing.T) {
	for _, failure := range []bool{false, true} {
		f := setupAgentAuthorityFixture(t)
		if failure {
			f.s.agentEdgeProbe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
				return routeprobe.Proof{}, errors.New("private-credential-and-path")
			}
		}
		response := performJSONRequest(t, f.s, http.MethodGet, "/v1/agent/edge-candidates", f.runtimeKey, nil)
		wantStatus, wantOutcome := 200, "ok"
		if failure {
			wantStatus, wantOutcome = 503, "grant_constraint"
		}
		rows := captureReceipts(t, f.s)
		if response.Code != wantStatus || len(rows) != 1 || rows[0].Kind != "agent-request" || rows[0].Outcome != wantOutcome {
			t.Fatal("decision or diagnostic attribution changed", response.Code, rows)
		}
		raw, _ := json.Marshal(rows)
		if strings.Contains(string(raw), "private-credential-and-path") || strings.Contains(string(raw), f.runtimeKey) {
			t.Fatal("private response diagnostics leaked")
		}
		if failure && !strings.Contains(string(raw), "route_proof_rejected") {
			t.Fatal("capture discarded candidate failure")
		}
	}
}

func TestCapacityReceiptIncludesOriginalSampleAndUnchangedRejection(t *testing.T) {
	now := time.Now().UTC()
	node, summary := agentCapacityFixture("edge-a", "8.8.8.8", now)
	*summary.Node.CPU.UsageNanoCores = 1_900_000_000
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/stats/summary") {
			_ = json.NewEncoder(w).Encode(summary)
		} else {
			_ = json.NewEncoder(w).Encode(node)
		}
	}))
	defer server.Close()
	s := &Server{}
	ctx, trace := s.beginCaptureTrace(context.Background(), "synthetic")
	_, err := readAgentCapacity(ctx, &clusterNodeClient{baseURL: server.URL, client: server.Client()}, "edge-a", "8.8.8.8",
		agentedge.CapacityPolicy{MaxNodeCPUPercent: 85, MaxNodeMemoryPercent: 85, FactMaxAgeSeconds: 120}, func() time.Time { return now })
	s.finishCaptureTrace(trace, "capacity", err, 0, 1)
	rows := captureReceipts(t, s)
	event := rows[0].Events[0]
	if err == nil || event.Stage != "agent-capacity-pressure" || event.Node != "edge-a" || event.Capacity.CPUNanoCores != 1_900_000_000 ||
		event.Capacity.CPUMilliCores != 2000 || event.Capacity.MaxCPUPercent != 85 || !event.Capacity.CPUTime.Equal(now.Add(-2*time.Second)) || !event.Capacity.ValidUntil.Equal(now.Add(115*time.Second)) {
		t.Fatal("rejection lost authenticated original capacity", err, event)
	}
}

func TestDNSObservationReadFailurePreservesSafeStageWithoutBody(t *testing.T) {
	for _, scenario := range []string{"status", "unsafe_header", "decode", "budget", "truncated", "ok"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch scenario {
				case "status":
					w.Header().Set("X-Fugue-DNS-Observation-Rejection", "state_replaced")
					http.Error(w, "private-password-content", 502)
				case "unsafe_header":
					w.Header().Set("X-Fugue-DNS-Observation-Rejection", "private-password-content")
					http.Error(w, "private-password-content", 502)
				case "decode":
					_, _ = w.Write([]byte("private-password-content"))
				case "budget":
					_, _ = w.Write([]byte(strings.Repeat("private-password-content", 8)))
				case "truncated":
					w.Header().Set("Content-Length", "1000")
					_, _ = w.Write([]byte("{}"))
				default:
					_, _ = w.Write([]byte("{}"))
				}
			}))
			defer server.Close()
			var snapshot dnsfacts.Snapshot
			err := readDNSPodObservation(context.Background(), &clusterNodeClient{baseURL: server.URL, client: server.Client()}, "/", &snapshot, 64)
			if scenario == "ok" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			wantClass := map[string]string{"status": "http_status", "unsafe_header": "http_status", "decode": "decode", "budget": "response_budget", "truncated": "unexpected_eof"}[scenario]
			if !errors.Is(err, errDNSRuntimeFacts) || captureErrorClass(err) != wantClass || strings.Contains(err.Error(), "private-password") {
				t.Fatal("unsafe or imprecise read failure", err, captureErrorClass(err))
			}
			if scenario == "status" && captureHTTPStatus(err) != 502 {
				t.Fatal("HTTP status disappeared")
			}
			if scenario == "status" && captureDNSRejection(err) != "state_replaced" || scenario == "unsafe_header" && captureDNSRejection(err) != "unrecognized" {
				t.Fatal("DNS rejection allowlist failed")
			}
		})
	}
}

func TestDNSObservationReplacementMetadataIsBoundedAndOptional(t *testing.T) {
	for _, scenario := range []string{"valid", "legacy", "unsafe", "duplicate", "wrong_status", "wrong_reason"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				status, reason := http.StatusServiceUnavailable, "state_replaced"
				if scenario == "wrong_status" {
					status = http.StatusBadGateway
				}
				if scenario == "wrong_reason" {
					reason = "listener_changed"
				}
				w.Header().Set("X-Fugue-DNS-Observation-Rejection", reason)
				if scenario != "legacy" {
					w.Header().Set(dnsfacts.ReplacementBaseHeader, dnsfacts.ReplacementRetainedFallback)
					w.Header().Set(dnsfacts.ReplacementSelectionHeader, dnsfacts.ReplacementRenewalToBase)
					w.Header().Set(dnsfacts.ReplacementRenewalHeader, dnsfacts.ReplacementChanged)
				}
				if scenario == "unsafe" {
					w.Header().Set(dnsfacts.ReplacementBaseHeader, "untrusted tenant metadata")
				}
				if scenario == "duplicate" {
					w.Header().Add(dnsfacts.ReplacementRenewalHeader, dnsfacts.ReplacementChanged)
				}
				http.Error(w, "private error body", status)
			}))
			defer server.Close()
			var snapshot dnsfacts.Snapshot
			err := readDNSPodSnapshot(context.Background(), &clusterNodeClient{baseURL: server.URL, client: server.Client()}, "/", &snapshot)
			if !errors.Is(err, errDNSRuntimeFacts) || err.Error() != errDNSRuntimeFacts.Error() || captureErrorClass(err) != "http_status" {
				t.Fatal("diagnostics changed the original rejection", err)
			}
			got := captureDNSStateReplacement(err)
			if scenario == "valid" {
				want := captureDNSReplacement{Base: dnsfacts.ReplacementRetainedFallback, Selection: dnsfacts.ReplacementRenewalToBase, Renewal: dnsfacts.ReplacementChanged}
				if got == nil || *got != want || captureDNSRejection(err) != "state_replaced" {
					t.Fatal("lost the original replacement observation", got)
				}
			} else if got != nil {
				t.Fatal("retained malformed or unrelated replacement details", got)
			}
		})
	}
}
