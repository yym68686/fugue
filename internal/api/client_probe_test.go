package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/clientmeasurement"
	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
	"fugue/internal/store"
)

func clientReportFixture(t *testing.T, server *Server) model.EdgeClientProbeReport {
	t.Helper()
	now, seed := time.Now().UTC().Add(-30*time.Second), strings.Repeat("ab", 32)
	report := model.EdgeClientProbeReport{Schema: "fugue.client-probe-report/v1", Plan: model.EdgeClientProbePlan{Schema: "fugue.client-probe-plan/v1", RoundID: "round-a", ObserverLabel: "authenticated-test-observer"}}
	for _, edge := range []string{"edge-a", "edge-b"} {
		permit, err := clientmeasurement.SignPermit(model.EdgeClientProbePermit{Schema: "fugue.client-probe-permit/v1", RoundID: "round-a", AttemptID: "attempt-" + edge, ObserverID: clientProbeObserver(model.Principal{ActorType: model.ActorTypeBootstrap, ActorID: "bootstrap-admin"}), Hostname: "app.example.test", Path: "/", TrafficClass: "streaming",
			EdgeID: edge, EdgeGroupID: "group-a", Address: "8.8.8.8", RouteDigest: "sha256:" + strings.Repeat("a", 64), BundleVersion: "bundle-a", IssuedAt: now, ExpiresAt: now.Add(2 * time.Minute), BodyBytes: clientmeasurement.BodyBytes, BodySeed: seed, BodySHA256: clientmeasurement.Digest(clientmeasurement.Payload(seed)), TargetEdgeIDs: []string{"edge-a", "edge-b"}}, server.clientProbeKeys())
		if err != nil {
			t.Fatal(err)
		}
		rtt := 50.0
		client := model.EdgeClientNetworkSample{ConnectionID: "connection-" + edge, Slot: "a", Scope: "tcp_peer:203.0.113.0/24", StartedAt: now.Add(time.Second), ObservedAt: now.Add(2 * time.Second), TCPInfoAvailable: true, RTTMS: &rtt,
			Backend: &model.EdgeClientNetworkBackend{Namespace: "system", PodName: "front", PodUID: "pod-a", PodVersion: "10", ServiceName: "public", ServiceUID: "service-a", ServiceVersion: "20", EndpointsDigest: "sha256:" + strings.Repeat("b", 64)}}
		attestation, err := clientmeasurement.SignAttestation(model.EdgeClientProbeAttestation{Schema: "fugue.client-probe-attestation/v1", AttemptID: permit.AttemptID, EdgeID: edge, EdgeGroupID: permit.EdgeGroupID, RouteDigest: permit.RouteDigest, BundleVersion: permit.BundleVersion, ObservedAt: client.ObservedAt, ClientNetwork: client}, server.clientProbeKeys())
		if err != nil {
			t.Fatal(err)
		}
		report.Plan.Permits = append(report.Plan.Permits, permit)
		report.Outcomes = append(report.Outcomes, model.EdgeClientProbeOutcome{AttemptID: permit.AttemptID, StartedAt: now.Add(time.Second), CompletedAt: now.Add(4 * time.Second), BytesReceived: clientmeasurement.BodyBytes, BodySHA256: permit.BodySHA256, BodySeconds: 2, Attestation: &attestation})
	}
	return report
}

func TestClientProbeReportAuthenticatesCompleteRoundAndKeepsLegacyCollectionSeparate(t *testing.T) {
	state := store.New(t.TempDir() + "/state.json")
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	server := NewServer(state, auth.New(state, "synthetic-admin"), nil, ServerConfig{BundleSigningKey: "synthetic-probe-key", BundleSigningKeyID: "key-a"})
	report := clientReportFixture(t, server)
	path := "/v1/admin/edge-quality/client-probes/report"
	for _, edit := range []func(*model.EdgeClientProbeReport){
		func(value *model.EdgeClientProbeReport) { value.Plan.Permits[0].Signature = "forged" },
		func(value *model.EdgeClientProbeReport) { value.Outcomes[0].Attestation.Signature = "forged" },
		func(value *model.EdgeClientProbeReport) { value.Outcomes = value.Outcomes[:1] },
		func(value *model.EdgeClientProbeReport) { value.Outcomes[0].BodySHA256 = "wrong-body" },
	} {
		raw, _ := json.Marshal(report)
		var changed model.EdgeClientProbeReport
		_ = json.Unmarshal(raw, &changed)
		edit(&changed)
		if response := performJSONRequest(t, server, http.MethodPost, path, "synthetic-admin", changed); response.Code == http.StatusOK {
			t.Fatal("forged or incomplete probe report admitted")
		}
	}
	if response := performJSONRequest(t, server, http.MethodPost, path, "invalid-key", report); response.Code != http.StatusUnauthorized {
		t.Fatal(response.Code)
	}
	response := performJSONRequest(t, server, http.MethodPost, path, "synthetic-admin", report)
	if response.Code != http.StatusOK {
		t.Fatal(response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), `"routing_authorized":false`) {
		t.Fatal("measurement retention claimed serving authority")
	}
	if response := performJSONRequest(t, server, http.MethodPost, path, "synthetic-admin", report); response.Code != http.StatusOK {
		t.Fatal("identical report retry failed", response.Body.String())
	}
	report.Outcomes[0].BodySeconds = 1
	if response := performJSONRequest(t, server, http.MethodPost, path, "synthetic-admin", report); response.Code == http.StatusOK {
		t.Fatal("immutable measurement changed on retry")
	}
	reports, err := state.ListEdgeClientProbeReports(context.Background(), "app.example.test", time.Now().Add(-time.Hour), 256)
	if err != nil || len(reports) != 1 || reports[0].Outcomes[0].BodySeconds != 2 {
		t.Fatal(reports, err)
	}
	samples, err := state.ListEdgeNetworkSamples(context.Background(), "app.example.test", time.Now().Add(-time.Hour), 256)
	if err != nil || len(samples) != 0 {
		t.Fatal("new report polluted older socket sample collection", samples, err)
	}
}

func TestClientProbePublicationReconstructsNetworkMetricsAndRejectsOmission(t *testing.T) {
	server := &Server{bundleSigningKey: "synthetic-key", bundleSigningKeyID: "key-a"}
	report := clientReportFixture(t, server)
	now := time.Now().UTC()
	quality := edgequality.DefaultDeliveryNetworkPolicy()
	snapshot := edgequality.Snapshot{Schema: edgequality.Schema, CapturedAt: now, Hostname: "app.example.test", Scope: "global", TrafficClass: "streaming", Policy: quality, ClientProbeReports: []model.EdgeClientProbeReport{report}}
	evidence := dnsserver.QualityAnswerEvidence{EdgeID: "edge-a", Hostname: snapshot.Hostname, Scope: "global"}
	for _, permit := range report.Plan.Permits {
		snapshot.Candidates = append(snapshot.Candidates, edgequality.Candidate{EdgeID: permit.EdgeID, EdgeGroupID: permit.EdgeGroupID})
		evidence.Proofs = append(evidence.Proofs, dnsserver.QualityRouteProof{EdgeID: permit.EdgeID, EdgeGroupID: permit.EdgeGroupID, Hostname: permit.Hostname, Path: permit.Path,
			Proof: routeprobe.Proof{Digest: permit.RouteDigest, Version: "renewed-bundle", CheckedAt: now}})
	}
	bindPhysicalQualityEvidence(&snapshot, evidence)
	observations, err := edgequality.ClientProbeObservations(snapshot)
	if err != nil || len(observations) != 2 || *observations[0].DownloadBPS != float64(clientmeasurement.BodyBytes)/2 || *observations[0].ClientNetworkMS != 50 {
		t.Fatal(observations, err)
	}
	snapshot.Observations = observations
	if err := validateBoundPhysicalQualitySnapshot(snapshot, evidence); err != nil {
		t.Fatal(err)
	}
	receipt, err := edgequality.Capture(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := edgequality.Replay(receipt); err != nil {
		t.Fatal(err)
	}
	changed := snapshot
	changed.Observations = changed.Observations[:1]
	if validateBoundPhysicalQualitySnapshot(changed, evidence) == nil {
		t.Fatal("publication omitted a complete client attempt")
	}
	changed = snapshot
	changed.ClientProbeReports = nil
	if _, err := edgequality.Capture(changed); err == nil {
		t.Fatal("derived client metrics trusted without original signed report")
	}
	report.Outcomes[0].Failure, report.Outcomes[0].BytesReceived, report.Outcomes[0].BodySeconds, report.Outcomes[0].BodySHA256, report.Outcomes[0].Attestation = "connect", 0, 0, "", nil
	snapshot.ClientProbeReports = []model.EdgeClientProbeReport{report}
	observations, err = edgequality.ClientProbeObservations(snapshot)
	if err != nil || len(observations) != 2 || *observations[0].ClientFailureRate != 1 || observations[0].ClientNetworkMS != nil || observations[0].DownloadBPS != nil {
		t.Fatal("failed client attempt invented timing or disappeared from denominator", observations, err)
	}
	report.Outcomes[0].Failure = "response"
	snapshot.ClientProbeReports = []model.EdgeClientProbeReport{report}
	observations, err = edgequality.ClientProbeObservations(snapshot)
	if err != nil || len(observations) != 1 {
		t.Fatal("server telemetry rejection was misclassified as network failure", observations, err)
	}
}
