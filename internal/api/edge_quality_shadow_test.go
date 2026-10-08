package api

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/dnsserver"
	"fugue/internal/edgequality"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
	"fugue/internal/store"
)

func TestPhysicalQualityShadowReadOnlyAndLegacyUnknown(t *testing.T) {
	filename := t.TempDir() + "/state.json"
	state := store.New(filename)
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	server := NewServer(state, auth.New(state, "shadow-admin"), nil, ServerConfig{})
	now := time.Now().UTC()
	for _, nodeID := range []string{"edge-a", "edge-b"} {
		if _, _, err := state.UpdateEdgeHeartbeat(model.EdgeNode{ID: nodeID, EdgeGroupID: "shared", Healthy: true, CaddyRouteCount: 5, RouteBundleVersion: "bundle-1", TLSStatus: model.EdgeTLSStatusReady}); err != nil {
			t.Fatal(err)
		}
	}
	if err := state.RecordEdgePerformanceSamples([]model.EdgePerformanceSample{
		{ID: "observation-a", EdgeID: "edge-a", EdgeGroupID: "shared", Hostname: "app.example.test", TrafficClass: "streaming", SampleCount: 100, TTFBMS: 90000, OriginConnectMS: 1, ClientTCPRTTMS: 2, ActiveRequests: 12, SampledAt: now},
		{ID: "other-class", EdgeID: "edge-b", EdgeGroupID: "shared", Hostname: "app.example.test", TrafficClass: "static_cacheable", SampleCount: 100, SampledAt: now},
	}, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	rtt := 2.5
	if err := state.RecordEdgeNetworkSamples(context.Background(), []model.EdgeNetworkSample{{ID: "network-a", EdgeID: "edge-a", EdgeGroupID: "shared", Hostname: "app.example.test", PathPrefix: "/", TrafficClass: "streaming", RouteDigest: "sha256:" + strings.Repeat("a", 64), BundleVersion: "bundle-1", ServiceTarget: "app.tenant.svc.cluster.local:3000", Source: "service_endpoint_tcp_info_v1", ServiceRTTMS: &rtt, ObservedAt: now}}, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(filename)
	response := performJSONRequest(t, server, http.MethodGet, "/v1/edge/quality-shadow/app.example.test?traffic_class=streaming", "shadow-admin", nil)
	if response.Code != 200 {
		t.Fatal(response.Code, response.Body.String())
	}
	var receipt edgequality.Receipt
	if err := json.Unmarshal(response.Body.Bytes(), &receipt); err != nil {
		t.Fatal(err)
	}
	if len(receipt.Snapshot.Observations) != 1 || len(receipt.Result.Candidates) != 2 || receipt.Result.PromotionReady || !receipt.Result.DNSUnchanged {
		t.Fatal(receipt)
	}
	if len(receipt.Snapshot.NetworkSamples) != 1 || *receipt.Snapshot.NetworkSamples[0].ServiceRTTMS != rtt {
		t.Fatal("captured socket evidence missing", receipt.Snapshot.NetworkSamples)
	}
	for _, candidate := range receipt.Result.Candidates {
		if candidate.Ready || candidate.Metrics["client_network_ms"].State != "unknown" || candidate.Metrics["service_network_ms"].State != "unknown" {
			t.Fatal(candidate)
		}
	}
	if _, err := edgequality.Replay(receipt); err != nil {
		t.Fatal(err)
	}
	receipt.Snapshot.NetworkSamples[0].ID = "tampered-network-record"
	if _, err := edgequality.Replay(receipt); err == nil {
		t.Fatal("network evidence was not covered by the receipt digest")
	}
	after, _ := os.ReadFile(filename)
	if string(before) != string(after) {
		t.Fatal("shadow read mutated serving state")
	}
	for _, query := range []string{"", "?traffic_class=bad", "?traffic_class=streaming&scope=invalid"} {
		if response := performJSONRequest(t, server, http.MethodGet, "/v1/edge/quality-shadow/app.example.test"+query, "shadow-admin", nil); response.Code != 400 {
			t.Fatal(query, response.Code)
		}
	}
	if response := performJSONRequest(t, server, http.MethodGet, "/v1/edge/quality-shadow/app.example.test?traffic_class=streaming", "invalid", nil); response.Code != 401 {
		t.Fatal(response.Code)
	}
}

func TestQualityNetworkSampleBindingRequiresExactPhysicalRouteProof(t *testing.T) {
	now := time.Now().UTC()
	rtt := 150.0
	digest := "sha256:" + strings.Repeat("a", 64)
	sample := model.EdgeNetworkSample{ID: "sample-a", EdgeID: "edge-a", EdgeGroupID: "shared", Hostname: "app.example.test", PathPrefix: "/", TrafficClass: "streaming",
		RouteDigest: digest, BundleVersion: "serving-bundle", ServiceTarget: "app.tenant.svc.cluster.local:3000", Source: "service_endpoint_tcp_info_v1", ServiceRTTMS: &rtt, ObservedAt: now.Add(-time.Second)}
	evidence := dnsserver.QualityAnswerEvidence{EdgeID: "edge-a", Hostname: sample.Hostname, Scope: "global", Proofs: []dnsserver.QualityRouteProof{{EdgeID: "edge-a", EdgeGroupID: "shared", Hostname: sample.Hostname, Path: "/",
		Proof: routeprobe.Proof{Digest: digest, Version: "serving-bundle", CheckedAt: now}}}}
	for _, test := range []struct {
		name string
		edit func(*model.EdgeNetworkSample)
		want int
	}{
		{"exact", func(sample *model.EdgeNetworkSample) {}, 1},
		{"sibling", func(sample *model.EdgeNetworkSample) { sample.EdgeID = "edge-b" }, 0},
		{"old_bundle", func(sample *model.EdgeNetworkSample) { sample.BundleVersion = "old" }, 0},
		{"wrong_route", func(sample *model.EdgeNetworkSample) { sample.RouteDigest = "sha256:" + strings.Repeat("b", 64) }, 0},
		{"wrong_path", func(sample *model.EdgeNetworkSample) { sample.PathPrefix = "/other" }, 0},
		{"different_class", func(sample *model.EdgeNetworkSample) { sample.TrafficClass = "dynamic_api" }, 0},
		{"future", func(sample *model.EdgeNetworkSample) { sample.ObservedAt = now.Add(time.Hour) }, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			modified := sample
			test.edit(&modified)
			snapshot := edgequality.Snapshot{Schema: edgequality.Schema, CapturedAt: now, Hostname: sample.Hostname, TrafficClass: sample.TrafficClass, Scope: "global", Policy: edgequality.DefaultShadowPolicy(),
				Candidates: []edgequality.Candidate{{EdgeID: "edge-a", EdgeGroupID: "shared"}}, NetworkSamples: []model.EdgeNetworkSample{modified}, Blockers: []string{"actual_dns_receipt_not_bound", "capacity_limit_unknown"}}
			bindPhysicalQualityEvidence(&snapshot, evidence)
			if snapshot.CurrentEdgeID != "edge-a" || len(snapshot.Observations) != test.want || !snapshot.Candidates[0].RouteProofVerified || len(snapshot.Blockers) != 1 || snapshot.Blockers[0] != "capacity_limit_unknown" {
				t.Fatal(snapshot)
			}
			if test.want > 0 && (snapshot.Observations[0].ClientNetworkMS != nil || snapshot.Observations[0].ServiceFailureRate != nil || snapshot.Observations[0].CapacityUtilization != nil) {
				t.Fatal("origin socket invented missing metrics")
			}
		})
	}
}
