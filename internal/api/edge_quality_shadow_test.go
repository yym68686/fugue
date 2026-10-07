package api

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"fugue/internal/auth"
	"fugue/internal/edgequality"
	"fugue/internal/model"
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
	for _, candidate := range receipt.Result.Candidates {
		if candidate.Ready || candidate.Metrics["client_network_ms"].State != "unknown" || candidate.Metrics["service_network_ms"].State != "unknown" {
			t.Fatal(candidate)
		}
	}
	if _, err := edgequality.Replay(receipt); err != nil {
		t.Fatal(err)
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
