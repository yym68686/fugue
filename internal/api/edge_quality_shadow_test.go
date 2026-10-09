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
		{"tcp_probe", func(sample *model.EdgeNetworkSample) {
			failed := false
			sample.Source, sample.ServiceConnectFailed = "service_endpoint_tcp_probe_v1", &failed
		}, 1},
		{"tcp_probe_failed", func(sample *model.EdgeNetworkSample) {
			failed := true
			sample.Source, sample.ServiceConnectFailed, sample.ServiceRTTMS = "service_endpoint_tcp_probe_v1", &failed, nil
		}, 1},
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

func TestQualityBindingReplacesRatherThanInheritsUnverifiedCooldown(t *testing.T) {
	now := time.Now().UTC()
	previous := now.Add(-time.Hour)
	snapshot := edgequality.Snapshot{LastSwitchAt: &previous}
	evidence := dnsserver.QualityAnswerEvidence{EdgeID: "edge-a"}
	bindPhysicalQualityEvidence(&snapshot, evidence)
	if snapshot.LastSwitchAt != nil {
		t.Fatal("legacy answer inherited unrelated physical cooldown")
	}
	evidence.PrimarySince = &previous
	bindPhysicalQualityEvidence(&snapshot, evidence)
	if snapshot.LastSwitchAt == nil || !snapshot.LastSwitchAt.Equal(previous) {
		t.Fatal("signed primary assignment not retained")
	}
	*snapshot.LastSwitchAt = now
	if !evidence.PrimarySince.Equal(previous) {
		t.Fatal("mutable snapshot aliased original evidence")
	}
}

func TestQualityClientNetworkBindingPreservesMissingMetricsAndScope(t *testing.T) {
	now := time.Now().UTC()
	rtt := 180.5
	digest := "sha256:" + strings.Repeat("a", 64)
	sample := model.EdgeNetworkSample{ID: "client-a", EdgeID: "edge-a", EdgeGroupID: "shared", Hostname: "app.example.test", PathPrefix: "/", TrafficClass: "streaming",
		RouteDigest: digest, BundleVersion: "serving-bundle", Source: "public_front_tcp_info_v1", ObservedAt: now.Add(-time.Second),
		ClientNetwork: &model.EdgeClientNetworkSample{ConnectionID: "connection-a", Slot: "b", Scope: "tcp_peer:203.0.113.0/24", StartedAt: now.Add(-time.Minute), ObservedAt: now.Add(-time.Second), TCPInfoAvailable: true, RTTMS: &rtt}}
	evidence := dnsserver.QualityAnswerEvidence{EdgeID: "edge-a", Hostname: sample.Hostname, Scope: "global", Proofs: []dnsserver.QualityRouteProof{{EdgeID: "edge-a", EdgeGroupID: "shared", Hostname: sample.Hostname, Path: "/",
		Proof: routeprobe.Proof{Digest: digest, Version: "serving-bundle", CheckedAt: now}}}}
	for _, scope := range []string{"global", "tcp_peer:203.0.113.0/24", "tcp_peer:198.51.100.0/24", "asn:64500"} {
		for _, available := range []bool{true, false} {
			modified := sample
			client := *sample.ClientNetwork
			modified.ClientNetwork = &client
			if !available {
				client.TCPInfoAvailable, client.RTTMS = false, nil
			}
			snapshot := edgequality.Snapshot{CapturedAt: now, Hostname: sample.Hostname, TrafficClass: sample.TrafficClass, Scope: scope,
				Candidates: []edgequality.Candidate{{EdgeID: "edge-a", EdgeGroupID: "shared"}}, NetworkSamples: []model.EdgeNetworkSample{modified}}
			bindPhysicalQualityEvidence(&snapshot, evidence)
			if scope != "global" && scope != client.Scope {
				if len(snapshot.Observations) != 0 {
					t.Fatal("TCP peer scope attributed to unrelated cohort", scope)
				}
				continue
			}
			if len(snapshot.Observations) != 1 {
				t.Fatal("exact client socket evidence not bound", snapshot)
			}
			observation := snapshot.Observations[0]
			if observation.ClientSource != "public_tcp_info" || observation.ServiceNetworkMS != nil || observation.ClientFailureRate != nil || observation.CapacityUtilization != nil || (observation.ClientNetworkMS != nil) != available {
				t.Fatal("client RTT fabricated missing metrics", observation)
			}
		}
	}
}

func TestQualityCapacityUsesWitnessDenominatorsAndOfflineBinding(t *testing.T) {
	now := time.Now().UTC()
	sample, proof := networkWitnessFixture(now)
	witness, err := networkRouteWitnessSample(sample, "203.0.113.5", proof, now)
	if err != nil {
		t.Fatal(err)
	}
	witness.RouteWitness.NodeCapacity = &model.EdgeNetworkNodeCapacity{Source: "kubelet_node_allocatable_v1", NodeUID: "node-uid-a", ObservedAt: now.Add(-5 * time.Second),
		ValidUntil: now.Add(115 * time.Second), CPUObservedAt: now.Add(-2 * time.Second), MemoryObservedAt: now.Add(-5 * time.Second),
		CPUUsageNanoCores: 250_000_000, CPUAllocatableMilliCores: 1000, MemoryWorkingSetBytes: 128 << 20, MemoryAllocatableBytes: 1 << 30, Pressure: []string{}}
	evidence := dnsserver.QualityAnswerEvidence{EdgeID: sample.EdgeID, Hostname: sample.Hostname, Scope: "global",
		Proofs: []dnsserver.QualityRouteProof{{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID, Hostname: sample.Hostname, Path: sample.PathPrefix, Proof: proof}}}
	snapshot := edgequality.Snapshot{Schema: edgequality.Schema, CapturedAt: now, Hostname: sample.Hostname, TrafficClass: sample.TrafficClass, Scope: "global",
		Policy: edgequality.DefaultNetworkPolicy(), Candidates: []edgequality.Candidate{{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID}}, NetworkSamples: []model.EdgeNetworkSample{witness}}
	bindPhysicalQualityEvidence(&snapshot, evidence)
	if len(snapshot.Observations) != 1 || snapshot.Observations[0].CapacityUtilization == nil || *snapshot.Observations[0].CapacityUtilization != 0.25 ||
		!snapshot.Observations[0].ObservedAt.Equal(witness.RouteWitness.NodeCapacity.ObservedAt) {
		t.Fatal("capacity denominator or metric time lost", snapshot.Observations)
	}
	receipt, err := edgequality.Capture(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := edgequality.Replay(receipt); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*edgequality.Snapshot){
		func(value *edgequality.Snapshot) { value.NetworkSamples = nil },
		func(value *edgequality.Snapshot) {
			value.Observations[0].CapacityUtilization = func() *float64 { changed := 0.01; return &changed }()
		},
		func(value *edgequality.Snapshot) { value.Observations[0].ObservedAt = now },
		func(value *edgequality.Snapshot) { value.Observations[0].RouteGeneration = "foreign" },
		func(value *edgequality.Snapshot) { value.Observations[0].Hostname = "other.example.test" },
	} {
		modified := snapshot
		modified.Observations = append([]edgequality.Observation(nil), snapshot.Observations...)
		edit(&modified)
		if _, err := edgequality.Capture(modified); err == nil {
			t.Fatal("capacity derivation trusted without original raw witness")
		}
	}
}
