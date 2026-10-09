package api

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestNetworkIngestCannotRebindForeignOrStaleEvidence(t *testing.T) {
	now := time.Now().UTC()
	sample := model.EdgeNetworkSample{ID: "sample-a", EdgeID: "edge-a", EdgeGroupID: "group-a", Hostname: "app.example.test",
		PathPrefix: "/", TrafficClass: "streaming", RouteDigest: "sha256:" + strings.Repeat("a", 64), BundleVersion: "bundle-one",
		ServiceTarget: "app.tenant.svc.cluster.local:3000", Source: "service_endpoint_tcp_info_v1", ObservedAt: now}
	req := edgeHeartbeatRequest{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID, RouteBundleVersion: sample.BundleVersion, NetworkSamples: []model.EdgeNetworkSample{sample}}
	active, inactive := true, false
	if got := sanitizeEdgeNetworkSamples(req, &active, now); len(got) != 1 || got[0].ServiceRTTMS != nil {
		t.Fatal("valid unknown observation lost", got)
	}
	for _, flag := range []*bool{nil, &inactive} {
		if len(sanitizeEdgeNetworkSamples(req, flag, now)) != 0 {
			t.Fatal("standby admitted")
		}
	}
	for _, mutation := range []func(*model.EdgeNetworkSample){
		func(sample *model.EdgeNetworkSample) { sample.EdgeID = "edge-other" },
		func(sample *model.EdgeNetworkSample) { sample.EdgeGroupID = "group-other" },
		func(sample *model.EdgeNetworkSample) { sample.BundleVersion = "stale-bundle" },
		func(sample *model.EdgeNetworkSample) { sample.ObservedAt = now.Add(time.Second) },
		func(sample *model.EdgeNetworkSample) { sample.ObservedAt = now.Add(-2 * time.Hour) },
		func(sample *model.EdgeNetworkSample) { sample.Source = "http_ttfb" },
	} {
		changed := sample
		mutation(&changed)
		req.NetworkSamples = []model.EdgeNetworkSample{changed}
		if len(sanitizeEdgeNetworkSamples(req, &active, now)) != 0 {
			t.Fatal("invalid evidence accepted", changed)
		}
	}
}

func TestNetworkIngestPreservesConfiguredProbeOutcomes(t *testing.T) {
	now := time.Now().UTC()
	sample, _ := networkWitnessFixture(now)
	failed := true
	sample.Source, sample.ServiceConnectFailed, sample.ServiceRTTMS = "service_endpoint_tcp_probe_v1", &failed, nil
	request := edgeHeartbeatRequest{EdgeID: sample.EdgeID, EdgeGroupID: sample.EdgeGroupID, RouteBundleVersion: sample.BundleVersion,
		NetworkSamples: []model.EdgeNetworkSample{sample}}
	active := true
	got := sanitizeEdgeNetworkSamples(request, &active, now)
	if len(got) != 1 || got[0].ServiceConnectFailed == nil || !*got[0].ServiceConnectFailed || got[0].ServiceRTTMS != nil {
		t.Fatal("failed probe disappeared or became zero RTT", got)
	}
	request.NetworkSamples[0].EdgeID = "foreign"
	if len(sanitizeEdgeNetworkSamples(request, &active, now)) != 0 {
		t.Fatal("probe source bypassed authenticated edge identity")
	}
}
