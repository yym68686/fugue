package model

import (
	"strings"
	"testing"
	"time"
)

func TestClientNetworkSampleNeverInventsUnknownRTTOrRawPeerIdentity(t *testing.T) {
	now := time.Now().UTC()
	base := EdgeNetworkSample{ID: "sample-a", EdgeID: "edge-a", EdgeGroupID: "group-a", Hostname: "app.example.test", PathPrefix: "/", TrafficClass: "streaming", RouteDigest: "sha256:" + strings.Repeat("a", 64), BundleVersion: "bundle-a", Source: "public_front_tcp_info_v1", ObservedAt: now,
		ClientNetwork: &EdgeClientNetworkSample{ConnectionID: "connection-a", Slot: "a", Scope: "tcp_peer:203.0.113.0/24", StartedAt: now.Add(-time.Minute), ObservedAt: now}}
	if err := ValidateEdgeNetworkSample(base); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*EdgeNetworkSample){
		func(sample *EdgeNetworkSample) { sample.ClientNetwork.Scope = "tcp_peer:203.0.113.3/32" },
		func(sample *EdgeNetworkSample) { sample.ClientNetwork.Scope = "tcp_peer:10.0.0.0/24" },
		func(sample *EdgeNetworkSample) { sample.ClientNetwork.Scope = "global" },
		func(sample *EdgeNetworkSample) { value := 0.0; sample.ClientNetwork.RTTMS = &value },
		func(sample *EdgeNetworkSample) { sample.ServiceTarget = "app.tenant.svc.cluster.local:3000" },
		func(sample *EdgeNetworkSample) { sample.ObservedAt = now.Add(time.Second) },
		func(sample *EdgeNetworkSample) { sample.Source = "service_endpoint_tcp_info_v1" },
		func(sample *EdgeNetworkSample) { sample.ClientNetwork = nil },
	} {
		sample := base
		client := *base.ClientNetwork
		sample.ClientNetwork = &client
		edit(&sample)
		if ValidateEdgeNetworkSample(sample) == nil {
			t.Fatal("invalid network provenance admitted", sample)
		}
	}
}
