package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestNetworkSamplesImmutableDeduplicatedBoundedAndScoped(t *testing.T) {
	state := New(filepath.Join(t.TempDir(), "state.json"))
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	value := 7.25
	sample := model.EdgeNetworkSample{ID: "sample-a", EdgeID: "edge-a", EdgeGroupID: "shared", Hostname: "app.example.test",
		PathPrefix: "/", TrafficClass: "streaming", RouteDigest: "sha256:" + strings.Repeat("a", 64), BundleVersion: "bundle-one",
		ServiceTarget: "app.tenant.svc.cluster.local:3000", Source: "service_endpoint_tcp_info_v1", ServiceRTTMS: &value, ObservedAt: now}
	ctx := context.Background()
	if err := state.RecordEdgeNetworkSamples(ctx, []model.EdgeNetworkSample{sample}, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	changed := sample
	changed.ServiceRTTMS = nil
	if err := state.RecordEdgeNetworkSamples(ctx, []model.EdgeNetworkSample{changed}, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	samples, err := state.ListEdgeNetworkSamples(ctx, sample.Hostname, now.Add(-time.Minute), 1)
	if err != nil || len(samples) != 1 || samples[0].ServiceRTTMS == nil || *samples[0].ServiceRTTMS != value {
		t.Fatal("observation changed on retry", samples, err)
	}
	sample.EdgeID = "edge-b"
	if err := state.RecordEdgeNetworkSamples(ctx, []model.EdgeNetworkSample{sample}, now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	samples, err = state.ListEdgeNetworkSamples(ctx, sample.Hostname, now.Add(-time.Minute), 10)
	if err != nil || len(samples) != 2 || samples[0].EdgeID == samples[1].EdgeID {
		t.Fatal("sibling physical edges merged", samples, err)
	}
	if samples, err := state.ListEdgeNetworkSamples(ctx, "other.example.test", now.Add(-time.Minute), 10); err != nil || len(samples) != 0 {
		t.Fatal("cross-host evidence", samples, err)
	}
	if _, err := state.ListEdgeNetworkSamples(ctx, "", now, 10); err == nil {
		t.Fatal("unbounded query allowed")
	}
	if err := state.RecordEdgeNetworkSamples(ctx, nil, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if samples, err := state.ListEdgeNetworkSamples(ctx, sample.Hostname, now.Add(-time.Minute), 10); err != nil || len(samples) != 0 {
		t.Fatal("retention failed", samples, err)
	}
}
