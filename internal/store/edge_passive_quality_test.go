package store

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestPassiveEdgeNetworkReadIsCrossHostAndEdgeBounded(t *testing.T) {
	store := New(filepath.Join(t.TempDir(), "state.json"))
	if err := store.Init(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	makeSample := func(id, host, edge string, at time.Time) model.EdgeNetworkSample {
		rtt := 5.0
		return model.EdgeNetworkSample{
			ID: id, Hostname: host, EdgeID: edge, EdgeGroupID: "group-a", RouteDigest: "sha256:" + strings.Repeat("a", 64),
			PathPrefix: "/", TrafficClass: "streaming", BundleVersion: "bundle", Source: "public_front_tcp_info_v1", ObservedAt: at,
			ClientNetwork: &model.EdgeClientNetworkSample{StartedAt: at, ObservedAt: at, ConnectionID: id, Slot: "a",
				Scope: "tcp_peer:198.51.100.0/24", TCPInfoAvailable: true, RTTMS: &rtt},
		}
	}
	first := makeSample("first", "a.example.test", "edge-a", now.Add(-time.Minute))
	second := makeSample("second", "b.example.test", "edge-a", now)
	upstream := makeSample("upstream", "b.example.test", "edge-a", now)
	upstream.Source, upstream.ClientNetwork, upstream.ServiceTarget = "service_endpoint_tcp_info_v1", nil, "fixture.tenant.svc.cluster.local:1234"
	if err := store.RecordEdgeNetworkSamples(t.Context(), []model.EdgeNetworkSample{first, second, upstream,
		makeSample("other", "a.example.test", "edge-b", now),
		makeSample("old", "a.example.test", "edge-a", now.Add(-time.Hour)),
	}, time.Time{}); err != nil {
		t.Fatal(err)
	}
	for limit := 1; limit <= 3; limit++ {
		got, err := store.ListPassiveEdgeNetworkSamples(t.Context(), []string{"edge-a"}, now.Add(-5*time.Minute), limit)
		if err != nil || len(got) != min(limit, 2) || got[0].ID != second.ID || limit > 1 && got[1].ID != first.ID {
			t.Fatalf("limit %d: %+v %v", limit, got, err)
		}
	}
	got, err := store.ListPassiveEdgeNetworkSamples(t.Context(), []string{"edge-b", "edge-a"}, now.Add(-5*time.Minute), 1)
	if err != nil || len(got) != 2 || got[0].ID != second.ID || got[1].ID != "other" {
		t.Fatalf("one edge exhausted batch: %+v %v", got, err)
	}
}
