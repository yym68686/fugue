package api

import (
	"context"
	"reflect"
	"testing"
	"time"

	"fugue/internal/dnsfacts"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
	"fugue/internal/store"
)

func TestDNSInventoryServingProjectionPreservesHistoryAndRechecksExpiry(t *testing.T) {
	now := time.Now().UTC()
	old := now.Add(-time.Hour)
	node := model.DNSNode{ID: "dns-a", EdgeGroupID: "group-a", Status: "healthy", Healthy: true, ServingGeneration: "stale", LKGGeneration: "stale", QueryCount: 45, QueryErrorCount: 2, CacheWriteErrors: 7, LastHeartbeatAt: &old}
	facts := platformDNSRuntimeFactsResponse{Ready: true, ReadyProbeIDs: []string{"probe"}, lkgGeneration: "verified-lkg", Backend: dnsRuntimeBackend{PodUID: "selected-pod"}, Snapshot: dnsfacts.Snapshot{NodeID: node.ID, EdgeGroupID: node.EdgeGroupID, ObservedAt: now.Add(-time.Second), CheckpointValidUntil: now.Add(time.Hour), Assignment: model.PlatformConsumerAssignment{ExpectedGeneration: "selected-generation"}, Facts: []dnsfacts.Probe{{ProbeID: "probe", Ready: true, Proof: routeprobe.Proof{ValidUntil: now.Add(time.Second)}}}}}
	facts.heartbeatValidUntil = now.Add(time.Minute)
	for _, tc := range []struct {
		name        string
		at          time.Time
		unavailable bool
		state       string
	}{
		{"ready", now, false, "ready"},
		{"proof expired after read", now.Add(2 * time.Second), false, "not_ready"},
		{"observation unavailable", now, true, "unknown"},
		{"heartbeat expired after read", now.Add(2 * time.Minute), false, "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := projectDNSInventoryServingFact(node, facts, tc.unavailable, tc.at)
			if got.ServingObservation.State != tc.state || got.Healthy != (tc.state == "ready") || got.QueryCount != node.QueryCount || got.QueryErrorCount != node.QueryErrorCount || got.CacheWriteErrors != node.CacheWriteErrors || got.LastHeartbeatAt != node.LastHeartbeatAt {
				t.Fatalf("unexpected serving/history projection: %+v", got)
			}
			if len(freshDNSNodes([]model.DNSNode{got}, now)) != 1 {
				t.Fatal("enrolled member was removed because legacy inventory was stale")
			}
			checks := []model.DNSDelegationNodeCheck{{ServingObservation: got.ServingObservation, CacheStatus: got.CacheStatus, DNSBundleVersion: got.DNSBundleVersion, CacheWriteErrors: got.CacheWriteErrors}}
			if dnsNodeCacheHealthyAll(checks) != (tc.state == "ready") {
				t.Fatal("historical counters overrode the selected cache readiness")
			}
		})
	}
	if node.ServingGeneration != "stale" || node.ServingObservation != nil {
		t.Fatal("source inventory mutated")
	}
}

func TestDNSInventoryUnenrolledNodesKeepExistingBehavior(t *testing.T) {
	st := store.New(t.TempDir() + "/state.json")
	if err := st.Init(); err != nil {
		t.Fatal(err)
	}
	s := &Server{store: st}
	nodes := []model.DNSNode{{ID: "external-a", EdgeGroupID: "group-a", Status: "degraded", Healthy: false, LastError: "external-observation"}}
	got, err := s.dnsInventoryServingFacts(context.Background(), nodes)
	if err != nil || !reflect.DeepEqual(got, nodes) {
		t.Fatal("unenrolled inventory changed", got, err)
	}
}
