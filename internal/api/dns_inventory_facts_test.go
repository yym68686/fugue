package api

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"fugue/internal/dnsfacts"
	"fugue/internal/model"
	"fugue/internal/routeprobe"
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
	st := newStoreTestDatabase(t, "")
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

func TestDNSPreflightSelectedObservationMessagesDoNotInventCacheErrors(t *testing.T) {
	now := time.Now().UTC()
	old := now.Add(-time.Hour)
	base := model.DNSNode{ID: "dns-a", EdgeGroupID: "group-a", PublicIPv4: "192.0.2.10", Zone: "example.test",
		Status: model.EdgeHealthHealthy, Healthy: true, DNSBundleVersion: "generation-a", CacheStatus: "ready", LastSeenAt: &old}
	s := &Server{dnsDelegationProbe: func(context.Context, model.DNSNode, string, string) dnsDelegationProbeResult {
		return dnsDelegationProbeResult{UDP53Reachable: true, TCP53Reachable: true, ProbeAnswers: []string{base.PublicIPv4}}
	}}
	policies := map[string]model.ClusterNodePolicyStatus{base.ID: {Ready: true}}
	for _, tc := range []struct {
		name, state, cacheStatus, bundle, want string
		selected, healthy, pass                bool
		writes, loads                          uint64
	}{
		{name: "unknown with zero errors", selected: true, state: "unknown", cacheStatus: "unknown", want: "selected DNS backend observation is unavailable"},
		{name: "unknown with historical errors", selected: true, state: "unknown", cacheStatus: "unknown", writes: 7, loads: 3, want: "selected DNS backend observation is unavailable"},
		{name: "not ready with known cache", selected: true, state: "not_ready", cacheStatus: "ready", bundle: "generation-a", want: "selected DNS backend is not ready"},
		{name: "not ready with unknown cache", selected: true, state: "not_ready", cacheStatus: "unknown", bundle: "generation-a", want: "selected DNS cache readiness has not passed"},
		{name: "ready ignores historical errors", selected: true, state: "ready", cacheStatus: "ready", bundle: "generation-a", healthy: true, writes: 7, loads: 3, pass: true},
		{name: "ready but cache unavailable", selected: true, state: "ready", cacheStatus: "unknown", bundle: "generation-a", healthy: true, want: "selected DNS cache readiness has not passed"},
		{name: "invalid state stays closed", selected: true, state: "unexpected", cacheStatus: "ready", bundle: "generation-a", want: "selected DNS backend observation is unavailable"},
		{name: "legacy write error", cacheStatus: "ready", bundle: "generation-a", healthy: true, writes: 1, want: "cache has unrecovered write/load errors"},
		{name: "legacy recovered load errors", cacheStatus: "ready", bundle: "generation-a", healthy: true, loads: 3, pass: true},
		{name: "legacy unrecovered load errors", cacheStatus: "error", bundle: "generation-a", healthy: true, loads: 3, want: "cache has unrecovered write/load errors"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := base
			node.Healthy, node.CacheStatus, node.DNSBundleVersion = tc.healthy, tc.cacheStatus, tc.bundle
			node.CacheWriteErrors, node.CacheLoadErrors = tc.writes, tc.loads
			if tc.selected {
				node.ServingObservation = &model.DNSNodeServingObservation{Source: "selected_artifact_consumer", State: tc.state, EvaluatedAt: now}
			}
			got := s.buildDNSDelegationNodeCheck(context.Background(), node, dnsDelegationPreflightOptions{}, policies, nil)
			if got.Pass != tc.pass || !got.ProbePass || !got.NodeReady || got.CacheWriteErrors != tc.writes || got.CacheLoadErrors != tc.loads || got.LastSeenAt != &old {
				t.Fatalf("diagnostic changed health gates or historical facts: %+v", got)
			}
			if tc.want != "" && !strings.Contains(got.Message, tc.want) {
				t.Fatalf("message %q does not explain %q", got.Message, tc.want)
			}
			if tc.selected && (strings.Contains(got.Message, "unrecovered write/load errors") || strings.Contains(got.Message, "DNS heartbeat is not healthy")) {
				t.Fatalf("selected observation misreported as legacy failure: %q", got.Message)
			}
			if tc.pass && got.Message != "" {
				t.Fatalf("healthy node has failure diagnostic: %q", got.Message)
			}
		})
	}
}
