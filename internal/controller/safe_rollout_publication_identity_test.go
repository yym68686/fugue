package controller

import (
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestSafeRolloutEdgeObserverAcceptsAppliedGroupPublication(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000000, 0).UTC()
	heartbeat := now.Add(time.Second)
	node := model.EdgeNode{RouteBundleVersion: "route-generation.p42.r3", ServingGeneration: "route-generation", LKGGeneration: "route-generation", CaddyAppliedVersion: "route-generation.p42.r3", LastHeartbeatAt: &heartbeat}
	if ready, reason := safeRolloutEdgeNodeBundleApplied(node, now); !ready {
		t.Fatalf("applied publication rejected: %s", reason)
	}
}

func TestSafeRolloutPublicationIdentityKeepsFailureGuards(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000000, 0).UTC()
	heartbeat := now.Add(time.Second)
	staleHeartbeat := now.Add(-time.Second)
	base := model.EdgeNode{
		RouteBundleVersion: "route-generation.p42.r3",
		ServingGeneration:  "route-generation", LKGGeneration: "route-generation",
		CaddyAppliedVersion: "route-generation.p42.r3", LastHeartbeatAt: &heartbeat,
	}
	for _, tc := range []struct {
		name   string
		change func(*model.EdgeNode)
		reason string
	}{
		{"different generation", func(n *model.EdgeNode) { n.ServingGeneration, n.LKGGeneration = "previous", "previous" }, "serving_lkg"},
		{"missing applied publication", func(n *model.EdgeNode) { n.CaddyAppliedVersion = "" }, "serving_lkg"},
		{"different applied publication", func(n *model.EdgeNode) { n.CaddyAppliedVersion = "route-generation.p41.r3" }, "serving_lkg"},
		{"edge error", func(n *model.EdgeNode) { n.LastError = "bundle rejected" }, "edge_reports_error"},
		{"proxy error", func(n *model.EdgeNode) { n.CaddyLastError = "apply failed" }, "edge_reports_error"},
		{"heartbeat before promotion", func(n *model.EdgeNode) { n.LastHeartbeatAt = &staleHeartbeat }, "heartbeat_before_promotion"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := base
			tc.change(&node)
			if ready, reason := safeRolloutEdgeNodeBundleApplied(node, now); ready || reason != tc.reason {
				t.Fatalf("expected rejection %q, got ready=%v reason=%q", tc.reason, ready, reason)
			}
		})
	}
	for _, suffix := range []string{".p0.r0", ".p01.r0", ".p1.r00", ".p1", ".p1.r-1", ".p1.r0.extra", ".p18446744073709551616.r0"} {
		t.Run("malformed "+suffix, func(t *testing.T) {
			node := base
			node.RouteBundleVersion = node.ServingGeneration + suffix
			node.CaddyAppliedVersion = node.RouteBundleVersion
			if ready, reason := safeRolloutEdgeNodeBundleApplied(node, now); ready || reason != "serving_lkg" {
				t.Fatalf("malformed publication must not prove generation equality: ready=%v reason=%q", ready, reason)
			}
		})
	}
	for _, version := range []string{"route-generation", "route-generation.p1.r0", "route-generation.p18446744073709551615.r18446744073709551615"} {
		t.Run("valid "+version, func(t *testing.T) {
			node := base
			node.RouteBundleVersion = version
			node.CaddyAppliedVersion = version
			if !strings.Contains(version, ".p") {
				node.CaddyAppliedVersion = "" // Preserve the existing legacy generation contract.
			}
			if ready, reason := safeRolloutEdgeNodeBundleApplied(node, now); !ready {
				t.Fatalf("expected applied generation ready, got %q", reason)
			}
		})
	}
}
