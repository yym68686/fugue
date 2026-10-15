package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/routeprobe"
)

func rolloutPublication(digit string, sequence, epoch int) string {
	return fmt.Sprintf("edgegroupbundle_%s.p%d.r%d", strings.Repeat(digit, 64), sequence, epoch)
}

func TestSafeRolloutLiveProofCanLeadHealthyInventory(t *testing.T) {
	for _, scenario := range []string{"heartbeat lags", "heartbeat catches up", "heartbeat passes proof", "heartbeat regresses", "same sequence different content", "recovery changes", "wrong target", "apply error", "address changes", "unhealthy", "legacy version"} {
		t.Run(scenario, func(t *testing.T) {
			now := time.Now().UTC()
			o, app, release, node, proof := appTrafficObserverFixture(t, now)
			node.RouteBundleVersion = rolloutPublication("a", 42, 3)
			node.CaddyAppliedVersion = node.RouteBundleVersion
			node.ServingGeneration = "edgegroupbundle_" + strings.Repeat("a", 64)
			node.LKGGeneration = node.ServingGeneration
			proof.Version = rolloutPublication("b", 43, 3)
			inventory := &changingEdgeNodeLister{nodes: []model.EdgeNode{node}}
			o.Store = inventory
			o.Probe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
				changeVersion := func(version string) {
					inventory.nodes[0].RouteBundleVersion = version
					inventory.nodes[0].CaddyAppliedVersion = version
				}
				switch scenario {
				case "heartbeat catches up":
					changeVersion(proof.Version)
				case "heartbeat passes proof":
					changeVersion(rolloutPublication("c", 44, 3))
				case "heartbeat regresses":
					changeVersion(rolloutPublication("d", 41, 3))
				case "same sequence different content":
					proof.Version = rolloutPublication("c", 42, 3)
				case "recovery changes":
					proof.Version = rolloutPublication("b", 43, 4)
				case "wrong target":
					proof.AppTrafficDigest = "sha256:" + strings.Repeat("c", 64)
				case "apply error":
					inventory.nodes[0].CaddyLastError = "apply failed"
				case "address changes":
					inventory.nodes[0].PublicIPv4 = "203.0.113.10"
				case "unhealthy":
					inventory.nodes[0].Healthy = false
				case "legacy version":
					proof.Version = "newer-bundle"
				}
				return proof, nil
			}
			got, err := o.observe(context.Background(), app, release, 20, now.Add(-time.Minute))
			want := scenario == "heartbeat lags" || scenario == "heartbeat catches up"
			if err != nil || got.Ready != want || got.RequiredNodes != 1 {
				t.Fatalf("unexpected observation: %+v error=%v", got, err)
			}
		})
	}
}

func TestSafeRolloutPublicationOrderRejectsMalformedVersions(t *testing.T) {
	base := rolloutPublication("a", 42, 3)
	for _, version := range []string{
		rolloutPublication("b", 41, 3), rolloutPublication("b", 42, 3), rolloutPublication("b", 43, 2), rolloutPublication("b", 43, 4),
		rolloutPublication("G", 43, 3), rolloutPublication("b", 43, 3) + ".extra", "legacy.p43.r3",
		"edgegroupbundle_" + strings.Repeat("b", 64) + ".p043.r3",
		"edgegroupbundle_" + strings.Repeat("b", 64) + ".p43.r03",
		"edgegroupbundle_" + strings.Repeat("b", 64) + ".p18446744073709551616.r3",
	} {
		if safeRolloutProofPublicationAtLeast(version, base) {
			t.Fatal("invalid ordering accepted", version)
		}
	}
	if !safeRolloutProofPublicationAtLeast(base, base) || !safeRolloutProofPublicationAtLeast(rolloutPublication("b", 43, 3), base) {
		t.Fatal("valid ordering rejected")
	}
}

func TestSafeRolloutWaitCoversPublicationAndHeartbeatConvergence(t *testing.T) {
	for _, converges := range []bool{true, false} {
		t.Run(fmt.Sprintf("converges=%v", converges), func(t *testing.T) {
			now := time.Now().UTC()
			start := now
			o, app, release, node, proof := appTrafficObserverFixture(t, now)
			node.RouteBundleVersion = rolloutPublication("a", 42, 3)
			node.CaddyAppliedVersion = node.RouteBundleVersion
			inventory := &changingEdgeNodeLister{nodes: []model.EdgeNode{node}}
			o.Store = inventory
			o.Now = func() time.Time { return now }
			o.Interval = 30 * time.Second
			o.Sleep = func(context.Context, time.Duration) error {
				now = now.Add(30 * time.Second)
				heartbeat := now
				inventory.nodes[0].LastHeartbeatAt = &heartbeat
				return nil
			}
			o.Probe = func(context.Context, string, string, string, string, time.Duration) (routeprobe.Proof, error) {
				p := proof
				p.Version = node.RouteBundleVersion
				p.CheckedAt, p.ValidUntil = now, now.Add(time.Minute)
				if !converges || now.Sub(start) < 6*time.Minute {
					p.AppTrafficDigest = "sha256:" + strings.Repeat("c", 64)
				}
				return p, nil
			}
			got, err := o.WaitForSafeRolloutEdgeRouteBundle(context.Background(), app, release, 20, start.Add(-time.Minute))
			if err != nil || got.Ready != converges {
				t.Fatalf("unexpected convergence: %+v error=%v", got, err)
			}
			wantElapsed := safeRolloutEdgeBundleWaitTimeout
			if converges {
				wantElapsed = 6 * time.Minute
			}
			if now.Sub(start) != wantElapsed {
				t.Fatalf("elapsed=%v want=%v", now.Sub(start), wantElapsed)
			}
			if got.Summary["wait_reason_counts"].(map[string]int)[node.ID+":app_traffic_proof_mismatch"] == 0 ||
				got.Summary["inventory_applied_versions"].(map[string]string)[node.ID] != node.RouteBundleVersion ||
				got.Summary["observed_proof_versions"].(map[string]string)[node.ID] != node.RouteBundleVersion {
				t.Fatalf("missing bounded wait evidence: %+v", got.Summary)
			}
		})
	}
}
