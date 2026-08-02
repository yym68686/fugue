package api

import (
	"errors"
	"testing"
	"time"

	"fugue/internal/model"
)

func TestEvaluatePlatformReleaseEvidenceFailsClosedAndIgnoresConnectedApplication5xx(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 2, 1, 2, 3, 0, time.UTC)
	release := "release-1"
	bundle := model.EdgeRouteBundle{Version: "bundle-1", Routes: []model.EdgeRouteBinding{{Status: model.EdgeRouteStatusActive}}}
	activation := model.EdgeActivationState{Phase: model.EdgeActivationPhaseEnforced, RouteAuthority: model.EdgeRouteAuthorityActiveEpoch}
	epochs := []model.EdgeActiveEpoch{{EdgeGroupID: "group-de", Slot: model.EdgeSlotB, ReleaseEpoch: release, FenceSequence: 2, MinHealthyInstances: 1}}
	instances := []model.EdgeNodeInstance{{
		EdgeID: "edge-de", EdgeGroupID: "group-de", Slot: model.EdgeSlotB, InstanceUID: "pod-b", ReleaseEpoch: release,
		EffectiveHealthy: true, ConsecutiveHealthy: 2, LastHeartbeatAt: now,
		Node: model.EdgeNode{Healthy: true, Status: model.EdgeHealthHealthy, TLSStatus: model.EdgeTLSStatusReady, CaddyAppliedVersion: bundle.Version},
	}}
	metrics := platformReleaseEvidenceMetrics{RequestCount: 10, Application5xxCount: 3, Classes: []string{model.PlatformErrorClassOriginConnectedApp5xx}}
	passed := evaluatePlatformReleaseEvidence(now, release, activation, instances, epochs, bundle, nil, metrics, nil)
	if passed.Status != "passed" || passed.EvidenceDigest == "" {
		t.Fatalf("connected application 5xx incorrectly blocked platform evidence: %+v", passed)
	}

	hard := metrics
	hard.HardFailureCount = 1
	hard.Classes = []string{model.PlatformErrorClassOriginConnect}
	if got := evaluatePlatformReleaseEvidence(now, release, activation, instances, epochs, bundle, nil, hard, nil); got.Status != "failed" {
		t.Fatalf("platform failure did not fail evidence: %+v", got)
	}
	if got := evaluatePlatformReleaseEvidence(now, release, activation, instances, epochs, bundle, nil, platformReleaseEvidenceMetrics{}, errors.New("clickhouse unavailable")); got.Status != "unknown" {
		t.Fatalf("missing metrics did not fail closed: %+v", got)
	}
}

func TestEvaluatePlatformReleaseEvidenceRequiresExactBundleAndFreshCohort(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 2, 1, 2, 3, 0, time.UTC)
	release := "release-1"
	bundle := model.EdgeRouteBundle{Version: "bundle-current", Routes: []model.EdgeRouteBinding{{Status: model.EdgeRouteStatusActive}}}
	activation := model.EdgeActivationState{Phase: model.EdgeActivationPhaseEnforced, RouteAuthority: model.EdgeRouteAuthorityActiveEpoch}
	epochs := []model.EdgeActiveEpoch{{EdgeGroupID: "group-us", Slot: model.EdgeSlotB, ReleaseEpoch: release, FenceSequence: 3, MinHealthyInstances: 1}}
	base := model.EdgeNodeInstance{EdgeID: "edge-us", EdgeGroupID: "group-us", Slot: model.EdgeSlotB, InstanceUID: "pod-b", ReleaseEpoch: release, EffectiveHealthy: true, ConsecutiveHealthy: 2, LastHeartbeatAt: now, Node: model.EdgeNode{Healthy: true, Status: model.EdgeHealthHealthy, TLSStatus: model.EdgeTLSStatusReady, CaddyAppliedVersion: bundle.Version}}
	metrics := platformReleaseEvidenceMetrics{RequestCount: 1}

	staleBundle := base
	staleBundle.Node.CaddyAppliedVersion = "bundle-old"
	if got := evaluatePlatformReleaseEvidence(now, release, activation, []model.EdgeNodeInstance{staleBundle}, epochs, bundle, nil, metrics, nil); got.Status != "failed" {
		t.Fatalf("stale bundle passed: %+v", got)
	}
	staleHeartbeat := base
	staleHeartbeat.LastHeartbeatAt = now.Add(-platformReleaseHeartbeatMaxAge - time.Nanosecond)
	if got := evaluatePlatformReleaseEvidence(now, release, activation, []model.EdgeNodeInstance{staleHeartbeat}, epochs, bundle, nil, metrics, nil); got.Status != "failed" {
		t.Fatalf("stale heartbeat passed: %+v", got)
	}
	wrongEpoch := epochs
	wrongEpoch[0].ReleaseEpoch = "release-old"
	if got := evaluatePlatformReleaseEvidence(now, release, activation, []model.EdgeNodeInstance{base}, wrongEpoch, bundle, nil, metrics, nil); got.Status != "failed" {
		t.Fatalf("wrong active epoch passed: %+v", got)
	}
}
