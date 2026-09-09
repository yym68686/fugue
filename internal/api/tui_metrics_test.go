package api

import (
	"context"
	"fugue/internal/model"
	"testing"
	"time"
)

func TestTUILiveSamplesRetainKubeletTimeAcrossCachedReads(t *testing.T) {
	_, server, _, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	at := time.Now().Add(-10 * time.Second).UTC().Truncate(time.Second)
	server.clusterNodeInventoryCache.set(clusterNodeInventoryCacheKey, []clusterNodeSnapshot{{
		node:    model.ClusterNode{Name: "node-a", ObservedAt: &at},
		pods:    []clusterNodePod{rightSizingTestPod("pod-a", "Running", app.ID, app.Name)},
		summary: &kubeNodeSummary{Pods: []kubeNodeSummaryPod{rightSizingTestPodUsage("pod-a", 100, 256, 80, 240, 1)}},
	}})
	first := server.appLiveResourceTimeseries(context.Background(), app)
	second := server.appLiveResourceTimeseries(context.Background(), app)
	if len(first) != 3 || len(second) != 3 {
		t.Fatalf("missing live samples: %v %v", first, second)
	}
	for i := range first {
		a := first[i]["points"].([]map[string]any)[0]
		b := second[i]["points"].([]map[string]any)[0]
		if a["observed_at"] != at.Format(time.RFC3339) || a["observed_at"] != b["observed_at"] {
			t.Fatal("cached read invented observation time")
		}
	}
}

func TestTUIResourceHistoryUsesCorrectPerReplicaSeries(t *testing.T) {
	state, server, _, app := setupAppConfigTestServer(t, appObservabilityTestSpec())
	now := time.Now().UTC().Truncate(time.Second)
	cpu := int64(150)
	oldCPU := int64(999)
	err := state.RecordResourceUsageSamples([]model.ResourceUsageSample{
		{TenantID: app.TenantID, TargetKind: rightSizingSampleTargetKind(model.ClusterNodeWorkloadKindApp), TargetID: app.ID, ObservedAt: now.Add(-time.Minute), CPUMilliCores: &cpu},
		{TenantID: app.TenantID, TargetKind: model.ClusterNodeWorkloadKindApp, TargetID: app.ID, ObservedAt: now.Add(-time.Minute), CPUMilliCores: &oldCPU},
	}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	series, err := server.appResourceTimeseries(app, appObservabilityWindow{Since: now.Add(-15 * time.Minute).Format(time.RFC3339), Until: now.Format(time.RFC3339)})
	if err != nil {
		t.Fatal(err)
	}
	points := series[0]["points"].([]map[string]any)
	if len(points) != 1 || points[0]["value"] != cpu || series[0]["interval_seconds"] != int(resourceUsageSampleInterval.Seconds()) {
		t.Fatalf("wrong measurement series %+v", series[0])
	}
	if points := series[1]["points"].([]map[string]any); points[0]["value"] != nil {
		t.Fatal("missing memory became zero")
	}
}
