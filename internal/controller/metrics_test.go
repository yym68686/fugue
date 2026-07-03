package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
	"fugue/internal/model"
)

func TestMetricsHandlerReportsControllerConfiguration(t *testing.T) {
	t.Parallel()

	service := New(nil, config.ControllerConfig{
		KubectlApply:              true,
		LeaderElectionEnabled:     true,
		PollInterval:              15 * time.Second,
		FallbackPollInterval:      30 * time.Second,
		ForegroundActivateWorkers: 4,
		GitHubSyncActivateWorkers: 2,
	}, nil)

	recorder := httptest.NewRecorder()
	service.MetricsHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusOK, recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, want := range []string{
		`fugue_component_info{component="controller"} 1.000000`,
		`fugue_controller_kubectl_apply_enabled 1.000000`,
		`fugue_controller_leader_election_enabled 1.000000`,
		`fugue_controller_poll_interval_seconds 15.000000`,
		`fugue_controller_workers_configured{lane="foreground_activate"} 4.000000`,
		`fugue_controller_workers_configured{lane="github_sync_activate"} 2.000000`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q:\n%s", want, body)
		}
	}
}

func TestMetricsHandlerDeduplicatesImageCachePrunePlanSamples(t *testing.T) {
	stateStore, _ := newImageCacheControllerTestStore(t)
	older := time.Date(2026, 7, 2, 19, 30, 0, 0, time.UTC)
	newer := older.Add(30 * time.Minute)
	for _, plan := range []model.ImageCachePrunePlan{
		{
			ID:                     "imgcacheprune_old",
			NodeID:                 "machine-1",
			ClusterNodeName:        "worker-1",
			RuntimeID:              "runtime-1",
			Mode:                   model.ImageCachePruneModeObserve,
			Status:                 model.ImageCachePrunePlanStatusPlanned,
			CandidateManifestCount: 3,
			ProtectedManifestCount: 1,
			PlannedDeleteBytes:     300,
			CreatedAt:              older,
		},
		{
			ID:                     "imgcacheprune_new",
			NodeID:                 "machine-1",
			ClusterNodeName:        "worker-1",
			RuntimeID:              "runtime-1",
			Mode:                   model.ImageCachePruneModeObserve,
			Status:                 model.ImageCachePrunePlanStatusPlanned,
			CandidateManifestCount: 4,
			ProtectedManifestCount: 2,
			PlannedDeleteBytes:     400,
			CreatedAt:              newer,
		},
	} {
		if _, err := stateStore.UpsertImageCachePrunePlan(plan); err != nil {
			t.Fatalf("upsert prune plan: %v", err)
		}
	}
	service := New(stateStore, config.ControllerConfig{}, nil)

	recorder := httptest.NewRecorder()
	service.MetricsHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	labels := `{cluster_node_name="worker-1",mode="observe",node_id="machine-1",runtime_id="runtime-1",status="planned"}`
	for metric, want := range map[string]string{
		"fugue_image_cache_candidate_manifest_count": "4.000000",
		"fugue_image_cache_prune_planned_bytes":      "400.000000",
		"fugue_image_cache_prune_skipped_count":      "2.000000",
	} {
		prefix := metric + labels
		if got := strings.Count(body, prefix); got != 1 {
			t.Fatalf("expected one sample for %s, got %d:\n%s", metric, got, body)
		}
		if !strings.Contains(body, prefix+" "+want) {
			t.Fatalf("expected latest sample %s %s in:\n%s", prefix, want, body)
		}
	}
	if strings.Contains(body, "fugue_image_cache_candidate_manifest_count"+labels+" 3.000000") ||
		strings.Contains(body, "fugue_image_cache_prune_planned_bytes"+labels+" 300.000000") ||
		strings.Contains(body, "fugue_image_cache_prune_skipped_count"+labels+" 1.000000") {
		t.Fatalf("expected older duplicate prune plan samples to be suppressed:\n%s", body)
	}
}

func TestMetricsHandlerReportsRegistryMaintenanceStatus(t *testing.T) {
	t.Parallel()

	lastGC := time.Date(2026, 6, 9, 21, 0, 0, 0, time.UTC)
	service := New(nil, config.ControllerConfig{}, nil)
	service.readRegistryMaintenance = func(context.Context) registryMaintenanceStatus {
		return registryMaintenanceStatus{
			JanitorPresent:        true,
			GCCronJobPresent:      true,
			GCRequested:           true,
			LastGCTimestamp:       lastGC,
			StorageUsedBytes:      100,
			StorageCapacityBytes:  200,
			UnreferencedBlobBytes: 30,
			UnreferencedBlobCount: 4,
			ProtectedDigestCount:  5,
		}
	}

	recorder := httptest.NewRecorder()
	service.MetricsHandler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, want := range []string{
		`fugue_registry_janitor_present{job="retention"} 1.000000`,
		`fugue_registry_janitor_present{job="gc"} 1.000000`,
		`fugue_registry_gc_requested 1.000000`,
		`fugue_registry_storage_used_bytes 100.000000`,
		`fugue_registry_storage_capacity_bytes 200.000000`,
		`fugue_registry_unreferenced_blob_bytes 30.000000`,
		`fugue_registry_protected_workload_digests 5.000000`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics output missing %q:\n%s", want, body)
		}
	}
}
