package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fugue/internal/config"
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
