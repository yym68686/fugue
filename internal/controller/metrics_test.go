package controller

import (
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
