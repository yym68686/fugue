package api

import (
	"net/http"
	"path/filepath"
	"testing"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/store"
)

func TestAppContinuityKeepsExplicitLatencyHistoryInQueuedIntent(t *testing.T) {
	t.Parallel()
	s := store.New(filepath.Join(t.TempDir(), "state.json"))
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.CreateTenant("Latency History")
	if err != nil {
		t.Fatal(err)
	}
	project, err := s.CreateProject(tenant.ID, "apps", "")
	if err != nil {
		t.Fatal(err)
	}
	runtime, _, err := s.CreateRuntime(tenant.ID, "worker", model.RuntimeTypeManagedOwned, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	app, err := s.CreateApp(tenant.ID, project.ID, "service", "", model.AppSpec{Image: "example/service:stable", RuntimeID: runtime.ID, Replicas: 1, Ports: []int{8080}})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(s, auth.New(s, "bootstrap-fixture"), nil, ServerConfig{})
	for _, seconds := range []int{1800, 300, 3601} {
		request := map[string]any{"zero_downtime": map[string]any{"enabled": true, "mode": "safe", "strategy": "stable_candidate", "gate": map[string]any{"window_seconds": 600, "latency_comparison_window_seconds": seconds, "min_candidate_requests": 20, "max_5xx_rate": .01}}}
		rec := performJSONRequest(t, server, http.MethodPatch, "/v1/apps/"+app.ID+"/continuity", "bootstrap-fixture", request)
		if seconds != 1800 {
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("invalid window accepted: %d %s", rec.Code, rec.Body.String())
			}
			continue
		}
		if rec.Code != http.StatusAccepted {
			t.Fatalf("valid policy rejected: %d %s", rec.Code, rec.Body.String())
		}
		var response struct {
			Operation    model.Operation              `json:"operation"`
			ZeroDowntime *model.AppZeroDowntimePolicy `json:"zero_downtime"`
		}
		mustDecodeJSON(t, rec, &response)
		op, err := s.GetOperation(response.Operation.ID)
		if err != nil {
			t.Fatal(err)
		}
		gate := op.DesiredSpec.Continuity.ZeroDowntime.Gate
		if gate.LatencyComparisonWindowSeconds != 1800 || gate.WindowSeconds != 600 || gate.MinCandidateRequests != 20 || gate.Max5xxRate != .01 || response.ZeroDowntime.Gate.LatencyComparisonWindowSeconds != 1800 {
			t.Fatalf("policy lost or gates changed: %+v", gate)
		}
		current, err := s.GetApp(app.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Spec.Continuity != nil || current.Spec.Image != app.Spec.Image {
			t.Fatal("queueing intent rewrote serving app")
		}
	}
}
