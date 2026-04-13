package api

import (
	"net/http"
	"testing"

	"fugue/internal/model"
)

func TestCreateAppStagesGitHubSource(t *testing.T) {
	t.Parallel()

	s, server, apiKey, existingApp := setupAppConfigTestServer(t, model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		Ports:     []int{8080},
		Replicas:  1,
		RuntimeID: "runtime_managed_shared",
	})

	recorder := performJSONRequest(t, server, http.MethodPost, "/v1/apps", apiKey, map[string]any{
		"tenant_id":  existingApp.TenantID,
		"project_id": existingApp.ProjectID,
		"name":       "worker",
		"spec": map[string]any{
			"runtime_id":   "runtime_managed_shared",
			"replicas":     1,
			"network_mode": "background",
		},
		"source": map[string]any{
			"repo_url":       "example/worker",
			"repo_branch":    "main",
			"build_strategy": "buildpacks",
		},
	})
	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d body=%s", http.StatusCreated, recorder.Code, recorder.Body.String())
	}

	var response struct {
		App model.App `json:"app"`
	}
	mustDecodeJSON(t, recorder, &response)
	if response.App.Name != "worker" {
		t.Fatalf("expected app name worker, got %q", response.App.Name)
	}
	if response.App.Status.Phase != "importing" {
		t.Fatalf("expected importing phase, got %q", response.App.Status.Phase)
	}
	if response.App.Source == nil {
		t.Fatal("expected staged source on created app")
	}
	if response.App.Source.Type != model.AppSourceTypeGitHubPublic {
		t.Fatalf("expected github public source, got %+v", response.App.Source)
	}
	if response.App.Source.RepoURL != "example/worker" || response.App.Source.RepoBranch != "main" || response.App.Source.BuildStrategy != model.AppBuildStrategyBuildpacks {
		t.Fatalf("unexpected created source %+v", response.App.Source)
	}

	storedApp, err := s.GetApp(response.App.ID)
	if err != nil {
		t.Fatalf("get stored app: %v", err)
	}
	if storedApp.Source == nil || storedApp.Source.Type != model.AppSourceTypeGitHubPublic {
		t.Fatalf("expected stored staged source, got %+v", storedApp.Source)
	}
	if storedApp.Status.Phase != "importing" {
		t.Fatalf("expected stored app phase importing, got %q", storedApp.Status.Phase)
	}
	if storedApp.Route != nil {
		t.Fatalf("expected no auto route without app base domain, got %+v", storedApp.Route)
	}
}
