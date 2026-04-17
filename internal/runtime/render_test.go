package runtime

import (
	"testing"

	"fugue/internal/model"
	"fugue/internal/workloadidentity"
)

func TestRendererInjectsWorkloadIdentityEnv(t *testing.T) {
	t.Parallel()

	renderer := Renderer{
		WorkloadIdentity: WorkloadIdentityConfig{
			APIBaseURL: "api.example.com",
			SigningKey: "signing-secret",
		},
	}
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Route: &model.AppRoute{
			Hostname:  "demo.example.com",
			PublicURL: "https://demo.example.com",
		},
		Spec: model.AppSpec{
			RuntimeID: "runtime_hk",
			Env: map[string]string{
				"APP_ENV": "production",
			},
		},
	}

	rendered := renderer.PrepareApp(app)
	if got := rendered.Spec.Env["APP_ENV"]; got != "production" {
		t.Fatalf("expected existing env to be preserved, got %q", got)
	}
	if got := rendered.Spec.Env["FUGUE_API_URL"]; got != "https://api.example.com" {
		t.Fatalf("expected FUGUE_API_URL to be normalized, got %q", got)
	}
	if got := rendered.Spec.Env["FUGUE_BASE_URL"]; got != "https://api.example.com" {
		t.Fatalf("expected FUGUE_BASE_URL to be normalized, got %q", got)
	}
	if got := rendered.Spec.Env["FUGUE_TENANT_ID"]; got != "tenant_demo" {
		t.Fatalf("expected FUGUE_TENANT_ID, got %q", got)
	}
	if got := rendered.Spec.Env["FUGUE_PROJECT_ID"]; got != "project_demo" {
		t.Fatalf("expected FUGUE_PROJECT_ID, got %q", got)
	}
	if got := rendered.Spec.Env["FUGUE_RUNTIME_ID"]; got != "runtime_hk" {
		t.Fatalf("expected FUGUE_RUNTIME_ID, got %q", got)
	}
	if got := rendered.Spec.Env["FUGUE_APP_HOSTNAME"]; got != "demo.example.com" {
		t.Fatalf("expected FUGUE_APP_HOSTNAME, got %q", got)
	}
	if got := rendered.Spec.Env["FUGUE_APP_URL"]; got != "https://demo.example.com" {
		t.Fatalf("expected FUGUE_APP_URL, got %q", got)
	}
	token := rendered.Spec.Env["FUGUE_TOKEN"]
	if token == "" {
		t.Fatal("expected FUGUE_TOKEN to be injected")
	}
	claims, err := workloadidentity.Parse("signing-secret", token)
	if err != nil {
		t.Fatalf("parse injected workload token: %v", err)
	}
	if claims.ProjectID != "project_demo" {
		t.Fatalf("expected project scope project_demo, got %q", claims.ProjectID)
	}
}

func TestRendererPrepareAppKeepsWorkloadTokenStableAcrossReconciles(t *testing.T) {
	t.Parallel()

	renderer := Renderer{
		WorkloadIdentity: WorkloadIdentityConfig{
			APIBaseURL: "api.example.com",
			SigningKey: "signing-secret",
		},
	}
	app := model.App{
		ID:        "app_demo",
		TenantID:  "tenant_demo",
		ProjectID: "project_demo",
		Name:      "demo",
		Spec: model.AppSpec{
			RuntimeID: "runtime_hk",
		},
	}

	first := renderer.PrepareApp(app)
	second := renderer.PrepareApp(app)
	if first.Spec.Env["FUGUE_TOKEN"] == "" {
		t.Fatal("expected FUGUE_TOKEN to be injected")
	}
	if first.Spec.Env["FUGUE_TOKEN"] != second.Spec.Env["FUGUE_TOKEN"] {
		t.Fatalf("expected stable FUGUE_TOKEN across repeated PrepareApp calls")
	}
}
