package store

import (
	"path/filepath"
	"testing"

	"fugue/internal/model"
)

func TestGeneratedEnvCreatedAndReused(t *testing.T) {
	t.Parallel()

	s := New(filepath.Join(t.TempDir(), "store.json"))
	if err := s.Init(); err != nil {
		t.Fatalf("init store: %v", err)
	}
	tenant, err := s.CreateTenant("Generated Env")
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	project, err := s.CreateProject(tenant.ID, "demo", "")
	if err != nil {
		t.Fatalf("create project: %v", err)
	}

	app, err := s.CreateApp(tenant.ID, project.ID, "demo", "", model.AppSpec{
		Image:     "nginx:1.27",
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: model.DefaultManagedRuntimeID,
		GeneratedEnv: map[string]model.AppGeneratedEnvSpec{
			"APP_SECRET": {
				Generate: model.AppGeneratedEnvGenerateRandom,
				Encoding: model.AppGeneratedEnvEncodingHex,
				Length:   12,
			},
		},
	})
	if err != nil {
		t.Fatalf("create app: %v", err)
	}
	first := app.Spec.Env["APP_SECRET"]
	if first == "" {
		t.Fatal("expected generated APP_SECRET")
	}
	if len(first) != 24 {
		t.Fatalf("expected 12 random bytes encoded as 24 hex chars, got %q", first)
	}

	deploySpec := model.AppSpec{
		Image:     app.Spec.Image,
		Ports:     []int{80},
		Replicas:  1,
		RuntimeID: app.Spec.RuntimeID,
		GeneratedEnv: map[string]model.AppGeneratedEnvSpec{
			"APP_SECRET": {},
		},
	}
	op, err := s.CreateOperation(model.Operation{
		TenantID:    tenant.ID,
		Type:        model.OperationTypeDeploy,
		AppID:       app.ID,
		DesiredSpec: &deploySpec,
	})
	if err != nil {
		t.Fatalf("create deploy operation: %v", err)
	}
	if op.DesiredSpec == nil || op.DesiredSpec.Env["APP_SECRET"] != first {
		t.Fatalf("expected deploy desired spec to reuse generated secret, got %+v", op.DesiredSpec)
	}

	deploySpec.Env = nil
	updated, err := s.SyncObservedManagedAppBaseline(app.ID, deploySpec, nil)
	if err != nil {
		t.Fatalf("sync observed baseline: %v", err)
	}
	if got := updated.Spec.Env["APP_SECRET"]; got != first {
		t.Fatalf("expected baseline sync to reuse %q, got %q", first, got)
	}
}
