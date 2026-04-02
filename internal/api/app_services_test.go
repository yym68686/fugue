package api

import (
	"testing"

	"fugue/internal/model"
)

func TestMergedAppEnvRepairsLegacyManagedPostgresBindingHost(t *testing.T) {
	app := model.App{
		Spec: model.AppSpec{
			Env: map[string]string{
				"APP_ENV": "prod",
			},
		},
		BackingServices: []model.BackingService{
			{
				ID:   "service_demo",
				Type: model.BackingServiceTypePostgres,
				Spec: model.BackingServiceSpec{
					Postgres: &model.AppPostgresSpec{
						Database:    "demo",
						User:        "root",
						Password:    "secret",
						ServiceName: "demo-postgres",
					},
				},
			},
		},
		Bindings: []model.ServiceBinding{
			{
				ServiceID: "service_demo",
				Env: map[string]string{
					"DB_TYPE":     "postgres",
					"DB_HOST":     "demo-postgres",
					"DB_PORT":     "5432",
					"DB_USER":     "legacy",
					"DB_PASSWORD": "legacy-secret",
					"DB_NAME":     "legacy",
					"KEEP":        "custom",
				},
			},
		},
	}

	env := mergedAppEnv(app)
	if got := env["DB_HOST"]; got != "demo-postgres-rw" {
		t.Fatalf("expected merged env DB_HOST to be repaired to rw service, got %q", got)
	}
	if got := env["DB_USER"]; got != "root" {
		t.Fatalf("expected merged env DB_USER to follow backing service spec, got %q", got)
	}
	if got := env["DB_NAME"]; got != "demo" {
		t.Fatalf("expected merged env DB_NAME to follow backing service spec, got %q", got)
	}
	if got := env["KEEP"]; got != "custom" {
		t.Fatalf("expected non-postgres binding env to be preserved, got %q", got)
	}
	if got := env["APP_ENV"]; got != "prod" {
		t.Fatalf("expected app env override to remain present, got %q", got)
	}
}
