package cli

import (
	"testing"

	"fugue/internal/model"
)

func TestAppPostgresRuntimeIDFallsBackToBackingServiceProjection(t *testing.T) {
	t.Parallel()

	app := model.App{
		ID:   "app-a",
		Spec: model.AppSpec{},
		BackingServices: []model.BackingService{{
			Type: "postgres", OwnerAppID: "app-a", Provisioner: "managed",
			Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{RuntimeID: "runtime-db"}},
		}},
	}
	if got := appPostgresRuntimeID(app); got != "runtime-db" {
		t.Fatalf("appPostgresRuntimeID() = %q, want runtime-db", got)
	}
}

func TestAppPostgresRuntimeIDPrefersDesiredSpec(t *testing.T) {
	t.Parallel()

	app := model.App{
		Spec: model.AppSpec{Postgres: &model.AppPostgresSpec{RuntimeID: "runtime-spec"}},
		BackingServices: []model.BackingService{{
			Type: "postgres", OwnerAppID: "app-a", Provisioner: "managed",
			Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{RuntimeID: "runtime-db"}},
		}},
	}
	if got := appPostgresRuntimeID(app); got != "runtime-spec" {
		t.Fatalf("appPostgresRuntimeID() = %q, want runtime-spec", got)
	}
}

func TestPostgresStatusDoesNotChooseUnrelatedService(t *testing.T) {
	app := model.App{ID: "app-a", Spec: model.AppSpec{RuntimeID: "runtime-app"}, BackingServices: []model.BackingService{
		{ID: "unrelated", OwnerAppID: "app-b", Type: "postgres", Provisioner: "managed", Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{RuntimeID: "wrong"}}},
		{ID: "owned", OwnerAppID: "app-a", Type: "postgres", Provisioner: "managed", Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{RuntimeID: "runtime-db"}}},
	}}
	if got := appPostgresRuntimeID(app); got != "runtime-db" {
		t.Fatal(got)
	}
	app.BackingServices = app.BackingServices[:1]
	if got := appPostgresRuntimeID(app); got != "" {
		t.Fatal("unrelated database selected", got)
	}
}
