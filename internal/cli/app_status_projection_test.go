package cli

import (
	"testing"

	"fugue/internal/model"
)

func TestAppPostgresRuntimeIDFallsBackToBackingServiceProjection(t *testing.T) {
	t.Parallel()

	app := model.App{
		Spec: model.AppSpec{},
		BackingServices: []model.BackingService{{
			Type: "postgres",
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
			Type: "postgres",
			Spec: model.BackingServiceSpec{Postgres: &model.AppPostgresSpec{RuntimeID: "runtime-db"}},
		}},
	}
	if got := appPostgresRuntimeID(app); got != "runtime-spec" {
		t.Fatalf("appPostgresRuntimeID() = %q, want runtime-spec", got)
	}
}
