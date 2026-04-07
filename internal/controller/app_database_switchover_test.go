package controller

import (
	"testing"

	"fugue/internal/model"
)

func TestDatabaseSwitchoverSpecClearsPendingPlacementRebalance(t *testing.T) {
	t.Parallel()

	base := model.AppSpec{
		Image:     "ghcr.io/example/demo:latest",
		RuntimeID: "runtime_app",
	}
	postgres := &model.AppPostgresSpec{
		Database:                         "demo",
		User:                             "demo",
		Password:                         "secret",
		RuntimeID:                        "runtime_source",
		FailoverTargetRuntimeID:          "runtime_target",
		Instances:                        2,
		SynchronousReplicas:              1,
		PrimaryPlacementPendingRebalance: true,
	}

	got := databaseSwitchoverSpec(base, postgres, "runtime_target", "runtime_source")
	if got.Postgres == nil {
		t.Fatalf("expected postgres spec, got %+v", got)
	}
	if got.Postgres.PrimaryPlacementPendingRebalance {
		t.Fatalf("expected explicit switchover to clear pending placement hold, got %+v", got.Postgres)
	}
}
