package api

import (
	"strings"

	"fugue/internal/model"
	"fugue/internal/store"
)

func prepareMigrateDesiredSpec(app model.App, spec *model.AppSpec, targetRuntimeID string) {
	if spec == nil {
		return
	}
	targetRuntimeID = strings.TrimSpace(targetRuntimeID)
	spec.RuntimeID = targetRuntimeID

	postgres := store.OwnedManagedPostgresSpec(app)
	if postgres == nil {
		return
	}
	postgres.RuntimeID = targetRuntimeID
	postgres.FailoverTargetRuntimeID = ""
	postgres.PrimaryPlacementPendingRebalance = false
	spec.Postgres = postgres
}
