package api

import (
	"strings"

	"fugue/internal/model"
)

func prepareMigrateDesiredSpec(app model.App, spec *model.AppSpec, targetRuntimeID string) {
	if spec == nil {
		return
	}
	targetRuntimeID = strings.TrimSpace(targetRuntimeID)
	spec.RuntimeID = targetRuntimeID
}
