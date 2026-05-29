package controller

import (
	"reflect"
	"strings"

	"fugue/internal/model"
)

func rolloutIntentForManagedOperation(op model.Operation, currentApp, desiredApp model.App) string {
	if managedDeployOperationIsRestartOnly(op, currentApp, desiredApp) {
		return model.AppRolloutIntentOnlineRestart
	}
	return ""
}

func managedDeployOperationIsRestartOnly(op model.Operation, currentApp, desiredApp model.App) bool {
	if op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return false
	}
	currentToken := strings.TrimSpace(currentApp.Spec.RestartToken)
	desiredToken := strings.TrimSpace(desiredApp.Spec.RestartToken)
	if desiredToken == "" || desiredToken == currentToken {
		return false
	}

	currentSpec := comparableRestartSpec(currentApp.Spec)
	desiredSpec := comparableRestartSpec(desiredApp.Spec)
	if !reflect.DeepEqual(currentSpec, desiredSpec) {
		return false
	}
	if !reflect.DeepEqual(model.AppOriginSource(currentApp), model.AppOriginSource(desiredApp)) {
		return false
	}
	if !reflect.DeepEqual(model.AppBuildSource(currentApp), model.AppBuildSource(desiredApp)) {
		return false
	}
	return true
}

func comparableRestartSpec(spec model.AppSpec) model.AppSpec {
	normalized, _ := model.StripFugueInjectedAppEnvFromSpec(spec)
	normalized.RestartToken = ""
	normalized.RolloutIntent = ""
	model.ApplyAppSpecDefaults(&normalized)
	return normalized
}
