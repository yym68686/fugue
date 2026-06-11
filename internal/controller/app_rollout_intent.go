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
	if managedDeployOperationIsImageOnly(op, currentApp, desiredApp) {
		return model.AppRolloutIntentOnlineImageUpdate
	}
	if managedDeployOperationIsResourceOnly(op, currentApp, desiredApp) {
		return model.AppRolloutIntentOnlineResourceUpdate
	}
	if managedDeployOperationIsLifecycleOnly(op, currentApp, desiredApp) {
		return model.AppRolloutIntentOnlineLifecycleUpdate
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

func managedDeployOperationIsImageOnly(op model.Operation, currentApp, desiredApp model.App) bool {
	if op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return false
	}
	if strings.TrimSpace(desiredApp.Spec.Image) == "" {
		return false
	}
	if strings.TrimSpace(currentApp.Spec.Image) == strings.TrimSpace(desiredApp.Spec.Image) &&
		reflect.DeepEqual(model.AppOriginSource(currentApp), model.AppOriginSource(desiredApp)) &&
		reflect.DeepEqual(model.AppBuildSource(currentApp), model.AppBuildSource(desiredApp)) {
		return false
	}

	currentSpec := comparableImageOnlySpec(currentApp.Spec)
	desiredSpec := comparableImageOnlySpec(desiredApp.Spec)
	return reflect.DeepEqual(currentSpec, desiredSpec)
}

func comparableImageOnlySpec(spec model.AppSpec) model.AppSpec {
	normalized, _ := model.StripFugueInjectedAppEnvFromSpec(spec)
	normalized.Image = ""
	normalized.RestartToken = ""
	normalized.RolloutIntent = ""
	model.ApplyAppSpecDefaults(&normalized)
	return normalized
}

func managedDeployOperationIsResourceOnly(op model.Operation, currentApp, desiredApp model.App) bool {
	if op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return false
	}
	if !managedDeployOperationResourcesDiffer(currentApp.Spec, desiredApp.Spec) {
		return false
	}

	currentSpec := comparableResourceOnlySpec(currentApp.Spec)
	desiredSpec := comparableResourceOnlySpec(desiredApp.Spec)
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

func managedDeployOperationResourcesDiffer(currentSpec, desiredSpec model.AppSpec) bool {
	if !reflect.DeepEqual(currentSpec.Resources, desiredSpec.Resources) {
		return true
	}

	var currentPostgresResources *model.ResourceSpec
	if currentSpec.Postgres != nil {
		currentPostgresResources = currentSpec.Postgres.Resources
	}
	var desiredPostgresResources *model.ResourceSpec
	if desiredSpec.Postgres != nil {
		desiredPostgresResources = desiredSpec.Postgres.Resources
	}
	return !reflect.DeepEqual(currentPostgresResources, desiredPostgresResources)
}

func comparableResourceOnlySpec(spec model.AppSpec) model.AppSpec {
	normalized, _ := model.StripFugueInjectedAppEnvFromSpec(spec)
	normalized.RolloutIntent = ""
	model.ApplyAppSpecDefaults(&normalized)
	normalized.Resources = nil
	if normalized.Postgres != nil {
		postgres := *normalized.Postgres
		postgres.Resources = nil
		normalized.Postgres = &postgres
	}
	return normalized
}

func managedDeployOperationIsLifecycleOnly(op model.Operation, currentApp, desiredApp model.App) bool {
	if op.Type != model.OperationTypeDeploy || op.DesiredSpec == nil {
		return false
	}
	if currentApp.Spec.TerminationGracePeriodSeconds == desiredApp.Spec.TerminationGracePeriodSeconds {
		return false
	}

	currentSpec := comparableLifecycleOnlySpec(currentApp.Spec)
	desiredSpec := comparableLifecycleOnlySpec(desiredApp.Spec)
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

func comparableLifecycleOnlySpec(spec model.AppSpec) model.AppSpec {
	normalized, _ := model.StripFugueInjectedAppEnvFromSpec(spec)
	normalized.RolloutIntent = ""
	normalized.TerminationGracePeriodSeconds = 0
	model.ApplyAppSpecDefaults(&normalized)
	return normalized
}
